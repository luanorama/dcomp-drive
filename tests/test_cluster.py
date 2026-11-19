"""Testes de operação com SQLite temporário e nós de armazenamento simulados."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from controller import db
from controller.main import app
from controller.workers import heartbeats, maintenance
from controller.workers.cluster import ClusterWorker
from storage_node.agent import send_heartbeats

DATA = b"arquivo distribuido"
HASH = hashlib.sha256(DATA).hexdigest()


class ClusterTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = patch.dict(os.environ, {"DB_PATH": str(Path(directory.name) / "test.db")})
        database.start()
        self.addCleanup(database.stop)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        for number in range(1, 6):
            self.register(f"node{number}")

    def register(self, node):
        response = self.client.post("/nodes/register", json={
            "node_id": node, "address": f"http://{node}:9000",
            "disks": [{"disk_id": f"{node}/disk1", "path": "/data/disk1",
                       "total_bytes": 10000000, "used_bytes": 0}],
        })
        response.raise_for_status()

    def call_heartbeat(self, node, *, total=10000000):
        return self.client.post(f"/nodes/{node}/heartbeat", json={
            "disks": [{"disk_id": f"{node}/disk1", "total_bytes": total, "used_bytes": 0}],
        })

    def nodes(self):
        return {n["node_id"]: n for n in self.client.get("/status").json()["nodes"]}

    def allocate(self, replication_factor=3):
        response = self.client.post("/files/allocate", json={
            "path": "/teste.txt", "size": len(DATA), "num_chunks": 1,
            "replication_factor": replication_factor,
        })
        response.raise_for_status()
        return response.json()

    def seed(self, replication_factor=3):
        allocation = self.allocate(replication_factor)
        chunk = allocation["chunks"][0]
        response = self.client.post("/files/commit", json={
            "path": "/teste.txt", "lease_id": allocation["lease_id"], "file_hash": HASH,
            "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"], "hash": HASH,
                        "size": len(DATA), "confirmed": [t["disk_id"] for t in chunk["targets"]]}],
        })
        self.assertEqual(response.json(), {"state": "intact"})
        return chunk["chunk_id"], chunk["targets"][0]["disk_id"].split("/")[0]

    def age(self, node, seconds):
        with closing(db.get_db()) as conn, conn:
            conn.execute("UPDATE nodes SET last_heartbeat = ? WHERE node_id = ?",
                         (time.time() - seconds, node))

    def state(self):
        response = self.client.get("/files/stat", params={"path": "/teste.txt"})
        response.raise_for_status()
        return response.json()

    @staticmethod
    def run_worker(action, handler):
        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                await action(client)

        asyncio.run(run())

    def test_monitor_repair_and_read_while_copying(self):
        chunk, node = self.seed()
        self.age(node, 7)
        heartbeats.check_heartbeats()
        self.assertEqual(self.nodes()[node]["status"], "suspect")
        self.call_heartbeat(node).raise_for_status()
        self.assertEqual(self.nodes()[node]["status"], "alive")

        worker = ClusterWorker()
        self.age(node, 11)
        asyncio.run(worker.monitor())
        self.assertEqual(self.nodes()[node]["status"], "dead")
        self.assertEqual(self.state()["state"], "under_replicated")
        self.assertEqual(self.call_heartbeat(node).status_code, 404)

        def copy(request):
            self.assertEqual(request.url.path, f"/chunks/{chunk}/copy")
            self.assertNotEqual(request.url.host, node)
            body = json.loads(request.content)
            self.assertEqual(body["disk_id"], f"{request.url.host}/disk1")
            self.assertTrue(body["target_disk_id"].startswith(("node4/", "node5/")))
            self.assertEqual(body["target_url"], f"http://{body['target_disk_id'].split('/')[0]}:9000")
            self.assertEqual(self.state()["state"], "recovering")
            response = self.client.get("/files/locate", params={"path": "/teste.txt"})
            response.raise_for_status()
            self.assertEqual(len(response.json()["chunks"][0]["replicas"]), 2)
            return httpx.Response(200, json={"ok": True})

        self.run_worker(worker.repair, copy)
        after = self.state()
        self.assertEqual(after["state"], "intact")
        ok = [r for r in after["chunks"][0]["replicas"] if r["state"] == "ok"]
        self.assertEqual(len({r["disk_id"].split("/")[0] for r in ok}), 3)
        self.assertIsNotNone(worker.recovery_time_s)

    def test_repair_respects_the_files_replication_factor(self):
        _, node = self.seed(replication_factor=2)
        self.age(node, 11)
        worker = ClusterWorker()
        asyncio.run(worker.monitor())
        self.run_worker(worker.repair, lambda request: httpx.Response(200, json={"ok": True}))
        state = self.state()
        self.assertEqual(state["state"], "intact")
        self.assertEqual(sum(r["state"] == "ok" for r in state["chunks"][0]["replicas"]), 2)
        self.assertEqual(maintenance.copy_candidates(), [])

    def test_commit_ignores_confirmations_from_dead_nodes(self):
        allocation = self.allocate()
        chunk = allocation["chunks"][0]
        for target in chunk["targets"][:2]:
            self.age(target["disk_id"].split("/")[0], 11)
        heartbeats.check_heartbeats()

        response = self.client.post("/files/commit", json={
            "path": "/teste.txt", "lease_id": allocation["lease_id"], "file_hash": HASH,
            "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"], "hash": HASH,
                        "size": len(DATA), "confirmed": [t["disk_id"] for t in chunk["targets"]]}],
        })
        self.assertEqual(response.status_code, 507)
        self.assertEqual(len(maintenance.pending_deletions()), 1)

    def test_drain_keeps_source_until_replacement_is_confirmed(self):
        chunk, node = self.seed()
        response = self.client.post(f"/nodes/{node}/drain")
        self.assertEqual(response.json(), {"ok": True, "queued_chunks": 1})
        self.call_heartbeat(node).raise_for_status()
        self.assertEqual(self.nodes()[node]["status"], "draining")
        worker = ClusterWorker()

        # Sem resposta de sucesso, não há migração nem exclusão.
        self.run_worker(worker.repair, lambda request: httpx.Response(503))
        self.assertEqual(self.nodes()[node]["drain_remaining_chunks"], 1)
        self.assertEqual(self.nodes()[node]["pending_deletions"], 0)

        worker.retry_at.clear()
        self.run_worker(worker.repair, lambda request: httpx.Response(200, json={"ok": True}))
        self.assertEqual(self.nodes()[node]["drain_remaining_chunks"], 0)
        self.assertEqual(self.nodes()[node]["pending_deletions"], 1)

        def delete(request):
            self.assertEqual(request.method, "DELETE")
            self.assertEqual(request.url.host, node)
            self.assertEqual(request.url.path, f"/chunks/{chunk}")
            return httpx.Response(204)

        self.run_worker(worker.maintain, delete)
        self.assertEqual(self.nodes()[node]["pending_deletions"], 0)

    def test_rejoin_checks_bytes_before_advertising_replica(self):
        chunk, node = self.seed()
        self.age(node, 11)
        heartbeats.check_heartbeats()
        self.register(node)
        self.assertEqual(self.nodes()[node]["status"], "dead")
        self.call_heartbeat(node).raise_for_status()

        def storage(request):
            if request.url.path == "/chunks":
                return httpx.Response(200, json={"chunks": [
                    {"chunk_id": chunk, "disk_id": f"{node}/disk1", "hash": HASH},
                ]})
            self.assertEqual(request.url.path, f"/chunks/{chunk}")
            return httpx.Response(200, content=DATA)

        self.run_worker(ClusterWorker().maintain, storage)
        self.assertEqual(self.nodes()[node]["status"], "alive")
        self.assertEqual(self.state()["state"], "intact")

    def test_failed_disk_invalidates_its_replica(self):
        _, node = self.seed()
        self.call_heartbeat(node, total=0).raise_for_status()
        self.assertEqual(self.nodes()[node]["status"], "alive")
        self.assertEqual(self.state()["state"], "under_replicated")
        replica = next(r for r in self.state()["chunks"][0]["replicas"]
                       if r["disk_id"] == f"{node}/disk1")
        self.assertEqual(replica["state"], "lost")

    def test_expired_lease_is_cleaned_without_entering_copy_queue(self):
        self.allocate()
        self.assertEqual(maintenance.copy_candidates(), [])
        with closing(db.get_db()) as conn, conn:
            conn.execute("UPDATE leases SET expires_at = ?", (time.time() - 1,))
        maintenance.expired_uploads()
        response = self.client.get("/files/stat", params={"path": "/teste.txt"})
        self.assertEqual(response.status_code, 404)
        self.assertEqual(len(maintenance.pending_deletions()), 3)
        self.allocate()

    def test_agent_registers_again_when_controller_requests_it(self):
        async def scenario(directory):
            cfg = SimpleNamespace(node_id="node1", data_dirs=[directory],
                                  controller_url="http://controller:8000", heartbeat_interval_s=0.01)
            seen = []
            complete = asyncio.Event()

            def controller(request):
                body = json.loads(request.content)
                seen.append(request.url.path)
                disk = body["disks"][0]
                self.assertGreater(disk["total_bytes"], 0)
                if request.url.path == "/nodes/register":
                    self.assertEqual(disk["path"], directory)
                    self.assertEqual(body["address"], "http://node1:9000")
                    return httpx.Response(200, json={"ok": True})
                self.assertNotIn("path", disk)
                if len(seen) == 2:
                    return httpx.Response(404)
                complete.set()
                return httpx.Response(200, json={"ok": True})

            async with httpx.AsyncClient(transport=httpx.MockTransport(controller)) as client:
                task = asyncio.create_task(send_heartbeats(client, cfg, "http://node1:9000"))
                try:
                    await asyncio.wait_for(complete.wait(), 2)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            self.assertEqual(seen, ["/nodes/register", "/nodes/node1/heartbeat"] * 2)

        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(scenario(directory))

    def test_lifespan_runs_monitor(self):
        self.age("node1", 11)
        with TestClient(app) as client:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                status = client.get("/status").json()
                if status["nodes"][0]["status"] == "dead":
                    return
                time.sleep(0.05)
        self.fail("O monitor não marcou o nó morto")


if __name__ == "__main__":
    unittest.main()
