"""Loop de monitoramento, cópia e manutenção do controlador."""
from __future__ import annotations

import asyncio
import time
from contextlib import closing

import httpx

from controller.config import get_config
from controller.db import get_db
from controller.logger import get_logger
from controller.workers import heartbeats, maintenance, reconciliation

cfg = get_config()
log = get_logger("controller.cluster_worker")


def file_recovering(path):
    with closing(get_db()) as conn, conn:
        conn.execute(
            """UPDATE entries SET state = 'recovering'
               WHERE path = ? AND state = 'under_replicated' """,
            (path,),
        )


def refresh_file_of_chunk(chunk_id):
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        chunk = conn.execute("SELECT path FROM chunks WHERE chunk_id = ?", (chunk_id,)).fetchone()
        if chunk:
            maintenance.refresh_files(conn, [chunk["path"]])


class ClusterWorker:
    def __init__(self):
        self.in_progress = {}
        self.retry_at = {}
        self.failures = []
        self.detection_time_s = None
        self.recovery_time_s = None

    def restored(self, chunk_id):
        for started, chunks in self.failures:
            chunks.discard(chunk_id)
            if not chunks:
                self.recovery_time_s = round(time.monotonic() - started, 3)
                log.info("failure_recovered", recovery_time_s=self.recovery_time_s)
        self.failures = [item for item in self.failures if item[1]]

    async def monitor(self):
        for elapsed, chunks in await asyncio.to_thread(heartbeats.check_heartbeats):
            self.detection_time_s = round(elapsed, 3)
            self.recovery_time_s = None
            if chunks:
                self.failures.append((time.monotonic(), set(chunks)))

    async def copy_chunk(self, client, chunk, sources, target):
        chunk_id = chunk["chunk_id"]
        self.in_progress[chunk_id] = {
            "chunk_id": chunk_id, "path": chunk["path"], "target_disk_id": target["disk_id"],
        }
        try:
            await asyncio.to_thread(file_recovering, chunk["path"])
            for source in sources:
                try:
                    response = await client.post(
                        f"{source['node_url'].rstrip('/')}/chunks/{chunk_id}/copy",
                        json={"disk_id": source["disk_id"], "target_url": target["node_url"],
                              "target_disk_id": target["disk_id"]},
                    )
                    response.raise_for_status()
                    if response.json().get("ok") is not True:
                        raise ValueError("Cópia sem confirmação do nó")
                except (httpx.HTTPError, ValueError, AttributeError) as error:
                    log.warning("replica_copy_failed", chunk_id=chunk_id,
                                disk_id=source["disk_id"], error=str(error))
                    continue

                complete = await asyncio.to_thread(maintenance.finish_copy, chunk, target)
                log.info("replica_copied", chunk_id=chunk_id,
                         disk_id=target["disk_id"], bytes=chunk["size"])
                if complete:
                    self.restored(chunk_id)
                return
            self.retry_at[chunk_id] = time.monotonic() + 3
        finally:
            self.in_progress.pop(chunk_id, None)
            await asyncio.to_thread(refresh_file_of_chunk, chunk_id)

    async def repair(self, client):
        candidates = await asyncio.to_thread(maintenance.copy_candidates)
        if cfg.rereplication_max_concurrent < 1:
            return
        jobs = []
        paths = set()
        now = time.monotonic()
        self.retry_at = {chunk_id: deadline for chunk_id, deadline in self.retry_at.items()
                         if deadline > now}
        for chunk in candidates:
            if chunk["path"] in paths or chunk["chunk_id"] in self.retry_at:
                continue
            plan = await asyncio.to_thread(maintenance.plan_copy, chunk)
            if plan:
                jobs.append((chunk, *plan))
                paths.add(chunk["path"])
                if len(jobs) >= cfg.rereplication_max_concurrent:
                    break
        await asyncio.gather(*(self.copy_chunk(client, *job) for job in jobs))

    async def maintain(self, client):
        for node in await asyncio.to_thread(reconciliation.returning_nodes):
            try:
                for chunk_id in await reconciliation.reconcile(client, node):
                    self.restored(chunk_id)
            except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
                log.warning("node_reconciliation_failed", node_id=node["node_id"], error=str(error))

        rows = await asyncio.to_thread(maintenance.pending_deletions)
        for i in range(0, len(rows), 4):
            await asyncio.gather(*(self.delete_chunk(client, row) for row in rows[i:i + 4]))

    async def delete_chunk(self, client, row):
        try:
            response = await client.delete(
                f"{row['node_url'].rstrip('/')}/chunks/{row['chunk_id']}",
                params={"disk_id": row["disk_id"]},
            )
            success = response.is_success or response.status_code == 404
        except httpx.HTTPError:
            success = False
        await asyncio.to_thread(maintenance.finish_delete, row, success)

    async def repeat(self, action):
        while True:
            try:
                await action()
            except Exception as error:
                log.exception("cluster_worker_failed", task=action.__name__, error=str(error))
            await asyncio.sleep(1)

    async def run(self):
        async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
            await asyncio.gather(
                self.repeat(self.monitor),
                self.repeat(lambda: self.repair(client)),
                self.repeat(lambda: self.maintain(client)),
                self.repeat(lambda: asyncio.to_thread(maintenance.expired_uploads)),
            )
