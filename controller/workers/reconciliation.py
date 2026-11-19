"""Confere o inventário de um nó antes de recolocá-lo no cluster."""
from __future__ import annotations

import asyncio
import hashlib
import time
from contextlib import closing
from uuid import UUID

import httpx

from controller.config import get_config
from controller.db import get_db
from controller.logger import get_logger
from controller.workers.maintenance import healthy_count, queue_delete, refresh_files, replication_target

cfg = get_config()
log = get_logger("controller.reconciliation")


def returning_nodes():
    with closing(get_db()) as conn:
        return conn.execute(
            "SELECT node_id, address, registered_at FROM node_rejoins JOIN nodes USING(node_id)"
        ).fetchall()


def known_chunks(node_id):
    with closing(get_db()) as conn:
        return {
            (r["chunk_id"], r["disk_id"]): r["hash"]
            for r in conn.execute(
                """SELECT r.chunk_id, r.disk_id, c.hash FROM replicas r
                   JOIN chunks c USING(chunk_id) JOIN disks d USING(disk_id)
                   WHERE d.node_id = ? AND c.hash IS NOT NULL AND r.state != 'pending'""",
                (node_id,),
            )
        }


def finish_rejoin(node, inventory, verified):
    node_id = node["node_id"]
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute(
            """SELECT registered_at, last_heartbeat, address FROM node_rejoins
               JOIN nodes USING(node_id) WHERE node_id = ?""",
            (node_id,),
        ).fetchone()
        if (not current or current["registered_at"] != node["registered_at"]
                or current["address"] != node["address"]
                or time.time() - current["last_heartbeat"] >= cfg.dead_after_s):
            return []

        draining = conn.execute("SELECT 1 FROM node_drains WHERE node_id = ?", (node_id,)).fetchone()
        conn.execute("UPDATE nodes SET status = ? WHERE node_id = ?",
                     ("draining" if draining else "alive", node_id))
        owned_disks = {
            r[0] for r in conn.execute("SELECT disk_id FROM disks WHERE node_id = ?", (node_id,))
        }
        paths = set()
        restored = []
        for chunk_id, disk_id in inventory:
            if disk_id not in owned_disks:
                continue
            replica = conn.execute(
                """SELECT r.state, c.hash, c.path FROM replicas r JOIN chunks c USING(chunk_id)
                   WHERE r.chunk_id = ? AND r.disk_id = ?""",
                (chunk_id, disk_id),
            ).fetchone()
            if replica and (replica["state"] == "pending" or replica["hash"] is None):
                continue
            if conn.execute(
                "SELECT 1 FROM staged_replicas WHERE chunk_id = ? AND disk_id = ?",
                (chunk_id, disk_id),
            ).fetchone():
                continue

            expected = verified.get((chunk_id, disk_id))
            desired = replication_target(conn, replica["path"]) if replica else None
            queued = conn.execute(
                "SELECT 1 FROM chunk_cleanup_queue WHERE chunk_id = ? AND disk_id = ?",
                (chunk_id, disk_id),
            ).fetchone()
            if (replica and expected and expected == replica["hash"]
                    and not queued and healthy_count(conn, chunk_id) < desired):
                conn.execute("UPDATE replicas SET state = 'ok' WHERE chunk_id = ? AND disk_id = ?",
                             (chunk_id, disk_id))
                paths.add(replica["path"])
                if healthy_count(conn, chunk_id) >= desired:
                    restored.append(chunk_id)
            elif not replica or replica["state"] != "ok":
                queue_delete(conn, chunk_id, disk_id)

        refresh_files(conn, paths)
        conn.execute("DELETE FROM node_rejoins WHERE node_id = ?", (node_id,))
    log.info("node_reconciled", node_id=node_id, inventory_size=len(inventory))
    return restored


async def reconcile(client, node):
    url = node["address"].rstrip("/")
    response = await client.get(f"{url}/chunks")
    response.raise_for_status()
    inventory = {}
    for item in response.json()["chunks"]:
        chunk_id, disk_id = item["chunk_id"], item["disk_id"]
        if not isinstance(chunk_id, str) or not isinstance(disk_id, str):
            raise ValueError("Inventário de chunks inválido")
        try:
            if str(UUID(chunk_id)) != chunk_id:
                continue
        except ValueError:
            continue
        inventory[(chunk_id, disk_id)] = item["hash"]

    known = await asyncio.to_thread(known_chunks, node["node_id"])
    verified = {}
    for (chunk_id, disk_id), expected in known.items():
        if inventory.get((chunk_id, disk_id)) != expected:
            continue
        response = await client.get(f"{url}/chunks/{chunk_id}", params={"disk_id": disk_id})
        if response.status_code in (404, 422):
            continue
        response.raise_for_status()
        if hashlib.sha256(response.content).hexdigest() == expected:
            verified[(chunk_id, disk_id)] = expected

    return await asyncio.to_thread(finish_rejoin, node, inventory, verified)
