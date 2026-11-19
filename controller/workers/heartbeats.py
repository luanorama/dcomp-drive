"""Detecção de falhas sem depender de chamadas aos nós."""
from __future__ import annotations

import time
from contextlib import closing

from controller.config import get_config
from controller.db import get_db
from controller.logger import get_logger
from controller.workers.maintenance import refresh_files

cfg = get_config()
log = get_logger("controller.heartbeats")


def check_heartbeats(now: float | None = None) -> list[tuple[float, list[str]]]:
    """Atualiza nós e réplicas na mesma transação e retorna as falhas detectadas."""
    now = time.time() if now is None else now
    failures = []
    transitions = []

    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        nodes = conn.execute(
            """
            SELECT node_id, status, last_heartbeat FROM nodes
            WHERE last_heartbeat <= ? AND status != 'dead'
            """,
            (now - cfg.suspect_after_s,),
        ).fetchall()
        for node in nodes:
            node_id = node["node_id"]
            elapsed = now - node["last_heartbeat"]
            status = "dead" if elapsed >= cfg.dead_after_s else "suspect"
            if status == node["status"]:
                continue

            conn.execute("UPDATE nodes SET status = ? WHERE node_id = ?", (status, node_id))
            transitions.append((node_id, node["status"], status, elapsed))
            if status != "dead":
                continue

            chunks = conn.execute(
                """SELECT DISTINCT r.chunk_id, c.path
                   FROM replicas r JOIN disks d USING(disk_id) JOIN chunks c USING(chunk_id)
                   WHERE d.node_id = ? AND r.state = 'ok'""",
                (node_id,),
            ).fetchall()
            conn.execute(
                """UPDATE replicas SET state = 'lost' WHERE state = 'ok'
                   AND disk_id IN (SELECT disk_id FROM disks WHERE node_id = ?)""",
                (node_id,),
            )
            refresh_files(conn, {chunk["path"] for chunk in chunks})
            failures.append((elapsed, [chunk["chunk_id"] for chunk in chunks]))

        conn.execute(
            """DELETE FROM node_rejoins WHERE node_id IN (
                   SELECT node_id FROM nodes WHERE status = 'dead' AND last_heartbeat <= ?
               )""",
            (now - cfg.dead_after_s,),
        )

    for node_id, previous, status, elapsed in transitions:
        log.info("node_status_changed", node_id=node_id, previous=previous,
                 status=status, seconds_since_heartbeat=round(elapsed, 3))
    return failures
