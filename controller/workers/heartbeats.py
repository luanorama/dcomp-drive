"""Detecção de falhas sem depender de chamadas aos nós."""
from __future__ import annotations

import time
from contextlib import closing
from dataclasses import dataclass

from controller.config import get_config
from controller.db import get_db
from controller.logger import get_logger
from controller.workers.maintenance import refresh_files

cfg = get_config()
log = get_logger("controller.heartbeats")


@dataclass(frozen=True)
class NodeFailure:
    seconds_since_heartbeat: float
    affected_chunks: int
    pending_chunks: frozenset[str]


def check_heartbeats(now: float | None = None) -> list[NodeFailure]:
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
                """SELECT DISTINCT r.chunk_id, c.path,
                       (SELECT COUNT(*)
                        FROM replicas other JOIN disks od USING(disk_id)
                        JOIN nodes onode USING(node_id)
                        WHERE other.chunk_id = r.chunk_id AND other.state = 'ok'
                          AND od.total_bytes > 0 AND onode.status = 'alive')
                        < COALESCE(e.replication_factor, ?) AS needs_repair
                   FROM replicas r JOIN disks d USING(disk_id) JOIN chunks c USING(chunk_id)
                   JOIN entries e ON e.path = c.path
                   WHERE d.node_id = ? AND r.state = 'ok'""",
                (cfg.replication_factor, node_id),
            ).fetchall()
            conn.execute(
                """UPDATE replicas SET state = 'lost' WHERE state = 'ok'
                   AND disk_id IN (SELECT disk_id FROM disks WHERE node_id = ?)""",
                (node_id,),
            )
            refresh_files(conn, {chunk["path"] for chunk in chunks})
            failures.append(NodeFailure(
                seconds_since_heartbeat=elapsed,
                affected_chunks=len(chunks),
                pending_chunks=frozenset(c["chunk_id"] for c in chunks if c["needs_repair"]),
            ))

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
