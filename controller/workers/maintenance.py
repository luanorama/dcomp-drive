"""Operações de recuperação que precisam de uma transação SQLite."""
from __future__ import annotations

import time
from contextlib import closing

from controller import repository as repo
from controller.config import get_config
from controller.db import get_db

cfg = get_config()


def refresh_files(conn, paths):
    for path in paths:
        conn.execute(
            """
            UPDATE entries SET state = CASE WHEN EXISTS (
                SELECT 1 FROM chunks c WHERE c.path = entries.path
                AND (SELECT COUNT(DISTINCT d.node_id)
                     FROM replicas r JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
                     WHERE r.chunk_id = c.chunk_id AND r.state = 'ok'
                       AND n.status IN ('alive', 'suspect', 'draining')) < ?
            ) THEN 'under_replicated' ELSE 'intact' END
            WHERE path = ? AND file_hash IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM leases WHERE leases.path = entries.path)
            """,
            (cfg.replication_factor, path),
        )


def queue_delete(conn, chunk_id, disk_id):
    conn.execute(
        "INSERT OR IGNORE INTO chunk_deletions(chunk_id, disk_id) VALUES (?, ?)",
        (chunk_id, disk_id),
    )


def copy_candidates():
    with closing(get_db()) as conn:
        return conn.execute(
            """SELECT c.chunk_id, c.size, c.path, c.hash,
                      COUNT(DISTINCT CASE WHEN r.state = 'ok'
                           AND n.status IN ('alive', 'draining') THEN d.node_id END) AS sources,
                      COUNT(DISTINCT CASE WHEN r.state = 'ok'
                           AND n.status = 'alive' THEN d.node_id END) AS healthy
               FROM chunks c JOIN entries e USING(path)
               LEFT JOIN replicas r USING(chunk_id) LEFT JOIN disks d USING(disk_id)
               LEFT JOIN nodes n USING(node_id)
               WHERE c.hash IS NOT NULL AND e.file_hash IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM leases WHERE leases.path = c.path)
               GROUP BY c.chunk_id
               HAVING sources > 0 AND (healthy < ? OR sources > healthy)
               ORDER BY sources, c.chunk_id""",
            (cfg.replication_factor,),
        ).fetchall()


def healthy_count(conn, chunk_id):
    return conn.execute(
        """SELECT COUNT(DISTINCT d.node_id)
           FROM replicas r JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
           WHERE r.chunk_id = ? AND r.state = 'ok' AND n.status = 'alive'""",
        (chunk_id,),
    ).fetchone()[0]


def retire_draining(conn, chunk_id):
    if healthy_count(conn, chunk_id) < cfg.replication_factor:
        return
    rows = conn.execute(
        """SELECT r.disk_id FROM replicas r JOIN disks d USING(disk_id)
           JOIN nodes n USING(node_id)
           WHERE r.chunk_id = ? AND r.state = 'ok' AND n.status = 'draining'""",
        (chunk_id,),
    ).fetchall()
    for row in rows:
        conn.execute("UPDATE replicas SET state = 'lost' WHERE chunk_id = ? AND disk_id = ?",
                     (chunk_id, row["disk_id"]))
        queue_delete(conn, chunk_id, row["disk_id"])


def plan_copy(chunk):
    chunk_id = chunk["chunk_id"]
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        if not repo.get_chunk(conn, chunk_id):
            return None
        if healthy_count(conn, chunk_id) >= cfg.replication_factor:
            retire_draining(conn, chunk_id)
            return None

        replicas = repo.get_live_replicas(conn, chunk_id)
        sources = [row for row in replicas if row["status"] in ("alive", "draining")]
        if not sources:
            return None

        deletions = conn.execute(
            "SELECT disk_id FROM chunk_deletions WHERE chunk_id = ?", (chunk_id,),
        ).fetchall()
        targets = repo.choose_replication_targets(
            conn, 1,
            exclude_disks=[row["disk_id"] for row in deletions],
            exclude_nodes=[row["node_id"] for row in replicas],
            required_bytes=chunk["size"],
        )
        return (sources, targets[0]) if targets else None


def finish_copy(chunk, target):
    chunk_id = chunk["chunk_id"]
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        node = repo.get_node(conn, target["node_id"])
        stored = repo.get_chunk(conn, chunk_id)
        existing = next(
            (r for r in repo.get_live_replicas(conn, chunk_id)
             if r["node_id"] == target["node_id"]), None,
        )
        if existing and existing["disk_id"] == target["disk_id"]:
            return healthy_count(conn, chunk_id) >= cfg.replication_factor

        if (not stored or stored["hash"] != chunk["hash"] or existing
                or not node or node["status"] != "alive" or node["address"] != target["node_url"]
                or healthy_count(conn, chunk_id) >= cfg.replication_factor
                or not conn.execute(
                    "SELECT 1 FROM disks WHERE disk_id = ? AND total_bytes > 0",
                    (target["disk_id"],),
                ).fetchone()
                or conn.execute(
                    "SELECT 1 FROM chunk_deletions WHERE chunk_id = ? AND disk_id = ?",
                    (chunk_id, target["disk_id"]),
                ).fetchone()):
            if node and node["address"] == target["node_url"]:
                queue_delete(conn, chunk_id, target["disk_id"])
            return False

        conn.execute(
            """INSERT INTO replicas(chunk_id, disk_id, state) VALUES (?, ?, 'ok')
               ON CONFLICT(chunk_id, disk_id) DO UPDATE SET state = 'ok'""",
            (chunk_id, target["disk_id"]),
        )
        retire_draining(conn, chunk_id)
        refresh_files(conn, [stored["path"]])
        return healthy_count(conn, chunk_id) >= cfg.replication_factor


def expired_uploads():
    """Remove só chunks sem commit de leases vencidos e agenda a limpeza física."""
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        for lease in repo.list_expired_leases(conn):
            path = lease["path"]
            for row in conn.execute(
                """SELECT r.chunk_id, r.disk_id FROM replicas r JOIN chunks c USING(chunk_id)
                   WHERE c.path = ? AND c.hash IS NULL""",
                (path,),
            ):
                queue_delete(conn, row["chunk_id"], row["disk_id"])
            conn.execute("DELETE FROM chunks WHERE path = ? AND hash IS NULL", (path,))
            conn.execute("DELETE FROM leases WHERE path = ?", (path,))
            conn.execute("DELETE FROM entries WHERE path = ? AND file_hash IS NULL", (path,))
            refresh_files(conn, [path])


def pending_deletions():
    with closing(get_db()) as conn:
        return conn.execute(
            """SELECT q.chunk_id, q.disk_id, n.address FROM chunk_deletions q
               JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
               WHERE n.status IN ('alive', 'draining')
               ORDER BY q.rowid LIMIT 12"""
        ).fetchall()


def finish_delete(row, success):
    with closing(get_db()) as conn, conn:
        if success:
            conn.execute("DELETE FROM chunk_deletions WHERE chunk_id = ? AND disk_id = ?",
                         (row["chunk_id"], row["disk_id"]))
        else:
            conn.execute(
                """UPDATE chunk_deletions SET rowid =
                       (SELECT COALESCE(MAX(rowid), 0) + 1 FROM chunk_deletions)
                   WHERE chunk_id = ? AND disk_id = ?""",
                (row["chunk_id"], row["disk_id"]),
            )
