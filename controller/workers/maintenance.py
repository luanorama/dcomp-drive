"""Operações de recuperação que precisam de uma transação SQLite."""
from __future__ import annotations

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
                AND (SELECT COUNT(*)
                     FROM replicas r JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
                     WHERE r.chunk_id = c.chunk_id AND r.state = 'ok'
                       AND d.total_bytes > 0
                       AND n.status IN ('alive', 'suspect', 'draining'))
                    < COALESCE(entries.replication_factor, ?)
            ) THEN 'under_replicated' ELSE 'intact' END
            WHERE path = ? AND file_hash IS NOT NULL
            """,
            (cfg.replication_factor, path),
        )


def queue_delete(conn, chunk_id, disk_id):
    conn.execute(
        """INSERT OR IGNORE INTO chunk_cleanup_queue(chunk_id, disk_id, node_url)
           SELECT ?, d.disk_id, n.address FROM disks d JOIN nodes n USING(node_id)
           WHERE d.disk_id = ?""",
        (chunk_id, disk_id),
    )


def copy_candidates():
    with closing(get_db()) as conn:
        return conn.execute(
            """SELECT c.chunk_id, c.size, c.path, c.hash,
                      COUNT(DISTINCT CASE WHEN r.state = 'ok'
                           AND n.status IN ('alive', 'draining') THEN d.node_id END) AS sources,
                      COUNT(DISTINCT CASE WHEN r.state = 'ok'
                           AND n.status = 'alive' THEN d.disk_id END) AS healthy,
                      SUM(CASE WHEN r.state = 'ok' AND n.status = 'draining'
                               THEN 1 ELSE 0 END) AS draining
               FROM chunks c JOIN entries e USING(path)
               LEFT JOIN replicas r USING(chunk_id) LEFT JOIN disks d USING(disk_id)
               LEFT JOIN nodes n USING(node_id)
               WHERE c.hash IS NOT NULL AND e.file_hash IS NOT NULL
               GROUP BY c.chunk_id
               HAVING sources > 0 AND (healthy < COALESCE(e.replication_factor, ?) OR draining > 0)
               ORDER BY sources, c.chunk_id""",
            (cfg.replication_factor,),
        ).fetchall()


def healthy_count(conn, chunk_id):
    return conn.execute(
        """SELECT COUNT(*)
           FROM replicas r JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
           WHERE r.chunk_id = ? AND r.state = 'ok' AND n.status = 'alive' AND d.total_bytes > 0""",
        (chunk_id,),
    ).fetchone()[0]


def replication_target(conn, path):
    entry = repo.get_entry(conn, path)
    return (entry and entry["replication_factor"]) or cfg.replication_factor


def retire_draining(conn, chunk_id, desired):
    if healthy_count(conn, chunk_id) < desired:
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
        stored = repo.get_chunk(conn, chunk_id)
        if not stored:
            return None
        desired = replication_target(conn, stored["path"])
        if healthy_count(conn, chunk_id) >= desired:
            retire_draining(conn, chunk_id, desired)
            return None

        replicas = repo.get_live_replicas(conn, chunk_id)
        sources = [row for row in replicas if row["status"] in ("alive", "draining")]
        if not sources:
            return None

        deletions = conn.execute(
            "SELECT disk_id FROM chunk_cleanup_queue WHERE chunk_id = ?", (chunk_id,),
        ).fetchall()
        excluded_disks = [row["disk_id"] for row in deletions]
        excluded_disks.extend(row["disk_id"] for row in replicas)
        targets = repo.choose_replication_targets(
            conn, 1, exclude_disks=excluded_disks,
            exclude_nodes=[row["node_id"] for row in replicas],
            required_bytes=stored["size"],
        )
        if not targets:
            # A alocação pode usar outra unidade da mesma máquina quando faltam máquinas.
            targets = repo.choose_replication_targets(
                conn, 1, exclude_disks=excluded_disks, required_bytes=stored["size"],
            )
        return (sources, targets[0]) if targets else None


def finish_copy(chunk, target):
    chunk_id = chunk["chunk_id"]
    with closing(get_db()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        node = repo.get_node(conn, target["node_id"])
        stored = repo.get_chunk(conn, chunk_id)
        desired = replication_target(conn, stored["path"]) if stored else cfg.replication_factor
        if any(r["disk_id"] == target["disk_id"] for r in repo.get_live_replicas(conn, chunk_id)):
            return healthy_count(conn, chunk_id) >= desired

        if (not stored or stored["hash"] != chunk["hash"]
                or not node or node["status"] != "alive" or node["address"] != target["node_url"]
                or healthy_count(conn, chunk_id) >= desired
                or not conn.execute(
                    "SELECT 1 FROM disks WHERE disk_id = ? AND total_bytes > 0",
                    (target["disk_id"],),
                ).fetchone()
                or conn.execute(
                    "SELECT 1 FROM chunk_cleanup_queue WHERE chunk_id = ? AND disk_id = ?",
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
        retire_draining(conn, chunk_id, desired)
        refresh_files(conn, [stored["path"]])
        return healthy_count(conn, chunk_id) >= desired


def expired_uploads():
    with closing(get_db()) as conn:
        repo.cleanup_expired_uploads(conn)


def pending_deletions():
    with closing(get_db()) as conn:
        return repo.due_chunk_cleanup(conn, limit=12)


def finish_delete(row, success):
    with closing(get_db()) as conn:
        repo.finish_chunk_cleanup(conn, row["chunk_id"], row["disk_id"], success, row["attempts"])
