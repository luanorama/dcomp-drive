"""Registro, heartbeat, drain e visibilidade do cluster."""
from __future__ import annotations

import sqlite3
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from controller.db import db_dependency
from controller.logger import get_logger
from controller import repository as repo
from controller.workers.maintenance import refresh_files

router = APIRouter()
log = get_logger("controller.cluster")


# ─────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────

class DiskUsage(BaseModel):
    disk_id: str
    total_bytes: int = Field(ge=0)
    used_bytes: int = Field(ge=0)


class DiskInfo(DiskUsage):
    path: str


class RegisterRequest(BaseModel):
    node_id: str
    address: str
    disks: list[DiskInfo]


class HeartbeatRequest(BaseModel):
    disks: list[DiskUsage]


# ─────────────────────────────────────────────────────────────
# Registro e heartbeat
# ─────────────────────────────────────────────────────────────

@router.post("/nodes/register")
def register_node(
    body: RegisterRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Registra (ou re-registra) um nó de armazenamento."""
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        previous = repo.get_node(conn, body.node_id)
        repo.register_node(conn, body.node_id, body.address,
                           [disk.model_dump() for disk in body.disks], commit=False)
        if previous and previous["status"] == "dead":
            conn.execute(
                """INSERT INTO node_rejoins(node_id, registered_at) VALUES (?, ?)
                   ON CONFLICT(node_id) DO UPDATE SET registered_at = excluded.registered_at""",
                (body.node_id, time.time()),
            )
        elif previous and previous["status"] == "suspect":
            repo.update_node_heartbeat(conn, body.node_id, [], commit=False)
    log.info("node_register_request", node_id=body.node_id, address=body.address)
    return {"ok": True}


@router.post("/nodes/{node_id}/heartbeat")
def heartbeat(
    node_id: str,
    body: HeartbeatRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Atualiza o sinal e o espaço informado sem desfazer o drain."""
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        node = repo.get_node(conn, node_id)
        awaiting_inventory = conn.execute(
            "SELECT 1 FROM node_rejoins WHERE node_id = ?", (node_id,),
        ).fetchone()
        if not node or (node["status"] == "dead" and not awaiting_inventory):
            raise HTTPException(
                status_code=404,
                detail={"error": f"Registre o nó '{node_id}' em /nodes/register."},
            )
        reported = {d.disk_id: d for d in body.disks}
        failed = {
            disk["disk_id"] for disk in repo.get_node_disks(conn, node_id)
            if disk["total_bytes"] > 0 and disk["disk_id"] in reported
            and reported[disk["disk_id"]].total_bytes == 0
        }
        repo.update_node_heartbeat(conn, node_id, [d.model_dump() for d in body.disks],
                                   commit=False)
        for disk_id in failed:
            paths = [row[0] for row in conn.execute(
                """SELECT DISTINCT c.path FROM chunks c JOIN replicas r USING(chunk_id)
                   WHERE r.disk_id = ? AND r.state = 'ok'""", (disk_id,),
            )]
            conn.execute("UPDATE replicas SET state = 'lost' WHERE disk_id = ? AND state = 'ok'",
                         (disk_id,))
            refresh_files(conn, paths)
    for disk_id in failed:
        log.warning("disk_marked_unavailable", node_id=node_id, disk_id=disk_id)
    return {"ok": True}


@router.post("/nodes/{node_id}/drain")
def drain_node(
    node_id: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """Retira o nó da alocação e deixa suas réplicas disponíveis até a migração."""
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        node = repo.get_node(conn, node_id)
        if node is None:
            raise HTTPException(status_code=404, detail={"error": f"Nó '{node_id}' não encontrado."})
        if node["status"] not in ("alive", "draining"):
            raise HTTPException(status_code=409, detail={"error": "O nó precisa estar disponível."})

        conn.execute("INSERT OR IGNORE INTO node_drains(node_id) VALUES (?)", (node_id,))
        conn.execute("UPDATE nodes SET status = 'draining' WHERE node_id = ?", (node_id,))
        queued = len({c["chunk_id"] for c in repo.list_chunks_of_node(conn, node_id)
                      if c["state"] == "ok"})

    log.info("node_drain_requested", node_id=node_id, queued_chunks=queued)
    return {"ok": True, "queued_chunks": queued}


# ─────────────────────────────────────────────────────────────
# Status e métricas
# ─────────────────────────────────────────────────────────────

@router.get("/status")
def get_status(
    request: Request,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """
    Estado geral do cluster:
    - Nós com status e discos
    - Contagem de arquivos por estado
    - Re-replicações em andamento
    """
    nodes = repo.list_nodes(conn)
    pending_cleanup = {
        row["node_id"]: row["waiting"] for row in conn.execute(
            """SELECT d.node_id, COUNT(*) AS waiting FROM chunk_cleanup_queue
               JOIN disks d USING(disk_id) GROUP BY d.node_id"""
        )
    }
    staged = {
        row["node_id"]: row["waiting"] for row in conn.execute(
            """SELECT d.node_id, COUNT(DISTINCT r.chunk_id) AS waiting
               FROM staged_replicas r JOIN disks d USING(disk_id) GROUP BY d.node_id"""
        )
    }
    nodes_out = []
    for n in nodes:
        disks = repo.get_node_disks(conn, n["node_id"])
        nodes_out.append({
            "node_id": n["node_id"],
            "address": n["address"],
            "status": n["status"],
            "last_heartbeat": n["last_heartbeat"],
            "drain_remaining_chunks": len({
                c["chunk_id"] for c in repo.list_chunks_of_node(conn, n["node_id"])
                if c["state"] in ("ok", "pending")
            }) + staged.get(n["node_id"], 0) if n["status"] == "draining" else None,
            "pending_deletions": pending_cleanup.get(n["node_id"], 0),
            "disks": [
                {
                    "disk_id": d["disk_id"],
                    "path": d["path"],
                    "total_bytes": d["total_bytes"],
                    "used_bytes": d["used_bytes"],
                    "free_bytes": max(0, d["total_bytes"] - d["used_bytes"]),
                }
                for d in disks
            ],
        })

    # Contagem de arquivos por estado
    rows = conn.execute(
        """
        SELECT state, COUNT(*) AS cnt
        FROM entries WHERE type = 'file'
        GROUP BY state
        """,
    ).fetchall()
    files_by_state = {r["state"]: r["cnt"] for r in rows}

    worker = getattr(request.app.state, "cluster_worker", None)
    return {
        "nodes": nodes_out,
        "files_by_state": files_by_state,
        "rereplications": list(worker.in_progress.values()) if worker else [],
    }


@router.get("/metrics")
def get_metrics(
    request: Request,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """Contagens e última medição de detecção e recuperação."""
    # Réplicas por estado
    rows = conn.execute(
        "SELECT state, COUNT(*) AS cnt FROM replicas GROUP BY state"
    ).fetchall()
    replicas_by_state = {r["state"]: r["cnt"] for r in rows}

    # Arquivos por estado
    rows2 = conn.execute(
        "SELECT state, COUNT(*) AS cnt FROM entries WHERE type='file' GROUP BY state"
    ).fetchall()
    files_by_state = {r["state"]: r["cnt"] for r in rows2}

    worker = getattr(request.app.state, "cluster_worker", None)
    return {
        "replicas_by_state": replicas_by_state,
        "files_by_state": files_by_state,
        "detection_time_s": worker.detection_time_s if worker else None,
        "recovery_time_s": worker.recovery_time_s if worker else None,
    }
