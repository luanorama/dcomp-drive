"""
controller/routers/cluster.py
Endpoints de nós, status e métricas — Pessoa C.

Este arquivo contém os STUBS mínimos para que o controlador suba sem erros
e os endpoints existam para a Pessoa B testar. A Pessoa C irá completar
com a lógica real de heartbeat, monitor de falhas e re-replicação.

Stubs entregues aqui (Pessoa A):
  POST /nodes/register        — registra nó no banco (usa repository)
  POST /nodes/{node_id}/heartbeat — atualiza last_heartbeat
  GET  /status                — lista nós e estado resumido
  GET  /metrics               — métricas básicas (placeholder)
"""
from __future__ import annotations

import sqlite3
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from controller.db import db_dependency
from controller.logger import get_logger
from controller import repository as repo

router = APIRouter()
log = get_logger("controller.cluster")


# ─────────────────────────────────────────────────────────────
# Modelos
# ─────────────────────────────────────────────────────────────

class DiskInfo(BaseModel):
    disk_id: str
    path: str
    total_bytes: int
    used_bytes: int


class RegisterRequest(BaseModel):
    node_id: str
    address: str
    disks: list[DiskInfo]


class HeartbeatRequest(BaseModel):
    disks: list[DiskInfo]


# ─────────────────────────────────────────────────────────────
# Registro e heartbeat (Pessoa C completará a lógica de monitor)
# ─────────────────────────────────────────────────────────────

@router.post("/nodes/register")
def register_node(
    body: RegisterRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Registra (ou re-registra) um nó de armazenamento."""
    repo.register_node(
        conn,
        node_id=body.node_id,
        address=body.address,
        disks=[d.model_dump() for d in body.disks],
    )
    log.info("node_register_request", node_id=body.node_id, address=body.address)
    return {"ok": True}


@router.post("/nodes/{node_id}/heartbeat")
def heartbeat(
    node_id: str,
    body: HeartbeatRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """
    Recebe heartbeat de um nó e atualiza last_heartbeat e espaço dos discos.
    A Pessoa C adicionará aqui a lógica de transição alive/suspect/dead.
    """
    ok = repo.update_node_heartbeat(
        conn, node_id, [d.model_dump() for d in body.disks]
    )
    if not ok:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Nó '{node_id}' não registrado. Chame /nodes/register primeiro."},
        )
    return {"ok": True}


@router.post("/nodes/{node_id}/drain")
def drain_node(
    node_id: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """
    Marca o nó como 'draining': para de receber novas réplicas mas ainda serve leituras.
    A Pessoa C enfileirará os chunks para re-replicação.
    Stub: apenas altera o status no banco.
    """
    node = repo.get_node(conn, node_id)
    if node is None:
        raise HTTPException(status_code=404, detail={"error": f"Nó '{node_id}' não encontrado."})

    repo.update_node_status(conn, node_id, "draining")

    # Conta chunks que precisarão ser movidos (Pessoa C completará a fila)
    chunks = repo.list_chunks_of_node(conn, node_id)
    queued = len([c for c in chunks if c["state"] == "ok"])

    log.info("node_drain_requested", node_id=node_id, queued_chunks=queued)
    return {"ok": True, "queued_chunks": queued}


# ─────────────────────────────────────────────────────────────
# Status e métricas (Pessoa C irá enriquecer)
# ─────────────────────────────────────────────────────────────

@router.get("/status")
def get_status(
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """
    Estado geral do cluster:
    - Nós com status e discos
    - Contagem de arquivos por estado
    - Re-replicações em andamento (placeholder — Pessoa C preenche)
    """
    nodes = repo.list_nodes(conn)
    nodes_out = []
    for n in nodes:
        disks = repo.get_node_disks(conn, n["node_id"])
        nodes_out.append({
            "node_id": n["node_id"],
            "address": n["address"],
            "status": n["status"],
            "last_heartbeat": n["last_heartbeat"],
            "disks": [
                {
                    "disk_id": d["disk_id"],
                    "path": d["path"],
                    "total_bytes": d["total_bytes"],
                    "used_bytes": d["used_bytes"],
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

    return {
        "nodes": nodes_out,
        "files_by_state": files_by_state,
        "rereplications": [],  # TODO: Pessoa C
    }


@router.get("/metrics")
def get_metrics(
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, Any]:
    """
    Métricas básicas do controlador em JSON.
    A Pessoa C adicionará tempos de detecção e recuperação.
    """
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

    return {
        "replicas_by_state": replicas_by_state,
        "files_by_state": files_by_state,
        "detection_time_s": None,   # TODO: Pessoa C
        "recovery_time_s": None,    # TODO: Pessoa C
    }
