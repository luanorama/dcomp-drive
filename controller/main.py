"""
controller/main.py
Ponto de entrada do controlador DComp Drive.

Inicializa o banco (schema SQLite WAL), monta os routers de arquivo e cluster,
e expõe /health. Para subir:
    uvicorn controller.main:app --host 0.0.0.0 --port 8000 --reload

A Pessoa C adicionará os background tasks (monitor de heartbeat, fila de
re-replicação e worker de expiração de leases) neste mesmo arquivo ou em
controller/workers/.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from controller.config import get_config
from controller.db import get_db
from controller.logger import get_logger
from controller.routers.files import router as files_router
from controller.routers.cluster import router as cluster_router

log = get_logger("controller.main")
cfg = get_config()


# ─────────────────────────────────────────────────────────────
# Lifespan: inicialização e encerramento
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializa o banco (cria schema se não existir) — RF10
    conn = get_db()
    conn.close()
    log.info(
        "controller_started",
        replication_factor=cfg.replication_factor,
        write_quorum=cfg.write_quorum,
        chunk_size_mb=cfg.chunk_size_mb,
        encryption=cfg.encryption_enabled,
    )

    # TODO (Pessoa C): iniciar aqui os background tasks:
    #   - asyncio.create_task(monitor_heartbeats())
    #   - asyncio.create_task(rereplication_worker())
    #   - asyncio.create_task(lease_expiry_worker())

    yield

    log.info("controller_stopped")


# ─────────────────────────────────────────────────────────────
# Aplicação FastAPI
# ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="DComp Drive — Controlador",
    description=(
        "Nó de metadados e controlador do sistema distribuído de armazenamento. "
        "Gerencia arquivos, diretórios, replicação, leases e detecção de falhas."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Routers da Pessoa A
app.include_router(files_router, tags=["Arquivos e Diretórios"])

# Routers da Pessoa C (stubs fornecidos pela Pessoa A)
app.include_router(cluster_router, tags=["Cluster e Nós"])


# ─────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["Sistema"])
def health() -> dict[str, Any]:
    """
    Verifica se o controlador está operacional.
    Retorna configurações principais para diagnóstico.
    """
    return {
        "ok": True,
        "replication_factor": cfg.replication_factor,
        "write_quorum": cfg.write_quorum,
        "chunk_size_mb": cfg.chunk_size_mb,
        "encryption_enabled": cfg.encryption_enabled,
    }


# ─────────────────────────────────────────────────────────────
# Handler global de erros
# ─────────────────────────────────────────────────────────────

@app.exception_handler(Exception)
async def generic_exception_handler(request, exc: Exception):
    log.exception("unhandled_exception", error=str(exc))
    return JSONResponse(
        status_code=500,
        content={"error": "Erro interno do controlador. Verifique os logs."},
    )
