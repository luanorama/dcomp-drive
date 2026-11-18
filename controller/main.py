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

async def _housekeeping() -> None:
    """Expira leases e faz deleção física com retry persistente."""
    import asyncio
    import httpx
    from controller import repository as repo

    while True:
        try:
            conn = get_db()
            try:
                expired = repo.cleanup_expired_uploads(conn)
                due = repo.due_chunk_cleanup(conn)
            finally:
                conn.close()
            if expired:
                log.warning('expired_leases_cleaned', count=expired)
            if due:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    for job in due:
                        success = False
                        try:
                            response = await client.delete(
                                f"{job['node_url'].rstrip('/')}/chunks/{job['chunk_id']}",
                                params={'disk_id': job['disk_id']},
                            )
                            success = response.status_code in (200, 202, 204, 404)
                        except httpx.HTTPError as exc:
                            log.warning('chunk_cleanup_unreachable',
                                        chunk_id=job['chunk_id'], error=str(exc))
                        conn = get_db()
                        try:
                            repo.finish_chunk_cleanup(
                                conn, job['chunk_id'], job['disk_id'], success,
                                job['attempts'],
                            )
                        finally:
                            conn.close()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception('housekeeping_failed', error=str(exc))
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from contextlib import suppress

    conn = get_db()
    conn.close()
    worker = asyncio.create_task(_housekeeping(), name='pessoa-a-lease-housekeeping')
    log.info('controller_started', replication_factor=cfg.replication_factor,
             write_quorum=cfg.write_quorum, chunk_size_mb=cfg.chunk_size_mb,
             encryption=cfg.encryption_enabled)
    try:
        yield
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
        log.info('controller_stopped')




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
