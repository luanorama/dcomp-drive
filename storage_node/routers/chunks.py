"""Rotas de chunks, cópia, estado e métricas do nó."""
from __future__ import annotations

import time
from typing import Any, Callable

import httpx
from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from starlette.background import BackgroundTask

from common.jsonlog import get_logger
from storage_node.disks import list_disks
from storage_node.scrubbing import scrub_once
from storage_node.settings import NodeSettings
from storage_node.store import ChunkStore, StoreError

log = get_logger("storage_node")


class CopyRequest(BaseModel):
    """Parâmetros para copiar um chunk a outro nó."""

    disk_id: str
    target_url: str
    target_disk_id: str


def install_chunk_routes(app: FastAPI, settings: NodeSettings, store: ChunkStore,
                         metrics: Any, reporter: Callable[[str, str], object]) -> None:
    """Registra as rotas usando os serviços montados pelo aplicativo."""
    def raise_store_error(exc: StoreError) -> None:
        raise HTTPException(status_code=exc.status_code, detail={"error": str(exc)}) from exc

    def report_later(chunk_id: str, disk_id: str) -> None:
        try:
            status = reporter(chunk_id, disk_id)
            status_code = getattr(status, "status_code", status)
            if status_code is None or status_code is True:
                status_code = 200
            elif status_code is False:
                status_code = 503
            if isinstance(status_code, int) and (status_code == 404 or 200 <= status_code < 300):
                store.delete(chunk_id, disk_id)
            log.info("corrupt_reported", chunk_id=chunk_id, disk_id=disk_id,
                     status=status_code)
        except (OSError, TimeoutError, ValueError) as exc:
            log.warning("corrupt_report_failed", chunk_id=chunk_id, disk_id=disk_id,
                        error=str(exc))

    @app.put("/chunks/{chunk_id}")
    def put_chunk(chunk_id: str, background_tasks: BackgroundTasks,
                  disk_id: str = Query(...), hash_value: str = Query(..., alias="hash"),
                  body: bytes = Body(default=b"")) -> dict[str, object]:
        started = time.perf_counter()
        try:
            result = store.put(chunk_id, disk_id, hash_value, body)
        except StoreError as exc:
            metrics.record("put", (time.perf_counter() - started) * 1000, len(body), error=True)
            log.warning("chunk_put_rejected", chunk_id=chunk_id, disk_id=disk_id,
                        reason=str(exc), size=len(body))
            if "Hash recebido" in str(exc):
                with metrics.lock:
                    metrics.hash_failures += 1
            raise_store_error(exc)
        duration_ms = (time.perf_counter() - started) * 1000
        metrics.record("put", duration_ms, len(body))
        log.info("chunk_put", chunk_id=chunk_id, disk_id=disk_id, size=len(body),
                 hash=hash_value, duration_ms=duration_ms)
        return {"ok": True, **result}

    @app.get("/chunks/{chunk_id}")
    def get_chunk(chunk_id: str, background_tasks: BackgroundTasks,
                  disk_id: str = Query(...)) -> Response:
        started = time.perf_counter()
        try:
            body, chunk_hash = store.get(chunk_id, disk_id)
        except StoreError as exc:
            metrics.record("get", (time.perf_counter() - started) * 1000, error=True)
            if exc.status_code == 422 and str(exc).startswith("Chunk corrompido:"):
                with metrics.lock:
                    metrics.hash_failures += 1
                log.warning("chunk_corrupt_detected", chunk_id=chunk_id, disk_id=disk_id,
                            source="get", error=str(exc))
                return JSONResponse(
                    status_code=422,
                    content={"detail": {"error": str(exc)}},
                    background=BackgroundTask(report_later, chunk_id, disk_id),
                )
            raise_store_error(exc)
        duration_ms = (time.perf_counter() - started) * 1000
        metrics.record("get", duration_ms, len(body))
        log.info("chunk_get", chunk_id=chunk_id, disk_id=disk_id, size=len(body),
                 hash=chunk_hash, duration_ms=duration_ms)
        return Response(content=body, media_type="application/octet-stream",
                        headers={"X-Chunk-Hash": chunk_hash})

    @app.delete("/chunks/{chunk_id}")
    def delete_chunk(chunk_id: str, disk_id: str = Query(...)) -> dict[str, object]:
        try:
            deleted = store.delete(chunk_id, disk_id)
        except StoreError as exc:
            raise_store_error(exc)
        with metrics.lock:
            metrics.counts["delete"] += 1
        log.info("chunk_deleted", chunk_id=chunk_id, disk_id=disk_id, deleted=deleted)
        return {"ok": True, "deleted": deleted}

    @app.get("/chunks")
    def list_chunks() -> dict[str, object]:
        return {"chunks": store.list_chunks()}

    @app.post("/chunks/{chunk_id}/copy")
    def copy_chunk(chunk_id: str, body: CopyRequest,
                   background_tasks: BackgroundTasks) -> dict[str, object]:
        started = time.perf_counter()
        log.info("chunk_copy_started", chunk_id=chunk_id, disk_id=body.disk_id,
                 target_disk_id=body.target_disk_id, target_url=body.target_url.rstrip("/"))
        try:
            data, chunk_hash = store.get(chunk_id, body.disk_id)
        except StoreError as exc:
            metrics.record("copy", (time.perf_counter() - started) * 1000, error=True)
            if exc.status_code == 422 and str(exc).startswith("Chunk corrompido:"):
                with metrics.lock:
                    metrics.hash_failures += 1
                report_later(chunk_id, body.disk_id)
                log.warning("chunk_corrupt_detected", chunk_id=chunk_id,
                            disk_id=body.disk_id, source="copy")
            log.error("chunk_copy_failed", chunk_id=chunk_id, disk_id=body.disk_id,
                      status=exc.status_code, error=str(exc))
            raise_store_error(exc)
        target = body.target_url.rstrip("/")
        timeout = httpx.Timeout(60.0, connect=5.0)
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.put(
                    f"{target}/chunks/{chunk_id}", params={"disk_id": body.target_disk_id,
                                                              "hash": chunk_hash},
                    content=data, headers={"Content-Type": "application/octet-stream"},
                )
        except httpx.HTTPError as exc:
            metrics.record("copy", (time.perf_counter() - started) * 1000, len(data), error=True)
            log.error("chunk_copy_failed", chunk_id=chunk_id, disk_id=body.disk_id,
                      target_url=target, error=str(exc))
            raise HTTPException(status_code=502, detail={"error": f"Destino indisponível: {exc}"}) from exc
        if response.status_code != 200:
            metrics.record("copy", (time.perf_counter() - started) * 1000, len(data), error=True)
            text = response.text[:500]
            log.error("chunk_copy_failed", chunk_id=chunk_id, disk_id=body.disk_id,
                      target_url=target, status=response.status_code)
            raise HTTPException(status_code=502, detail={
                "error": f"Destino respondeu HTTP {response.status_code}: {text}"})
        try:
            destination_json = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail={"error": "Destino retornou resposta inválida."}) from exc
        if destination_json.get("hash") != chunk_hash:
            raise HTTPException(status_code=502, detail={"error": "Hash de confirmação do destino diverge."})
        duration_s = time.perf_counter() - started
        metrics.record("copy", duration_s * 1000, len(data))
        log.info("chunk_copy_done", chunk_id=chunk_id, disk_id=body.disk_id,
                 target_disk_id=body.target_disk_id, size=len(data), hash=chunk_hash,
                 duration_ms=duration_s * 1000)
        return {"ok": True, "bytes": len(data), "duration_s": duration_s}

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"ok": True, "node_id": settings.node_id, "disks": list_disks(settings)}

    @app.get("/metrics")
    def get_metrics() -> dict[str, Any]:
        return metrics.snapshot(settings, store)

    @app.post("/scrub")
    def scrub() -> dict[str, object]:
        log.info("scrub_started", node_id=settings.node_id)
        result = scrub_once(store, reporter)
        metrics.record_scrub(result)
        log.info("scrub_finished", node_id=settings.node_id, **result)
        return result
