"""Aplicação HTTP do nó de armazenamento."""
from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Callable

import httpx
from fastapi import FastAPI

from common.jsonlog import get_logger
from storage_node.disks import list_disks
from storage_node.routers.chunks import install_chunk_routes
from storage_node.scrubbing import ScrubWorker
from storage_node.settings import NodeSettings
from storage_node.store import ChunkStore

log = get_logger("storage_node")


class NodeMetrics:
    """Contadores de operação e janela de latências do nó."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.counts: dict[str, int] = {"put": 0, "get": 0, "copy": 0, "delete": 0}
        self.errors: dict[str, int] = {"put": 0, "get": 0, "copy": 0}
        self.bytes: dict[str, int] = {"put": 0, "get": 0, "copy": 0}
        self.latencies: dict[str, deque[float]] = {
            name: deque(maxlen=1000) for name in ("put", "get", "copy")
        }
        self.hash_failures = 0
        self.scrub = {"runs": 0, "last_run_ts": None, "last_duration_s": 0.0,
                      "last_checked": 0, "last_corrupt": 0, "total_corrupt_detected": 0}

    def record(self, name: str, duration_ms: float = 0, size: int = 0,
               *, error: bool = False) -> None:
        with self.lock:
            self.counts[name] = self.counts.get(name, 0) + 1
            if name in self.latencies:
                self.latencies[name].append(duration_ms)
                self.bytes[name] = self.bytes.get(name, 0) + size
            if error and name in self.errors:
                self.errors[name] += 1

    def record_scrub(self, result: dict[str, object]) -> None:
        corrupt = result.get("corrupt", [])
        with self.lock:
            self.scrub["runs"] = int(self.scrub["runs"]) + 1
            self.scrub["last_run_ts"] = time.time()
            self.scrub["last_duration_s"] = float(result.get("duration_s", 0.0))
            self.scrub["last_checked"] = int(result.get("checked", 0))
            self.scrub["last_corrupt"] = len(corrupt) if isinstance(corrupt, list) else 0
            self.scrub["total_corrupt_detected"] = (
                int(self.scrub["total_corrupt_detected"]) + int(self.scrub["last_corrupt"])
            )

    def snapshot(self, settings: NodeSettings, store: ChunkStore) -> dict[str, Any]:
        with self.lock:
            operations: dict[str, Any] = {}
            for name in ("put", "get", "copy"):
                values = sorted(self.latencies[name])
                p95 = values[min(len(values) - 1, int((len(values) - 1) * 0.95))] if values else 0.0
                operations[name] = {
                    "count": self.counts[name], "errors": self.errors[name],
                    "bytes": self.bytes[name], "latency_ms": {
                        "avg": sum(values) / len(values) if values else 0.0,
                        "p95": p95, "max": values[-1] if values else 0.0,
                    },
                }
            return {
                "node_id": settings.node_id, "uptime_s": max(0.0, time.time() - self.started_at),
                "chunks_stored": store.chunk_count(), "disks": list_disks(settings),
                **operations, "delete": {"count": self.counts["delete"]},
                "hash_failures": self.hash_failures, "scrub": dict(self.scrub),
            }


def _make_reporter(settings: NodeSettings) -> Callable[[str, str], int]:
    """Cria o melhor esforço de denúncia ao controlador."""
    def report(chunk_id: str, disk_id: str) -> int:
        try:
            response = httpx.post(
                f"{settings.controller_url}/chunks/corrupt",
                json={"chunk_id": chunk_id, "disk_id": disk_id}, timeout=5.0,
            )
            return response.status_code
        except httpx.HTTPError as exc:
            raise OSError(f"Não foi possível denunciar a corrupção: {exc}") from exc
    return report


def create_app(settings: NodeSettings, *, report_corrupt: Callable[[str, str], object] | None = None) -> FastAPI:
    """Cria o nó e permite substituir a denúncia ao controlador nos testes."""
    settings.validate()
    if settings.hash_algorithm != "sha256":
        raise ValueError("O nó suporta somente hash_algorithm=sha256.")
    store = ChunkStore(settings)
    reporter = report_corrupt or _make_reporter(settings)
    metrics = NodeMetrics()
    worker = ScrubWorker(store, reporter, settings.scrub_interval_s, metrics.record_scrub)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("node_started", node_id=settings.node_id, address=settings.address,
                 disks=len(store.disk_ids))
        worker.start()
        agent_task: asyncio.Task[Any] | None = None
        try:
            # O módulo do agent pertence à Pessoa C e pode ser incorporado depois.
            try:
                import importlib.util
                if importlib.util.find_spec("storage_node.agent") is not None:
                    from storage_node import agent
                    agent_task = asyncio.create_task(agent.run(settings, lambda: list_disks(settings)))
            except (ImportError, AttributeError, RuntimeError, ValueError) as exc:
                log.error("agent_start_failed", node_id=settings.node_id, error=str(exc))
            yield
        finally:
            if agent_task is not None:
                agent_task.cancel()
                try:
                    await agent_task
                except asyncio.CancelledError:
                    pass
            worker.stop()
            log.info("node_stopped", node_id=settings.node_id)

    app = FastAPI(title="DComp Drive — Nó de Armazenamento", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.metrics = metrics
    app.state.report_corrupt = reporter
    install_chunk_routes(app, settings, store, metrics, reporter)
    return app
