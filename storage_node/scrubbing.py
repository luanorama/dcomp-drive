"""Verificação periódica de integridade e denúncia de chunks corrompidos."""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable

from common.jsonlog import get_logger
from storage_node.store import ChunkStore

log = get_logger("storage_node.scrubbing")
ReportCorrupt = Callable[[str, str], object]


def _report_status(report_corrupt: ReportCorrupt, chunk_id: str, disk_id: str) -> int | None:
    """Normaliza a resposta do controlador; falhas mantêm a quarentena."""
    try:
        result = report_corrupt(chunk_id, disk_id)
    except (OSError, TimeoutError, ValueError) as exc:
        log.warning("corrupt_report_failed", chunk_id=chunk_id, disk_id=disk_id,
                    error=str(exc))
        return None
    if result is None:
        return 200
    if isinstance(result, bool):
        return 200 if result else 503
    if isinstance(result, int):
        return result
    status = getattr(result, "status_code", None)
    return int(status) if isinstance(status, int) else None


def _finish_quarantine(path: Path, status: int | None) -> None:
    """Descarta o arquivo em quarentena só após resposta terminal."""
    if status == 404 or (status is not None and 200 <= status < 300):
        path.unlink(missing_ok=True)


def scrub_once(store: ChunkStore, report_corrupt: ReportCorrupt) -> dict[str, object]:
    """Verifica todos os chunks em blocos de 1 MiB e reenvia denúncias pendentes."""
    started = time.perf_counter()
    checked = 0
    corrupt: list[dict[str, str]] = []

    for chunk_id, disk_id, path in store.corrupt_files():
        status = _report_status(report_corrupt, chunk_id, disk_id)
        log.info("corrupt_reported", chunk_id=chunk_id, disk_id=disk_id, status=status)
        _finish_quarantine(path, status)

    for item in store.list_chunks():
        chunk_id = str(item["chunk_id"])
        disk_id = str(item["disk_id"])
        path = store.find(chunk_id, disk_id)
        if path is None:
            continue
        checked += 1
        started_hash = time.perf_counter()
        try:
            actual_hash = store._hash_file(path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            log.error("scrub_io_failed", chunk_id=chunk_id, disk_id=disk_id,
                      error=str(exc))
            continue
        expected_hash = str(item["hash"])
        if actual_hash == expected_hash:
            continue
        try:
            quarantined = store.quarantine_path(path)
        except FileNotFoundError:
            continue
        duration_s = time.perf_counter() - started_hash
        corrupt.append({"chunk_id": chunk_id, "disk_id": disk_id})
        log.warning("scrub_corrupt_detected", chunk_id=chunk_id, disk_id=disk_id,
                    size=item["size"], duration_s=duration_s)
        status = _report_status(report_corrupt, chunk_id, disk_id)
        log.info("corrupt_reported", chunk_id=chunk_id, disk_id=disk_id, status=status)
        _finish_quarantine(quarantined, status)

    duration_s = time.perf_counter() - started
    return {"checked": checked, "corrupt": corrupt, "duration_s": duration_s}


class ScrubWorker:
    """Executa scrubbing em thread daemon, fora do event loop."""

    def __init__(self, store: ChunkStore, report_corrupt: ReportCorrupt,
                 interval_s: float, on_result: Callable[[dict[str, object]], None]) -> None:
        self.store = store
        self.report_corrupt = report_corrupt
        self.interval_s = interval_s
        self.on_result = on_result
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Inicia uma thread se o intervalo for positivo."""
        if self.interval_s <= 0 or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="dcomp-scrubber", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Sinaliza parada e aguarda a passada corrente."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_s + 1.0))
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            log.info("scrub_started")
            try:
                result = scrub_once(self.store, self.report_corrupt)
                self.on_result(result)
                log.info("scrub_finished", **result)
            except (OSError, ValueError, RuntimeError) as exc:
                log.exception("scrub_failed", error=str(exc))
