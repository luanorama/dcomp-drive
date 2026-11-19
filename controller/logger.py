"""
controller/logger.py
Logger JSON estruturado para o controlador (RF12).
Uso:
    from controller.logger import get_logger
    log = get_logger("files")
    log.info("file_allocated", path="/foo/bar.txt", lease_id="...")
"""
from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Formata cada linha de log como um objeto JSON com campos padrão."""

    def format(self, record: logging.LogRecord) -> str:
        # Campos obrigatórios (RF12)
        payload: dict[str, Any] = {
            "ts": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)
            ),
            "component": record.name,
            "level": record.levelname,
            "event": record.getMessage(),
        }

        # Campos extras passados via LogRecord.extra (ex: file_id, chunk_id)
        for key in ("file_id", "chunk_id", "node_id", "disk_id", "path", "lease_id"):
            val = getattr(record, key, None)
            if val is not None:
                payload[key] = val

        # Qualquer outro campo extra adicionado pelo chamador
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


class _ContextLogger(logging.LoggerAdapter):
    """
    Wrapper que injeta campos de contexto no LogRecord para o JsonFormatter
    capturá-los sem precisar de threading.local.
    """

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:  # type: ignore[override]
        extra = kwargs.setdefault("extra", {})
        # Mescla contexto persistente do adapter com extras pontuais
        extra.update(self.extra)
        return msg, kwargs

    # Atalho: log.info("event", path=..., chunk_id=...)
    def _log_with_fields(
        self, level: int, event: str, exc_info: bool = False, **fields: Any
    ) -> None:
        # 'exc_info' é um atributo nativo do LogRecord — não pode entrar no dict extra
        extra = {"extra_fields": {**self.extra, **fields}}
        self.logger.log(level, event, extra=extra, exc_info=exc_info)

    def debug(self, event: str, **fields: Any) -> None:  # type: ignore[override]
        self._log_with_fields(logging.DEBUG, event, **fields)

    def info(self, event: str, **fields: Any) -> None:  # type: ignore[override]
        self._log_with_fields(logging.INFO, event, **fields)

    def warning(self, event: str, **fields: Any) -> None:  # type: ignore[override]
        self._log_with_fields(logging.WARNING, event, **fields)

    def error(self, event: str, **fields: Any) -> None:  # type: ignore[override]
        self._log_with_fields(logging.ERROR, event, **fields)

    def exception(self, event: str, **fields: Any) -> None:  # type: ignore[override]
        # exc_info=True captura o traceback atual — não passa como campo extra
        self._log_with_fields(logging.ERROR, event, exc_info=True, **fields)


def _setup_root_logger() -> None:
    root = logging.getLogger()
    if root.handlers:
        return  # já configurado
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


def get_logger(component: str, **context: Any) -> _ContextLogger:
    """
    Retorna um logger JSON estruturado identificado por `component`.
    Os `context` kwargs são incluídos em todos os logs emitidos por ele.

    Exemplo:
        log = get_logger("controller.files", path="/docs/readme.txt")
        log.info("file_allocated", lease_id="abc123", num_chunks=3)
    """
    _setup_root_logger()
    base = logging.getLogger(component)
    return _ContextLogger(base, context)
