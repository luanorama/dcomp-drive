"""Logger JSON compacto, sem depender de campos reservados do logging."""
from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

_LOCK = threading.Lock()
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
_FILES: dict[str, TextIO] = {}


class JsonLogger:
    """Escreve eventos JSON de um componente com contexto fixo."""

    def __init__(self, component: str, context: dict[str, Any]) -> None:
        self.component = component
        self.context = context

    def _write(self, level: str, event: str, fields: dict[str, Any]) -> None:
        client_mode = self.component.startswith("client")
        minimum = os.environ.get("DCOMP_LOG_LEVEL", "WARNING" if client_mode else "INFO")
        destination = os.environ.get("DCOMP_LOG_FILE") if client_mode else None
        visible = _LEVELS.get(level, 20) >= _LEVELS.get(minimum.upper(), 20)
        if not visible and not destination:
            return
        row = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "component": self.component,
            "level": level.lower(),
            "event": event,
            **self.context,
            **fields,
        }
        line = json.dumps(row, ensure_ascii=False, default=str, separators=(",", ":"))
        with _LOCK:
            if visible:
                stream = sys.stderr if client_mode else sys.stdout
                stream.write(line + "\n")
                stream.flush()
            if destination:
                key = str(Path(destination).expanduser())
                handle = _FILES.get(key)
                if handle is None or handle.closed:
                    Path(key).parent.mkdir(parents=True, exist_ok=True)
                    handle = open(key, "a", encoding="utf-8")
                    _FILES[key] = handle
                handle.write(line + "\n")
                handle.flush()

    def debug(self, event: str, **fields: Any) -> None:
        self._write("DEBUG", event, fields)

    def info(self, event: str, **fields: Any) -> None:
        self._write("INFO", event, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._write("WARNING", event, fields)

    def error(self, event: str, **fields: Any) -> None:
        self._write("ERROR", event, fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._write("ERROR", event, fields)


def get_logger(component: str, **context: Any) -> JsonLogger:
    """Cria um logger estruturado com contexto constante."""
    return JsonLogger(component, context)
