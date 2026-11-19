"""Descoberta de espaço nos discos configurados."""
from __future__ import annotations

import shutil
from pathlib import Path

from storage_node.settings import NodeSettings


def list_disks(settings: NodeSettings | None = None) -> list[dict[str, int | str]]:
    """Devolve discos no formato esperado por `/nodes/register`."""
    settings = settings or NodeSettings.from_env()
    settings.validate()
    result: list[dict[str, int | str]] = []
    for root in settings.data_dirs:
        path = root.expanduser().resolve()
        path.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(path)
        result.append({
            "disk_id": f"{settings.node_id}/{path.name}",
            "path": str(path),
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
        })
    return result
