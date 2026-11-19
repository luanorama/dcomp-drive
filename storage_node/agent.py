"""Registro automático e sinais de vida do nó de armazenamento."""
from __future__ import annotations

import asyncio
import os
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from controller.config import get_config
from controller.logger import get_logger

log = get_logger("storage_node.agent")


def disk_usage(node_id, data_dirs, *, include_path=False):
    disks = []
    for directory in data_dirs:
        path = Path(directory)
        try:
            usage = shutil.disk_usage(path)
            total, used = usage.total, usage.used
        except OSError as error:
            log.warning("disk_unavailable", node_id=node_id, path=str(path), error=str(error))
            total, used = 0, 0
        disk = {"disk_id": f"{node_id}/{path.name}", "total_bytes": total, "used_bytes": used}
        if include_path:
            disk["path"] = str(path)
        disks.append(disk)
    return disks


async def send_heartbeats(client, cfg, address):
    node_id = cfg.node_id
    controller = cfg.controller_url.rstrip("/")
    registered = False
    while True:
        try:
            if not registered:
                disks = await asyncio.to_thread(disk_usage, node_id, cfg.data_dirs, include_path=True)
                response = await client.post(f"{controller}/nodes/register", json={
                    "node_id": node_id, "address": address, "disks": disks,
                })
                response.raise_for_status()
                registered = True
                log.info("registered", node_id=node_id, controller_url=controller)
            else:
                disks = await asyncio.to_thread(disk_usage, node_id, cfg.data_dirs)
                response = await client.post(f"{controller}/nodes/{node_id}/heartbeat",
                                             json={"disks": disks})
                if response.status_code == 404:
                    registered = False
                    continue
                response.raise_for_status()
        except httpx.HTTPError as error:
            log.warning("heartbeat_failed", node_id=node_id, error=str(error))
        await asyncio.sleep(cfg.heartbeat_interval_s)


def attach_agent(app):
    """Mantém o lifespan do armazenamento e acrescenta o agente de cluster."""
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        cfg = get_config()
        if not cfg.node_id or not cfg.data_dirs:
            raise ValueError("Defina NODE_ID e DATA_DIRS para iniciar o nó.")
        if len({Path(path).name for path in cfg.data_dirs}) != len(cfg.data_dirs):
            raise ValueError("As pastas de dados precisam ter nomes diferentes por nó.")
        address = os.environ.get("NODE_ADDRESS", f"http://{cfg.node_id}:9000")
        async with original_lifespan(application) as state:
            async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
                task = asyncio.create_task(send_heartbeats(client, cfg, address))
                try:
                    yield state
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    app.router.lifespan_context = lifespan
