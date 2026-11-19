"""Configuração e validação do nó de armazenamento."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from controller.config import get_config


@dataclass(frozen=True)
class NodeSettings:
    """Valores necessários para iniciar um nó."""

    node_id: str
    data_dirs: tuple[Path, ...]
    address: str
    controller_url: str
    scrub_interval_s: float = 300.0
    hash_algorithm: str = "sha256"
    heartbeat_interval_s: float = 3.0

    @classmethod
    def from_env(cls) -> "NodeSettings":
        """Lê as configurações globais e variáveis específicas do nó."""
        cfg = get_config()
        node_id = os.environ.get("NODE_ID") or cfg.node_id
        if not node_id:
            raise ValueError("NODE_ID é obrigatório para iniciar o nó.")
        raw_dirs = os.environ.get("DATA_DIRS")
        if raw_dirs:
            # Windows usa ';' pois ':' faz parte de caminhos como C:\\dados.
            separator = ";" if ";" in raw_dirs else ":"
            directories = [Path(item.strip()) for item in raw_dirs.split(separator) if item.strip()]
        else:
            directories = [Path(item) for item in cfg.data_dirs]
        if not directories:
            raise ValueError("DATA_DIRS deve indicar pelo menos uma pasta de armazenamento.")
        port = os.environ.get("NODE_PORT", "9000")
        address = os.environ.get("NODE_ADDRESS", f"http://{node_id}:{port}").rstrip("/")
        settings = cls(
            node_id=node_id,
            data_dirs=tuple(directories),
            address=address,
            controller_url=os.environ.get("CONTROLLER_URL", cfg.controller_url).rstrip("/"),
            scrub_interval_s=float(os.environ.get("SCRUB_INTERVAL_S", cfg.scrub_interval_s)),
            hash_algorithm=os.environ.get("HASH_ALGORITHM", cfg.hash_algorithm).lower(),
            heartbeat_interval_s=float(os.environ.get("HEARTBEAT_INTERVAL_S", cfg.heartbeat_interval_s)),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        """Rejeita configurações que impediriam integridade ou persistência."""
        if not self.node_id.strip():
            raise ValueError("NODE_ID não pode ser vazio.")
        if not self.data_dirs:
            raise ValueError("É necessário configurar pelo menos um disco.")
        if self.hash_algorithm != "sha256":
            raise ValueError("O nó suporta somente hash_algorithm=sha256.")
        if self.scrub_interval_s < 0 or self.heartbeat_interval_s <= 0:
            raise ValueError("Intervalos de scrubbing/heartbeat inválidos.")
        basenames = [path.expanduser().resolve().name for path in self.data_dirs]
        if len(basenames) != len(set(basenames)):
            raise ValueError("Discos com o mesmo nome de pasta não são permitidos.")

    def disk_ids(self) -> tuple[str, ...]:
        """Retorna os identificadores estáveis dos discos configurados."""
        return tuple(f"{self.node_id}/{path.expanduser().resolve().name}" for path in self.data_dirs)
