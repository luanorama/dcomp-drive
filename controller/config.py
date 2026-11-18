"""
controller/config.py
Carrega config.yaml e permite sobrescrita por variáveis de ambiente.
Qualquer componente importa `get_config()` para acessar os parâmetros.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


# Caminho padrão do arquivo de configuração; pode ser sobrescrito por
# CONFIG_PATH=... no ambiente.
_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _env_override(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Sobrescreve chaves do config com variáveis de ambiente mapeadas.
    Mantém o tipo original da chave (int, float, bool, str).
    """
    env_map: dict[str, tuple[str, type]] = {
        "REPLICATION_FACTOR":        ("replication_factor", int),
        "WRITE_QUORUM":              ("write_quorum", int),
        "CHUNK_SIZE_MB":             ("chunk_size_mb", int),
        "HEARTBEAT_INTERVAL_S":      ("heartbeat_interval_s", int),
        "SUSPECT_AFTER_S":           ("suspect_after_s", int),
        "DEAD_AFTER_S":              ("dead_after_s", int),
        "DISK_USAGE_LIMIT_PCT":      ("disk_usage_limit_pct", int),
        "LEASE_TTL_S":               ("lease_ttl_s", int),
        "REREPLICATION_MAX_CONCURRENT": ("rereplication_max_concurrent", int),
        "SCRUB_INTERVAL_S":          ("scrub_interval_s", int),
        "CONTROLLER_URL":            ("controller_url", str),
        "UNDER_REPLICATED_POLICY":   ("under_replicated_policy", str),
        "NODE_ID":                   ("node_id", str),
        "ENCRYPTION_ENABLED":        (None, None),  # tratamento especial
    }

    for env_key, (cfg_key, cast) in env_map.items():
        value = os.environ.get(env_key)
        if value is None:
            continue
        if env_key == "ENCRYPTION_ENABLED":
            cfg.setdefault("encryption", {})["enabled"] = value.lower() in ("1", "true", "yes")
        else:
            cfg[cfg_key] = cast(value)

    # DATA_DIRS é uma lista separada por ':'
    data_dirs_env = os.environ.get("DATA_DIRS")
    if data_dirs_env:
        cfg["data_dirs"] = [d.strip() for d in data_dirs_env.split(":") if d.strip()]

    return cfg


class Settings:
    """Configurações do sistema, lidas uma vez e compartilhadas."""

    def __init__(self, path: Path | None = None) -> None:
        config_path = path or Path(os.environ.get("CONFIG_PATH", _DEFAULT_CONFIG_PATH))
        raw = _load_yaml(config_path)
        raw = _env_override(raw)

        # Replicação
        self.replication_factor: int = int(raw.get("replication_factor", 3))
        self.write_quorum: int = int(raw.get("write_quorum", 2))
        self.under_replicated_policy: str = raw.get("under_replicated_policy", "accept")

        # Chunks
        self.chunk_size_mb: int = int(raw.get("chunk_size_mb", 4))
        self.chunk_size_bytes: int = self.chunk_size_mb * 1024 * 1024

        # Criptografia
        enc = raw.get("encryption", {})
        self.encryption_enabled: bool = bool(enc.get("enabled", False))
        self.encryption_algorithm: str = enc.get("algorithm", "aes-256-gcm")
        self.drive_key: str | None = os.environ.get("DRIVE_KEY")  # chave AES via env

        # Hash
        self.hash_algorithm: str = raw.get("hash_algorithm", "sha256")

        # Heartbeat
        self.heartbeat_interval_s: int = int(raw.get("heartbeat_interval_s", 3))
        self.suspect_after_s: int = int(raw.get("suspect_after_s", 6))
        self.dead_after_s: int = int(raw.get("dead_after_s", 10))

        # Espaço
        self.disk_usage_limit_pct: int = int(raw.get("disk_usage_limit_pct", 90))

        # Concorrência
        self.lease_ttl_s: int = int(raw.get("lease_ttl_s", 60))

        # Re-replicação
        self.rereplication_max_concurrent: int = int(
            raw.get("rereplication_max_concurrent", 2)
        )

        # Scrubbing
        self.scrub_interval_s: int = int(raw.get("scrub_interval_s", 300))

        # URLs
        self.controller_url: str = raw.get("controller_url", "http://controller:8000")

        # Nó (usado pelo storage_node)
        self.node_id: str | None = raw.get("node_id")
        self.data_dirs: list[str] = raw.get("data_dirs", [])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Settings(N={self.replication_factor}, W={self.write_quorum}, "
            f"chunk={self.chunk_size_mb}MB, enc={self.encryption_enabled})"
        )


@lru_cache(maxsize=1)
def get_config() -> Settings:
    """Retorna a instância singleton de Settings."""
    return Settings()
