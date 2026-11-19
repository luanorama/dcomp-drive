"""Validação e cálculo de hashes usados pelos componentes B."""
from __future__ import annotations

import hashlib
import re

UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def sha256_hex(data: bytes) -> str:
    """Retorna o SHA-256 em hexadecimal minúsculo."""
    return hashlib.sha256(data).hexdigest()


def valid_chunk_id(value: str) -> bool:
    """Confere o formato UUID exigido para um chunk."""
    return bool(UUID_PATTERN.fullmatch(value))


def valid_hash(value: str) -> bool:
    """Confere o formato hexadecimal de SHA-256."""
    return bool(HASH_PATTERN.fullmatch(value))
