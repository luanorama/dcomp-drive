"""Cifra de chunks no cliente usando AES-256-GCM."""
from __future__ import annotations

import base64
import binascii
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"DCDRIVE1"
NONCE_SIZE = 12
TAG_SIZE = 16
OVERHEAD = len(MAGIC) + NONCE_SIZE + TAG_SIZE


class CryptoError(ValueError):
    """Erro de configuração ou autenticação de conteúdo cifrado."""


def load_key(value: str | None = None) -> bytes:
    """Lê DRIVE_KEY em hexadecimal de 64 caracteres ou base64."""
    value = os.environ.get("DRIVE_KEY") if value is None else value
    if not value:
        raise CryptoError(
            "DRIVE_KEY não definida. Gere uma chave com: "
            "python3 -c \"import os;print(os.urandom(32).hex())\""
        )
    key: bytes
    if len(value) == 64:
        try:
            key = bytes.fromhex(value)
        except ValueError:
            key = b""
        if len(key) == 32:
            return key
    try:
        key = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        try:
            padded = value + "=" * (-len(value) % 4)
            key = base64.urlsafe_b64decode(padded)
        except (binascii.Error, ValueError) as exc:
            raise CryptoError("DRIVE_KEY deve ser uma chave de 32 bytes em hex ou base64.") from exc
    if len(key) != 32:
        raise CryptoError("DRIVE_KEY decodificada deve conter exatamente 32 bytes.")
    return key


def validate_algorithm(encryption_enabled: bool, algorithm: str) -> None:
    """Falha cedo se a configuração pedir cifra incompatível."""
    if encryption_enabled and algorithm.lower() != "aes-256-gcm":
        raise CryptoError("A criptografia habilitada exige encryption.algorithm=aes-256-gcm.")


def stored_size(plain_size: int, num_chunks: int, encryption_enabled: bool) -> int:
    """Calcula o tamanho enviado ao controlador, incluindo nonce e tag."""
    if plain_size < 0 or num_chunks < 1:
        raise ValueError("Tamanho original ou número de chunks inválido.")
    return plain_size + (OVERHEAD * num_chunks if encryption_enabled else 0)


def encrypt_chunk(plain: bytes, chunk_id: str, idx: int, key: bytes) -> bytes:
    """Cifra um chunk com nonce novo e AAD vinculada à sua posição."""
    nonce = os.urandom(NONCE_SIZE)
    aad = f"{chunk_id}:{idx}".encode("utf-8")
    encrypted = AESGCM(key).encrypt(nonce, plain, aad)
    return MAGIC + nonce + encrypted


def decrypt_chunk(stored: bytes, chunk_id: str, idx: int, key: bytes) -> bytes:
    """Valida o formato e autentica o chunk antes de devolver texto claro."""
    if not stored.startswith(MAGIC):
        raise CryptoError("Arquivo não está cifrado; desative encryption.enabled para lê-lo.")
    if len(stored) < OVERHEAD:
        raise CryptoError("Chunk cifrado incompleto.")
    nonce = stored[len(MAGIC):len(MAGIC) + NONCE_SIZE]
    ciphertext = stored[len(MAGIC) + NONCE_SIZE:]
    aad = f"{chunk_id}:{idx}".encode("utf-8")
    try:
        return AESGCM(key).decrypt(nonce, ciphertext, aad)
    except InvalidTag as exc:
        raise CryptoError("chave incorreta ou dado adulterado") from exc


def ensure_plain_chunk(stored: bytes) -> None:
    """Recusa conteúdo cifrado quando a opção de cifra está desligada."""
    if stored.startswith(MAGIC):
        raise CryptoError("Arquivo cifrado; habilite encryption.enabled e configure DRIVE_KEY.")
