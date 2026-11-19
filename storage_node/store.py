"""Operações seguras de armazenamento local, sem dependência de HTTP."""
from __future__ import annotations

import errno
import hashlib
import os
import shutil
import uuid
from pathlib import Path

from common.hashing import sha256_hex, valid_chunk_id, valid_hash
from common.jsonlog import get_logger
from storage_node.settings import NodeSettings

log = get_logger("storage_node.store")


class StoreError(Exception):
    """Erro local de armazenamento com categoria útil para a API."""

    def __init__(self, message: str, *, status_code: int = 500) -> None:
        super().__init__(message)
        self.status_code = status_code


class ChunkStore:
    """Armazena chunks em pastas particionadas por prefixo UUID."""

    def __init__(self, settings: NodeSettings) -> None:
        settings.validate()
        self.settings = settings
        self.roots: dict[str, Path] = {}
        self._corrupt_roots: dict[str, Path] = {}
        for raw_root in settings.data_dirs:
            root = raw_root.expanduser().resolve()
            disk_id = f"{settings.node_id}/{root.name}"
            if disk_id in self.roots:
                raise ValueError(f"Identificador de disco duplicado: {disk_id}")
            self.roots[disk_id] = root
            (root / "chunks").mkdir(parents=True, exist_ok=True)
            temporary = root / "tmp"
            temporary.mkdir(parents=True, exist_ok=True)
            for path in temporary.iterdir():
                if path.is_file():
                    path.unlink(missing_ok=True)
            self._corrupt_roots[disk_id] = root / "chunks"

    @property
    def disk_ids(self) -> tuple[str, ...]:
        """Lista de chaves exatas aceitas pela API."""
        return tuple(self.roots)

    def _root(self, disk_id: str) -> Path:
        root = self.roots.get(disk_id)
        if root is None:
            raise StoreError("Disco desconhecido.", status_code=404)
        return root

    @staticmethod
    def _validate_chunk_id(chunk_id: str) -> None:
        if not valid_chunk_id(chunk_id):
            raise StoreError("chunk_id deve ser um UUID válido.", status_code=422)

    @staticmethod
    def _validate_hash(hash_value: str) -> None:
        if not valid_hash(hash_value):
            raise StoreError("hash deve conter 64 caracteres hexadecimais minúsculos.", status_code=422)

    def _chunk_dir(self, disk_id: str, chunk_id: str, *, create: bool = False) -> Path:
        self._validate_chunk_id(chunk_id)
        directory = self._root(disk_id) / "chunks" / chunk_id[:2]
        if create:
            directory.mkdir(parents=True, exist_ok=True)
        return directory

    def put(self, chunk_id: str, disk_id: str, expected_hash: str, data: bytes) -> dict[str, int | str]:
        """Grava e relê um chunk antes de confirmar a escrita."""
        self._validate_chunk_id(chunk_id)
        self._validate_hash(expected_hash)
        root = self._root(disk_id)
        actual_hash = sha256_hex(data)
        if actual_hash != expected_hash:
            raise StoreError("Hash recebido não confere.", status_code=422)
        try:
            if shutil.disk_usage(root).free < len(data):
                raise StoreError("Espaço insuficiente no disco.", status_code=507)
        except OSError as exc:
            raise StoreError(f"Não foi possível consultar o espaço do disco: {exc}") from exc

        directory = self._chunk_dir(disk_id, chunk_id, create=True)
        temp_path = root / "tmp" / f"{uuid.uuid4().hex}.part"
        destination = directory / f"{chunk_id}.{expected_hash}.chunk"
        try:
            with open(temp_path, "xb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            if self._hash_file(temp_path) != expected_hash:
                raise StoreError("O hash mudou após gravar o chunk.", status_code=422)
            for old in directory.glob(f"{chunk_id}.*.chunk"):
                if old != destination:
                    old.unlink(missing_ok=True)
            os.replace(temp_path, destination)
            try:
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except OSError as exc:
                # Em plataformas sem fsync de diretórios, o arquivo já foi confirmado.
                log.warning("directory_fsync_unavailable", disk_id=disk_id, error=str(exc))
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise StoreError("Espaço insuficiente no disco.", status_code=507) from exc
            raise StoreError(f"Falha ao gravar chunk: {exc}") from exc
        finally:
            temp_path.unlink(missing_ok=True)
        return {"hash": expected_hash, "size": len(data)}

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def find(self, chunk_id: str, disk_id: str) -> Path | None:
        """Localiza o único chunk visível sem aceitar caminhos externos."""
        directory = self._chunk_dir(disk_id, chunk_id)
        if not directory.is_dir():
            return None
        matches = sorted(directory.glob(f"{chunk_id}.*.chunk"))
        return matches[0] if matches else None

    def get(self, chunk_id: str, disk_id: str) -> tuple[bytes, str]:
        """Lê e valida o hash antes de devolver bytes."""
        self._validate_chunk_id(chunk_id)
        path = self.find(chunk_id, disk_id)
        if path is None:
            raise StoreError("Chunk não encontrado.", status_code=404)
        expected_hash = path.name[len(chunk_id) + 1 : -len(".chunk")]
        if not valid_hash(expected_hash):
            raise StoreError("Nome de chunk inválido.", status_code=422)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise StoreError("Chunk não encontrado.", status_code=404) from None
        actual_hash = sha256_hex(data)
        if actual_hash != expected_hash:
            self.quarantine_path(path)
            raise StoreError("Chunk corrompido: hash não confere.", status_code=422)
        return data, expected_hash

    def quarantine_path(self, path: Path) -> Path:
        """Retira um arquivo da listagem ativa e o move para quarentena."""
        corrupt = path.with_name(path.name + ".corrupt")
        corrupt.unlink(missing_ok=True)
        os.replace(path, corrupt)
        return corrupt

    def corrupt_files(self, disk_id: str | None = None) -> list[tuple[str, str, Path]]:
        """Lista arquivos em quarentena para uma nova tentativa de denúncia."""
        result: list[tuple[str, str, Path]] = []
        selected = [disk_id] if disk_id is not None else list(self.roots)
        for current in selected:
            root = self._root(current)
            chunks = root / "chunks"
            if not chunks.exists():
                continue
            for path in chunks.rglob("*.chunk.corrupt"):
                stem = path.name[:-len(".chunk.corrupt")]
                chunk_id, separator, hash_value = stem.partition(".")
                if separator and valid_chunk_id(chunk_id) and valid_hash(hash_value):
                    result.append((chunk_id, current, path))
        return result

    def delete(self, chunk_id: str, disk_id: str) -> bool:
        """Remove arquivos ativos e em quarentena; é idempotente."""
        self._validate_chunk_id(chunk_id)
        directory = self._chunk_dir(disk_id, chunk_id)
        deleted = False
        if directory.is_dir():
            for path in directory.glob(f"{chunk_id}.*.chunk*"):
                if path.is_file():
                    path.unlink(missing_ok=True)
                    deleted = True
        return deleted

    def list_chunks(self) -> list[dict[str, int | str]]:
        """Lista metadados extraídos dos nomes, sem reler os conteúdos."""
        result: list[dict[str, int | str]] = []
        for disk_id, root in self.roots.items():
            chunks = root / "chunks"
            if not chunks.exists():
                continue
            for path in chunks.rglob("*.chunk"):
                if path.name.endswith(".corrupt") or not path.is_file():
                    continue
                chunk_id, separator, remainder = path.name.partition(".")
                hash_value = remainder[:-len(".chunk")] if remainder.endswith(".chunk") else ""
                if separator and valid_chunk_id(chunk_id) and valid_hash(hash_value):
                    try:
                        size = path.stat().st_size
                    except FileNotFoundError:
                        continue
                    result.append({"chunk_id": chunk_id, "disk_id": disk_id,
                                   "size": size, "hash": hash_value})
        return result

    def chunk_count(self) -> int:
        """Conta chunks ativos em todos os discos."""
        return len(self.list_chunks())
