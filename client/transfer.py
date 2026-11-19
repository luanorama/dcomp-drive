"""Transferência de chunks entre cliente, controlador e nós."""
from __future__ import annotations

import hashlib
import httpx
import os
import socket
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from common.hashing import sha256_hex, valid_hash
from common.jsonlog import get_logger
from controller.config import get_config
from client.api import ApiError, ControllerClient, make_http, parse_error
from client.crypto import (CryptoError, OVERHEAD, decrypt_chunk, encrypt_chunk,
                           ensure_plain_chunk, load_key, validate_algorithm)

log = get_logger("client.transfer")


class TransferError(Exception):
    """Falha operacional que pode ser exibida sem traceback."""


def _settings(encrypted: bool | None, algorithm: str | None,
              key_value: str | None) -> tuple[int, bool, str, bytes | None, int, int]:
    cfg = get_config()
    if cfg.hash_algorithm.lower() != "sha256":
        raise TransferError("O cliente suporta somente hash_algorithm=sha256.")
    chunk_size = int(cfg.chunk_size_bytes)
    enabled = cfg.encryption_enabled if encrypted is None else encrypted
    cipher = algorithm or cfg.encryption_algorithm
    try:
        validate_algorithm(enabled, cipher)
        key = load_key(key_value) if enabled else None
    except CryptoError as exc:
        raise TransferError(str(exc)) from exc
    return chunk_size, enabled, cipher, key, int(cfg.write_quorum), int(cfg.replication_factor)


def _api_message(exc: ApiError, operation: str, *, lease_ttl_s: int) -> str:
    if operation == "locate":
        if exc.status == 404:
            return "Arquivo não encontrado no DComp Drive."
        if exc.status == 409:
            return "Arquivo ainda não possui uma versão confirmada."
        if exc.status == 503:
            return f"Arquivo indisponível: {exc.message}"
    if exc.status == 409 and operation == "commit":
        return (f"{exc.message} Lease expirou (TTL = {lease_ttl_s} s); para arquivos grandes, "
                "aumente LEASE_TTL_S no controlador.")
    if exc.status == 409 and operation == "allocate":
        return f"Outro escritor está gravando este caminho: {exc.message}"
    if exc.status == 404 and operation == "stat":
        return "Arquivo não encontrado."
    if exc.status == 404 and operation == "rm":
        return "Arquivo não encontrado; para remover um diretório use rmdir."
    return exc.message


def _send_one_target(target: dict[str, Any], chunk_id: str, data: bytes,
                     chunk_hash: str, transport: httpx.BaseTransport | None
                     ) -> tuple[str, str | None, float]:
    """Envia bytes idênticos ao destino, com retentativas limitadas."""
    disk_id = str(target["disk_id"])
    url = str(target["node_url"]).rstrip("/") + f"/chunks/{chunk_id}"
    last_error = "falha desconhecida"
    started = time.perf_counter()
    transient_retries = 0
    retried_422 = False
    retry_count = 0
    with make_http(transport, control=False) as client:
        for _ in range(4):
            try:
                response = client.put(
                    url, params={"disk_id": disk_id, "hash": chunk_hash},
                    content=data,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=httpx.Timeout(60.0, connect=5.0),
                )
                if response.status_code == 200:
                    try:
                        confirmed_hash = response.json().get("hash")
                    except ValueError:
                        confirmed_hash = None
                    if confirmed_hash == chunk_hash:
                        return disk_id, None, (time.perf_counter() - started) * 1000
                    last_error = "confirmação do nó contém hash divergente"
                    retry = not retried_422
                    retried_422 = True
                elif response.status_code == 422:
                    last_error = parse_error(response)
                    retry = not retried_422
                    retried_422 = True
                elif response.status_code >= 500:
                    last_error = f"HTTP {response.status_code}: {parse_error(response)}"
                    retry = transient_retries < 2
                    if retry:
                        transient_retries += 1
                else:
                    last_error = f"HTTP {response.status_code}: {parse_error(response)}"
                    retry = False
            except httpx.HTTPError as exc:
                last_error = str(exc)
                retry = transient_retries < 2
                if retry:
                    transient_retries += 1
            if not retry:
                break
            time.sleep(0.3 * (2 ** retry_count))
            retry_count += 1
    return disk_id, last_error, (time.perf_counter() - started) * 1000


def put_file(local_path: str | Path, remote_path: str, controller: ControllerClient,
             *, transport: httpx.BaseTransport | None = None,
             replication_factor: int | None = None, encrypted: bool | None = None,
             encryption_algorithm: str | None = None, key_value: str | None = None,
             chunk_size_bytes: int | None = None) -> dict[str, Any]:
    """Divide, opcionalmente cifra, replica e confirma um arquivo."""
    cfg = get_config()
    configured_chunk, encrypt_enabled, _, key, configured_w, configured_n = _settings(
        encrypted, encryption_algorithm, key_value,
    )
    chunk_size = int(chunk_size_bytes or configured_chunk)
    if chunk_size <= 0:
        raise TransferError("chunk_size_bytes precisa ser maior que zero.")
    path = Path(local_path).expanduser()
    try:
        before = path.stat()
        if not path.is_file():
            raise TransferError("O arquivo local precisa ser um arquivo regular.")
        stream = open(path, "rb")
    except OSError as exc:
        raise TransferError(f"Não foi possível ler o arquivo local: {exc}") from exc
    size = before.st_size
    num_chunks = max(1, (size + chunk_size - 1) // chunk_size)
    stored_total = size + (OVERHEAD * num_chunks if encrypt_enabled else 0)
    started = time.perf_counter()
    log.info("put_started", path=remote_path, bytes=size, chunks=num_chunks)
    allocated = False
    try:
        try:
            health = controller.health()
            write_quorum = int(health.get("write_quorum", configured_w))
        except (ApiError, TypeError, ValueError) as exc:
            write_quorum = configured_w
            log.warning("write_quorum_fallback", write_quorum=write_quorum,
                        error=str(exc))
        if write_quorum < 1:
            raise TransferError("O quórum W informado pelo controlador é inválido.")
        desired_n = replication_factor if replication_factor is not None else configured_n
        if desired_n < write_quorum:
            raise TransferError(f"Nível de réplicas ({desired_n}) não pode ser menor que W ({write_quorum}).")
        payload: dict[str, Any] = {
            "path": remote_path, "size": stored_total, "num_chunks": num_chunks,
            "client_id": f"{socket.gethostname()}:{os.getpid()}",
        }
        if replication_factor is not None:
            payload["replication_factor"] = replication_factor
        try:
            allocation = controller.allocate(payload)
        except ApiError as exc:
            raise TransferError(_api_message(exc, "allocate", lease_ttl_s=cfg.lease_ttl_s)) from exc
        allocated = True
        lease_id = allocation["lease_id"]
        plan = {int(item["idx"]): item for item in allocation["chunks"]}
        if set(plan) != set(range(num_chunks)):
            raise TransferError("O controlador devolveu uma alocação incompleta; o lease expirará sozinho.")
        warning = allocation.get("warning")
        if warning:
            log.warning("allocation_warning", path=remote_path, warning=warning)

        file_hasher = hashlib.sha256()
        commit_chunks: list[dict[str, Any]] = []
        bytes_read = 0
        with stream:
            for idx in range(num_chunks):
                chunk_info = plan[idx]
                chunk_id = str(chunk_info["chunk_id"])
                expected_plain = max(0, min(chunk_size, size - idx * chunk_size))
                plain = stream.read(expected_plain) if expected_plain else b""
                if len(plain) != expected_plain:
                    raise TransferError("O arquivo local mudou durante a leitura; lease expirará sozinho.")
                bytes_read += len(plain)
                stored = encrypt_chunk(plain, chunk_id, idx, key) if encrypt_enabled and key is not None else plain
                chunk_hash = sha256_hex(stored)
                file_hasher.update(stored)
                targets = list(chunk_info.get("targets", []))
                outcomes: list[tuple[str, str | None, float]] = []
                if targets:
                    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
                        futures = [pool.submit(_send_one_target, target, chunk_id,
                                               stored, chunk_hash, transport)
                                   for target in targets]
                        for future in as_completed(futures):
                            outcomes.append(future.result())
                confirmed = [disk for disk, error, _ in outcomes if error is None]
                failed = [(disk, error, duration_ms) for disk, error, duration_ms in outcomes
                          if error is not None]
                for disk, error, duration_ms in failed:
                    log.warning("chunk_upload_failed", chunk_id=chunk_id,
                                disk_id=disk, error=error, duration_ms=duration_ms)
                for disk, error, duration_ms in outcomes:
                    if error is not None:
                        continue
                    log.info("chunk_uploaded", chunk_id=chunk_id, disk_id=disk,
                             size=len(stored), duration_ms=duration_ms)
                if len(confirmed) < write_quorum:
                    raise TransferError(
                        f"Chunk {idx}: somente {len(confirmed)}/{write_quorum} réplicas confirmaram. "
                        f"O lease expira sozinho após {cfg.lease_ttl_s} s; nenhum commit foi enviado."
                    )
                commit_chunks.append({
                    "idx": idx, "chunk_id": chunk_id, "hash": chunk_hash,
                    "size": len(stored), "confirmed": confirmed,
                })
            if stream.read(1):
                raise TransferError("O arquivo local cresceu durante a leitura; lease expirará sozinho.")
        after = path.stat()
        if bytes_read != size or after.st_size != before.st_size or after.st_mtime_ns != before.st_mtime_ns:
            raise TransferError("O arquivo local mudou durante a leitura; lease expirará sozinho.")
        if sum(int(item["size"]) for item in commit_chunks) != stored_total:
            raise TransferError("Tamanho dos chunks enviados diverge da alocação; lease expirará sozinho.")
        try:
            committed = controller.commit({
                "path": remote_path, "lease_id": lease_id,
                "file_hash": file_hasher.hexdigest(), "chunks": commit_chunks,
            })
        except ApiError as exc:
            raise TransferError(_api_message(exc, "commit", lease_ttl_s=cfg.lease_ttl_s)) from exc
        duration_s = max(time.perf_counter() - started, 1e-9)
        replicas_min = min(len(item["confirmed"]) for item in commit_chunks)
        summary = {
            "path": remote_path, "bytes": size, "stored_bytes": stored_total,
            "chunks": num_chunks, "file_hash": file_hasher.hexdigest(),
            "state": committed["state"], "replicas_min": replicas_min,
            "replication_factor": desired_n, "duration_s": duration_s,
            "throughput_mb_s": size / (1024 * 1024) / duration_s,
            "warning": warning,
        }
        warnings = [str(warning)] if warning else []
        if committed["state"] == "under_replicated":
            warnings.append(
                f"gravado SUB-REPLICADO ({replicas_min}/{desired_n} réplicas); "
                "a recuperação automática completará"
            )
        summary["warning"] = "\n".join(warnings) or None
        log.info("put_done", **summary)
        return summary
    except (TransferError, ApiError) as exc:
        log.warning("op_failed", op="put", path=remote_path, error=str(exc))
        raise
    except OSError as exc:
        log.error("op_failed", op="put", path=remote_path, error=str(exc))
        raise TransferError(f"Falha ao ler o arquivo local: {exc}") from exc
    finally:
        if not stream.closed:
            stream.close()
        # O controlador limpa alocações abandonadas quando o lease expira.
        if allocated:
            log.debug("lease_cleanup_deferred", path=remote_path)


def _report_bad_replica(controller: ControllerClient, chunk_id: str,
                        disk_id: str) -> None:
    """Denuncia corrupção sem interromper o fallback para outra réplica."""
    try:
        controller.report_corrupt(chunk_id, disk_id)
        log.warning("corrupt_reported", chunk_id=chunk_id, disk_id=disk_id)
    except ApiError as exc:
        log.warning("corrupt_report_failed", chunk_id=chunk_id, disk_id=disk_id,
                    status=exc.status, error=exc.message)


def get_file(remote_path: str, destination: str | Path, controller: ControllerClient,
             *, transport: httpx.BaseTransport | None = None,
             encrypted: bool | None = None, encryption_algorithm: str | None = None,
             key_value: str | None = None) -> dict[str, Any]:
    """Baixa e valida todos os chunks antes de substituir o destino."""
    cfg = get_config()
    _, decrypt_enabled, _, key, _, _ = _settings(encrypted, encryption_algorithm, key_value)
    dest = Path(destination).expanduser()
    if dest.exists() and dest.is_dir():
        raise TransferError("O destino de get é um diretório.")
    parent = dest.parent if str(dest.parent) else Path(".")
    if not parent.is_dir() or not os.access(parent, os.W_OK):
        raise TransferError("A pasta de destino não existe ou não permite gravação.")
    try:
        located = controller.locate(remote_path)
    except ApiError as exc:
        raise TransferError(_api_message(exc, "locate", lease_ttl_s=cfg.lease_ttl_s)) from exc
    chunks = sorted(located.get("chunks", []), key=lambda item: int(item.get("idx", -1)))
    if not chunks:
        raise TransferError("O controlador não devolveu chunks para este arquivo.")

    file_hasher = hashlib.sha256()
    stored_total = 0
    plain_total = 0
    started = time.perf_counter()
    log.info("get_started", path=remote_path, chunks=len(chunks))
    fd, temporary_name = tempfile.mkstemp(prefix=f".{dest.name}.part-", dir=str(parent))
    temp_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as output:
            for chunk in chunks:
                idx = int(chunk["idx"])
                chunk_id = str(chunk["chunk_id"])
                expected_hash = str(chunk.get("hash", ""))
                if not valid_hash(expected_hash):
                    raise TransferError(f"Hash inválido nos metadados do chunk {idx}.")
                replicas = list(chunk.get("replicas", []))
                stored: bytes | None = None
                attempted = 0
                for replica in replicas:
                    attempted += 1
                    disk_id = str(replica["disk_id"])
                    url = str(replica["node_url"]).rstrip("/") + f"/chunks/{chunk_id}"
                    replica_reason = "réplica indisponível"
                    received: bytes | None = None
                    with make_http(transport, control=False) as node_client:
                        for attempt in range(3):
                            try:
                                response = node_client.get(
                                    url, params={"disk_id": disk_id},
                                    timeout=httpx.Timeout(60.0, connect=5.0),
                                )
                            except httpx.HTTPError as exc:
                                replica_reason = str(exc)
                                if attempt < 2:
                                    time.sleep(0.3 * (2 ** attempt))
                                    continue
                                break
                            if response.status_code == 200:
                                received = response.content
                                break
                            if response.status_code == 422:
                                replica_reason = parse_error(response)
                                _report_bad_replica(controller, chunk_id, disk_id)
                                break
                            if response.status_code >= 500:
                                replica_reason = f"HTTP {response.status_code}"
                                if attempt < 2:
                                    time.sleep(0.3 * (2 ** attempt))
                                    continue
                                break
                            replica_reason = f"HTTP {response.status_code}: {parse_error(response)}"
                            break
                    if received is None:
                        log.warning("replica_read_failed", chunk_id=chunk_id,
                                    disk_id=disk_id, reason=replica_reason)
                        continue
                    if sha256_hex(received) != expected_hash:
                        _report_bad_replica(controller, chunk_id, disk_id)
                        log.warning("replica_read_failed", chunk_id=chunk_id,
                                    disk_id=disk_id, reason="hash divergente")
                        continue
                    stored = received
                    break
                if stored is None:
                    raise TransferError(
                        f"Chunk idx {idx} irrecuperável: tentadas {attempted} réplicas."
                    )
                file_hasher.update(stored)
                stored_total += len(stored)
                if decrypt_enabled:
                    if key is None:
                        raise TransferError("DRIVE_KEY não está disponível.")
                    try:
                        plain = decrypt_chunk(stored, chunk_id, idx, key)
                    except CryptoError as exc:
                        # Hash já conferiu: erro GCM é de chave/dado, não de réplica.
                        raise TransferError(str(exc)) from exc
                else:
                    try:
                        ensure_plain_chunk(stored)
                    except CryptoError as exc:
                        raise TransferError(str(exc)) from exc
                    plain = stored
                output.write(plain)
                plain_total += len(plain)
            output.flush()
            os.fsync(output.fileno())
        expected_file_hash = str(located.get("file_hash") or "")
        if expected_file_hash and file_hasher.hexdigest() != expected_file_hash:
            raise TransferError("Hash do arquivo completo não confere; destino não foi alterado.")
        if not expected_file_hash:
            log.warning("file_hash_missing", path=remote_path)
        expected_size = int(located.get("size", stored_total))
        if stored_total != expected_size:
            raise TransferError("Tamanho armazenado difere dos metadados; destino não foi alterado.")
        os.replace(temp_path, dest)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    duration_s = max(time.perf_counter() - started, 1e-9)
    result = {
        "path": remote_path, "destination": str(dest), "bytes": plain_total,
        "stored_bytes": stored_total, "chunks": len(chunks),
        "file_hash": file_hasher.hexdigest(), "state": "ok",
        "replicas_min": min((len(item.get("replicas", [])) for item in chunks), default=0),
        "duration_s": duration_s,
        "throughput_mb_s": plain_total / (1024 * 1024) / duration_s,
    }
    log.info("get_done", **result)
    return result
