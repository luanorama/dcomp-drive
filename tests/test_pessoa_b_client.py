"""Testes da CLI, criptografia e transferências com MockTransport."""
from __future__ import annotations

import base64
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from client.api import ApiError, ControllerClient, parse_error
from client.cli import main, validate_remote_path
from client.crypto import CryptoError, decrypt_chunk, encrypt_chunk, load_key
from client.transfer import TransferError, get_file, put_file
from common.hashing import sha256_hex


class MockCluster:
    """Controlador e nós simulados no mesmo transporte HTTP."""

    def __init__(self, *, quorum: int = 2, target_count: int = 3) -> None:
        self.quorum = quorum
        self.target_count = target_count
        self.allocations: list[dict[str, Any]] = []
        self.commits: list[dict[str, Any]] = []
        self.data: dict[tuple[str, str], bytes] = {}
        self.put_responses: dict[str, list[int]] = {}
        self.get_responses: dict[str, tuple[int, bytes]] = {}
        self.put_bodies: list[bytes] = []
        self.get_hosts: list[str] = []
        self.reports: list[dict[str, Any]] = []
        self.commit_state = "intact"
        self.fail_alloc_status: int | None = None
        self.fail_locate_status: int | None = None
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json={"ok": True, "write_quorum": self.quorum,
                                             "replication_factor": self.target_count})
        if path == "/files/allocate":
            if self.fail_alloc_status:
                return httpx.Response(self.fail_alloc_status,
                                      json={"detail": {"error": "allocate falhou"}})
            payload = json.loads(request.content)
            chunks = []
            for idx in range(payload["num_chunks"]):
                chunk_id = str(uuid.uuid4())
                chunks.append({"idx": idx, "chunk_id": chunk_id, "targets": [
                    {"node_url": f"http://node{target}", "disk_id": f"node{target}/disk1"}
                    for target in range(1, self.target_count + 1)
                ]})
            allocation = {"lease_id": f"lease-{len(self.allocations)}", "version": 1,
                          "warning": None, "chunks": chunks}
            self.allocations.append({"request": payload, "response": allocation})
            return httpx.Response(200, json=allocation)
        if path == "/files/commit":
            payload = json.loads(request.content)
            self.commits.append(payload)
            return httpx.Response(200, json={"state": self.commit_state})
        if path == "/files/locate":
            if self.fail_locate_status:
                status = self.fail_locate_status
                return httpx.Response(status, json={"detail": {"error": "locate falhou"}})
            if not self.allocations:
                return httpx.Response(404, json={"detail": {"error": "ausente"}})
            allocation = self.allocations[-1]
            committed = self.commits[-1] if self.commits else None
            if committed is None:
                return httpx.Response(404)
            chunks = []
            for chunk in committed["chunks"]:
                allocated = allocation["response"]["chunks"][chunk["idx"]]
                chunks.append({"idx": chunk["idx"], "chunk_id": chunk["chunk_id"],
                               "hash": chunk["hash"], "replicas": [
                    {"node_url": f"http://node{target}", "disk_id": f"node{target}/disk1"}
                    for target in range(1, self.target_count + 1)
                ]})
            return httpx.Response(200, json={
                "size": sum(chunk["size"] for chunk in committed["chunks"]),
                "file_hash": committed["file_hash"], "chunks": chunks,
            })
        if path == "/chunks/corrupt":
            self.reports.append(json.loads(request.content))
            return httpx.Response(200, json={"ok": True})
        if path.startswith("/chunks/") and request.method == "PUT":
            chunk_id = path.split("/")[-1]
            body = request.content
            self.put_bodies.append(body)
            sequence = self.put_responses.get(request.url.host, [])
            if sequence:
                status = sequence.pop(0)
                if status != 200:
                    return httpx.Response(status, json={"detail": {"error": "destino"}})
            chunk_hash = request.url.params.get("hash", "")
            self.data[(request.url.host, chunk_id)] = body
            return httpx.Response(200, json={"ok": True, "hash": chunk_hash, "size": len(body)})
        if path.startswith("/chunks/") and request.method == "GET":
            self.get_hosts.append(request.url.host)
            if request.url.host in self.get_responses:
                status, body = self.get_responses[request.url.host]
                return httpx.Response(status, content=body)
            chunk_id = path.split("/")[-1]
            body = self.data.get((request.url.host, chunk_id), b"")
            return httpx.Response(200, content=body)
        return httpx.Response(404, json={"detail": {"error": "não encontrado"}})

    def controller(self) -> ControllerClient:
        return ControllerClient("http://controller", transport=self.transport)


@pytest.mark.parametrize("size", [0, 1, 3, 4, 5])
@pytest.mark.parametrize("encrypted", [False, True])
def test_put_sizes_hash_and_exact_stored_size(tmp_path: Path, size: int,
                                               encrypted: bool) -> None:
    cluster = MockCluster()
    controller = cluster.controller()
    source = tmp_path / f"source-{size}-{encrypted}.bin"
    source.write_bytes(bytes((idx % 251 for idx in range(size))))
    key = "ab" * 32 if encrypted else None
    summary = put_file(source, f"/arquivo-{size}-{encrypted}", controller,
                       replication_factor=3, encrypted=encrypted,
                       key_value=key, chunk_size_bytes=4, transport=cluster.transport)
    allocation = cluster.allocations[0]
    commit = cluster.commits[0]
    n_chunks = max(1, (size + 3) // 4)
    assert allocation["request"]["num_chunks"] == n_chunks
    assert allocation["request"]["size"] == size + (36 * n_chunks if encrypted else 0)
    assert sum(item["size"] for item in commit["chunks"]) == allocation["request"]["size"]
    stored = b"".join(cluster.data[("node1", item["chunk_id"])] for item in commit["chunks"])
    assert commit["file_hash"] == hashlib.sha256(stored).hexdigest()
    assert summary["stored_bytes"] == len(stored)
    assert len(commit["chunks"]) == n_chunks
    controller.close()


def test_put_10_mib_uses_three_chunks(tmp_path: Path) -> None:
    cluster = MockCluster()
    controller = cluster.controller()
    source = tmp_path / "large.bin"
    source.write_bytes(b"x" * (10 * 1024 * 1024))
    summary = put_file(source, "/large.bin", controller, replication_factor=3,
                       chunk_size_bytes=4 * 1024 * 1024, transport=cluster.transport)
    assert summary["chunks"] == 3
    assert cluster.allocations[0]["request"]["size"] == 10 * 1024 * 1024
    assert len(cluster.commits[0]["chunks"]) == 3
    controller.close()


def test_quorum_failure_sends_no_commit(tmp_path: Path) -> None:
    cluster = MockCluster()
    cluster.put_responses = {"node1": [507, 507, 507], "node2": [507, 507, 507],
                             "node3": [507, 507, 507]}
    controller = cluster.controller()
    source = tmp_path / "one.bin"
    source.write_bytes(b"x")
    with pytest.raises(TransferError, match=r"0/2.*lease expira"):
        put_file(source, "/one.bin", controller, replication_factor=3,
                 chunk_size_bytes=4, transport=cluster.transport)
    assert cluster.commits == []
    controller.close()


def test_under_replicated_commit_is_success_with_warning(tmp_path: Path) -> None:
    cluster = MockCluster()
    cluster.commit_state = "under_replicated"
    cluster.put_responses = {"node3": [503, 503, 503]}
    controller = cluster.controller()
    source = tmp_path / "one.bin"
    source.write_bytes(b"ok")
    summary = put_file(source, "/one.bin", controller, replication_factor=3,
                       chunk_size_bytes=4, transport=cluster.transport)
    assert summary["state"] == "under_replicated"
    assert "SUB-REPLICADO" in summary["warning"]
    assert len(cluster.commits) == 1
    controller.close()


def test_retry_reuses_identical_encrypted_bytes(tmp_path: Path) -> None:
    cluster = MockCluster()
    cluster.put_responses = {"node1": [503, 200]}
    controller = cluster.controller()
    source = tmp_path / "retry.bin"
    source.write_bytes(b"retry me")
    put_file(source, "/retry.bin", controller, replication_factor=3,
             encrypted=True, key_value="12" * 32, chunk_size_bytes=32,
             transport=cluster.transport)
    assert cluster.put_bodies[0] == cluster.put_bodies[1]
    assert len(cluster.put_bodies[0]) == len(source.read_bytes()) + 36
    controller.close()


def test_get_uses_next_replica_and_reports_hash_mismatch(tmp_path: Path) -> None:
    cluster = MockCluster()
    controller = cluster.controller()
    chunk_id = str(uuid.uuid4())
    good = b"correct content"
    digest = sha256_hex(good)
    cluster.commits.append({"file_hash": digest, "chunks": [
        {"idx": 0, "chunk_id": chunk_id, "hash": digest, "size": len(good)}
    ]})
    cluster.allocations.append({"response": {"chunks": [{"idx": 0, "chunk_id": chunk_id}]}})
    cluster.get_responses = {"node1": (200, b"wrong content"), "node2": (200, good)}
    dest = tmp_path / "download.bin"
    result = get_file("/get.bin", dest, controller, transport=cluster.transport)
    assert result["bytes"] == len(good)
    assert dest.read_bytes() == good
    assert cluster.get_hosts == ["node1", "node2"]
    assert cluster.reports == [{"chunk_id": chunk_id, "disk_id": "node1/disk1"}]
    controller.close()


def test_get_all_bad_leaves_old_destination_and_no_part(tmp_path: Path) -> None:
    cluster = MockCluster()
    controller = cluster.controller()
    chunk_id = str(uuid.uuid4())
    good = b"expected"
    digest = sha256_hex(good)
    cluster.commits.append({"file_hash": digest, "chunks": [
        {"idx": 0, "chunk_id": chunk_id, "hash": digest, "size": len(good)}
    ]})
    cluster.allocations.append({"response": {"chunks": [{"idx": 0, "chunk_id": chunk_id}]}})
    cluster.get_responses = {f"node{i}": (200, b"bad") for i in range(1, 4)}
    dest = tmp_path / "existing.bin"
    dest.write_bytes(b"keep this")
    with pytest.raises(TransferError, match="irrecuperável"):
        get_file("/bad.bin", dest, controller, transport=cluster.transport)
    assert dest.read_bytes() == b"keep this"
    assert list(tmp_path.glob(".*.part-*")) == []
    assert len(cluster.reports) == 3
    controller.close()


def test_crypto_flags_wrong_key_and_aad(tmp_path: Path) -> None:
    key = load_key("34" * 32)
    assert len(load_key(base64.b64encode(key).decode("ascii"))) == 32
    chunk_a = str(uuid.uuid4())
    chunk_b = str(uuid.uuid4())
    cipher = encrypt_chunk(b"plain", chunk_a, 0, key)
    assert decrypt_chunk(cipher, chunk_a, 0, key) == b"plain"
    with pytest.raises(CryptoError, match="chave incorreta ou dado adulterado"):
        decrypt_chunk(cipher, chunk_a, 1, key)
    with pytest.raises(CryptoError, match="chave incorreta ou dado adulterado"):
        decrypt_chunk(cipher, chunk_b, 0, key)

    cluster = MockCluster()
    controller = cluster.controller()
    cid = str(uuid.uuid4())
    plain = b"not encrypted"
    digest = sha256_hex(plain)
    cluster.commits.append({"file_hash": digest, "chunks": [
        {"idx": 0, "chunk_id": cid, "hash": digest, "size": len(plain)}
    ]})
    cluster.allocations.append({"response": {"chunks": [{"idx": 0, "chunk_id": cid}]}})
    cluster.get_responses = {"node1": (200, plain)}
    with pytest.raises(TransferError, match="não está cifrado"):
        get_file("/plain", tmp_path / "plain", controller, encrypted=True,
                 key_value="34" * 32, transport=cluster.transport)
    controller.close()

    encrypted_cluster = MockCluster()
    encrypted_controller = encrypted_cluster.controller()
    encrypted_id = str(uuid.uuid4())
    encrypted_bytes = encrypt_chunk(b"ciphertext", encrypted_id, 0, key)
    encrypted_hash = sha256_hex(encrypted_bytes)
    encrypted_cluster.commits.append({"file_hash": encrypted_hash, "chunks": [
        {"idx": 0, "chunk_id": encrypted_id, "hash": encrypted_hash,
         "size": len(encrypted_bytes)}
    ]})
    encrypted_cluster.allocations.append({"response": {"chunks": [
        {"idx": 0, "chunk_id": encrypted_id}
    ]}})
    encrypted_cluster.get_responses = {"node1": (200, encrypted_bytes)}
    with pytest.raises(TransferError, match="Arquivo cifrado"):
        get_file("/cipher", tmp_path / "cipher-disabled", encrypted_controller,
                 encrypted=False, transport=encrypted_cluster.transport)
    encrypted_controller.close()


def test_crypto_errors_before_network_and_wrong_key_does_not_fallback(tmp_path: Path) -> None:
    cluster = MockCluster()
    controller = cluster.controller()
    source = tmp_path / "source"
    source.write_bytes(b"data")
    with pytest.raises(TransferError, match="DRIVE_KEY não definida"):
        put_file(source, "/encrypted", controller, encrypted=True, key_value="",
                 transport=cluster.transport)
    assert cluster.allocations == []
    controller.close()

    good_key = b"A" * 32
    cid = str(uuid.uuid4())
    stored = encrypt_chunk(b"secret", cid, 0, good_key)
    digest = sha256_hex(stored)
    cluster = MockCluster()
    controller = cluster.controller()
    cluster.commits.append({"file_hash": digest, "chunks": [
        {"idx": 0, "chunk_id": cid, "hash": digest, "size": len(stored)}
    ]})
    cluster.allocations.append({"response": {"chunks": [{"idx": 0, "chunk_id": cid}]}})
    cluster.get_responses = {"node1": (200, stored), "node2": (200, stored)}
    with pytest.raises(TransferError, match="chave incorreta ou dado adulterado"):
        get_file("/cipher", tmp_path / "cipher", controller, encrypted=True,
                 key_value=(b"B" * 32).hex(), transport=cluster.transport)
    assert cluster.get_hosts == ["node1"]
    controller.close()


@pytest.mark.parametrize("payload,expected", [
    ({"detail": {"error": "one"}}, "one"),
    ({"detail": [{"msg": "two"}]}, "two"),
    ({"error": "three"}, "three"),
])
def test_error_parser_json_shapes(payload: dict[str, Any], expected: str) -> None:
    response = httpx.Response(422, json=payload)
    assert parse_error(response) == expected


def test_error_parser_text_empty_and_path_validation(capsys: pytest.CaptureFixture[str]) -> None:
    assert parse_error(httpx.Response(500, text="erro simples")) == "erro simples"
    assert "vazia" in parse_error(httpx.Response(503))
    for path in ("/", "//a", "/a/", "relative"):
        with pytest.raises(ValueError):
            validate_remote_path(path)
    assert validate_remote_path("/", allow_root=True) == "/"
    assert validate_remote_path("/a") == "/a"
    assert main(["put", "/", "source.bin", "--controller", "http://127.0.0.1:1"]) == 2
    assert "caminho remoto inválido" in capsys.readouterr().err


@pytest.mark.parametrize("status,expected", [(404, "não encontrado"), (409, "versão confirmada"),
                                                (503, "indisponível")])
def test_get_controller_errors_have_clear_messages(tmp_path: Path, status: int,
                                                     expected: str) -> None:
    cluster = MockCluster()
    cluster.fail_locate_status = status
    controller = cluster.controller()
    with pytest.raises(TransferError, match=expected):
        get_file("/x", tmp_path / "x", controller, transport=cluster.transport)
    controller.close()
