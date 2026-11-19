"""Testes do núcleo do nó de armazenamento da Pessoa B."""
from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import uuid

from fastapi.testclient import TestClient

from common.hashing import sha256_hex
from storage_node.app import create_app
from storage_node.disks import list_disks
from storage_node.scrubbing import scrub_once
from storage_node.settings import NodeSettings
from storage_node.store import ChunkStore


def settings_for(root: Path, *, node: str = "node1", interval: float = 0) -> NodeSettings:
    return NodeSettings(node, (root / "disk1",), "http://127.0.0.1:9000",
                        "http://127.0.0.1:8000", interval)


def put(client: TestClient, chunk_id: str, body: bytes, disk_id: str = "node1/disk1"):
    return client.put(
        f"/chunks/{chunk_id}", params={"disk_id": disk_id, "hash": sha256_hex(body)},
        content=body, headers={"Content-Type": "application/octet-stream"},
    )


def test_put_get_idempotent_and_empty_chunk(tmp_path: Path) -> None:
    app = create_app(settings_for(tmp_path), report_corrupt=lambda *_: 200)
    with TestClient(app) as client:
        chunk_id = str(uuid.uuid4())
        body = b"conteudo de teste"
        for _ in range(2):
            response = put(client, chunk_id, body)
            assert response.status_code == 200, response.text
            assert response.json() == {"ok": True, "hash": sha256_hex(body), "size": len(body)}
        result = client.get(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"})
        assert result.status_code == 200
        assert result.content == body
        assert result.headers["X-Chunk-Hash"] == sha256_hex(body)

        empty_id = str(uuid.uuid4())
        empty = put(client, empty_id, b"")
        assert empty.status_code == 200
        assert empty.json()["size"] == 0


def test_invalid_hash_chunk_and_disk_do_not_write(tmp_path: Path) -> None:
    app = create_app(settings_for(tmp_path), report_corrupt=lambda *_: 200)
    with TestClient(app) as client:
        chunk_id = str(uuid.uuid4())
        mismatch = client.put(
            f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1", "hash": "0" * 64},
            content=b"data", headers={"Content-Type": "application/octet-stream"},
        )
        assert mismatch.status_code == 422
        assert list((tmp_path / "disk1").rglob("*.part")) == []
        assert list((tmp_path / "disk1").rglob("*.chunk")) == []

        bad_uuid = client.put(
            "/chunks/../../outside", params={"disk_id": "node1/disk1", "hash": sha256_hex(b"x")},
            content=b"x", headers={"Content-Type": "application/octet-stream"},
        )
        assert bad_uuid.status_code in (404, 422)
        assert not (tmp_path / "outside").exists()

        bad_disk = put(client, str(uuid.uuid4()), b"x", "node1/../elsewhere")
        assert bad_disk.status_code == 404


def test_delete_is_idempotent_and_removes_quarantine(tmp_path: Path) -> None:
    app = create_app(settings_for(tmp_path), report_corrupt=lambda *_: 200)
    chunk_id = str(uuid.uuid4())
    with TestClient(app) as client:
        first = client.delete(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"})
        assert first.status_code == 200 and first.json()["deleted"] is False
        ok = put(client, chunk_id, b"x")
        assert ok.status_code == 200
        store = app.state.store
        active = store.find(chunk_id, "node1/disk1")
        assert active is not None
        corrupt = store.quarantine_path(active)
        assert corrupt.exists()
        deleted = client.delete(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"})
        assert deleted.status_code == 200 and deleted.json()["deleted"] is True
        assert not corrupt.exists()


def test_get_detects_corruption_quarantines_and_reports_once(tmp_path: Path) -> None:
    reports: list[tuple[str, str]] = []
    app = create_app(settings_for(tmp_path), report_corrupt=lambda chunk, disk: reports.append((chunk, disk)) or 503)
    chunk_id = str(uuid.uuid4())
    with TestClient(app) as client:
        assert put(client, chunk_id, b"abcdef").status_code == 200
        path = app.state.store.find(chunk_id, "node1/disk1")
        assert path is not None
        path.write_bytes(b"xbcdef")
        response = client.get(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"})
        assert response.status_code == 422
        assert response.json()["detail"]["error"]
        assert reports == [(chunk_id, "node1/disk1")]
        assert list(path.parent.glob("*.chunk.corrupt"))
        assert client.get(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"}).status_code == 404
        assert client.get("/chunks").json()["chunks"] == []


def test_successful_get_corruption_report_discards_quarantine(tmp_path: Path) -> None:
    app = create_app(settings_for(tmp_path), report_corrupt=lambda *_: 200)
    chunk_id = str(uuid.uuid4())
    with TestClient(app) as client:
        assert put(client, chunk_id, b"expected").status_code == 200
        path = app.state.store.find(chunk_id, "node1/disk1")
        assert path is not None
        path.write_bytes(b"modified")
        response = client.get(f"/chunks/{chunk_id}", params={"disk_id": "node1/disk1"})
        assert response.status_code == 422
        assert list(path.parent.glob("*.chunk.corrupt")) == []


def test_scrub_discards_reported_corruption_and_retries_failed_report(tmp_path: Path) -> None:
    settings = settings_for(tmp_path)
    store = ChunkStore(settings)
    chunk_id = str(uuid.uuid4())
    store.put(chunk_id, "node1/disk1", sha256_hex(b"correct"), b"correct")
    active = store.find(chunk_id, "node1/disk1")
    assert active is not None
    active.write_bytes(b"corrupt")
    success_calls: list[tuple[str, str]] = []
    result = scrub_once(store, lambda chunk, disk: success_calls.append((chunk, disk)) or 200)
    assert result["checked"] == 1
    assert result["corrupt"] == [{"chunk_id": chunk_id, "disk_id": "node1/disk1"}]
    assert success_calls == [(chunk_id, "node1/disk1")]
    assert store.corrupt_files() == []

    second_id = str(uuid.uuid4())
    store.put(second_id, "node1/disk1", sha256_hex(b"intact"), b"intact")
    other = store.find(second_id, "node1/disk1")
    assert other is not None
    other.write_bytes(b"changed")
    attempts: list[tuple[str, str]] = []
    failing = lambda chunk, disk: attempts.append((chunk, disk)) or 503
    scrub_once(store, failing)
    assert len(store.corrupt_files()) == 1
    scrub_once(store, failing)
    assert attempts.count((second_id, "node1/disk1")) == 2


def test_listing_restart_tmp_cleanup_and_disk_shape(tmp_path: Path) -> None:
    settings = settings_for(tmp_path)
    store = ChunkStore(settings)
    chunk_id = str(uuid.uuid4())
    body = b"persistent"
    store.put(chunk_id, "node1/disk1", sha256_hex(body), body)
    temp_dir = tmp_path / "disk1" / "tmp"
    orphan = temp_dir / "orphan.part"
    orphan.write_bytes(b"staging")
    restarted = ChunkStore(settings)
    assert not orphan.exists()
    assert restarted.get(chunk_id, "node1/disk1")[0] == body
    assert restarted.list_chunks() == [{"chunk_id": chunk_id, "disk_id": "node1/disk1",
                                         "size": len(body), "hash": sha256_hex(body)}]
    disks = list_disks(settings)
    assert disks[0]["disk_id"] == "node1/disk1"
    assert isinstance(disks[0]["total_bytes"], int)
    assert isinstance(disks[0]["used_bytes"], int)
    assert disks[0]["path"] == str((tmp_path / "disk1").resolve())


def test_duplicate_disk_basename_is_rejected(tmp_path: Path) -> None:
    settings = NodeSettings("node1", (tmp_path / "one" / "disk", tmp_path / "two" / "disk"),
                            "http://localhost:9000", "http://localhost:8000", 0)
    try:
        ChunkStore(settings)
    except ValueError as exc:
        assert "mesmo nome" in str(exc)
    else:
        raise AssertionError("Esperava erro para basenames repetidos")


def test_health_metrics_and_unknown_hash_algorithm(tmp_path: Path) -> None:
    settings = settings_for(tmp_path)
    with TestClient(create_app(settings, report_corrupt=lambda *_: 200)) as client:
        health = client.get("/health").json()
        assert health["ok"] is True and health["node_id"] == "node1"
        metrics = client.get("/metrics").json()
        assert {"put", "get", "copy", "delete", "scrub", "hash_failures"} <= metrics.keys()
    bad = NodeSettings("node1", settings.data_dirs, settings.address,
                       settings.controller_url, 0, "sha512")
    try:
        create_app(bad)
    except ValueError as exc:
        assert "sha256" in str(exc)
    else:
        raise AssertionError("Algoritmo não suportado foi aceito")
