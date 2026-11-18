"""
tests/test_controller.py
Testes básicos da Pessoa A — controlador de metadados.

Cobertura:
  - /health
  - /dirs (criar, listar, remover, remover não vazio → 409)
  - /files/allocate + /files/commit + /files/locate + /files/stat
  - Lease de escrita: dois allocates simultâneos → 409
  - Leitura durante escrita → 409 (recovering)
  - Reinício do controlador sem perda de dados (RF10)
  - /chunks/corrupt
  - /files/move e DELETE /files

Execução:
    pytest tests/test_controller.py -v
"""
from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Sobrescreve DB_PATH antes de importar o app
import os

_TMP_DIR = tempfile.mkdtemp()
os.environ["DB_PATH"] = str(Path(_TMP_DIR) / "test_controller.db")
os.environ["CONFIG_PATH"] = str(Path(__file__).parent.parent / "config.yaml")

# Importa após configurar env
from controller.main import app  # noqa: E402
from controller.db import get_db  # noqa: E402
from controller import repository as repo  # noqa: E402


def _make_client_with_nodes() -> TestClient:
    """Cria cliente de teste com banco fresco e 3 nós registrados."""
    db_path = Path(os.environ["DB_PATH"])
    if db_path.exists():
        db_path.unlink()
    conn = get_db(db_path)
    repo.register_node(conn, "node1", "http://node1:9000", [
        {"disk_id": "node1/disk1", "path": "/data/disk1",
         "total_bytes": 10 * 1024**3, "used_bytes": 0},
        {"disk_id": "node1/disk2", "path": "/data/disk2",
         "total_bytes": 10 * 1024**3, "used_bytes": 0},
    ])
    repo.register_node(conn, "node2", "http://node2:9000", [
        {"disk_id": "node2/disk1", "path": "/data/disk1",
         "total_bytes": 10 * 1024**3, "used_bytes": 0},
    ])
    repo.register_node(conn, "node3", "http://node3:9000", [
        {"disk_id": "node3/disk1", "path": "/data/disk1",
         "total_bytes": 10 * 1024**3, "used_bytes": 0},
    ])
    conn.close()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def client():
    """Fixture que reinicia o banco e retorna um TestClient limpo para cada teste."""
    return _make_client_with_nodes()


# ─────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert "replication_factor" in body


# ─────────────────────────────────────────────────────────────
# Diretórios
# ─────────────────────────────────────────────────────────────

def test_create_and_list_dir(client):
    r = client.post("/dirs", json={"path": "/docs"})
    assert r.status_code == 201, r.text

    r = client.get("/dirs", params={"path": "/"})
    assert r.status_code == 200
    names = [e["name"] for e in r.json()["entries"]]
    assert "docs" in names


def test_create_nested_dir(client):
    client.post("/dirs", json={"path": "/projetos"})
    r = client.post("/dirs", json={"path": "/projetos/alpha"})
    assert r.status_code == 201, r.text


def test_create_dir_no_parent_fails(client):
    r = client.post("/dirs", json={"path": "/inexistente/sub"})
    assert r.status_code == 404


def test_create_dir_duplicate_fails(client):
    client.post("/dirs", json={"path": "/dup"})
    r = client.post("/dirs", json={"path": "/dup"})
    assert r.status_code == 409


def test_remove_empty_dir(client):
    client.post("/dirs", json={"path": "/vazio"})
    r = client.delete("/dirs", params={"path": "/vazio"})
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_remove_nonempty_dir_fails(client):
    client.post("/dirs", json={"path": "/cheio"})
    client.post("/dirs", json={"path": "/cheio/sub"})
    r = client.delete("/dirs", params={"path": "/cheio"})
    assert r.status_code == 409


def test_remove_nonexistent_dir_fails(client):
    r = client.delete("/dirs", params={"path": "/fantasma"})
    assert r.status_code == 404


# ─────────────────────────────────────────────────────────────
# Allocate + Commit + Locate + Stat (fluxo completo de escrita)
# ─────────────────────────────────────────────────────────────

def _commit_file(client, path: str, parent: str = "/docs") -> None:
    """Aloca, commita e retorna. Garante que o diretório pai existe."""
    client.post("/dirs", json={"path": parent})
    alloc = client.post("/files/allocate", json={
        "path": path, "size": 10, "num_chunks": 1, "client_id": "test"
    }).json()
    assert "lease_id" in alloc, f"Allocate falhou: {alloc}"
    chunk = alloc["chunks"][0]
    confirmed = [t["disk_id"] for t in chunk["targets"]]
    r = client.post("/files/commit", json={
        "path": path,
        "lease_id": alloc["lease_id"],
        "file_hash": "hashtest",
        "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"],
                    "hash": "hashtest", "size": 10, "confirmed": confirmed}],
    })
    assert r.status_code == 200, f"Commit falhou: {r.text}"


def test_allocate_returns_lease_and_targets(client):
    client.post("/dirs", json={"path": "/docs"})
    r = client.post("/files/allocate", json={
        "path": "/docs/readme.txt", "size": 100, "num_chunks": 1, "client_id": "cli-test"
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert "lease_id" in body
    assert len(body["chunks"]) == 1
    chunk = body["chunks"][0]
    assert chunk["idx"] == 0
    assert len(chunk["targets"]) >= 1


def test_commit_full_flow(client):
    client.post("/dirs", json={"path": "/docs"})
    alloc = client.post("/files/allocate", json={
        "path": "/docs/file1.bin", "size": 100, "num_chunks": 1
    }).json()

    chunk = alloc["chunks"][0]
    file_hash = hashlib.sha256(b"hello").hexdigest()
    chunk_hash = hashlib.sha256(b"hello").hexdigest()
    confirmed = [t["disk_id"] for t in chunk["targets"]]

    r = client.post("/files/commit", json={
        "path": "/docs/file1.bin",
        "lease_id": alloc["lease_id"],
        "file_hash": file_hash,
        "chunks": [{
            "idx": 0,
            "chunk_id": chunk["chunk_id"],
            "hash": chunk_hash,
            "size": 100,
            "confirmed": confirmed,
        }],
    })
    assert r.status_code == 200, r.text
    state = r.json()["state"]
    assert state in ("intact", "under_replicated")


def test_locate_after_commit(client):
    client.post("/dirs", json={"path": "/docs"})
    alloc = client.post("/files/allocate", json={
        "path": "/docs/file2.txt", "size": 50, "num_chunks": 1
    }).json()
    chunk = alloc["chunks"][0]
    confirmed = [t["disk_id"] for t in chunk["targets"]]

    client.post("/files/commit", json={
        "path": "/docs/file2.txt",
        "lease_id": alloc["lease_id"],
        "file_hash": "abc",
        "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"], "hash": "abc",
                    "size": 50, "confirmed": confirmed}],
    })

    r = client.get("/files/locate", params={"path": "/docs/file2.txt"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["file_hash"] == "abc"
    assert len(body["chunks"]) == 1


def test_stat_after_commit(client):
    client.post("/dirs", json={"path": "/docs"})
    alloc = client.post("/files/allocate", json={
        "path": "/docs/meta.txt", "size": 200, "num_chunks": 1
    }).json()
    chunk = alloc["chunks"][0]
    confirmed = [t["disk_id"] for t in chunk["targets"]]

    client.post("/files/commit", json={
        "path": "/docs/meta.txt",
        "lease_id": alloc["lease_id"],
        "file_hash": "hashxyz",
        "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"], "hash": "hashxyz",
                    "size": 200, "confirmed": confirmed}],
    })

    r = client.get("/files/stat", params={"path": "/docs/meta.txt"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["file_hash"] == "hashxyz"
    assert body["version"] == 1
    assert body["state"] in ("intact", "under_replicated")


# ─────────────────────────────────────────────────────────────
# Concorrência — lease (RF8)
# ─────────────────────────────────────────────────────────────

def test_second_allocate_same_path_returns_409(client):
    """Dois allocates simultâneos no mesmo arquivo → 409."""
    client.post("/dirs", json={"path": "/docs"})
    client.post("/files/allocate", json={
        "path": "/docs/conflito.txt", "size": 10, "num_chunks": 1
    })
    r2 = client.post("/files/allocate", json={
        "path": "/docs/conflito.txt", "size": 10, "num_chunks": 1
    })
    assert r2.status_code == 409, r2.text


def test_locate_during_write_returns_409(client):
    """
    Leitura de arquivo cujo estado é 'recovering' (upload em andamento)
    deve retornar 409 — garante que nenhum cliente leia arquivo parcial.
    """
    client.post("/dirs", json={"path": "/docs"})
    # Aloca mas NÃO comita
    client.post("/files/allocate", json={
        "path": "/docs/parcial.txt", "size": 10, "num_chunks": 1
    })
    r = client.get("/files/locate", params={"path": "/docs/parcial.txt"})
    assert r.status_code == 409, r.text


# ─────────────────────────────────────────────────────────────
# Corrupção de réplica (RF4 — stub)
# ─────────────────────────────────────────────────────────────

def test_report_corrupt_marks_replica(client):
    _commit_file(client, "/docs/corrupt_test.txt")

    locate = client.get("/files/locate", params={"path": "/docs/corrupt_test.txt"}).json()
    chunk_id = locate["chunks"][0]["chunk_id"]
    disk_id = locate["chunks"][0]["replicas"][0]["disk_id"]

    r = client.post("/chunks/corrupt", json={
        "chunk_id": chunk_id,
        "disk_id": disk_id,
    })
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True


def test_report_corrupt_nonexistent_chunk(client):
    r = client.post("/chunks/corrupt", json={
        "chunk_id": str(uuid.uuid4()), "disk_id": "node1/disk1"
    })
    assert r.status_code == 404


# ─────────────────────────────────────────────────────────────
# Move e Delete (RF1)
# ─────────────────────────────────────────────────────────────

def test_move_file(client):
    _commit_file(client, "/docs/origem.txt")
    r = client.post("/files/move", json={"from": "/docs/origem.txt", "to": "/docs/destino.txt"})
    assert r.status_code == 200, r.text
    # Origem não deve mais existir
    r2 = client.get("/files/stat", params={"path": "/docs/origem.txt"})
    assert r2.status_code == 404
    # Destino deve existir
    r3 = client.get("/files/stat", params={"path": "/docs/destino.txt"})
    assert r3.status_code == 200


def test_delete_file(client):
    _commit_file(client, "/docs/deletar.txt")
    r = client.delete("/files", params={"path": "/docs/deletar.txt"})
    assert r.status_code == 200, r.text
    r2 = client.get("/files/stat", params={"path": "/docs/deletar.txt"})
    assert r2.status_code == 404


# ─────────────────────────────────────────────────────────────
# Persistência — RF10
# ─────────────────────────────────────────────────────────────

def test_data_persists_after_restart(client):
    """
    Simula reinício do controlador criando nova conexão com o mesmo arquivo
    de banco e verifica que os metadados continuam presentes.
    """
    _commit_file(client, "/docs/persistir.txt")

    # "Reinicia" abrindo nova conexão com o mesmo DB
    db_path = Path(os.environ["DB_PATH"])
    conn2 = get_db(db_path)
    entry = repo.get_entry(conn2, "/docs/persistir.txt")
    conn2.close()

    assert entry is not None, "Metadados não persistiram após reinício"
    assert entry["type"] == "file"
