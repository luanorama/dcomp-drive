"""
controller/db.py
Conexão e schema SQLite do controlador (RF10).

Decisões:
- PRAGMA journal_mode=WAL  → múltiplos leitores + 1 escritor sem travar
- PRAGMA busy_timeout=5000 → evita 'database is locked' entre rotas e workers
- PRAGMA foreign_keys=ON   → integridade referencial
- row_factory = sqlite3.Row → acesso por nome de coluna

O schema é criado automaticamente ao chamar get_db() pela primeira vez.
O arquivo do banco fica em /data/controller.db (configurável por DB_PATH).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

# Caminho do banco de dados; sobrescrito por variável de ambiente no Docker
_DEFAULT_DB_PATH = Path(os.environ.get("DB_PATH", "/data/controller.db"))

# SQL de criação do schema (idempotente — usa IF NOT EXISTS)
_SCHEMA_SQL = """
-- ============================================================
-- Tabela: nodes
-- Mantém o registro de cada nó de armazenamento.
-- status: alive | suspect | dead | draining
-- ============================================================
CREATE TABLE IF NOT EXISTS nodes (
    node_id        TEXT PRIMARY KEY,
    address        TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'alive'
                        CHECK(status IN ('alive', 'suspect', 'dead', 'draining')),
    last_heartbeat REAL
);

-- ============================================================
-- Tabela: disks
-- Um nó tem 1 ou mais discos (pastas de dados distintas).
-- disk_id: formato "node_id/disk_name" (ex: "node1/disk1")
-- ============================================================
CREATE TABLE IF NOT EXISTS disks (
    disk_id     TEXT PRIMARY KEY,
    node_id     TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    path        TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    used_bytes  INTEGER NOT NULL DEFAULT 0
);

-- ============================================================
-- Tabela: entries
-- Árvore de arquivos e diretórios do sistema.
-- type: 'file' | 'dir'
-- state: 'intact' | 'under_replicated' | 'recovering'
-- ============================================================
CREATE TABLE IF NOT EXISTS entries (
    path        TEXT PRIMARY KEY,
    type        TEXT NOT NULL CHECK(type IN ('file', 'dir')),
    size        INTEGER NOT NULL DEFAULT 0,
    created_at  REAL    NOT NULL,
    modified_at REAL    NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    file_hash   TEXT,
    state       TEXT    NOT NULL DEFAULT 'intact'
                    CHECK(state IN ('intact', 'under_replicated', 'recovering'))
);

-- ============================================================
-- Tabela: chunks
-- Cada arquivo é dividido em chunks ordenados por idx.
-- chunk_id: UUID gerado pelo controlador na alocação.
-- ============================================================
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    path     TEXT    NOT NULL REFERENCES entries(path) ON DELETE CASCADE,
    idx      INTEGER NOT NULL,
    size     INTEGER NOT NULL DEFAULT 0,
    hash     TEXT,
    UNIQUE(path, idx)
);

-- ============================================================
-- Tabela: replicas
-- Cada chunk pode ter múltiplas réplicas em discos distintos.
-- state: ok | pending | lost | corrupt
-- ============================================================
CREATE TABLE IF NOT EXISTS replicas (
    chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    disk_id  TEXT NOT NULL REFERENCES disks(disk_id)  ON DELETE CASCADE,
    state    TEXT NOT NULL DEFAULT 'pending'
                 CHECK(state IN ('ok', 'pending', 'lost', 'corrupt')),
    PRIMARY KEY (chunk_id, disk_id)
);

-- ============================================================
-- Tabela: leases
-- Controle de concorrência: um escritor por caminho por vez.
-- ============================================================
CREATE TABLE IF NOT EXISTS leases (
    path       TEXT PRIMARY KEY,
    client_id  TEXT,
    lease_id   TEXT NOT NULL,
    expires_at REAL NOT NULL
);

-- Estado do drain separado do estado de conectividade do nó.
CREATE TABLE IF NOT EXISTS node_drains (
    node_id TEXT PRIMARY KEY REFERENCES nodes(node_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS node_rejoins (
    node_id TEXT PRIMARY KEY REFERENCES nodes(node_id) ON DELETE CASCADE,
    registered_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk_deletions (
    chunk_id TEXT NOT NULL,
    disk_id TEXT NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    PRIMARY KEY (chunk_id, disk_id)
);

-- ============================================================
-- Índices para consultas frequentes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_chunks_path       ON chunks(path);
CREATE INDEX IF NOT EXISTS idx_replicas_chunk    ON replicas(chunk_id);
CREATE INDEX IF NOT EXISTS idx_replicas_disk     ON replicas(disk_id);
CREATE INDEX IF NOT EXISTS idx_replicas_state    ON replicas(state);
CREATE INDEX IF NOT EXISTS idx_disks_node        ON disks(node_id);
CREATE INDEX IF NOT EXISTS idx_entries_type      ON entries(type);
CREATE INDEX IF NOT EXISTS idx_leases_expires    ON leases(expires_at);
"""


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    """Aplica PRAGMAs de desempenho e segurança em cada conexão."""
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")  # equilibra durabilidade e velocidade


def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    """
    Abre (ou cria) o banco SQLite e retorna uma conexão configurada.

    Cada chamada retorna uma NOVA conexão — adequado para uso por
    request (FastAPI Depends) ou por worker em background.
    O schema é criado automaticamente se ainda não existir.

    Uso em FastAPI:
        def db_dep() -> Generator[sqlite3.Connection, None, None]:
            conn = get_db()
            try:
                yield conn
            finally:
                conn.close()
    """
    path = db_path or _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    conn.executescript(_SCHEMA_SQL)
    conn.commit()
    return conn


def db_dependency():
    """
    Generator para usar como FastAPI Depends.
    Garante fechamento da conexão após cada request.

    Exemplo:
        @router.get("/files/stat")
        def stat(conn: sqlite3.Connection = Depends(db_dependency)):
            ...
    """
    conn = get_db()
    try:
        yield conn
    finally:
        conn.close()
