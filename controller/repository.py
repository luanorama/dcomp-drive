"""
controller/repository.py
Funções Python de acesso ao banco de dados do controlador.

IMPORTANTE: Este módulo é a interface que a Pessoa C (workers em background)
usa para ler e alterar o estado do banco SEM fazer chamadas HTTP internas.
Todas as funções recebem uma conexão sqlite3.Connection aberta pelo chamador.

Funções exportadas:
  — Nós e discos
      register_node / update_node_status / update_disk_space
      get_node / list_nodes / get_node_disks

  — Chunks e réplicas
      list_chunks_of_node
      get_under_replicated_chunks
      mark_replica_as
      mark_node_replicas_lost
      count_live_replicas

  — Posicionamento
      choose_replication_targets

  — Leases
      create_lease / get_lease / delete_lease / list_expired_leases

  — Entries (arquivos/dirs) — usadas internamente pelos routers
      get_entry / list_dir_entries / create_dir / delete_dir / rename_entry
      update_entry_state
"""
from __future__ import annotations

import sqlite3
import time
from typing import Any

from controller.config import get_config
from controller.logger import get_logger

log = get_logger("controller.repository")
cfg = get_config()


# ─────────────────────────────────────────────────────────────
# Tipos auxiliares
# ─────────────────────────────────────────────────────────────

def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(r) if r is not None else None


def _rows(rs: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rs]


# ─────────────────────────────────────────────────────────────
# Nós e discos
# ─────────────────────────────────────────────────────────────

def register_node(
    conn: sqlite3.Connection,
    node_id: str,
    address: str,
    disks: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    """
    Registra (ou atualiza) um nó e seus discos.
    Chamada pelo router de registro (Pessoa C) e pelo agente do nó.
    """
    now = time.time()
    conn.execute(
        """
        INSERT INTO nodes(node_id, address, status, last_heartbeat)
        VALUES(?, ?, 'alive', ?)
        ON CONFLICT(node_id) DO UPDATE SET
            address        = excluded.address,
            last_heartbeat = excluded.last_heartbeat
        """,
        (node_id, address, now),
    )
    for d in disks:
        conn.execute(
            """
            INSERT INTO disks(disk_id, node_id, path, total_bytes, used_bytes)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(disk_id) DO UPDATE SET
                path        = excluded.path,
                total_bytes = excluded.total_bytes,
                used_bytes  = excluded.used_bytes
            WHERE disks.node_id = excluded.node_id
            """,
            (d["disk_id"], node_id, d["path"], d["total_bytes"], d["used_bytes"]),
        )
    if commit:
        conn.commit()
    log.info("node_registered", node_id=node_id, address=address)


def update_node_status(
    conn: sqlite3.Connection, node_id: str, status: str
) -> None:
    """
    Atualiza o status de um nó: alive | suspect | dead | draining.
    Usada pelo monitor de falhas da Pessoa C.
    """
    conn.execute(
        "UPDATE nodes SET status = ? WHERE node_id = ?", (status, node_id)
    )
    conn.commit()
    log.info("node_status_changed", node_id=node_id, status=status)


def update_node_heartbeat(
    conn: sqlite3.Connection,
    node_id: str,
    disks: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> bool:
    """
    Atualiza last_heartbeat e espaço dos discos.
    Retorna False se o nó não existir no banco.
    """
    now = time.time()
    cur = conn.execute(
        """UPDATE nodes SET last_heartbeat = ?,
               status = CASE WHEN status = 'suspect' THEN
                   CASE WHEN EXISTS (SELECT 1 FROM node_drains WHERE node_id = nodes.node_id)
                        THEN 'draining' ELSE 'alive' END
               ELSE status END
           WHERE node_id = ?""",
        (now, node_id),
    )
    if cur.rowcount == 0:
        return False
    for d in disks:
        conn.execute(
            """
            UPDATE disks SET total_bytes = ?, used_bytes = ?
            WHERE disk_id = ? AND node_id = ?
            """,
            (d["total_bytes"], d["used_bytes"], d["disk_id"], node_id),
        )
    if commit:
        conn.commit()
    return True


def get_node(conn: sqlite3.Connection, node_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
    ).fetchone()
    return _row(row)


def list_nodes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
    return _rows(rows)


def get_node_disks(
    conn: sqlite3.Connection, node_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM disks WHERE node_id = ?", (node_id,)
    ).fetchall()
    return _rows(rows)


def update_disk_space(
    conn: sqlite3.Connection,
    disk_id: str,
    total_bytes: int,
    used_bytes: int,
) -> None:
    conn.execute(
        "UPDATE disks SET total_bytes = ?, used_bytes = ? WHERE disk_id = ?",
        (total_bytes, used_bytes, disk_id),
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────
# Posicionamento (RF3, RF7)
# ─────────────────────────────────────────────────────────────

def choose_replication_targets(
    conn: sqlite3.Connection,
    n: int,
    exclude_disks: list[str] | None = None,
    exclude_nodes: list[str] | None = None,
    required_bytes: int = 0,
) -> list[dict[str, Any]]:
    """
    Escolhe até `n` discos para replicação respeitando as regras:
      1. Nó deve estar com status = 'alive' (nunca suspect, dead ou draining)
      2. Disco não pode estar em exclude_disks
      3. Nó não pode estar em exclude_nodes
      4. Disco não pode estar acima do limite de uso (disk_usage_limit_pct)
      5. Ordena por espaço livre decrescente (greedy)

    Retorna lista de dicts com: disk_id, node_id, path, node_url (address do nó).
    """
    if n <= 0:
        return []
    exclude_disks = exclude_disks or []
    exclude_nodes = exclude_nodes or []
    limit_pct = cfg.disk_usage_limit_pct / 100.0

    rows = conn.execute(
        """
        SELECT d.disk_id, d.node_id, d.path,
               d.total_bytes, d.used_bytes,
               n.address AS node_url
        FROM   disks d
        JOIN   nodes n USING(node_id)
        WHERE  n.status = 'alive'
          AND  d.total_bytes > 0
          AND  CAST(d.used_bytes AS REAL) / d.total_bytes < ?
          AND  d.total_bytes - d.used_bytes >= ?
        ORDER BY (d.total_bytes - d.used_bytes) DESC
        """,
        (limit_pct, required_bytes),
    ).fetchall()

    chosen: list[dict[str, Any]] = []
    seen_nodes: set[str] = set(exclude_nodes)
    seen_disks: set[str] = set(exclude_disks)

    for row in rows:
        d = dict(row)
        if d["disk_id"] in seen_disks:
            continue
        if d["node_id"] in seen_nodes:
            continue
        chosen.append(d)
        seen_disks.add(d["disk_id"])
        seen_nodes.add(d["node_id"])
        if len(chosen) == n:
            break

    return chosen


# ─────────────────────────────────────────────────────────────
# Chunks e réplicas
# ─────────────────────────────────────────────────────────────

def list_chunks_of_node(
    conn: sqlite3.Connection, node_id: str
) -> list[dict[str, Any]]:
    """
    Lista todos os chunks (com hash e estado da réplica) armazenados
    em qualquer disco do nó. Usada pela Pessoa C na reconciliação.
    """
    rows = conn.execute(
        """
        SELECT c.chunk_id, c.path, c.idx, c.hash,
               r.disk_id, r.state
        FROM   replicas r
        JOIN   chunks   c USING(chunk_id)
        JOIN   disks    d ON d.disk_id = r.disk_id
        WHERE  d.node_id = ?
        """,
        (node_id,),
    ).fetchall()
    return _rows(rows)


def get_under_replicated_chunks(
    conn: sqlite3.Connection, limit: int = 100
) -> list[dict[str, Any]]:
    """
    Retorna chunks com menos réplicas vivas (state='ok') que N,
    ordenados por número de réplicas vivas (prioridade para os mais críticos).
    Usada pela fila de re-replicação da Pessoa C.
    """
    n = cfg.replication_factor
    rows = conn.execute(
        """
        SELECT c.chunk_id, c.path, c.hash,
               COUNT(CASE WHEN r.state = 'ok' THEN 1 END) AS live_replicas
        FROM   chunks   c
        JOIN   replicas r USING(chunk_id)
        GROUP  BY c.chunk_id
        HAVING live_replicas < ?
        ORDER  BY live_replicas ASC
        LIMIT  ?
        """,
        (n, limit),
    ).fetchall()
    return _rows(rows)


def mark_replica_as(
    conn: sqlite3.Connection, chunk_id: str, disk_id: str, state: str
) -> None:
    """
    Atualiza o estado de uma réplica: ok | pending | lost | corrupt.
    Usada pela Pessoa C (monitor de falhas, re-replicação) e pelo
    stub /chunks/corrupt da Pessoa A.
    """
    conn.execute(
        "UPDATE replicas SET state = ? WHERE chunk_id = ? AND disk_id = ?",
        (state, chunk_id, disk_id),
    )
    conn.commit()
    log.info("replica_state_changed", chunk_id=chunk_id, disk_id=disk_id, state=state)


def mark_node_replicas_lost(
    conn: sqlite3.Connection, node_id: str
) -> int:
    """
    Marca todas as réplicas de discos do nó como 'lost'.
    Chamada pela Pessoa C ao declarar um nó como morto.
    Retorna o número de réplicas afetadas.
    """
    cur = conn.execute(
        """
        UPDATE replicas SET state = 'lost'
        WHERE  disk_id IN (SELECT disk_id FROM disks WHERE node_id = ?)
          AND  state   != 'lost'
        """,
        (node_id,),
    )
    conn.commit()
    affected = cur.rowcount
    log.info("node_replicas_marked_lost", node_id=node_id, count=affected)
    return affected


def count_live_replicas(
    conn: sqlite3.Connection, chunk_id: str
) -> int:
    row = conn.execute(
        """SELECT COUNT(DISTINCT d.node_id) FROM replicas r JOIN disks d USING(disk_id)
           JOIN nodes n USING(node_id) WHERE r.chunk_id = ? AND r.state = 'ok'
           AND n.status IN ('alive', 'suspect', 'draining')""",
        (chunk_id,),
    ).fetchone()
    return row[0] if row else 0


def get_chunk(
    conn: sqlite3.Connection, chunk_id: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,)
    ).fetchone()
    return _row(row)


def get_live_replicas(
    conn: sqlite3.Connection, chunk_id: str
) -> list[dict[str, Any]]:
    """Retorna réplicas com state='ok' e endereço do nó para leitura."""
    rows = conn.execute(
        """
        SELECT r.chunk_id, r.disk_id, r.state,
               n.address AS node_url, n.node_id, n.status
        FROM   replicas r
        JOIN   disks    d USING(disk_id)
        JOIN   nodes    n USING(node_id)
        WHERE  r.chunk_id = ?
          AND  r.state    = 'ok'
          AND  n.status   IN ('alive', 'suspect', 'draining')
        ORDER  BY CASE n.status WHEN 'alive' THEN 0 WHEN 'draining' THEN 1 ELSE 2 END
        """,
        (chunk_id,),
    ).fetchall()
    return _rows(rows)


def add_replica(
    conn: sqlite3.Connection,
    chunk_id: str,
    disk_id: str,
    state: str = "pending",
) -> None:
    conn.execute(
        """
        INSERT INTO replicas(chunk_id, disk_id, state)
        VALUES(?, ?, ?)
        ON CONFLICT(chunk_id, disk_id) DO UPDATE SET state = excluded.state
        """,
        (chunk_id, disk_id, state),
    )
    conn.commit()


def delete_pending_replicas_of_lease(
    conn: sqlite3.Connection, path: str
) -> list[dict[str, Any]]:
    """
    Remove réplicas pending e chunks do arquivo com esse path que não foram
    commitados. Retorna lista de (chunk_id, disk_id, node_url) para o worker
    chamar DELETE /chunks/{chunk_id} nos nós.
    Chamada pelo worker de expiração de leases da Pessoa C.
    """
    # Lê as réplicas pending para retornar ao worker
    rows = conn.execute(
        """
        SELECT r.chunk_id, r.disk_id, n.address AS node_url
        FROM   replicas r
        JOIN   chunks   c USING(chunk_id)
        JOIN   disks    d USING(disk_id)
        JOIN   nodes    n ON n.node_id = d.node_id
        WHERE  c.path  = ?
          AND  r.state = 'pending'
        """,
        (path,),
    ).fetchall()
    to_delete = _rows(rows)

    # Deleta chunks (cascata remove réplicas via FK)
    conn.execute(
        "DELETE FROM chunks WHERE path = ? AND chunk_id IN "
        "(SELECT chunk_id FROM replicas WHERE state = 'pending')",
        (path,),
    )
    conn.commit()
    return to_delete


def update_entry_state(
    conn: sqlite3.Connection, path: str, state: str
) -> None:
    conn.execute(
        "UPDATE entries SET state = ? WHERE path = ?", (state, path)
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────
# Leases (RF8)
# ─────────────────────────────────────────────────────────────

def create_lease(
    conn: sqlite3.Connection,
    path: str,
    lease_id: str,
    client_id: str | None,
    *,
    commit: bool = True,
) -> bool:
    """
    Cria um lease de escrita para o path.
    Retorna False se já existe um lease ativo (conflito → HTTP 409).
    """
    now = time.time()
    expires_at = now + cfg.lease_ttl_s

    # Limpa leases expirados deste path antes de verificar conflito
    conn.execute(
        "DELETE FROM leases WHERE path = ? AND expires_at < ?", (path, now)
    )

    # Tenta inserir
    try:
        conn.execute(
            "INSERT INTO leases(path, client_id, lease_id, expires_at) VALUES(?,?,?,?)",
            (path, client_id, lease_id, expires_at),
        )
        if commit:
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False  # Já existe lease ativo → 409


def get_lease(
    conn: sqlite3.Connection, path: str
) -> dict[str, Any] | None:
    now = time.time()
    row = conn.execute(
        "SELECT * FROM leases WHERE path = ? AND expires_at > ?", (path, now)
    ).fetchone()
    return _row(row)


def delete_lease(conn: sqlite3.Connection, path: str) -> None:
    conn.execute("DELETE FROM leases WHERE path = ?", (path,))
    conn.commit()


def list_expired_leases(
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """
    Retorna todos os leases expirados.
    O worker da Pessoa C chama esta função periodicamente para limpar
    uploads que nunca foram commitados.
    """
    now = time.time()
    rows = conn.execute(
        "SELECT * FROM leases WHERE expires_at < ?", (now,)
    ).fetchall()
    return _rows(rows)


# ─────────────────────────────────────────────────────────────
# Entries — arquivos e diretórios (RF1)
# ─────────────────────────────────────────────────────────────

def get_entry(
    conn: sqlite3.Connection, path: str
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM entries WHERE path = ?", (path,)
    ).fetchone()
    return _row(row)


def create_dir(conn: sqlite3.Connection, path: str) -> None:
    now = time.time()
    conn.execute(
        """
        INSERT INTO entries(path, type, size, created_at, modified_at, version, state)
        VALUES(?, 'dir', 0, ?, ?, 1, 'intact')
        """,
        (path, now, now),
    )
    conn.commit()


def delete_dir(conn: sqlite3.Connection, path: str) -> bool:
    """Remove o diretório somente se não houver entries filhas. Retorna True se removeu."""
    prefix = path.rstrip("/") + "/"
    child = conn.execute(
        "SELECT 1 FROM entries WHERE path LIKE ? LIMIT 1", (prefix + "%",)
    ).fetchone()
    if child:
        return False  # Diretório não vazio → 409
    conn.execute("DELETE FROM entries WHERE path = ?", (path,))
    conn.commit()
    return True


def list_dir_entries(
    conn: sqlite3.Connection, path: str
) -> list[dict[str, Any]]:
    """Lista entradas imediatas dentro do diretório (apenas um nível)."""
    prefix = path.rstrip("/") + "/"
    rows = conn.execute(
        """
        SELECT path, type, size, state
        FROM   entries
        WHERE  path LIKE ?
          AND  path NOT LIKE ?
        """,
        (prefix + "%", prefix + "%/%"),
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["name"] = d["path"].rsplit("/", 1)[-1]
        result.append(d)
    return result


def rename_entry(
    conn: sqlite3.Connection, from_path: str, to_path: str
) -> None:
    """
    Renomeia/move um arquivo ou diretório.
    Para diretórios, atualiza também todos os filhos recursivamente.

    Usa PRAGMA defer_foreign_keys=ON para que as verificações de FK só
    ocorram no COMMIT, depois de entries e chunks serem atualizados juntos.
    """
    entry = get_entry(conn, from_path)
    if entry is None:
        raise ValueError(f"Entry not found: {from_path}")

    now = time.time()

    # Adia verificação de FK até o fim da transação
    conn.execute("PRAGMA defer_foreign_keys=ON;")

    if entry["type"] == "dir":
        old_prefix = from_path.rstrip("/") + "/"
        new_prefix = to_path.rstrip("/") + "/"
        children = conn.execute(
            "SELECT path FROM entries WHERE path LIKE ? ORDER BY path DESC",
            (old_prefix + "%",),
        ).fetchall()
        for child in children:
            old_child = child["path"]
            new_child = new_prefix + old_child[len(old_prefix):]
            # Atualiza chunks antes da entry (FK de chunks aponta para entries)
            conn.execute(
                "UPDATE chunks SET path = ? WHERE path = ?",
                (new_child, old_child),
            )
            conn.execute(
                "UPDATE entries SET path = ?, modified_at = ? WHERE path = ?",
                (new_child, now, old_child),
            )

    # Atualiza chunks do arquivo/dir raiz antes de mudar a entry
    conn.execute(
        "UPDATE chunks SET path = ? WHERE path = ?", (to_path, from_path)
    )
    conn.execute(
        "UPDATE entries SET path = ?, modified_at = ? WHERE path = ?",
        (to_path, now, from_path),
    )
    conn.commit()
    # Restaura comportamento padrão (defer_foreign_keys é por transação)

