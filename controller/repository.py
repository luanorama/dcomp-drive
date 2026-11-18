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
            status         = 'alive',
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
            """,
            (d["disk_id"], node_id, d["path"], d["total_bytes"], d["used_bytes"]),
        )
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
) -> bool:
    """
    Atualiza last_heartbeat e espaço dos discos.
    Retorna False se o nó não existir no banco.
    """
    now = time.time()
    cur = conn.execute(
        "UPDATE nodes SET last_heartbeat = ?, status = 'alive' WHERE node_id = ?",
        (now, node_id),
    )
    if cur.rowcount == 0:
        return False
    for d in disks:
        conn.execute(
            """
            UPDATE disks SET total_bytes = ?, used_bytes = ?
            WHERE disk_id = ?
            """,
            (d["total_bytes"], d["used_bytes"], d["disk_id"]),
        )
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
) -> list[dict[str, Any]]:
    """Prioriza máquinas distintas; se faltarem máquinas, usa outro disco.

    Nunca repete o mesmo disco e nunca usa nós explicitamente excluídos.
    Nós suspeitos/mortos/draining e discos acima do limite são inelegíveis.
    """
    excluded_disks = set(exclude_disks or [])
    excluded_nodes = set(exclude_nodes or [])
    rows = conn.execute(
        """
        SELECT d.disk_id, d.node_id, d.path, d.total_bytes, d.used_bytes,
               n.address AS node_url
        FROM disks d JOIN nodes n USING(node_id)
        WHERE n.status = 'alive'
          AND d.total_bytes > 0
          AND CAST(d.used_bytes AS REAL) / d.total_bytes < ?
        ORDER BY (d.total_bytes - d.used_bytes) DESC, d.disk_id
        """, (cfg.disk_usage_limit_pct / 100.0,),
    ).fetchall()
    candidates = [dict(r) for r in rows if r['disk_id'] not in excluded_disks
                  and r['node_id'] not in excluded_nodes]
    result: list[dict[str, Any]] = []
    used_disks = set(excluded_disks)
    used_nodes = set(excluded_nodes)

    # Primeira rodada: maximiza a diversidade de máquinas.
    for disk in candidates:
        if disk['node_id'] in used_nodes:
            continue
        result.append(disk)
        used_nodes.add(disk['node_id'])
        used_disks.add(disk['disk_id'])
        if len(result) >= n:
            return result

    # Segunda rodada: somente se faltar máquina; disco distinto é obrigatório.
    for disk in candidates:
        if disk['disk_id'] in used_disks:
            continue
        result.append(disk)
        used_disks.add(disk['disk_id'])
        if len(result) >= n:
            break
    return result




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
    """Chunks CONFIRMADOS abaixo do N próprio do arquivo, do mais crítico ao menos.

    Réplicas em nós dead/draining não contam como disponíveis para reparo.
    """
    rows = conn.execute(
        """
        SELECT c.chunk_id, c.path, c.hash,
               COUNT(CASE WHEN r.state = 'ok' AND n.status IN ('alive', 'suspect')
                          THEN 1 END) AS live_replicas
        FROM chunks c
        JOIN entries e ON e.path = c.path
        LEFT JOIN replicas r ON r.chunk_id = c.chunk_id
        LEFT JOIN disks d ON d.disk_id = r.disk_id
        LEFT JOIN nodes n ON n.node_id = d.node_id
        GROUP BY c.chunk_id
        HAVING live_replicas < COALESCE(e.replication_factor, ?)
        ORDER BY live_replicas ASC, c.path, c.idx
        LIMIT ?
        """, (cfg.replication_factor, limit),
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
        "SELECT COUNT(*) FROM replicas WHERE chunk_id = ? AND state = 'ok'",
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
               n.address AS node_url, n.node_id
        FROM   replicas r
        JOIN   disks    d USING(disk_id)
        JOIN   nodes    n USING(node_id)
        WHERE  r.chunk_id = ?
          AND  r.state    = 'ok'
          AND  n.status   IN ('alive', 'suspect')
        ORDER  BY n.status ASC   -- alive primeiro
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


def _enqueue_cleanup(conn: sqlite3.Connection,
                     rows: list[dict[str, Any]], delay_s: float = 0) -> None:
    """Agenda deleção física durável; o worker tentará novamente se falhar."""
    for row in rows:
        conn.execute(
            """INSERT INTO chunk_cleanup_queue(chunk_id, disk_id, node_url, not_before)
               VALUES(?,?,?,?)
               ON CONFLICT(chunk_id,disk_id) DO UPDATE SET
                 node_url=excluded.node_url,
                 not_before=excluded.not_before""",
            (row['chunk_id'], row['disk_id'], row['node_url'], time.time() + delay_s),
        )


def abort_staged_upload(conn: sqlite3.Connection, path: str, lease_id: str) -> int:
    """Desfaz alocação sob transação aberta, sem afetar versão commitada.

    Todas as réplicas alocadas são agendadas para deleção, inclusive aquelas
    que o cliente conseguiu gravar antes de o lease vencer. Não dá commit;
    chamador é responsável pela atomicidade.
    """
    rows = _rows(conn.execute(
        """SELECT s.chunk_id, r.disk_id, n.address AS node_url
           FROM staged_chunks s JOIN staged_replicas r USING(chunk_id)
           JOIN disks d ON d.disk_id = r.disk_id
           JOIN nodes n ON n.node_id = d.node_id
           WHERE s.path=? AND s.lease_id=?""", (path, lease_id),
    ).fetchall())
    _enqueue_cleanup(conn, rows)
    conn.execute("DELETE FROM staged_chunks WHERE path=? AND lease_id=?",
                 (path, lease_id))
    conn.execute("DELETE FROM leases WHERE path=? AND lease_id=?",
                 (path, lease_id))
    # Apenas entry nova/incompleta deve ser removida: nunca apagar versão anterior.
    conn.execute(
        """DELETE FROM entries WHERE path=? AND state='recovering'
           AND file_hash IS NULL
           AND NOT EXISTS(SELECT 1 FROM chunks WHERE chunks.path=entries.path)""",
        (path,),
    )
    return len(rows)


def cleanup_expired_uploads(conn: sqlite3.Connection, path: str | None = None) -> int:
    """Limpa leases expirados atomicamente, podendo limitar a um path."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        now = time.time()
        if path is None:
            rows = conn.execute(
                'SELECT path, lease_id FROM leases WHERE expires_at<=?', (now,),
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT path, lease_id FROM leases WHERE path=? AND expires_at<=?',
                (path, now),
            ).fetchall()
        for row in rows:
            abort_staged_upload(conn, row['path'], row['lease_id'])
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise


def due_chunk_cleanup(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    """Lê trabalhos de deleção física vencidos (idempotentes)."""
    return _rows(conn.execute(
        """SELECT q.chunk_id, q.disk_id, q.attempts,
                  COALESCE(n.address, q.node_url) AS node_url
           FROM chunk_cleanup_queue q
           LEFT JOIN disks d ON d.disk_id = q.disk_id
           LEFT JOIN nodes n ON n.node_id = d.node_id
           WHERE q.not_before <= ?
           ORDER BY q.not_before LIMIT ?""",
        (time.time(), limit),
    ).fetchall())


def finish_chunk_cleanup(conn: sqlite3.Connection, chunk_id: str,
                         disk_id: str, success: bool, attempts: int = 0) -> None:
    if success:
        conn.execute(
            'DELETE FROM chunk_cleanup_queue WHERE chunk_id=? AND disk_id=?',
            (chunk_id, disk_id),
        )
    else:
        delay = min(60, 2 ** min(attempts + 1, 6))
        conn.execute(
            """UPDATE chunk_cleanup_queue SET attempts=attempts+1,
               not_before=? WHERE chunk_id=? AND disk_id=?""",
            (time.time() + delay, chunk_id, disk_id),
        )
    conn.commit()


def delete_pending_replicas_of_lease(
    conn: sqlite3.Connection, path: str
) -> list[dict[str, Any]]:
    """Compatibilidade: remove apenas staging, nunca chunks já commitados."""
    lease = conn.execute('SELECT lease_id FROM leases WHERE path=?', (path,)).fetchone()
    if lease is None:
        return []
    rows = _rows(conn.execute(
        """SELECT s.chunk_id, r.disk_id, n.address AS node_url
           FROM staged_chunks s JOIN staged_replicas r USING(chunk_id)
           JOIN disks d ON d.disk_id = r.disk_id
           JOIN nodes n ON n.node_id = d.node_id
           WHERE s.path=? AND s.lease_id=?""",
        (path, lease['lease_id']),
    ).fetchall())
    abort_staged_upload(conn, path, lease['lease_id'])
    conn.commit()
    return rows




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

