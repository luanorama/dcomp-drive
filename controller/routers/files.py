"""
controller/routers/files.py
Endpoints de arquivos, diretórios e corrupção de réplicas — Pessoa A.

Rotas implementadas:
  POST   /files/allocate       — aloca chunks e devolve destinos
  POST   /files/commit         — confirma upload com quórum W
  GET    /files/locate         — retorna localização das réplicas para leitura
  GET    /files/stat           — metadados completos do arquivo
  DELETE /files                — remove arquivo
  POST   /files/move           — renomeia/move arquivo

  POST   /dirs                 — cria diretório
  GET    /dirs                 — lista diretório
  DELETE /dirs                 — remove diretório (somente vazio)

  POST   /chunks/corrupt       — marca réplica como corrupt (stub para Pessoa B)
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from controller.config import get_config
from controller.db import db_dependency
from controller.logger import get_logger
from controller import repository as repo

router = APIRouter()
log = get_logger("controller.files")
cfg = get_config()


# ─────────────────────────────────────────────────────────────
# Modelos Pydantic (request / response)
# ─────────────────────────────────────────────────────────────

class AllocateRequest(BaseModel):
    path: str
    size: int = Field(..., ge=0)
    num_chunks: int = Field(..., ge=1)
    client_id: str | None = None
    replication_factor: int | None = Field(default=None, ge=1)


class AllocateTarget(BaseModel):
    node_url: str
    disk_id: str


class AllocateChunk(BaseModel):
    idx: int
    chunk_id: str
    targets: list[AllocateTarget]


class AllocateResponse(BaseModel):
    lease_id: str
    version: int
    warning: str | None = None
    chunks: list[AllocateChunk]


class CommitChunk(BaseModel):
    idx: int
    chunk_id: str
    hash: str
    size: int = Field(..., ge=0)
    confirmed: list[str]  # disk_ids onde o chunk foi gravado com sucesso


class CommitRequest(BaseModel):
    path: str
    lease_id: str
    file_hash: str
    chunks: list[CommitChunk]


class CommitResponse(BaseModel):
    state: str  # "intact" | "under_replicated"


class LocateReplica(BaseModel):
    node_url: str
    disk_id: str


class LocateChunk(BaseModel):
    idx: int
    chunk_id: str
    hash: str
    replicas: list[LocateReplica]


class LocateResponse(BaseModel):
    size: int
    file_hash: str
    chunks: list[LocateChunk]


class StatReplica(BaseModel):
    disk_id: str
    state: str


class StatChunk(BaseModel):
    idx: int
    replicas: list[StatReplica]


class StatResponse(BaseModel):
    path: str
    size: int
    modified_at: float
    file_hash: str | None
    version: int
    state: str
    chunks: list[StatChunk]
    replication_factor: int | None = None


class MoveRequest(BaseModel):
    from_path: str = Field(..., alias="from")
    to_path: str = Field(..., alias="to")

    model_config = {"populate_by_name": True}


class DirCreateRequest(BaseModel):
    path: str


class CorruptRequest(BaseModel):
    chunk_id: str
    disk_id: str


# ─────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────

def _require_alive_lease(
    conn: sqlite3.Connection, path: str, lease_id: str
) -> dict[str, Any]:
    """Valida que o lease_id coincide com o lease ativo do path. HTTP 409 se não."""
    lease = repo.get_lease(conn, path)
    if lease is None or lease["lease_id"] != lease_id:
        raise HTTPException(
            status_code=409,
            detail={"error": f"Lease inválido ou expirado para '{path}'."},
        )
    return lease


def _parent_exists(conn: sqlite3.Connection, path: str) -> bool:
    """Verifica se o diretório pai existe no banco."""
    parent = "/".join(path.rstrip("/").split("/")[:-1]) or "/"
    if parent == "/":
        return True  # raiz sempre existe
    entry = repo.get_entry(conn, parent)
    return entry is not None and entry["type"] == "dir"


async def _notify_delete_chunk(node_url: str, chunk_id: str, disk_id: str) -> None:
    """Envia DELETE /chunks/{chunk_id}?disk_id= ao nó de forma não bloqueante."""
    try:
        async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
            await client.delete(
                f"{node_url}/chunks/{chunk_id}",
                params={"disk_id": disk_id},
            )
    except Exception as exc:  # nó pode estar fora — ignora
        log.warning(
            "chunk_delete_failed",
            chunk_id=chunk_id,
            disk_id=disk_id,
            node_url=node_url,
            error=str(exc),
        )


# ─────────────────────────────────────────────────────────────
# Arquivos — Alocação (RF1, RF3, RF7, RF8)
# ─────────────────────────────────────────────────────────────

@router.post("/files/allocate", response_model=AllocateResponse, status_code=200)
def allocate_file(
    body: AllocateRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> AllocateResponse:
    """Reserva destinos de escrita sem alterar a versão visível do arquivo.

    BEGIN IMMEDIATE serializa escritores concorrentes entre conexões/processos.
    Um novo upload fica isolado em staged_chunks/staged_replicas até o commit.
    """
    path = body.path.rstrip('/')
    if not path.startswith('/') or not path or path == '/' or '//' in path:
        raise HTTPException(status_code=422, detail={'error': 'Caminho de arquivo inválido.'})
    n = body.replication_factor or cfg.replication_factor
    if n < cfg.write_quorum:
        raise HTTPException(status_code=422, detail={
            'error': f'N={n} não pode ser menor que W={cfg.write_quorum}.'})

    lease_id = str(uuid.uuid4())
    now = time.time()
    chunks_out: list[AllocateChunk] = []
    warning: str | None = None
    conn.execute('BEGIN IMMEDIATE')
    try:
        # Um lease expirado é cancelado antes de conceder outro, no mesmo lock.
        expired = conn.execute(
            'SELECT lease_id FROM leases WHERE path=? AND expires_at<=?',
            (path, now),
        ).fetchone()
        if expired:
            repo.abort_staged_upload(conn, path, expired['lease_id'])
        active = conn.execute(
            'SELECT 1 FROM leases WHERE path=?', (path,),
        ).fetchone()
        if active:
            raise HTTPException(status_code=409, detail={
                'error': f"Outro cliente já está gravando em '{path}'."})
        if not _parent_exists(conn, path):
            raise HTTPException(status_code=404, detail={
                'error': f"Diretório pai de '{path}' não existe."})
        existing = repo.get_entry(conn, path)
        if existing and existing['type'] != 'file':
            raise HTTPException(status_code=409, detail={
                'error': f"'{path}' já existe como diretório."})
        version = existing['version'] + 1 if existing else 1
        if existing is None:
            conn.execute(
                """INSERT INTO entries(path,type,size,created_at,modified_at,
                          version,file_hash,state)
                   VALUES(?, 'file', 0, ?, ?, 1, NULL, 'recovering')""",
                (path, now, now),
            )
        # Entry existente não é alterada até o COMMIT (RF8).
        conn.execute(
            """INSERT INTO leases(path,client_id,lease_id,expires_at,
                       replication_factor,expected_size,expected_chunks)
               VALUES(?,?,?,?,?,?,?)""",
            (path, body.client_id, lease_id, now + cfg.lease_ttl_s,
             n, body.size, body.num_chunks),
        )
        for idx in range(body.num_chunks):
            targets = repo.choose_replication_targets(conn, n)
            if len(targets) < cfg.write_quorum or (len(targets) < n and
                    cfg.under_replicated_policy == 'reject'):
                raise HTTPException(status_code=507, detail={
                    'error': f"Destinos insuficientes para '{path}': "
                             f"{len(targets)} disponíveis (N={n}, W={cfg.write_quorum})."})
            if len(targets) < n:
                warning = f'Apenas {len(targets)} discos disponíveis (N={n}); sub-replicado.'
            else:
                node_count = len({target['node_id'] for target in targets})
                if node_count < n:
                    warning = (
                        f'{n} réplicas em apenas {node_count} nós; '
                        'a perda de um nó pode eliminar mais de uma réplica.'
                    )
            chunk_id = str(uuid.uuid4())
            conn.execute(
                'INSERT INTO staged_chunks(chunk_id,path,lease_id,idx) VALUES(?,?,?,?)',
                (chunk_id, path, lease_id, idx),
            )
            for target in targets:
                conn.execute(
                    'INSERT INTO staged_replicas(chunk_id,disk_id) VALUES(?,?)',
                    (chunk_id, target['disk_id']),
                )
            chunks_out.append(AllocateChunk(
                idx=idx, chunk_id=chunk_id,
                targets=[AllocateTarget(node_url=t['node_url'], disk_id=t['disk_id'])
                         for t in targets],
            ))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    log.info('file_allocated', path=path, lease_id=lease_id, version=version,
             num_chunks=body.num_chunks, replication_factor=n, warning=warning)
    return AllocateResponse(lease_id=lease_id, version=version,
                            warning=warning, chunks=chunks_out)




# ─────────────────────────────────────────────────────────────
# Arquivos — Commit (RF3, RF8)
# ─────────────────────────────────────────────────────────────

@router.post("/files/commit", response_model=CommitResponse, status_code=200)
def commit_file(
    body: CommitRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> CommitResponse:
    """Valida todo o upload antes de publicar os novos metadados atomicamente.

    Em falha de quórum, o staging é apagado e a versão anterior permanece.
    """
    conn.execute('BEGIN IMMEDIATE')
    try:
        lease = conn.execute(
            """SELECT * FROM leases
               WHERE path=? AND lease_id=? AND expires_at>?""",
            (body.path, body.lease_id, time.time()),
        ).fetchone()
        if not lease:
            raise HTTPException(status_code=409, detail={
                'error': 'Lease inválido ou expirado.'})
        staged = conn.execute(
            'SELECT * FROM staged_chunks WHERE path=? AND lease_id=? ORDER BY idx',
            (body.path, body.lease_id),
        ).fetchall()
        if not staged or len(staged) != len(body.chunks):
            raise HTTPException(status_code=422, detail={
                'error': 'A lista de chunks não corresponde à alocação.'})
        if lease['expected_chunks'] is not None and len(staged) != lease['expected_chunks']:
            raise HTTPException(status_code=422, detail={
                'error': 'Quantidade de chunks incompatível com o lease.'})
        supplied = {c.idx: c for c in body.chunks}
        if len(supplied) != len(staged):
            raise HTTPException(status_code=422, detail={
                'error': 'Índices de chunks repetidos.'})
        n = int(lease['replication_factor'])
        w = cfg.write_quorum
        total_size = 0
        under_replicated = False
        confirmations: dict[str, set[str]] = {}
        unconfirmed: list[CommitChunk] = []
        for expected in staged:
            chunk = supplied.get(expected['idx'])
            if chunk is None or chunk.chunk_id != expected['chunk_id']:
                raise HTTPException(status_code=422, detail={
                    'error': 'chunk_id/idx não pertence a este lease e path.'})
            assigned = conn.execute(
                """SELECT r.disk_id, d.total_bytes, n.status FROM staged_replicas r
                   JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
                   WHERE r.chunk_id=?""",
                (chunk.chunk_id,),
            ).fetchall()
            disks = {r['disk_id'] for r in assigned}
            provided = set(chunk.confirmed)
            if len(chunk.confirmed) != len(provided) or not provided <= disks:
                raise HTTPException(status_code=422, detail={
                    'error': 'Discos confirmados não pertencem à alocação ou estão repetidos.'})
            available = {r['disk_id'] for r in assigned
                         if r['status'] != 'dead' and r['total_bytes'] > 0}
            confirmed = provided & available
            confirmations[chunk.chunk_id] = confirmed
            total_size += chunk.size
            if len(confirmed) < w:
                unconfirmed.append(chunk)
            if len(confirmed) < n:
                under_replicated = True
        if lease['expected_size'] is not None and total_size != lease['expected_size']:
            raise HTTPException(status_code=422, detail={
                'error': 'Tamanho confirmado difere do tamanho informado na alocação.'})
        if unconfirmed:
            failed = unconfirmed[0]
            repo.abort_staged_upload(conn, body.path, body.lease_id)
            conn.commit()
            raise HTTPException(status_code=507, detail={
                'error': f'Quórum não atingido no chunk {failed.idx}: '
                         f'{len(confirmations[failed.chunk_id])}/{w} confirmações.'})

        # Agenda remoção das réplicas antigas com prazo de leitura em andamento.
        old = [dict(r) for r in conn.execute(
            """SELECT r.chunk_id,r.disk_id,n.address AS node_url
               FROM chunks c JOIN replicas r USING(chunk_id)
               JOIN disks d USING(disk_id) JOIN nodes n USING(node_id)
               WHERE c.path=?""", (body.path,),
        ).fetchall()]
        repo._enqueue_cleanup(conn, old, delay_s=60)
        conn.execute('DELETE FROM chunks WHERE path=?', (body.path,))
        for expected in staged:
            chunk = supplied[expected['idx']]
            conn.execute(
                'INSERT INTO chunks(chunk_id,path,idx,size,hash) VALUES(?,?,?,?,?)',
                (chunk.chunk_id, body.path, chunk.idx, chunk.size, chunk.hash),
            )
            allocated = conn.execute(
                'SELECT disk_id FROM staged_replicas WHERE chunk_id=?',
                (chunk.chunk_id,),
            ).fetchall()
            for disk in allocated:
                state = 'ok' if disk['disk_id'] in confirmations[chunk.chunk_id] else 'lost'
                conn.execute(
                    'INSERT INTO replicas(chunk_id,disk_id,state) VALUES(?,?,?)',
                    (chunk.chunk_id, disk['disk_id'], state),
                )
        state = 'under_replicated' if under_replicated else 'intact'
        conn.execute(
            """UPDATE entries SET file_hash=?,size=?,modified_at=?,
                      state=?,replication_factor=?,
                      version=CASE WHEN file_hash IS NULL THEN 1 ELSE version+1 END
               WHERE path=?""",
            (body.file_hash, total_size, time.time(), state, n, body.path),
        )
        conn.execute('DELETE FROM staged_chunks WHERE path=? AND lease_id=?',
                     (body.path, body.lease_id))
        conn.execute('DELETE FROM leases WHERE path=? AND lease_id=?',
                     (body.path, body.lease_id))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    log.info('file_committed', path=body.path, state=state,
             file_hash=body.file_hash, num_chunks=len(staged))
    return CommitResponse(state=state)




# ─────────────────────────────────────────────────────────────
# Arquivos — Localização para leitura (RF1)
# ─────────────────────────────────────────────────────────────

@router.get("/files/locate", response_model=LocateResponse)
def locate_file(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> LocateResponse:
    """Lê uma snapshot SQLite consistente, mesmo durante sobrescrita."""
    conn.execute('BEGIN')
    try:
        entry = repo.get_entry(conn, path)
        if entry is None or entry['type'] != 'file':
            raise HTTPException(status_code=404, detail={'error': 'Arquivo não encontrado.'})
        if entry['file_hash'] is None:
            raise HTTPException(status_code=409, detail={
                'error': 'Arquivo ainda não possui versão confirmada.'})
        chunks = conn.execute(
            'SELECT chunk_id,idx,hash FROM chunks WHERE path=? ORDER BY idx',
            (path,),
        ).fetchall()
        located: list[LocateChunk] = []
        for chunk in chunks:
            replicas = repo.get_live_replicas(conn, chunk['chunk_id'])
            if not replicas:
                raise HTTPException(status_code=503, detail={
                    'error': f"Chunk {chunk['idx']} sem réplicas disponíveis."})
            located.append(LocateChunk(
                idx=chunk['idx'], chunk_id=chunk['chunk_id'], hash=chunk['hash'] or '',
                replicas=[LocateReplica(node_url=r['node_url'], disk_id=r['disk_id'])
                          for r in replicas],
            ))
        result = LocateResponse(size=entry['size'], file_hash=entry['file_hash'] or '',
                                chunks=located)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise




# ─────────────────────────────────────────────────────────────
# Arquivos — Metadados (RF1)
# ─────────────────────────────────────────────────────────────

@router.get("/files/stat", response_model=StatResponse)
def stat_file(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> StatResponse:
    """Metadados de uma única snapshot de leitura."""
    conn.execute('BEGIN')
    try:
        entry = repo.get_entry(conn, path)
        if entry is None or entry['type'] != 'file':
            raise HTTPException(status_code=404, detail={'error': 'Arquivo não encontrado.'})
        chunks = conn.execute(
            'SELECT chunk_id, idx FROM chunks WHERE path=? ORDER BY idx', (path,),
        ).fetchall()
        details = []
        for chunk in chunks:
            replicas = conn.execute(
                'SELECT disk_id,state FROM replicas WHERE chunk_id=?',
                (chunk['chunk_id'],),
            ).fetchall()
            details.append(StatChunk(
                idx=chunk['idx'],
                replicas=[StatReplica(disk_id=r['disk_id'],state=r['state'])
                          for r in replicas],
            ))
        result = StatResponse(path=entry['path'],size=entry['size'],
                              modified_at=entry['modified_at'],file_hash=entry['file_hash'],
                              version=entry['version'],state=entry['state'],
                              chunks=details, replication_factor=entry['replication_factor'])
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise




# ─────────────────────────────────────────────────────────────
# Arquivos — Remoção (RF1)
# ─────────────────────────────────────────────────────────────

@router.delete("/files")
async def delete_file(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """
    Remove arquivo, seus chunks e réplicas do banco.
    Envia DELETE /chunks/{chunk_id} para cada réplica 'ok' nos nós.
    """
    entry = repo.get_entry(conn, path)
    if entry is None or entry["type"] != "file":
        raise HTTPException(status_code=404, detail={"error": f"Arquivo '{path}' não encontrado."})

    # Verifica lease ativo (409 se outro escritor)
    lease = repo.get_lease(conn, path)
    if lease is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": f"Arquivo '{path}' está sendo gravado. Não é possível remover agora."},
        )

    # Coleta réplicas para deletar nos nós
    to_delete = conn.execute(
        """
        SELECT r.chunk_id, r.disk_id, n.address AS node_url
        FROM   replicas r
        JOIN   chunks   c USING(chunk_id)
        JOIN   disks    d USING(disk_id)
        JOIN   nodes    n ON n.node_id = d.node_id
        WHERE  c.path = ? AND r.state IN ('ok', 'pending')
        """,
        (path,),
    ).fetchall()

    # Remove do banco (cascata remove chunks e réplicas via FK)
    conn.execute("DELETE FROM entries WHERE path = ?", (path,))
    conn.commit()

    # Notifica nós (best-effort — não bloqueia resposta ao cliente)
    for row in to_delete:
        await _notify_delete_chunk(row["node_url"], row["chunk_id"], row["disk_id"])

    log.info("file_deleted", path=path)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Arquivos — Renomear/Mover (RF1)
# ─────────────────────────────────────────────────────────────

@router.post("/files/move")
def move_file(
    body: MoveRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Renomeia ou move um arquivo. O destino não pode já existir."""
    src = repo.get_entry(conn, body.from_path)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"'{body.from_path}' não encontrado."})

    dst = repo.get_entry(conn, body.to_path)
    if dst is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": f"'{body.to_path}' já existe."},
        )

    if not _parent_exists(conn, body.to_path):
        raise HTTPException(
            status_code=404,
            detail={"error": f"Diretório pai de '{body.to_path}' não existe."},
        )

    # Verifica lease ativo no arquivo de origem
    lease = repo.get_lease(conn, body.from_path)
    if lease is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": f"Arquivo '{body.from_path}' está sendo gravado."},
        )

    repo.rename_entry(conn, body.from_path, body.to_path)
    log.info("file_moved", from_path=body.from_path, to_path=body.to_path)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Diretórios (RF1)
# ─────────────────────────────────────────────────────────────

@router.post("/dirs", status_code=201)
def create_dir(
    body: DirCreateRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Cria um diretório. O diretório pai deve existir."""
    path = body.path.rstrip("/")

    if repo.get_entry(conn, path) is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": f"'{path}' já existe."},
        )

    if not _parent_exists(conn, path):
        raise HTTPException(
            status_code=404,
            detail={"error": f"Diretório pai de '{path}' não existe."},
        )

    repo.create_dir(conn, path)
    log.info("dir_created", path=path)
    return {"ok": True}


@router.get("/dirs")
def list_dir(
    path: str = "/",
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, list]:
    """Lista o conteúdo imediato de um diretório."""
    path = path.rstrip("/") or "/"

    # Raiz não precisa estar na tabela
    if path != "/":
        entry = repo.get_entry(conn, path)
        if entry is None or entry["type"] != "dir":
            raise HTTPException(
                status_code=404,
                detail={"error": f"Diretório '{path}' não encontrado."},
            )

    entries = repo.list_dir_entries(conn, path)
    return {
        "entries": [
            {"name": e["name"], "type": e["type"], "size": e["size"]}
            for e in entries
        ]
    }


@router.delete("/dirs")
def remove_dir(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """Remove diretório somente se estiver vazio."""
    path = path.rstrip("/")
    entry = repo.get_entry(conn, path)
    if entry is None or entry["type"] != "dir":
        raise HTTPException(
            status_code=404,
            detail={"error": f"Diretório '{path}' não encontrado."},
        )

    if not repo.delete_dir(conn, path):
        raise HTTPException(
            status_code=409,
            detail={"error": f"Diretório '{path}' não está vazio."},
        )

    log.info("dir_removed", path=path)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Corrupção de réplica — stub para a Pessoa B (RF4)
# ─────────────────────────────────────────────────────────────

@router.post("/chunks/corrupt")
def report_corrupt(
    body: CorruptRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> dict[str, bool]:
    """
    Marca uma réplica como corrompida.
    Chamado pelo cliente (get) ou pelo nó (scrubbing) ao detectar hash errado.
    A re-replicação será disparada pela Pessoa C ao encontrar réplicas corrupt.
    """
    # Verifica que o chunk existe
    chunk = repo.get_chunk(conn, body.chunk_id)
    if chunk is None:
        raise HTTPException(
            status_code=404,
            detail={"error": f"Chunk '{body.chunk_id}' não encontrado."},
        )

    repo.mark_replica_as(conn, body.chunk_id, body.disk_id, "corrupt")

    # Atualiza estado do arquivo para 'recovering' se necessário
    live = repo.count_live_replicas(conn, body.chunk_id)
    entry = repo.get_entry(conn, chunk['path']) or {}
    desired_n = entry.get('replication_factor') or cfg.replication_factor
    if live < desired_n:
        repo.update_entry_state(conn, chunk["path"], "under_replicated")

    log.warning(
        "replica_reported_corrupt",
        chunk_id=body.chunk_id,
        disk_id=body.disk_id,
        live_replicas=live,
    )
    return {"ok": True}
