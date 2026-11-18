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
        async with httpx.AsyncClient(timeout=5.0) as client:
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
    """
    Aloca espaço para um novo arquivo:
    1. Verifica conflito de lease (409 se outro escritor ativo).
    2. Cria lease de escrita.
    3. Para cada chunk, escolhe N destinos pelo algoritmo de posicionamento.
    4. Registra chunks e réplicas como 'pending' no banco.
    5. Retorna a lista de destinos para o cliente gravar diretamente.

    O diretório pai deve existir previamente.
    """
    path = body.path.rstrip("/")

    # Verifica diretório pai
    if not _parent_exists(conn, path):
        raise HTTPException(
            status_code=404,
            detail={"error": f"Diretório pai de '{path}' não existe."},
        )

    # Verifica se já existe arquivo commitado (não impede sobrescrita, mas
    # exige que não haja outro lease ativo — RF8)
    existing_lease = repo.get_lease(conn, path)
    if existing_lease is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": f"Outro cliente já está gravando em '{path}'."},
        )

    # Cria entry do tipo file (ou substitui versão anterior)
    existing = repo.get_entry(conn, path)
    version = (existing["version"] + 1) if existing else 1
    now = time.time()

    if existing is None:
        conn.execute(
            """
            INSERT INTO entries(path, type, size, created_at, modified_at, version, file_hash, state)
            VALUES(?, 'file', ?, ?, ?, ?, NULL, 'recovering')
            """,
            (path, body.size, now, now, version),
        )
    else:
        if existing["type"] != "file":
            raise HTTPException(
                status_code=409,
                detail={"error": f"'{path}' já existe como diretório."},
            )
        conn.execute(
            """
            UPDATE entries
            SET size = ?, modified_at = ?, version = ?, state = 'recovering'
            WHERE path = ?
            """,
            (body.size, now, version, path),
        )
    conn.commit()

    # Cria lease
    lease_id = str(uuid.uuid4())
    if not repo.create_lease(conn, path, lease_id, body.client_id):
        raise HTTPException(
            status_code=409,
            detail={"error": f"Conflito de lease para '{path}'."},
        )

    # Alocação de destinos por chunk
    n = cfg.replication_factor
    warning: str | None = None
    chunks_out: list[AllocateChunk] = []

    for idx in range(body.num_chunks):
        chunk_id = str(uuid.uuid4())

        # Escolhe N destinos sem repetir disco nem máquina
        targets = repo.choose_replication_targets(conn, n)

        if len(targets) < n:
            if len(targets) == 0:
                # Sem nenhum nó disponível — aborta
                repo.delete_lease(conn, path)
                raise HTTPException(
                    status_code=507,
                    detail={
                        "error": (
                            f"Sem nós de armazenamento disponíveis para "
                            f"{n} réplicas."
                        )
                    },
                )
            if cfg.under_replicated_policy == "reject":
                repo.delete_lease(conn, path)
                raise HTTPException(
                    status_code=507,
                    detail={
                        "error": (
                            f"Nós insuficientes: {len(targets)} disponíveis, "
                            f"{n} necessários (política=reject)."
                        )
                    },
                )
            # Política accept: continua como sub-replicado
            warning = (
                f"Apenas {len(targets)} nó(s) disponível(eis); "
                f"arquivo será sub-replicado (N={n})."
            )

        # Registra chunk e réplicas pending no banco
        conn.execute(
            "INSERT INTO chunks(chunk_id, path, idx, size) VALUES(?,?,?,0)",
            (chunk_id, path, idx),
        )
        for t in targets:
            conn.execute(
                "INSERT INTO replicas(chunk_id, disk_id, state) VALUES(?,?,'pending')",
                (chunk_id, t["disk_id"]),
            )

        chunks_out.append(
            AllocateChunk(
                idx=idx,
                chunk_id=chunk_id,
                targets=[
                    AllocateTarget(node_url=t["node_url"], disk_id=t["disk_id"])
                    for t in targets
                ],
            )
        )

    conn.commit()
    log.info(
        "file_allocated",
        path=path,
        lease_id=lease_id,
        num_chunks=body.num_chunks,
        version=version,
        warning=warning,
    )

    return AllocateResponse(
        lease_id=lease_id,
        version=version,
        warning=warning,
        chunks=chunks_out,
    )


# ─────────────────────────────────────────────────────────────
# Arquivos — Commit (RF3, RF8)
# ─────────────────────────────────────────────────────────────

@router.post("/files/commit", response_model=CommitResponse, status_code=200)
def commit_file(
    body: CommitRequest,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> CommitResponse:
    """
    Confirma um upload:
    1. Valida o lease.
    2. Para cada chunk, verifica se atingiu o quórum W de confirmações.
    3. Atualiza hash, tamanho e estado das réplicas no banco.
    4. Se quórum atingido para todos os chunks → arquivo fica 'intact'
       (ou 'under_replicated' se algum chunk ficou abaixo de N réplicas).
    5. Remove o lease.
    6. Libera o estado 'recovering' da entry.

    Se quórum W não for atingido em nenhum chunk → HTTP 507.
    """
    path = body.path
    _require_alive_lease(conn, path, body.lease_id)

    n = cfg.replication_factor
    w = cfg.write_quorum
    any_under_replicated = False

    for chunk_info in body.chunks:
        confirmed = chunk_info.confirmed  # lista de disk_ids onde gravou com sucesso
        if len(confirmed) < w:
            # Quórum não atingido — aborta tudo
            repo.delete_lease(conn, path)
            # Reverte entry para estado anterior (remove ou deixa como sub-replicado)
            existing = repo.get_entry(conn, path)
            if existing and existing["version"] == 1:
                # Primeira versão nunca commitada → remove entry
                conn.execute("DELETE FROM entries WHERE path = ?", (path,))
            else:
                conn.execute(
                    "UPDATE entries SET state = 'under_replicated' WHERE path = ?",
                    (path,),
                )
            conn.commit()
            raise HTTPException(
                status_code=507,
                detail={
                    "error": (
                        f"Quórum de escrita não atingido no chunk {chunk_info.idx}: "
                        f"{len(confirmed)}/{w} réplicas confirmadas."
                    )
                },
            )

        # Atualiza hash e tamanho do chunk
        conn.execute(
            "UPDATE chunks SET hash = ?, size = ? WHERE chunk_id = ?",
            (chunk_info.hash, chunk_info.size, chunk_info.chunk_id),
        )

        # Marca réplicas confirmadas como 'ok' e as não confirmadas como 'lost'
        for disk_id in confirmed:
            conn.execute(
                "UPDATE replicas SET state = 'ok' WHERE chunk_id = ? AND disk_id = ?",
                (chunk_info.chunk_id, disk_id),
            )
        # Réplicas pending que não foram confirmadas
        conn.execute(
            """
            UPDATE replicas SET state = 'lost'
            WHERE  chunk_id = ? AND state = 'pending'
              AND  disk_id  NOT IN ({})
            """.format(",".join("?" * len(confirmed))),
            [chunk_info.chunk_id] + list(confirmed),
        )

        # Verifica se ficou sub-replicado
        if len(confirmed) < n:
            any_under_replicated = True

    # Atualiza a entry com hash do arquivo, tamanho e versão final
    total_size = sum(c.size for c in body.chunks)
    final_state = "under_replicated" if any_under_replicated else "intact"
    now = time.time()
    conn.execute(
        """
        UPDATE entries
        SET file_hash = ?, size = ?, modified_at = ?, state = ?
        WHERE path = ?
        """,
        (body.file_hash, total_size, now, final_state, path),
    )

    # Remove lease
    repo.delete_lease(conn, path)
    conn.commit()

    log.info(
        "file_committed",
        path=path,
        file_hash=body.file_hash,
        state=final_state,
        num_chunks=len(body.chunks),
    )
    return CommitResponse(state=final_state)


# ─────────────────────────────────────────────────────────────
# Arquivos — Localização para leitura (RF1)
# ─────────────────────────────────────────────────────────────

@router.get("/files/locate", response_model=LocateResponse)
def locate_file(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> LocateResponse:
    """
    Devolve a localização de todos os chunks com suas réplicas vivas.
    O cliente usa esta resposta para ler diretamente dos nós.
    Leitura só vê a última versão commitada (state != 'recovering').
    """
    entry = repo.get_entry(conn, path)
    if entry is None or entry["type"] != "file":
        raise HTTPException(status_code=404, detail={"error": f"Arquivo '{path}' não encontrado."})
    if entry["state"] == "recovering":
        raise HTTPException(
            status_code=409,
            detail={"error": f"Arquivo '{path}' está sendo gravado. Tente novamente em instantes."},
        )

    chunks = conn.execute(
        "SELECT chunk_id, idx, hash FROM chunks WHERE path = ? ORDER BY idx",
        (path,),
    ).fetchall()

    located: list[LocateChunk] = []
    for c in chunks:
        replicas = repo.get_live_replicas(conn, c["chunk_id"])
        if not replicas:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": (
                        f"Chunk {c['idx']} do arquivo '{path}' não tem réplicas vivas no momento."
                    )
                },
            )
        located.append(
            LocateChunk(
                idx=c["idx"],
                chunk_id=c["chunk_id"],
                hash=c["hash"] or "",
                replicas=[
                    LocateReplica(node_url=r["node_url"], disk_id=r["disk_id"])
                    for r in replicas
                ],
            )
        )

    return LocateResponse(
        size=entry["size"],
        file_hash=entry["file_hash"] or "",
        chunks=located,
    )


# ─────────────────────────────────────────────────────────────
# Arquivos — Metadados (RF1)
# ─────────────────────────────────────────────────────────────

@router.get("/files/stat", response_model=StatResponse)
def stat_file(
    path: str,
    conn: sqlite3.Connection = Depends(db_dependency),
) -> StatResponse:
    """Retorna metadados completos do arquivo: tamanho, hash, versão, estado e réplicas."""
    entry = repo.get_entry(conn, path)
    if entry is None or entry["type"] != "file":
        raise HTTPException(status_code=404, detail={"error": f"Arquivo '{path}' não encontrado."})

    chunks = conn.execute(
        "SELECT chunk_id, idx FROM chunks WHERE path = ? ORDER BY idx", (path,)
    ).fetchall()

    stat_chunks: list[StatChunk] = []
    for c in chunks:
        replicas = conn.execute(
            "SELECT disk_id, state FROM replicas WHERE chunk_id = ?", (c["chunk_id"],)
        ).fetchall()
        stat_chunks.append(
            StatChunk(
                idx=c["idx"],
                replicas=[StatReplica(disk_id=r["disk_id"], state=r["state"]) for r in replicas],
            )
        )

    return StatResponse(
        path=entry["path"],
        size=entry["size"],
        modified_at=entry["modified_at"],
        file_hash=entry["file_hash"],
        version=entry["version"],
        state=entry["state"],
        chunks=stat_chunks,
    )


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
    if live < cfg.replication_factor:
        repo.update_entry_state(conn, chunk["path"], "under_replicated")

    log.warning(
        "replica_reported_corrupt",
        chunk_id=body.chunk_id,
        disk_id=body.disk_id,
        live_replicas=live,
    )
    return {"ok": True}
