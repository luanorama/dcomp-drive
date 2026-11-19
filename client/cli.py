"""Interface de linha de comando do DComp Drive."""
from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import httpx

from common.jsonlog import get_logger
from controller.config import get_config
from client.api import ApiError, ControllerClient
from client.transfer import TransferError, get_file, put_file

log = get_logger("client.cli")

_STATE_PT = {"intact": "íntegro", "under_replicated": "sub-replicado",
             "recovering": "em recuperação", "alive": "vivo", "suspect": "suspeito",
             "dead": "morto", "draining": "drenando"}


class UsageError(ValueError):
    """Argumento do usuário inválido sem falha operacional."""


def validate_remote_path(path: str, *, allow_root: bool = False) -> str:
    """Confere o formato absoluto de caminhos remotos do controlador."""
    if allow_root and path == "/":
        return path
    if (not path.startswith("/") or "//" in path or path.endswith("/") or
            path == "/" or "\x00" in path):
        raise UsageError(
            "caminho remoto inválido: use / inicial, sem // e sem / final "
            "(somente ls aceita /)"
        )
    return path


def _size(value: int | float | None) -> str:
    """Formata bytes para leitura humana."""
    size = float(value or 0)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TiB"


def _date(value: Any) -> str:
    """Converte epoch do controlador para ISO-8601 no fuso local."""
    try:
        return datetime.fromtimestamp(float(value)).astimezone().isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError, OverflowError):
        return "desconhecida"


def _human_ls(result: dict[str, Any]) -> str:
    lines: list[str] = []
    for entry in result.get("entries", []):
        name = str(entry.get("name", "?"))
        if entry.get("type") == "dir":
            lines.append(f"d {name}/")
        else:
            lines.append(f"- {name}  {_size(entry.get('size'))}")
    return "\n".join(lines) if lines else "(vazio)"


def _human_stat(meta: dict[str, Any]) -> str:
    chunks = meta.get("chunks", [])
    replication_factor = int(meta.get("replication_factor") or 0)
    lines = [
        f"Caminho: {meta.get('path', '?')}",
        f"Tamanho armazenado: {_size(meta.get('size'))}",
        f"Modificado: {_date(meta.get('modified_at'))}",
        f"Hash: {meta.get('file_hash') or 'indisponível'}",
        f"Versão: {meta.get('version', '?')}",
        f"Estado: {_STATE_PT.get(str(meta.get('state')), meta.get('state', '?'))}",
        f"Réplicas desejadas: {replication_factor or '?'}",
    ]
    ok_counts: list[int] = []
    for chunk in chunks:
        replicas = chunk.get("replicas", [])
        ok_counts.append(sum(1 for replica in replicas if replica.get("state") == "ok"))
        lines.append(f"Chunk {chunk.get('idx', '?')}:")
        for replica in replicas:
            state = str(replica.get("state", "?"))
            lines.append(f"  {replica.get('disk_id', '?')} — {_STATE_PT.get(state, state)}")
    if chunks:
        minimum = min(ok_counts)
        lines.append(f"Réplicas ok mín./N: {minimum}/{replication_factor or '?'}")
    return "\n".join(lines)


def _human_status(status: dict[str, Any]) -> str:
    lines: list[str] = ["Nós:"]
    nodes = status.get("nodes", [])
    for node in nodes if isinstance(nodes, list) else []:
        state = str(node.get("status", "?"))
        lines.append(
            f"  {node.get('node_id', '?')} — {_STATE_PT.get(state, state)} — "
            f"{node.get('address', '?')} — heartbeat {_date(node.get('last_heartbeat'))}"
        )
        disks = node.get("disks", [])
        for disk in disks if isinstance(disks, list) else []:
            total = int(disk.get("total_bytes") or 0)
            used = int(disk.get("used_bytes") or 0)
            percent = (used / total * 100) if total else 0
            lines.append(f"    {disk.get('disk_id', '?')}: {_size(used)}/{_size(total)} ({percent:.1f}%)")
    counts = status.get("files_by_state", {})
    lines.append("Arquivos:")
    if isinstance(counts, dict):
        for state, count in sorted(counts.items()):
            lines.append(f"  {_STATE_PT.get(str(state), state)}: {count}")
    rereps = status.get("rereplications", [])
    lines.append(f"Re-replicações em andamento: {len(rereps) if isinstance(rereps, (list, dict)) else 0}")
    if isinstance(rereps, list):
        for item in rereps:
            lines.append("  " + json.dumps(item, ensure_ascii=False, separators=(",", ":")))
    elif isinstance(rereps, dict):
        for key, item in rereps.items():
            lines.append(f"  {key}: {json.dumps(item, ensure_ascii=False, separators=(',', ':'))}")
    elif rereps is not None:
        lines.append("  " + json.dumps(rereps, ensure_ascii=False, separators=(",", ":")))
    return "\n".join(lines)


def _summary(summary: dict[str, Any], *, operation: str) -> str:
    duration = float(summary.get("duration_s", 0.0))
    throughput = float(summary.get("throughput_mb_s", 0.0))
    if operation == "put":
        state = str(summary.get("state", "?"))
        state_pt = _STATE_PT.get(state, state)
        text = (f"OK {summary['path']} — {_size(summary['bytes'])}, {summary['chunks']} chunks, "
                f"{state_pt}, réplicas {summary['replicas_min']}/{summary['replication_factor']}, "
                f"{duration:.2f} s, {throughput:.2f} MiB/s")
    else:
        text = (f"OK {summary['path']} para {summary['destination']} — {_size(summary['bytes'])}, "
                f"{summary['chunks']} chunks, {duration:.2f} s, {throughput:.2f} MiB/s")
    warning = summary.get("warning")
    return text + (f"\nAVISO: {warning}" if warning else "")


def _common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--controller", default=argparse.SUPPRESS,
                        help="URL do controlador")
    parser.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                        help="imprime uma resposta JSON")
    parser.add_argument("--verbose", action="store_true", default=argparse.SUPPRESS,
                        help="mostra logs detalhados")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dcomp-cli", description="Cliente do DComp Drive")
    parser.add_argument("--controller", help="URL do controlador")
    parser.add_argument("--json", action="store_true", help="imprime uma resposta JSON")
    parser.add_argument("--verbose", action="store_true", help="mostra logs detalhados")
    sub = parser.add_subparsers(dest="command", required=True)

    command = sub.add_parser("mkdir", help="cria diretório")
    command.add_argument("path")
    _common_options(command)
    command = sub.add_parser("put", help="envia arquivo")
    command.add_argument("remote")
    command.add_argument("local")
    command.add_argument("--replicas", type=int)
    _common_options(command)
    command = sub.add_parser("get", help="baixa arquivo")
    command.add_argument("remote")
    command.add_argument("destination")
    _common_options(command)
    command = sub.add_parser("ls", help="lista diretório")
    command.add_argument("path", nargs="?", default="/")
    _common_options(command)
    command = sub.add_parser("stat", help="consulta metadados")
    command.add_argument("remote")
    _common_options(command)
    command = sub.add_parser("mv", help="move arquivo")
    command.add_argument("source")
    command.add_argument("destination")
    _common_options(command)
    command = sub.add_parser("rm", help="remove arquivo")
    command.add_argument("remote")
    _common_options(command)
    command = sub.add_parser("rmdir", help="remove diretório vazio")
    command.add_argument("path")
    _common_options(command)
    command = sub.add_parser("status", help="mostra estado do cluster")
    _common_options(command)
    command = sub.add_parser("drain", help="esvazia nó")
    command.add_argument("node_id")
    _common_options(command)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Executa um comando e devolve o código de saída da CLI."""
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    if getattr(args, "verbose", False):
        os.environ["DCOMP_LOG_LEVEL"] = "INFO"
    try:
        if args.command == "ls":
            validate_remote_path(args.path, allow_root=True)
        elif args.command in ("mkdir", "rmdir"):
            validate_remote_path(args.path)
        elif args.command in ("put", "get"):
            validate_remote_path(args.remote)
        elif args.command in ("stat", "rm"):
            validate_remote_path(args.remote)
        elif args.command == "mv":
            validate_remote_path(args.source)
            validate_remote_path(args.destination)
        if args.command == "put" and args.replicas is not None and args.replicas < 1:
            raise UsageError("--replicas precisa ser maior que zero")
    except UsageError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 2

    cfg = get_config()
    controller_url = getattr(args, "controller", None) or cfg.controller_url
    client = ControllerClient(controller_url)
    try:
        if args.command == "mkdir":
            result = client.mkdir(args.path)
            human = "OK diretório criado"
        elif args.command == "put":
            result = put_file(args.local, args.remote, client,
                              replication_factor=args.replicas)
            human = _summary(result, operation="put")
        elif args.command == "get":
            result = get_file(args.remote, args.destination, client)
            human = _summary(result, operation="get")
        elif args.command == "ls":
            result = client.list_dir(args.path)
            human = _human_ls(result)
        elif args.command == "stat":
            try:
                result = client.stat(args.remote)
                human = _human_stat(result)
            except ApiError as exc:
                if exc.status != 404:
                    raise
                try:
                    client.list_dir(args.remote)
                except ApiError:
                    raise TransferError("Arquivo não encontrado.") from exc
                raise TransferError("O caminho é um diretório; use ls.") from exc
        elif args.command == "mv":
            result = client.move(args.source, args.destination)
            human = "OK"
        elif args.command == "rm":
            try:
                result = client.remove_file(args.remote)
                human = "OK"
            except ApiError as exc:
                if exc.status != 404:
                    raise
                try:
                    client.list_dir(args.remote)
                except ApiError:
                    raise TransferError("Arquivo não encontrado.") from exc
                raise TransferError("O caminho é um diretório; use rmdir.") from exc
        elif args.command == "rmdir":
            result = client.remove_dir(args.path)
            human = "OK"
        elif args.command == "status":
            result = client.status()
            human = _human_status(result)
        elif args.command == "drain":
            result = client.drain(args.node_id)
            human = f"OK, {result.get('queued_chunks', 0)} chunks enfileirados"
        else:
            raise UsageError("comando desconhecido")
        if getattr(args, "json", False):
            print(json.dumps(result, ensure_ascii=False, default=str))
        elif human:
            print(human)
        return 0
    except KeyboardInterrupt:
        print("ERRO: operação cancelada pelo usuário.", file=sys.stderr)
        log.warning("op_failed", op=args.command, error="interrompido")
        return 130
    except (TransferError, ApiError, OSError, httpx.HTTPError) as exc:
        message = exc.message if isinstance(exc, ApiError) else str(exc)
        print(f"ERRO: {message}", file=sys.stderr)
        log.warning("op_failed", op=args.command, error=message)
        if getattr(args, "verbose", False):
            traceback.print_exc(file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERRO: falha inesperada: {exc}", file=sys.stderr)
        log.error("op_failed", op=args.command, error=str(exc))
        if getattr(args, "verbose", False):
            traceback.print_exc(file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
