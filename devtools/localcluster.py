"""Controlador real e nós HTTP reais para testes e demonstrações locais."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from common.jsonlog import get_logger

log = get_logger("devtools.localcluster")


class LocalCluster:
    """Sobe um controlador e vários nós em subprocessos isolados."""

    def __init__(self, base_dir: str | Path, n_nodes: int = 5,
                 disks_per_node: int = 2, env: dict[str, str] | None = None) -> None:
        if n_nodes < 1 or disks_per_node < 1:
            raise ValueError("O cluster precisa de pelo menos um nó e um disco por nó.")
        self.base_dir = Path(base_dir).expanduser().resolve()
        self.n_nodes = n_nodes
        self.disks_per_node = disks_per_node
        self.env = {str(key): str(value) for key, value in (env or {}).items()}
        self.root = Path(__file__).resolve().parent.parent
        self.controller_port = 0
        self.node_ports: dict[int, int] = {}
        self.controller_process: subprocess.Popen[bytes] | None = None
        self.node_processes: dict[int, subprocess.Popen[bytes]] = {}
        self.controller_url = ""
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._started = False

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _base_env(self) -> dict[str, str]:
        result = os.environ.copy()
        result.update({
            "PYTHONPATH": str(self.root) + (os.pathsep + os.environ["PYTHONPATH"]
                                             if os.environ.get("PYTHONPATH") else ""),
            "CONFIG_PATH": str(self.root / "config.yaml"),
            "REPLICATION_FACTOR": "3", "WRITE_QUORUM": "2",
        })
        result.update(self.env)
        return result

    def _launch_controller(self) -> None:
        self.controller_port = self._free_port()
        self.controller_url = f"http://127.0.0.1:{self.controller_port}"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        child_env = self._base_env()
        child_env["DB_PATH"] = str(self.base_dir / "controller.db")
        self.controller_process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "controller.main:app", "--host",
             "127.0.0.1", "--port", str(self.controller_port)],
            cwd=self.root, env=child_env, stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )

    def _node_data_dirs(self, index: int) -> list[Path]:
        node_root = self.base_dir / f"node{index}"
        return [node_root / f"disk{disk}" for disk in range(1, self.disks_per_node + 1)]

    def _node_env(self, index: int) -> dict[str, str]:
        child_env = self._base_env()
        data_dirs = self._node_data_dirs(index)
        separator = ";" if os.name == "nt" else ":"
        child_env.update({
            "NODE_ID": f"node{index}",
            "DATA_DIRS": separator.join(str(path) for path in data_dirs),
            "NODE_ADDRESS": self.node_url(index),
            "NODE_PORT": str(self.node_ports[index]),
            "CONTROLLER_URL": self.controller_url,
            "SCRUB_INTERVAL_S": str(self.env.get("SCRUB_INTERVAL_S", "0")),
        })
        return child_env

    def node_url(self, index: int) -> str:
        """URL do nó numerado a partir de 1."""
        if index not in self.node_ports:
            raise KeyError(f"Nó {index} não está configurado.")
        return f"http://127.0.0.1:{self.node_ports[index]}"

    def disk_dirs(self, index: int) -> list[Path]:
        """Pastas locais dos discos de um nó numerado a partir de 1."""
        return self._node_data_dirs(index)

    def _launch_node(self, index: int) -> None:
        if index not in self.node_ports:
            self.node_ports[index] = self._free_port()
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "storage_node.main:app", "--host",
             "127.0.0.1", "--port", str(self.node_ports[index])],
            cwd=self.root, env=self._node_env(index), stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        self.node_processes[index] = process

    @staticmethod
    def _wait_health(url: str, process: subprocess.Popen[bytes], timeout_s: float = 25.0) -> None:
        deadline = time.monotonic() + timeout_s
        last_error = "sem resposta"
        with httpx.Client(timeout=1.0) as client:
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"O processo em {url} encerrou com código {process.returncode}.")
                try:
                    response = client.get(f"{url}/health")
                    if response.status_code == 200 and response.json().get("ok") is True:
                        return
                    last_error = f"HTTP {response.status_code}"
                except (httpx.HTTPError, ValueError) as exc:
                    last_error = str(exc)
                time.sleep(0.05)
        raise TimeoutError(f"Serviço {url} não ficou pronto: {last_error}")

    def _synthetic_disks(self, index: int) -> list[dict[str, int | str]]:
        total = 100 * 1024**3
        disks = []
        for disk in range(1, self.disks_per_node + 1):
            ordinal = (index - 1) * self.disks_per_node + disk
            disks.append({
                "disk_id": f"node{index}/disk{disk}",
                "path": str(self._node_data_dirs(index)[disk - 1].resolve()),
                "total_bytes": total,
                "used_bytes": ordinal * 1024**2,
            })
        return disks

    def _register_node(self, index: int) -> None:
        health = httpx.get(f"{self.node_url(index)}/health", timeout=3.0).json()
        disks = health.get("disks", [])
        payload = {
            "node_id": f"node{index}", "address": self.node_url(index),
            "disks": [{"disk_id": disk["disk_id"], "path": disk["path"],
                       **synthetic} for disk, synthetic in zip(disks, self._synthetic_disks(index))],
        }
        response = httpx.post(f"{self.controller_url}/nodes/register", json=payload, timeout=5.0)
        if response.status_code != 200:
            raise RuntimeError(f"Registro do node{index} falhou: {response.status_code} {response.text}")

    def start(self) -> "LocalCluster":
        """Inicia controlador, nós, registros e heartbeats periódicos."""
        if self._started:
            return self
        try:
            self._launch_controller()
            assert self.controller_process is not None
            self._wait_health(self.controller_url, self.controller_process)
            for index in range(1, self.n_nodes + 1):
                self._launch_node(index)
                process = self.node_processes[index]
                self._wait_health(self.node_url(index), process)
                self._register_node(index)
            self._heartbeat_stop.clear()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, name="dcomp-local-heartbeat", daemon=True,
            )
            self._heartbeat_thread.start()
            self._started = True
            return self
        except BaseException:
            self.stop()
            raise

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(1.0):
            for index, process in list(self.node_processes.items()):
                if process.poll() is not None:
                    continue
                try:
                    response = httpx.post(
                        f"{self.controller_url}/nodes/node{index}/heartbeat",
                        json={"disks": self._synthetic_disks(index)}, timeout=2.0,
                    )
                    if response.status_code != 200:
                        # Re-registration makes the harness robust to controller restarts.
                        self._register_node(index)
                except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                    log.warning("heartbeat_failed", node_id=f"node{index}", error=str(exc))
                    continue

    def expected_targets(self, n: int) -> list[str]:
        """Alvos previsíveis selecionados primeiro por máquina e espaço livre."""
        result: list[str] = []
        for index in range(1, self.n_nodes + 1):
            result.append(f"node{index}/disk1")
            if len(result) == n:
                return result
        for index in range(1, self.n_nodes + 1):
            for disk in range(2, self.disks_per_node + 1):
                result.append(f"node{index}/disk{disk}")
                if len(result) == n:
                    return result
        return result

    def kill_node(self, index: int) -> None:
        """Interrompe um nó e aguarda o processo encerrar."""
        process = self.node_processes.get(index)
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def start_node(self, index: int) -> None:
        """Reinicia um nó usando as mesmas pastas e porta."""
        if not 1 <= index <= self.n_nodes:
            raise ValueError("Índice de nó inválido.")
        existing = self.node_processes.get(index)
        if existing is not None and existing.poll() is None:
            return
        self._launch_node(index)
        process = self.node_processes[index]
        self._wait_health(self.node_url(index), process)
        self._register_node(index)

    def stop(self) -> None:
        """Encerra heartbeats e todos os subprocessos mesmo em falhas."""
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=3.0)
            self._heartbeat_thread = None
        for process in list(self.node_processes.values()) + ([self.controller_process]
                                                             if self.controller_process else []):
            if process is None or process.poll() is not None:
                continue
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self.node_processes.clear()
        self.controller_process = None
        self._started = False

    def __enter__(self) -> "LocalCluster":
        return self.start()

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.stop()


def main() -> int:
    """Sobe um cluster de desenvolvimento até Ctrl+C."""
    import argparse
    parser = argparse.ArgumentParser(description="Cluster local do DComp Drive")
    parser.add_argument("--directory", default=".dcomp-localcluster")
    parser.add_argument("--nodes", type=int, default=5)
    args = parser.parse_args()
    cluster = LocalCluster(args.directory, n_nodes=args.nodes)
    try:
        cluster.start()
        print(json.dumps({"controller_url": cluster.controller_url,
                          "nodes": [cluster.node_url(i) for i in range(1, args.nodes + 1)]},
                         ensure_ascii=False), flush=True)
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        return 130
    finally:
        cluster.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
