"""Grava e baixa uma amostra usando o contrato HTTP do projeto."""
from __future__ import annotations

import hashlib
import sys
import time

import httpx

controller = "http://127.0.0.1:8000"
sample = b"DComp Drive: teste de recuperacao\n"
digest = hashlib.sha256(sample).hexdigest()


def data(response):
    response.raise_for_status()
    return response.json()


def seed(client, path):
    config = data(client.get(f"{controller}/health"))
    status = data(client.get(f"{controller}/status"))
    alive = [node for node in status["nodes"] if node["status"] == "alive"]
    if len(alive) <= config["replication_factor"]:
        raise SystemExit("O teste precisa de pelo menos um nó vivo além das réplicas.")

    allocation = data(client.post(f"{controller}/files/allocate", json={
        "path": path, "size": len(sample), "num_chunks": 1,
    }))
    chunk = allocation["chunks"][0]
    targets = chunk["targets"]
    for target in targets:
        data(client.put(f"{target['node_url']}/chunks/{chunk['chunk_id']}",
                        params={"disk_id": target["disk_id"], "hash": digest}, content=sample))
    data(client.post(f"{controller}/files/commit", json={
        "path": path, "lease_id": allocation["lease_id"], "file_hash": digest,
        "chunks": [{"idx": 0, "chunk_id": chunk["chunk_id"], "hash": digest,
                    "size": len(sample), "confirmed": [target["disk_id"] for target in targets]}],
    }))
    print(targets[0]["disk_id"].split("/", 1)[0])


def verify(client, path, node_id):
    config = data(client.get(f"{controller}/health"))
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = data(client.get(f"{controller}/status"))
        node = next(node for node in status["nodes"] if node["node_id"] == node_id)
        state = data(client.get(f"{controller}/files/stat", params={"path": path}))
        replicas = state["chunks"][0]["replicas"]
        live = {r["disk_id"].split("/", 1)[0] for r in replicas if r["state"] == "ok"}
        if (node["status"] == "dead" and state["state"] == "intact"
                and node_id not in live and len(live) == config["replication_factor"]):
            break
        time.sleep(1)
    else:
        raise SystemExit(f"Replicação não restabelecida. Nó: {node!r}. Arquivo: {state!r}")

    located = data(client.get(f"{controller}/files/locate", params={"path": path}))
    chunk = located["chunks"][0]
    for replica in chunk["replicas"]:
        response = client.get(f"{replica['node_url']}/chunks/{chunk['chunk_id']}",
                              params={"disk_id": replica["disk_id"]})
        response.raise_for_status()
        if hashlib.sha256(response.content).hexdigest() != digest:
            raise SystemExit(f"Hash diferente em {replica['disk_id']}")
    print(f"Nó {node_id} morto, {len(live)} réplicas, SHA-256 correto em todas as cópias.")
    print(data(client.get(f"{controller}/metrics")))


if __name__ == "__main__":
    action, path, *arguments = sys.argv[1:]
    with httpx.Client(timeout=10, trust_env=False) as client:
        if action == "seed":
            seed(client, path)
        elif action == "verify":
            verify(client, path, arguments[0])
        else:
            raise SystemExit("Use seed PATH ou verify PATH NODE_ID")
