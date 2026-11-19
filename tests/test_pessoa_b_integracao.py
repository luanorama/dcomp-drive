"""Integração contra controlador e nós reais iniciados pelo LocalCluster."""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from pathlib import Path

import httpx
import pytest

from client.api import ApiError, ControllerClient
from client.cli import main
from client.transfer import TransferError, get_file, put_file
from common.hashing import sha256_hex
from devtools.localcluster import LocalCluster
from controller.config import get_config


@pytest.fixture(scope="module")
def cluster(tmp_path_factory: pytest.TempPathFactory):
    base = tmp_path_factory.mktemp("dcomp-local-cluster")
    instance = LocalCluster(base, n_nodes=5, disks_per_node=2,
                            env={"LEASE_TTL_S": "60", "SCRUB_INTERVAL_S": "0"})
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


@pytest.fixture(scope="module")
def lease_cluster(tmp_path_factory: pytest.TempPathFactory):
    base = tmp_path_factory.mktemp("dcomp-lease-cluster")
    instance = LocalCluster(base, n_nodes=3, disks_per_node=2,
                            env={"LEASE_TTL_S": "3", "SCRUB_INTERVAL_S": "0"})
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


def controller_for(cluster: LocalCluster) -> ControllerClient:
    return ControllerClient(cluster.controller_url)


def ensure_dir(cluster: LocalCluster, path: str) -> None:
    client = controller_for(cluster)
    try:
        try:
            client.mkdir(path)
        except ApiError as exc:
            if exc.status != 409:
                raise
    finally:
        client.close()


def write_local(path: Path, size: int, *, random_data: bool = False) -> bytes:
    data = os.urandom(size) if random_data else bytes((idx % 251 for idx in range(size)))
    path.write_bytes(data)
    return data


def test_real_put_get_and_replication_for_edge_sizes(cluster: LocalCluster, tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    try:
        sizes = [0, 1, 4 * 1024 * 1024, 4 * 1024 * 1024 + 1, 10 * 1024 * 1024]
        for index, size in enumerate(sizes):
            source = tmp_path / f"source-{index}.bin"
            original = write_local(source, size, random_data=(size == 10 * 1024 * 1024))
            remote = f"/integration/size-{index}.bin"
            put_file(source, remote, controller, replication_factor=3)
            destination = tmp_path / f"download-{index}.bin"
            get_file(remote, destination, controller)
            expected_hash = hashlib.sha256(original).hexdigest()
            assert hashlib.sha256(destination.read_bytes()).hexdigest() == expected_hash
            meta = controller.stat(remote)
            assert meta["state"] == "intact"
            assert len(meta["chunks"]) == max(1, (size + 4 * 1024 * 1024 - 1) // (4 * 1024 * 1024))
            for chunk in meta["chunks"]:
                replicas = chunk["replicas"]
                assert len([replica for replica in replicas if replica["state"] == "ok"]) == 3
                assert len({replica["disk_id"].split("/", 1)[0] for replica in replicas}) == 3
    finally:
        controller.close()


def test_encryption_hides_known_marker_and_round_trips(cluster: LocalCluster,
                                                         tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    marker = b"MARCADOR-CLARO-NAO-PODE-APARECER-987654321"
    source = tmp_path / "plain.bin"
    source.write_bytes(marker + b" payload")
    remote = "/integration/encrypted.bin"
    key = "56" * 32
    try:
        put_file(source, remote, controller, encrypted=True, key_value=key)
        meta = controller.stat(remote)
        assert meta["size"] == source.stat().st_size + 36 * len(meta["chunks"])
        located = controller.locate(remote)
        for chunk in located["chunks"]:
            replica = chunk["replicas"][0]
            response = httpx.get(
                f"{replica['node_url']}/chunks/{chunk['chunk_id']}",
                params={"disk_id": replica["disk_id"]}, timeout=10,
            )
            assert response.status_code == 200
            assert marker not in response.content
        destination = tmp_path / "decrypted.bin"
        get_file(remote, destination, controller, encrypted=True, key_value=key)
        assert destination.read_bytes() == source.read_bytes()
    finally:
        controller.close()


def _corrupt_disk_copy(cluster: LocalCluster, node_url: str, disk_id: str,
                       chunk_id: str) -> Path:
    node_index = next(i for i in range(1, cluster.n_nodes + 1)
                      if cluster.node_url(i) == node_url)
    disk_name = disk_id.split("/", 1)[1]
    directory = next(path for path in cluster.disk_dirs(node_index) if path.name == disk_name)
    matches = list((directory / "chunks" / chunk_id[:2]).glob(f"{chunk_id}.*.chunk"))
    assert len(matches) == 1
    current = matches[0].read_bytes()
    matches[0].write_bytes((b"X" + current[1:]) if current else b"X")
    return matches[0]


def test_scrub_and_direct_get_report_corruption(cluster: LocalCluster, tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    source = tmp_path / "scrub.bin"
    source.write_bytes(b"scrub-me")
    remote = "/integration/scrub.bin"
    try:
        put_file(source, remote, controller, replication_factor=3)
        located = controller.locate(remote)
        chunk = located["chunks"][0]
        replica = chunk["replicas"][0]
        corrupt_path = _corrupt_disk_copy(cluster, replica["node_url"],
                                          replica["disk_id"], chunk["chunk_id"])
        scrub = httpx.post(f"{replica['node_url']}/scrub", timeout=10).json()
        assert {"chunk_id": chunk["chunk_id"], "disk_id": replica["disk_id"]} in scrub["corrupt"]
        meta = controller.stat(remote)
        assert meta["state"] == "under_replicated"
        states = [r["state"] for r in meta["chunks"][0]["replicas"]]
        assert "corrupt" in states
        assert not corrupt_path.exists()
        output = tmp_path / "scrub-get.bin"
        get_file(remote, output, controller)
        assert output.read_bytes() == source.read_bytes()

        direct_source = tmp_path / "direct.bin"
        direct_source.write_bytes(b"direct-get-corruption")
        direct_remote = "/integration/direct-corrupt.bin"
        put_file(direct_source, direct_remote, controller, replication_factor=3)
        direct_chunk = controller.locate(direct_remote)["chunks"][0]
        direct_replica = direct_chunk["replicas"][0]
        _corrupt_disk_copy(cluster, direct_replica["node_url"],
                           direct_replica["disk_id"], direct_chunk["chunk_id"])
        response = httpx.get(
            f"{direct_replica['node_url']}/chunks/{direct_chunk['chunk_id']}",
            params={"disk_id": direct_replica["disk_id"]}, timeout=10,
        )
        assert response.status_code == 422
        deadline = time.monotonic() + 5
        direct_meta = controller.stat(direct_remote)
        while direct_meta["state"] != "under_replicated" and time.monotonic() < deadline:
            time.sleep(0.05)
            direct_meta = controller.stat(direct_remote)
        assert direct_meta["state"] == "under_replicated"
    finally:
        controller.close()


def test_copy_verifies_source_and_reports_offline_target(cluster: LocalCluster,
                                                         tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    source = tmp_path / "copy.bin"
    source.write_bytes(b"copy me")
    remote = "/integration/copy.bin"
    try:
        put_file(source, remote, controller, replication_factor=3)
        chunk = controller.locate(remote)["chunks"][0]
        origin = chunk["replicas"][0]
        destination_node = 4
        destination_disk = "node4/disk1"
        copied = httpx.post(
            f"{origin['node_url']}/chunks/{chunk['chunk_id']}/copy",
            json={"disk_id": origin["disk_id"], "target_url": cluster.node_url(destination_node),
                  "target_disk_id": destination_disk}, timeout=15,
        )
        assert copied.status_code == 200, copied.text
        response = httpx.get(
            f"{cluster.node_url(destination_node)}/chunks/{chunk['chunk_id']}",
            params={"disk_id": destination_disk}, timeout=5,
        )
        assert response.content == source.read_bytes()

        offline = httpx.post(
            f"{origin['node_url']}/chunks/{chunk['chunk_id']}/copy",
            json={"disk_id": origin["disk_id"], "target_url": "http://127.0.0.1:1",
                  "target_disk_id": "node5/disk1"}, timeout=10,
        )
        assert offline.status_code == 502

        corrupt_id = str(uuid.uuid4())
        payload = b"bad origin"
        put_response = httpx.put(
            f"{origin['node_url']}/chunks/{corrupt_id}",
            params={"disk_id": origin["disk_id"], "hash": sha256_hex(payload)},
            content=payload, headers={"Content-Type": "application/octet-stream"}, timeout=5,
        )
        assert put_response.status_code == 200
        _corrupt_disk_copy(cluster, origin["node_url"], origin["disk_id"], corrupt_id)
        bad_copy = httpx.post(
            f"{origin['node_url']}/chunks/{corrupt_id}/copy",
            json={"disk_id": origin["disk_id"], "target_url": cluster.node_url(5),
                  "target_disk_id": "node5/disk1"}, timeout=10,
        )
        assert bad_copy.status_code == 422
        absent = httpx.get(f"{cluster.node_url(5)}/chunks/{corrupt_id}",
                           params={"disk_id": "node5/disk1"}, timeout=5)
        assert absent.status_code == 404
    finally:
        controller.close()


def test_all_corrupt_replicas_leave_destination_untouched(cluster: LocalCluster,
                                                           tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    source = tmp_path / "all-corrupt.bin"
    source.write_bytes(b"every copy becomes bad")
    remote = "/integration/all-corrupt.bin"
    destination = tmp_path / "existing.bin"
    destination.write_bytes(b"keep")
    try:
        put_file(source, remote, controller, replication_factor=3)
        chunk = controller.locate(remote)["chunks"][0]
        for replica in chunk["replicas"]:
            _corrupt_disk_copy(cluster, replica["node_url"], replica["disk_id"], chunk["chunk_id"])
        with pytest.raises(TransferError, match="irrecuperável"):
            get_file(remote, destination, controller)
        assert destination.read_bytes() == b"keep"
        assert list(tmp_path.glob(".*.part-*")) == []
    finally:
        controller.close()


def test_node_failure_fallback_and_under_replicated_state(cluster: LocalCluster,
                                                           tmp_path: Path) -> None:
    ensure_dir(cluster, "/integration")
    controller = controller_for(cluster)
    source = tmp_path / "node-failure.bin"
    source.write_bytes(b"write with node one offline")
    remote = "/integration/node-failure.bin"
    try:
        cluster.kill_node(1)
        put_file(source, remote, controller, replication_factor=3)
        meta = controller.stat(remote)
        assert meta["state"] == "under_replicated"
        states = [replica["state"] for replica in meta["chunks"][0]["replicas"]]
        assert states.count("ok") == 2 and states.count("lost") == 1
        destination = tmp_path / "node-failure-get.bin"
        get_file(remote, destination, controller)
        assert destination.read_bytes() == source.read_bytes()
        cluster.start_node(1)

        healthy_source = tmp_path / "fallback.bin"
        healthy_source.write_bytes(b"read with only node three reachable")
        healthy_remote = "/integration/fallback.bin"
        put_file(healthy_source, healthy_remote, controller, replication_factor=3)
        cluster.kill_node(1)
        cluster.kill_node(2)
        fallback = tmp_path / "fallback-get.bin"
        get_file(healthy_remote, fallback, controller)
        assert fallback.read_bytes() == healthy_source.read_bytes()
        cluster.start_node(1)
        cluster.start_node(2)
    finally:
        if controller:
            controller.close()
        cluster.start_node(1)
        cluster.start_node(2)


def test_cli_lease_overwrite_directories_status_drain_and_logs(
    cluster: LocalCluster, tmp_path: Path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert main(["--controller", cluster.controller_url, "mkdir", "/integration/ops"]) == 0
    output = capsys.readouterr().out
    assert "diretório criado" in output
    local = tmp_path / "writer.bin"
    local.write_bytes(b"first version")
    assert main(["--controller", cluster.controller_url, "put",
                 "/integration/ops/lease.bin", str(local)]) == 0
    capsys.readouterr()

    local.write_bytes(b"second version with overwrite")
    assert main(["--controller", cluster.controller_url, "put",
                 "/integration/ops/lease.bin", str(local)]) == 0
    capsys.readouterr()
    controller = controller_for(cluster)
    try:
        assert controller.stat("/integration/ops/lease.bin")["version"] == 2
    finally:
        controller.close()
    downloaded = tmp_path / "overwrite.bin"
    assert main(["--controller", cluster.controller_url, "get",
                 "/integration/ops/lease.bin", str(downloaded)]) == 0
    capsys.readouterr()
    assert downloaded.read_bytes() == local.read_bytes()

    assert main(["--controller", cluster.controller_url, "stat",
                 "/integration/ops"]) == 1
    assert "é um diretório" in capsys.readouterr().err
    assert main(["--controller", cluster.controller_url, "mkdir", "/integration/ops/sub"]) == 0
    capsys.readouterr()
    assert main(["--controller", cluster.controller_url, "rmdir", "/integration/ops"]) == 1
    assert "não está vazio" in capsys.readouterr().err
    assert main(["--controller", cluster.controller_url, "mv",
                 "/integration/ops/lease.bin", "/integration/ops/moved.bin"]) == 0
    capsys.readouterr()
    assert main(["--controller", cluster.controller_url, "ls", "/integration/ops"]) == 0
    assert "moved.bin" in capsys.readouterr().out
    assert main(["--controller", cluster.controller_url, "status"]) == 0
    status_text = capsys.readouterr().out
    assert "Nós:" in status_text and "Arquivos:" in status_text
    assert main(["--controller", cluster.controller_url, "drain", "node5"]) == 0
    assert "enfileirados" in capsys.readouterr().out

    controller = controller_for(cluster)
    try:
        located = controller.locate("/integration/ops/moved.bin")
    finally:
        controller.close()
    chunk_ids = [item["chunk_id"] for item in located["chunks"]]
    assert main(["--controller", cluster.controller_url, "rm",
                 "/integration/ops/moved.bin"]) == 0
    capsys.readouterr()
    assert all(not list(root.rglob(f"{chunk_id}.*.chunk"))
               for index in range(1, 6) for root in cluster.disk_dirs(index)
               for chunk_id in chunk_ids)
    assert main(["--controller", cluster.controller_url, "rmdir", "/integration/ops/sub"]) == 0
    capsys.readouterr()
    assert main(["--controller", cluster.controller_url, "rmdir", "/integration/ops"]) == 0
    capsys.readouterr()

    log_file = tmp_path / "client-events.jsonl"
    monkeypatch.setenv("DCOMP_LOG_FILE", str(log_file))
    log_source = tmp_path / "logged.bin"
    log_source.write_bytes(b"event log")
    assert main(["--controller", cluster.controller_url, "put",
                 "/integration/logged.bin", str(log_source)]) == 0
    capsys.readouterr()
    assert main(["--controller", cluster.controller_url, "get",
                 "/integration/logged.bin", str(tmp_path / "logged-out.bin")]) == 0
    capsys.readouterr()
    events = [json.loads(line)["event"] for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert "put_done" in events and "get_done" in events


def test_lease_conflict_expires_and_next_put_succeeds(
    lease_cluster: LocalCluster, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    ensure_dir(lease_cluster, "/lease")
    source = tmp_path / "lease-source.bin"
    source.write_bytes(b"lease expires")
    remote = "/lease/file.bin"
    controller = controller_for(lease_cluster)
    try:
        held = controller.allocate({"path": remote, "size": source.stat().st_size,
                                   "num_chunks": 1, "client_id": "lease-test"})
    finally:
        controller.close()
    assert held["lease_id"]
    current = get_config()
    previous_ttl = current.lease_ttl_s
    current.lease_ttl_s = 3
    try:
        first = main(["--controller", lease_cluster.controller_url, "put", remote, str(source)])
        assert first == 1
        assert "Outro escritor" in capsys.readouterr().err
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            result = main(["--controller", lease_cluster.controller_url, "put", remote, str(source)])
            if result == 0:
                break
            capsys.readouterr()
            time.sleep(0.1)
        assert result == 0
        capsys.readouterr()
        client = controller_for(lease_cluster)
        try:
            assert client.stat(remote)["state"] == "intact"
        finally:
            client.close()
    finally:
        current.lease_ttl_s = previous_ttl
