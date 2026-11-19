"""Regressões de escrita/versão: executar após aplicar o patch.

python -m pytest -q tests/test_controller.py tests/test_pessoa_a_regressoes.py
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import time

import pytest
from fastapi.testclient import TestClient

from tests.test_controller import _make_client_with_nodes
from controller.main import app
from controller.db import get_db
from controller import repository as repo


@pytest.fixture
def client():
    return _make_client_with_nodes()


def allocate(client, name, size=5, n=None):
    request = {'path': name, 'size': size, 'num_chunks': 1}
    if n is not None:
        request['replication_factor'] = n
    response = client.post('/files/allocate', json=request)
    assert response.status_code == 200, response.text
    return response.json()


def commit(client, name, allocation, hash_value, size=5, count=None):
    chunk = allocation['chunks'][0]
    disks = [t['disk_id'] for t in chunk['targets']]
    response = client.post('/files/commit', json={
        'path': name, 'lease_id': allocation['lease_id'], 'file_hash': hash_value,
        'chunks': [{'idx': 0, 'chunk_id': chunk['chunk_id'],
                    'size': size, 'hash': hash_value,
                    'confirmed': disks if count is None else disks[:count]}],
    })
    return response


def test_overwrite_stays_visible_until_commit(client):
    client.post('/dirs', json={'path': '/docs'})
    first = allocate(client, '/docs/a.txt')
    assert commit(client, '/docs/a.txt', first, 'old').status_code == 200
    old = client.get('/files/locate', params={'path': '/docs/a.txt'}).json()
    second = allocate(client, '/docs/a.txt', size=7)
    during = client.get('/files/locate', params={'path': '/docs/a.txt'})
    assert during.status_code == 200, during.text
    assert during.json()['file_hash'] == 'old'
    assert during.json()['chunks'][0]['chunk_id'] == old['chunks'][0]['chunk_id']
    assert commit(client, '/docs/a.txt', second, 'new', size=7).status_code == 200
    after = client.get('/files/locate', params={'path': '/docs/a.txt'}).json()
    assert after['file_hash'] == 'new'
    assert after['chunks'][0]['chunk_id'] != old['chunks'][0]['chunk_id']
    meta = client.get('/files/stat', params={'path': '/docs/a.txt'}).json()
    assert meta['version'] == 2


def test_quorum_failure_preserves_previous_commit(client):
    client.post('/dirs', json={'path': '/docs'})
    first = allocate(client, '/docs/a.txt')
    assert commit(client, '/docs/a.txt', first, 'old').status_code == 200
    second = allocate(client, '/docs/a.txt')
    failed = commit(client, '/docs/a.txt', second, 'new', count=1)
    assert failed.status_code == 507, failed.text
    locate = client.get('/files/locate', params={'path': '/docs/a.txt'})
    assert locate.status_code == 200
    assert locate.json()['file_hash'] == 'old'
    assert client.get('/files/stat', params={'path': '/docs/a.txt'}).json()['version'] == 1
    third = allocate(client, '/docs/a.txt')
    assert third['version'] == 2


def test_confirmed_fake_disk_is_rejected_without_corrupting_old(client):
    client.post('/dirs', json={'path': '/docs'})
    first = allocate(client, '/docs/a.txt')
    assert commit(client, '/docs/a.txt', first, 'old').status_code == 200
    second = allocate(client, '/docs/a.txt')
    chunk = second['chunks'][0]
    bad = client.post('/files/commit', json={
        'path': '/docs/a.txt', 'lease_id': second['lease_id'], 'file_hash': 'wrong',
        'chunks': [{'idx': 0, 'chunk_id': chunk['chunk_id'], 'hash': 'wrong',
                    'size': 5, 'confirmed': ['node99/unknown', 'node99/unknown']}],
    })
    assert bad.status_code == 422, bad.text
    assert client.get('/files/locate', params={'path': '/docs/a.txt'}).json()['file_hash'] == 'old'
    assert commit(client, '/docs/a.txt', second, 'new').status_code == 200


def test_expired_lease_releases_new_file_and_restores_existing(client):
    client.post('/dirs', json={'path': '/docs'})
    first = allocate(client, '/docs/a.txt')
    assert commit(client, '/docs/a.txt', first, 'old').status_code == 200
    unfinished = allocate(client, '/docs/a.txt')
    temporary = allocate(client, '/docs/unfinished.txt')
    conn = get_db()
    conn.execute('UPDATE leases SET expires_at=?', (time.time() - 1,))
    conn.commit()
    assert repo.cleanup_expired_uploads(conn) == 2
    assert repo.get_lease(conn, '/docs/a.txt') is None
    assert repo.get_entry(conn, '/docs/unfinished.txt') is None
    conn.close()
    assert client.get('/files/locate', params={'path': '/docs/a.txt'}).json()['file_hash'] == 'old'
    assert allocate(client, '/docs/a.txt')['version'] == 2


def test_replication_fallback_to_other_disk_same_machine(client):
    conn = get_db()
    repo.update_node_status(conn, 'node3', 'dead')
    targets = repo.choose_replication_targets(conn, 3)
    assert len(targets) == 3
    assert len({t['disk_id'] for t in targets}) == 3
    assert len({t['node_id'] for t in targets}) == 2
    conn.close()


def test_per_file_replication_factor(client):
    client.post('/dirs', json={'path': '/docs'})
    a = allocate(client, '/docs/a.txt', n=2)
    assert len(a['chunks'][0]['targets']) == 2
    assert commit(client, '/docs/a.txt', a, 'h').status_code == 200
    conn = get_db()
    assert repo.get_entry(conn, '/docs/a.txt')['replication_factor'] == 2
    conn.close()
    bad = client.post('/files/allocate', json={
        'path': '/docs/b.txt', 'size': 5, 'num_chunks': 1,
        'replication_factor': 1,
    })
    assert bad.status_code == 422


def test_commit_rejects_chunk_from_previous_version(client):
    client.post('/dirs', json={'path': '/docs'})
    first = allocate(client, '/docs/a.txt')
    assert commit(client, '/docs/a.txt', first, 'old').status_code == 200
    second = allocate(client, '/docs/a.txt')
    old_id = first['chunks'][0]['chunk_id']
    disks = [t['disk_id'] for t in second['chunks'][0]['targets']]
    wrong = client.post('/files/commit', json={
        'path': '/docs/a.txt', 'lease_id': second['lease_id'], 'file_hash': 'new',
        'chunks': [{'idx': 0, 'chunk_id': old_id, 'hash': 'new',
                    'size': 5, 'confirmed': disks}],
    })
    assert wrong.status_code == 422
    assert client.get('/files/locate', params={'path': '/docs/a.txt'}).json()['file_hash'] == 'old'
    assert commit(client, '/docs/a.txt', second, 'new').status_code == 200


def test_expiry_enqueues_physical_cleanup(client):
    client.post('/dirs', json={'path': '/docs'})
    pending = allocate(client, '/docs/pending.txt')
    expected = len(pending['chunks'][0]['targets'])
    conn = get_db()
    conn.execute('UPDATE leases SET expires_at=? WHERE path=?',
                 (time.time() - 1, '/docs/pending.txt'))
    conn.commit()
    assert repo.cleanup_expired_uploads(conn, '/docs/pending.txt') == 1
    queued = conn.execute(
        'SELECT COUNT(*) FROM chunk_cleanup_queue WHERE chunk_id=?',
        (pending['chunks'][0]['chunk_id'],),
    ).fetchone()[0]
    assert queued == expected
    conn.close()


def test_two_real_concurrent_allocates_one_lease(client):
    client.post('/dirs', json={'path': '/docs'})
    gate = Barrier(2)
    def send():
        local = TestClient(app, raise_server_exceptions=False)
        gate.wait(timeout=10)
        return local.post('/files/allocate', json={
            'path': '/docs/same.txt', 'size': 5, 'num_chunks': 1,
        }).status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(send) for _ in range(2)]
        results = [f.result(timeout=20) for f in futures]
    assert sorted(results) == [200, 409]
