"""Regressões do roteiro de falhas."""
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from uuid import uuid4


def test_corruption_script_changes_the_stored_chunk(tmp_path):
    disk = tmp_path / "disk1"
    chunk_id = str(uuid4())
    content = b"chunk da demonstracao"
    directory = disk / "chunks" / chunk_id[:2]
    directory.mkdir(parents=True)
    active = directory / f"{chunk_id}.{sha256(content).hexdigest()}.chunk"
    active.write_bytes(content)
    quarantined = active.with_suffix(".chunk.corrupt")
    quarantined.write_bytes(content)
    script = Path(__file__).resolve().parents[1] / "scripts" / "corrupt_chunk.py"

    subprocess.run([sys.executable, str(script), str(disk), chunk_id], check=True)

    assert active.read_bytes() == bytes([content[0] ^ 1]) + content[1:]
    assert quarantined.read_bytes() == content
