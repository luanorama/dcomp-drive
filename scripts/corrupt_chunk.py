"""Altera um byte de uma réplica ativa para a demonstração de integridade."""
from __future__ import annotations

import argparse
from pathlib import Path
from uuid import UUID


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("disk", type=Path, help="Pasta de dados do disco")
    parser.add_argument("chunk_id", type=UUID)
    args = parser.parse_args()

    chunk_id = str(args.chunk_id)
    directory = args.disk / "chunks" / chunk_id[:2]
    replicas = list(directory.glob(f"{chunk_id}.*.chunk"))
    if len(replicas) != 1:
        parser.exit(1, f"Esperava uma réplica ativa de {chunk_id} em {directory}; "
                       f"encontrei {len(replicas)}.\n")

    path = replicas[0]
    with path.open("r+b") as stream:
        first = stream.read(1)
        stream.seek(0)
        stream.write(bytes([first[0] ^ 1]) if first else b"\x01")
    print(f"Chunk alterado: {path}")


if __name__ == "__main__":
    main()
