"""Leitura streaming de arquivos JSON grandes em chunks."""

from __future__ import annotations

from pathlib import Path
from typing import Generator

import ijson
import pandas as pd


def _first_non_whitespace_byte(path: Path) -> bytes:
    with path.open("rb") as file:
        while True:
            byte = file.read(1)
            if not byte:
                return b""
            if byte not in b" \t\r\n":
                return byte


def iter_json_chunks(path: Path, chunk_size: int = 100_000) -> Generator[pd.DataFrame, None, None]:
    """Itera registros JSON em lotes para evitar estouro de memoria."""
    if chunk_size <= 0:
        raise ValueError("chunk_size deve ser maior que zero")

    first_byte = _first_non_whitespace_byte(path)
    prefix = "item" if first_byte == b"[" else "parametros.item"

    buffer: list[dict] = []
    with path.open("rb") as file:
        for item in ijson.items(file, prefix):
            if not isinstance(item, dict):
                continue
            buffer.append(item)
            if len(buffer) >= chunk_size:
                yield pd.DataFrame(buffer)
                buffer = []

    if buffer:
        yield pd.DataFrame(buffer)
