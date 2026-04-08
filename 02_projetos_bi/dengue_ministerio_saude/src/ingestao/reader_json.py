"""Leitor padronizado de arquivos JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def read_json_file(path: Path, normalize: bool = True) -> pd.DataFrame | dict[str, Any] | list[Any]:
    """Le JSON e retorna DataFrame normalizado ou estrutura original."""
    with path.open("r", encoding="utf-8") as file:
        payload: dict[str, Any] | list[Any] = json.load(file)

    if not normalize:
        return payload

    if isinstance(payload, list):
        return pd.json_normalize(payload)

    return pd.json_normalize(payload)
