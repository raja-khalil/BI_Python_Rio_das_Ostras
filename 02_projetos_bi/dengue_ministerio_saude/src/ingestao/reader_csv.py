"""Leitor padronizado de arquivos CSV."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_csv_file(path: Path, encoding: str = "utf-8", sep: str = ",") -> pd.DataFrame:
    """Le arquivo CSV e retorna DataFrame."""
    return pd.read_csv(path, encoding=encoding, sep=sep)
