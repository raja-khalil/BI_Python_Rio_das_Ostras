"""Leitor padronizado de arquivos XML."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from lxml import etree


def read_xml_file(path: Path, xpath: str | None = None) -> pd.DataFrame:
    """Le XML e tenta converter elementos para DataFrame."""
    tree = etree.parse(str(path))
    root = tree.getroot()

    nodes = root.xpath(xpath) if xpath else list(root)
    rows: list[dict[str, str]] = []

    for node in nodes:
        row: dict[str, str] = {}
        for child in node:
            row[child.tag] = (child.text or "").strip()
        if row:
            rows.append(row)

    return pd.DataFrame(rows)
