"""Executa carga da dimensão CNES de estabelecimentos."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.config.settings import get_settings
from src.ingestao.cnes_loader import load_dim_cnes_estabelecimento
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _latest_extracted_file(base_dir: Path) -> Path | None:
    if not base_dir.exists():
        return None
    dirs = sorted([p for p in base_dir.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)
    for d in dirs:
        files = sorted(
            [f for f in d.iterdir() if f.is_file() and f.suffix.lower() in {".csv", ".txt"}],
            key=lambda p: p.name,
        )
        if files:
            return files[0]
    return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga da dimensão CNES para o banco")
    parser.add_argument(
        "--file-path",
        type=str,
        default="",
        help="Arquivo CNES CSV/TXT. Se vazio, tenta o mais recente em data/raw/external/cnes/extracted/<yyyymm>/",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    file_path = Path(args.file_path) if args.file_path else None

    if not file_path:
        base = settings.raw_dir / "external" / "cnes" / "extracted"
        file_path = _latest_extracted_file(base)

    if not file_path or not file_path.exists():
        raise FileNotFoundError(
            "Arquivo CNES nao encontrado. Informe --file-path ou baixe/extrai com run_cnes_downloader.py"
        )

    total = load_dim_cnes_estabelecimento(file_path=file_path, schema=settings.db_schema)
    logger.info("Carga CNES concluida. total_rows=%s arquivo=%s", total, file_path)


if __name__ == "__main__":
    main()
