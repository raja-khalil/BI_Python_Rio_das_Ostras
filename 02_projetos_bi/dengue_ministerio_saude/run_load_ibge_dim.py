"""Executa carga da dimensao IBGE de municipios."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.config.settings import get_settings
from src.ingestao.ibge_loader import load_dim_ibge_municipio
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga da dimensao IBGE de municipios")
    parser.add_argument(
        "--csv-path",
        type=str,
        default="",
        help="Caminho do CSV IBGE. Se vazio, usa data/external/ibge/ibge_censo_2022_municipios_basico_br.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    default_csv = settings.data_dir / "external" / "ibge" / "ibge_censo_2022_municipios_basico_br.csv"
    csv_path = Path(args.csv_path) if args.csv_path else default_csv

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV IBGE nao encontrado: {csv_path}")

    total = load_dim_ibge_municipio(csv_path=csv_path, schema=settings.db_schema)
    logger.info("Carga concluida. total_rows=%s", total)


if __name__ == "__main__":
    main()
