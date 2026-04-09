"""Downloader da base CNES (apenas ultimo arquivo)."""

from __future__ import annotations

import argparse
import os

from src.config.settings import get_settings
from src.ingestao.cnes_downloader import CnesDownloader
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, str(default)).lower()
    return raw in {"1", "true", "t", "yes", "y", "sim"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baixa o arquivo CNES mais recente")
    parser.add_argument(
        "--extract-zip",
        action="store_true",
        default=_env_bool("CNES_EXTRACT_ZIP", True),
        help="Extrai o arquivo .zip apos download",
    )
    parser.add_argument(
        "--keep-zip",
        action="store_true",
        default=_env_bool("CNES_KEEP_ZIP", False),
        help="Mantem o zip original apos extracao",
    )
    parser.add_argument(
        "--check-interval-days",
        type=int,
        default=int(_env("CNES_CHECK_INTERVAL_DAYS", "40")),
        help="Intervalo (dias) entre verificacoes no site do CNES",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Forca nova verificacao e download quando houver arquivo mais novo",
    )
    parser.add_argument(
        "--periodic",
        action="store_true",
        default=_env_bool("CNES_PERIODIC", False),
        help="Executa em loop periodico",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=int(_env("CNES_INTERVAL_MINUTES", "60")),
        help="Intervalo (minutos) entre ciclos no modo periodico",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    downloader = CnesDownloader(settings=get_settings())

    if args.periodic:
        logger.info("Executando downloader CNES em modo periodico")
        downloader.sync_periodic(
            interval_minutes=args.interval_minutes,
            extract_zip=args.extract_zip,
            keep_zip=args.keep_zip,
            check_interval_days=args.check_interval_days,
        )
        return

    logger.info("Executando downloader CNES em modo unico")
    summary = downloader.sync_once(
        extract_zip=args.extract_zip,
        keep_zip=args.keep_zip,
        check_interval_days=args.check_interval_days,
        force=args.force,
    )
    logger.info("Resumo downloader CNES: %s", summary)


if __name__ == "__main__":
    main()
