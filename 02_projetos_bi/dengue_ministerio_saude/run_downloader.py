"""Downloader de arquivos do portal de dados abertos (Sinan/Dengue)."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os

from src.config.settings import get_settings
from src.ingestao.portal_downloader import DenguePortalDownloader
from src.utils.logger import get_logger


logger = get_logger(__name__)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, str(default)).lower()
    return raw in {"1", "true", "t", "yes", "y", "sim"}


def _parse_args() -> argparse.Namespace:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(description="Baixa recursos de dengue por ano/formato do portal SUS")
    parser.add_argument(
        "--formats",
        default=_env("PORTAL_FORMATS", "json,csv,xml"),
        help="Formatos separados por virgula. Ex: json,csv,xml",
    )
    parser.add_argument(
        "--year-start",
        type=int,
        default=int(_env("PORTAL_YEAR_START", "2000")),
        help="Ano inicial para download",
    )
    parser.add_argument(
        "--year-end",
        type=int,
        default=int(_env("PORTAL_YEAR_END", str(current_year))),
        help="Ano final para download",
    )
    parser.add_argument(
        "--extract-zip",
        action="store_true",
        default=_env_bool("PORTAL_EXTRACT_ZIP", True),
        help="Extrai arquivos .zip apos download",
    )
    parser.add_argument(
        "--keep-zip",
        action="store_true",
        default=_env_bool("PORTAL_KEEP_ZIP", False),
        help="Mantem o zip original apos extracao",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Forca download mesmo sem alteracao em metadata_modified",
    )
    parser.add_argument(
        "--periodic",
        action="store_true",
        default=_env_bool("PORTAL_PERIODIC", False),
        help="Executa em loop periodico",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=int(_env("PORTAL_INTERVAL_MINUTES", "60")),
        help="Intervalo (minutos) para modo periodico",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    formats = [item.strip().lower() for item in args.formats.split(",") if item.strip()]
    if not formats:
        raise ValueError("Informe ao menos um formato em --formats")
    if args.year_start > args.year_end:
        raise ValueError("--year-start nao pode ser maior que --year-end")

    downloader = DenguePortalDownloader(settings=get_settings())

    if args.periodic:
        logger.info("Executando downloader em modo periodico")
        downloader.sync_periodic(
            formats=formats,
            year_start=args.year_start,
            year_end=args.year_end,
            interval_minutes=args.interval_minutes,
            extract_zip=args.extract_zip,
            keep_zip=args.keep_zip,
        )
        return

    logger.info("Executando downloader em modo unico")
    summary = downloader.sync_once(
        formats=formats,
        year_start=args.year_start,
        year_end=args.year_end,
        extract_zip=args.extract_zip,
        keep_zip=args.keep_zip,
        force=args.force,
    )
    logger.info("Resumo downloader: %s", summary)


if __name__ == "__main__":
    main()
