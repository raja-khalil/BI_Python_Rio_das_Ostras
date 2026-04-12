"""Carga eficiente dos JSONs extraidos do portal para PostgreSQL."""

from __future__ import annotations

import argparse
from datetime import date
from itertools import chain
from pathlib import Path

from src.banco.loaders import carregar_dataframe_postgres, deletar_por_intervalo_data
from src.banco.schema import garantir_colunas_fato_dengue
from src.config.settings import get_settings
from src.ingestao.reader_json_stream import iter_json_chunks
from src.transformacao.cleaning import pipeline_limpeza_padrao
from src.transformacao.dengue_fato import preparar_fato_dengue
from src.utils.logger import get_logger
from src.validacao.validators import validar_colunas_obrigatorias


logger = get_logger(__name__)

TARGET_TABLE = "fato_dengue_casos"
TARGET_DATE_COL = "data_notificacao"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill de JSON extraido por ano")
    parser.add_argument("--year-start", type=int, default=2000)
    parser.add_argument("--year-end", type=int, default=date.today().year)
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Leitura do JSON")
    parser.add_argument("--db-chunksize", type=int, default=1_000, help="Lote de escrita no banco")
    parser.add_argument(
        "--skip-delete-year",
        action="store_true",
        help="Nao apaga dados do ano antes da recarga",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua para os proximos arquivos se um ano falhar",
    )
    return parser.parse_args()


def _list_json_files(base_dir: Path, year_start: int, year_end: int) -> list[tuple[int, Path]]:
    files: list[tuple[int, Path]] = []
    for year in range(year_start, year_end + 1):
        year_dir = base_dir / str(year)
        if not year_dir.exists():
            continue
        for file in sorted(year_dir.glob("*.json")):
            files.append((year, file))
    return files


def main() -> None:
    args = _parse_args()
    settings = get_settings()

    source_base = settings.raw_dir / "json" / "portal_sus" / "extracted"
    garantir_colunas_fato_dengue(schema=settings.db_schema, table_name=TARGET_TABLE)
    files = _list_json_files(source_base, args.year_start, args.year_end)
    if not files:
        logger.warning("Nenhum arquivo JSON encontrado em %s", source_base)
        return

    logger.info("Arquivos para carga: %s", len(files))

    years_processed: set[int] = set()
    total_rows = 0
    failed_files: list[tuple[int, str, str]] = []

    for year, file_path in files:
        logger.info("Processando arquivo %s (ano=%s)", file_path, year)

        try:
            # Protecao contra corrida/remoção externa:
            # so remove dados do ano apos validar que o JSON abre e entrega primeiro chunk.
            if not file_path.exists():
                raise FileNotFoundError(f"Arquivo nao encontrado antes da leitura: {file_path}")

            chunk_iter = iter_json_chunks(path=file_path, chunk_size=args.chunk_size)
            try:
                first_chunk = next(chunk_iter)
            except StopIteration:
                first_chunk = None

            if not args.skip_delete_year and year not in years_processed:
                deleted = deletar_por_intervalo_data(
                    schema=settings.db_schema,
                    table_name=TARGET_TABLE,
                    coluna_data=TARGET_DATE_COL,
                    data_inicio=date(year, 1, 1),
                    data_fim=date(year, 12, 31),
                )
                years_processed.add(year)
                logger.info("Registros removidos para recarga do ano %s: %s", year, deleted)

            file_rows = 0
            # Processa primeiro chunk validado e depois segue iterador restante.
            chunks_to_process = chain(
                [first_chunk] if first_chunk is not None else [],
                chunk_iter,
            )

            for raw_chunk in chunks_to_process:
                if raw_chunk.empty:
                    continue

                cleaned = pipeline_limpeza_padrao(raw_chunk)
                fato = preparar_fato_dengue(cleaned)
                if fato.empty:
                    continue

                validar_colunas_obrigatorias(fato, ["municipio", "data_notificacao"])

                carregar_dataframe_postgres(
                    df=fato,
                    table_name=TARGET_TABLE,
                    schema=settings.db_schema,
                    if_exists="append",
                    chunksize=args.db_chunksize,
                )
                file_rows += len(fato)
                total_rows += len(fato)
                logger.info("Chunk carregado (%s linhas) arquivo=%s", len(fato), file_path.name)

            logger.info("Arquivo concluido: %s linhas carregadas (%s)", file_rows, file_path.name)
        except Exception as exc:
            logger.exception("Falha no arquivo %s (ano=%s)", file_path, year)
            rollback_deleted = deletar_por_intervalo_data(
                schema=settings.db_schema,
                table_name=TARGET_TABLE,
                coluna_data=TARGET_DATE_COL,
                data_inicio=date(year, 1, 1),
                data_fim=date(year, 12, 31),
            )
            logger.warning(
                "Rollback aplicado para ano %s. Linhas removidas apos falha: %s",
                year,
                rollback_deleted,
            )
            failed_files.append((year, file_path.name, str(exc)))
            if not args.continue_on_error:
                break

    logger.info("Backfill finalizado. total_rows=%s", total_rows)
    if failed_files:
        logger.error("Arquivos com falha: %s", failed_files)
        raise RuntimeError(f"Falhas em {len(failed_files)} arquivo(s). Use --continue-on-error para seguir.")


if __name__ == "__main__":
    main()
