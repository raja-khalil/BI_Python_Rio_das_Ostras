"""Ponto de entrada de producao para pipeline de dengue."""

from __future__ import annotations

from datetime import date, timedelta
import os

import pandas as pd

from src.banco.loaders import (
    carregar_dataframe_postgres,
    deletar_por_intervalo_data,
    truncar_tabela,
)
from src.banco.metadata import (
    PipelineContext,
    finalizar_execucao,
    garantir_tabelas_metadados,
    iniciar_execucao,
    obter_ultima_data_sucesso,
)
from src.config.settings import get_settings
from src.ingestao.orchestrator import IngestionOrchestrator, IngestionRequest
from src.transformacao.cleaning import pipeline_limpeza_padrao
from src.transformacao.dengue_fato import preparar_fato_dengue
from src.utils.logger import get_logger
from src.validacao.validators import validar_colunas_obrigatorias, validar_dataframe_nao_vazio


logger = get_logger(__name__)

PIPELINE_NAME = "dengue_ms_pipeline"
DATA_SOURCE = "arboviroses_dengue"
TARGET_TABLE = "fato_dengue_casos"
TARGET_DATE_COLUMN = "data_notificacao"


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    return int(_env_str(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    raw = _env_str(name, str(default)).lower()
    return raw in {"1", "true", "t", "yes", "y", "sim"}


def _run_batch(
    orchestrator: IngestionOrchestrator,
    request: IngestionRequest,
) -> tuple[pd.DataFrame, date | None]:
    df_bruto = orchestrator.run(request)
    if df_bruto.empty:
        return df_bruto, None

    df_tratado = pipeline_limpeza_padrao(df_bruto)
    df_fato = preparar_fato_dengue(df_tratado)

    validar_dataframe_nao_vazio(df_fato)
    validar_colunas_obrigatorias(df_fato, ["municipio", "data_notificacao"])

    max_data = pd.to_datetime(df_fato["data_notificacao"], errors="coerce").max()
    last_date = None if pd.isna(max_data) else max_data.date()
    return df_fato, last_date


def executar_historico(
    orchestrator: IngestionOrchestrator,
    schema: str,
) -> tuple[int, date | None]:
    start_year = _env_int("HISTORICAL_START_YEAR", 2000)
    end_year = _env_int("HISTORICAL_END_YEAR", 2026)
    batch_years = _env_int("HISTORICAL_BATCH_YEARS", 3)
    truncate_before_load = _env_bool("HISTORICAL_TRUNCATE_BEFORE_LOAD", True)

    if start_year > end_year:
        raise ValueError("HISTORICAL_START_YEAR nao pode ser maior que HISTORICAL_END_YEAR")

    total_rows = 0
    global_last_date: date | None = None
    first_batch = True

    for batch_start in range(start_year, end_year + 1, batch_years):
        batch_end = min(batch_start + batch_years - 1, end_year)
        logger.info("Processando lote historico: %s-%s", batch_start, batch_end)

        request = IngestionRequest(
            source_type="api",
            source_identifier=DATA_SOURCE,
            historical=True,
            start_year=batch_start,
            end_year=batch_end,
            api_limit=get_settings().api_page_size,
            api_max_pages_per_year=get_settings().api_max_pages_per_year,
        )

        df_fato, batch_last_date = _run_batch(orchestrator, request)
        if df_fato.empty:
            continue

        if first_batch and truncate_before_load:
            logger.info("Truncando tabela de destino antes da carga historica")
            truncar_tabela(schema=schema, table_name=TARGET_TABLE)

        carregar_dataframe_postgres(
            df=df_fato,
            table_name=TARGET_TABLE,
            schema=schema,
            if_exists="append",
        )
        first_batch = False
        total_rows += len(df_fato)

        if batch_last_date and (global_last_date is None or batch_last_date > global_last_date):
            global_last_date = batch_last_date

    return total_rows, global_last_date


def executar_incremental(
    orchestrator: IngestionOrchestrator,
    schema: str,
    context: PipelineContext,
) -> tuple[int, date | None]:
    overlap_days = _env_int("INCREMENTAL_OVERLAP_DAYS", 2)
    fallback_start_date = date(_env_int("INCREMENTAL_FALLBACK_YEAR", date.today().year), 1, 1)

    last_success_date = obter_ultima_data_sucesso(context)
    if last_success_date:
        since_date = last_success_date - timedelta(days=overlap_days)
    else:
        since_date = fallback_start_date

    logger.info("Executando incremental a partir de %s", since_date.isoformat())

    request = IngestionRequest(
        source_type="api",
        source_identifier=DATA_SOURCE,
        historical=False,
        incremental_key="dt_notific",
        start_year=since_date.year,
        end_year=date.today().year,
        since_date=since_date,
        api_limit=get_settings().api_page_size,
        api_max_pages_per_year=get_settings().api_max_pages_per_year,
    )

    df_fato, last_date = _run_batch(orchestrator, request)
    if df_fato.empty:
        return 0, last_success_date

    deleted = deletar_por_intervalo_data(
        schema=schema,
        table_name=TARGET_TABLE,
        coluna_data=TARGET_DATE_COLUMN,
        data_inicio=since_date,
    )
    logger.info("Registros removidos para recarga incremental: %s", deleted)

    carregar_dataframe_postgres(
        df=df_fato,
        table_name=TARGET_TABLE,
        schema=schema,
        if_exists="append",
    )
    return len(df_fato), last_date


def main() -> None:
    """Executa pipeline em modo historico ou incremental."""
    settings = get_settings()
    context = PipelineContext(
        pipeline_name=PIPELINE_NAME,
        data_source=DATA_SOURCE,
        schema=settings.db_schema,
    )

    garantir_tabelas_metadados(schema=settings.db_schema)
    execution_mode = _env_str("PIPELINE_MODE", "incremental").lower()
    run_id = iniciar_execucao(context=context, execution_mode=execution_mode)

    rows_loaded = 0
    last_success_date: date | None = None

    try:
        orchestrator = IngestionOrchestrator(settings=settings)

        if execution_mode == "historical":
            logger.info("Modo de execucao: historico")
            rows_loaded, last_success_date = executar_historico(
                orchestrator=orchestrator,
                schema=settings.db_schema,
            )
        elif execution_mode == "incremental":
            logger.info("Modo de execucao: incremental")
            rows_loaded, last_success_date = executar_incremental(
                orchestrator=orchestrator,
                schema=settings.db_schema,
                context=context,
            )
        else:
            raise ValueError("PIPELINE_MODE invalido. Use 'historical' ou 'incremental'")

        finalizar_execucao(
            context=context,
            run_id=run_id,
            status="success",
            rows_loaded=rows_loaded,
            last_success_date=last_success_date,
            message=f"Execucao concluida no modo {execution_mode}",
        )
        logger.info("Pipeline finalizado com sucesso. rows_loaded=%s", rows_loaded)
    except Exception as exc:
        finalizar_execucao(
            context=context,
            run_id=run_id,
            status="failed",
            rows_loaded=rows_loaded,
            last_success_date=None,
            message=str(exc),
        )
        logger.exception("Falha na execucao do pipeline")
        raise


if __name__ == "__main__":
    main()
