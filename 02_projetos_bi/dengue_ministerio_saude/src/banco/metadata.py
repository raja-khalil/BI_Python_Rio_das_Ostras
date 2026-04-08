"""Controle de metadados de execucao de pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import uuid4

from sqlalchemy import text

from src.banco.database import get_engine


@dataclass(frozen=True)
class PipelineContext:
    """Identificacao logica do pipeline."""

    pipeline_name: str
    data_source: str
    schema: str


def garantir_tabelas_metadados(schema: str) -> None:
    """Cria tabelas de metadados se ainda nao existirem."""
    engine = get_engine()
    statements = [
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.bi_pipeline_watermark (
                pipeline_name VARCHAR(120) NOT NULL,
                data_source VARCHAR(120) NOT NULL,
                last_success_date DATE,
                last_run_started_at TIMESTAMPTZ,
                last_run_finished_at TIMESTAMPTZ,
                last_status VARCHAR(20),
                last_row_count BIGINT,
                last_message TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (pipeline_name, data_source)
            )
            """
        ),
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.bi_pipeline_execucoes (
                run_id UUID PRIMARY KEY,
                pipeline_name VARCHAR(120) NOT NULL,
                data_source VARCHAR(120) NOT NULL,
                execution_mode VARCHAR(30) NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                status VARCHAR(20) NOT NULL,
                rows_loaded BIGINT NOT NULL DEFAULT 0,
                message TEXT
            )
            """
        ),
    ]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(stmt)


def obter_ultima_data_sucesso(context: PipelineContext) -> date | None:
    """Le watermark de sucesso para incremental."""
    engine = get_engine()
    stmt = text(
        f"""
        SELECT last_success_date
        FROM {context.schema}.bi_pipeline_watermark
        WHERE pipeline_name = :pipeline_name
          AND data_source = :data_source
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            stmt,
            {
                "pipeline_name": context.pipeline_name,
                "data_source": context.data_source,
            },
        ).scalar_one_or_none()
    return result


def iniciar_execucao(context: PipelineContext, execution_mode: str) -> str:
    """Registra inicio da execucao e retorna run_id."""
    engine = get_engine()
    run_id = str(uuid4())
    now = datetime.now(timezone.utc)

    stmt_exec = text(
        f"""
        INSERT INTO {context.schema}.bi_pipeline_execucoes (
            run_id, pipeline_name, data_source, execution_mode, started_at, status
        )
        VALUES (
            :run_id::uuid, :pipeline_name, :data_source, :execution_mode, :started_at, :status
        )
        """
    )
    stmt_upsert = text(
        f"""
        INSERT INTO {context.schema}.bi_pipeline_watermark (
            pipeline_name, data_source, last_run_started_at, last_status, updated_at
        )
        VALUES (:pipeline_name, :data_source, :started_at, :status, NOW())
        ON CONFLICT (pipeline_name, data_source)
        DO UPDATE SET
            last_run_started_at = EXCLUDED.last_run_started_at,
            last_status = EXCLUDED.last_status,
            updated_at = NOW()
        """
    )

    params = {
        "run_id": run_id,
        "pipeline_name": context.pipeline_name,
        "data_source": context.data_source,
        "execution_mode": execution_mode,
        "started_at": now,
        "status": "running",
    }

    with engine.begin() as conn:
        conn.execute(stmt_exec, params)
        conn.execute(stmt_upsert, params)

    return run_id


def finalizar_execucao(
    context: PipelineContext,
    run_id: str,
    status: str,
    rows_loaded: int,
    last_success_date: date | None,
    message: str | None = None,
) -> None:
    """Finaliza execucao com status e watermark atualizado."""
    engine = get_engine()
    now = datetime.now(timezone.utc)

    stmt_exec = text(
        f"""
        UPDATE {context.schema}.bi_pipeline_execucoes
        SET finished_at = :finished_at,
            status = :status,
            rows_loaded = :rows_loaded,
            message = :message
        WHERE run_id = :run_id::uuid
        """
    )

    stmt_upsert = text(
        f"""
        INSERT INTO {context.schema}.bi_pipeline_watermark (
            pipeline_name,
            data_source,
            last_success_date,
            last_run_finished_at,
            last_status,
            last_row_count,
            last_message,
            updated_at
        )
        VALUES (
            :pipeline_name,
            :data_source,
            :last_success_date,
            :finished_at,
            :status,
            :rows_loaded,
            :message,
            NOW()
        )
        ON CONFLICT (pipeline_name, data_source)
        DO UPDATE SET
            last_success_date = CASE
                WHEN EXCLUDED.last_success_date IS NOT NULL
                THEN EXCLUDED.last_success_date
                ELSE {context.schema}.bi_pipeline_watermark.last_success_date
            END,
            last_run_finished_at = EXCLUDED.last_run_finished_at,
            last_status = EXCLUDED.last_status,
            last_row_count = EXCLUDED.last_row_count,
            last_message = EXCLUDED.last_message,
            updated_at = NOW()
        """
    )

    params = {
        "run_id": run_id,
        "pipeline_name": context.pipeline_name,
        "data_source": context.data_source,
        "last_success_date": last_success_date if status == "success" else None,
        "finished_at": now,
        "status": status,
        "rows_loaded": rows_loaded,
        "message": message,
    }

    with engine.begin() as conn:
        conn.execute(stmt_exec, params)
        conn.execute(stmt_upsert, params)
