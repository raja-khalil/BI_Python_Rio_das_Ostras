"""Acesso a dados analiticos para o dashboard."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.banco.database import get_engine

CACHE_TTL_SECONDS = 1800  # 30 minutos para reduzir round-trips ao banco.

def _norm_classificacoes(classificacoes: Sequence[str] | None) -> tuple[str, ...]:
    if not classificacoes:
        return tuple()
    return tuple(str(v).strip() for v in classificacoes if str(v).strip())


def _resolve_column(columns: set[str], candidates: list[str]) -> str | None:
    """Resolve nome de coluna com match case-insensitive."""
    lower_map = {col.lower(): col for col in columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


@st.cache_data(show_spinner=False, ttl=3600)
def load_fato_columns() -> set[str]:
    """Retorna conjunto de colunas da fato para checagem de disponibilidade."""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='saude'
          AND table_name='fato_dengue_casos'
    """
    df = pd.read_sql(sql, get_engine())
    return set(df["column_name"].astype(str).tolist())


@st.cache_data(show_spinner=False, ttl=3600)
def table_exists(schema_name: str, table_name: str) -> bool:
    """Verifica existencia de tabela no banco."""
    sql = text(
        """
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_name = :table_name
        ) AS exists_flag
        """
    )
    df = pd.read_sql(sql, get_engine(), params={"schema_name": schema_name, "table_name": table_name})
    return bool(df.iloc[0]["exists_flag"]) if not df.empty else False


@st.cache_data(show_spinner=False, ttl=3600)
def relation_exists(schema_name: str, relation_name: str) -> bool:
    """Verifica existencia de tabela/view/materialized view no schema."""
    sql = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = :schema_name
              AND c.relname = :relation_name
              AND c.relkind IN ('r', 'v', 'm')
        ) AS exists_flag
        """
    )
    df = pd.read_sql(sql, get_engine(), params={"schema_name": schema_name, "relation_name": relation_name})
    return bool(df.iloc[0]["exists_flag"]) if not df.empty else False


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_ano(classificacoes: tuple[str, ...] = tuple()) -> pd.DataFrame:
    """Retorna totais anuais de casos."""
    sql = text(
        """
        SELECT
            EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
            COUNT(*)::BIGINT AS total_casos
        FROM saude.fato_dengue_casos
        WHERE data_notificacao IS NOT NULL
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1
        ORDER BY 1
        """
    )
    return pd.read_sql(
        sql,
        get_engine(),
        params={
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_mes_uf(classificacoes: tuple[str, ...] = tuple(), data_inicio: str | None = None) -> pd.DataFrame:
    """Retorna serie mensal por UF."""
    if relation_exists("saude", "mv_painel1_mes_uf_classif"):
        sql = text(
            """
            SELECT
                ano,
                mes_referencia,
                uf,
                SUM(total_casos)::BIGINT AS total_casos
            FROM saude.mv_painel1_mes_uf_classif
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR mes_referencia >= CAST(:data_inicio AS DATE))
              AND (
                  :classificacao_vazia
                  OR classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3
            ORDER BY 2, 3
            """
        )
    else:
        sql = text(
            """
            SELECT
                EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
                DATE_TRUNC('month', data_notificacao)::DATE AS mes_referencia,
                COALESCE(NULLIF(TRIM(uf), ''), 'NI') AS uf,
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos
            WHERE data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR data_notificacao >= CAST(:data_inicio AS DATE))
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3
            ORDER BY 2, 3
            """
        )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_mes_rio_das_ostras(classificacoes: tuple[str, ...] = tuple()) -> pd.DataFrame:
    """Retorna totais mensais de Rio das Ostras (regra de negocio oficial)."""
    sql = text(
        """
        SELECT
            EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
            DATE_TRUNC('month', data_notificacao)::DATE AS mes_referencia,
            COUNT(*)::BIGINT AS total_casos
        FROM saude.fato_dengue_casos
        WHERE data_notificacao IS NOT NULL
          AND COALESCE(NULLIF(TRIM(uf), ''), 'NI') IN ('RJ', '33')
          AND (
                UPPER(TRIM(municipio)) = '3304524'
                OR UPPER(TRIM(municipio)) LIKE '330452%%'
                OR UPPER(TRIM(municipio)) = 'RIO DAS OSTRAS'
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1, 2
        ORDER BY 2
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_mes_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna totais mensais do municipio foco no RJ."""
    if relation_exists("saude", "mv_painel1_2_mes_municipio_rj"):
        sql = text(
            """
            SELECT
                ano,
                mes_referencia,
                SUM(total_casos)::BIGINT AS total_casos
            FROM saude.mv_painel1_2_mes_municipio_rj
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR mes_referencia >= CAST(:data_inicio AS DATE))
              AND UPPER(municipio_nome) = UPPER(:municipio_nome)
              AND (
                  :classificacao_vazia
                  OR classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2
            ORDER BY 2
            """
        )
    else:
        sql = text(
            """
            SELECT
                EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
                DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2
            ORDER BY 2
            """
        )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_status_rio_das_ostras(classificacoes: tuple[str, ...] = tuple()) -> pd.DataFrame:
    """Retorna distribuicao de classificacao final e evolucao para Rio das Ostras."""
    sql = text(
        """
        SELECT
            EXTRACT(YEAR FROM data_notificacao)::INTEGER AS ano,
            DATE_TRUNC('month', data_notificacao)::DATE AS mes_referencia,
            COALESCE(NULLIF(TRIM(classificacao_final), ''), 'NI') AS classificacao_final,
            COALESCE(NULLIF(TRIM(evolucao_caso), ''), 'NI') AS evolucao_caso,
            COUNT(*)::BIGINT AS total_casos
        FROM saude.fato_dengue_casos
        WHERE data_notificacao IS NOT NULL
          AND COALESCE(NULLIF(TRIM(uf), ''), 'NI') IN ('RJ', '33')
          AND (
                UPPER(TRIM(municipio)) = '3304524'
                OR UPPER(TRIM(municipio)) LIKE '330452%%'
                OR UPPER(TRIM(municipio)) = 'RIO DAS OSTRAS'
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1, 2, 3, 4
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_status_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna distribuicao de classificacao/evolucao por mes no municipio foco."""
    if relation_exists("saude", "mv_painel1_2_mes_municipio_rj"):
        sql = text(
            """
            SELECT
                ano,
                mes_referencia,
                classificacao_final,
                evolucao_caso,
                SUM(total_casos)::BIGINT AS total_casos
            FROM saude.mv_painel1_2_mes_municipio_rj
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR mes_referencia >= CAST(:data_inicio AS DATE))
              AND UPPER(municipio_nome) = UPPER(:municipio_nome)
              AND (
                  :classificacao_vazia
                  OR classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4
            ORDER BY 2
            """
        )
    else:
        sql = text(
            """
            SELECT
                EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
                DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
                COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') AS classificacao_final,
                COALESCE(NULLIF(TRIM(f.evolucao_caso), ''), 'NI') AS evolucao_caso,
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4
            ORDER BY 2
            """
        )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_populacao_refs(municipio_nome: str) -> pd.DataFrame:
    """Retorna populacao de referencia para municipio, RJ e Brasil."""
    sql = text(
        """
        WITH pop AS (
            SELECT
                SUM(total_pessoas)::BIGINT AS pop_brasil,
                SUM(total_pessoas) FILTER (WHERE UPPER(nm_uf) = 'RIO DE JANEIRO')::BIGINT AS pop_rj,
                SUM(total_pessoas) FILTER (WHERE UPPER(nm_mun) = UPPER(:municipio_nome))::BIGINT AS pop_municipio
            FROM saude.dim_ibge_municipio
        )
        SELECT pop_brasil, pop_rj, pop_municipio
        FROM pop
        """
    )
    return pd.read_sql(sql, get_engine(), params={"municipio_nome": municipio_nome})


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_sexo_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Distribuicao por sexo no municipio foco, quando coluna existir."""
    columns = load_fato_columns()
    sexo_col = _resolve_column(columns, ["cs_sexo", "sexo"])
    if sexo_col is None:
        return pd.DataFrame()

    sql = text(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(f."{sexo_col}"), ''), 'NI') AS sexo,
            COUNT(*)::BIGINT AS total_casos
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1
        ORDER BY 2 DESC
        """
    )
    return pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_unidade_notificadora_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    top_n: int = 10,
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Distribuicao por unidade notificadora no municipio foco, quando coluna existir."""
    columns = load_fato_columns()
    unidade_col = _resolve_column(columns, ["id_unidade", "ID_UNIDADE"])
    has_mv = relation_exists("saude", "mv_painel2_mes_unidade_municipio_rj")
    if unidade_col is None and not has_mv:
        return pd.DataFrame()
    has_cnes_dim = table_exists("saude", "dim_cnes_estabelecimento")
    select_nome = (
        "COALESCE(NULLIF(TRIM(cn.nome_fantasia), ''), NULLIF(TRIM(cn.nome_empresarial), ''), NULLIF(TRIM(cn.razao_social), ''), '') AS unidade_nome,"
        if has_cnes_dim
        else "''::VARCHAR AS unidade_nome,"
    )
    join_cnes = (
        f"""
        LEFT JOIN (
            SELECT
                LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(cnes), ''), '0'), '[^0-9]', '', 'g'), 7, '0') AS cnes_norm,
                MAX(NULLIF(TRIM(nome_fantasia), '')) AS nome_fantasia,
                MAX(NULLIF(TRIM(nome_empresarial), '')) AS nome_empresarial,
                MAX(NULLIF(TRIM(razao_social), '')) AS razao_social
            FROM saude.dim_cnes_estabelecimento
            GROUP BY 1
        ) cn
            ON LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(src.unidade_notificadora), ''), '0'), '[^0-9]', '', 'g'), 7, '0')
             = cn.cnes_norm
        """
        if has_cnes_dim
        else ""
    )
    if has_mv:
        sql = text(
            f"""
            SELECT
                src.unidade_notificadora,
                {select_nome}
                SUM(src.total_casos)::BIGINT AS total_casos
            FROM saude.mv_painel2_mes_unidade_municipio_rj src
            {join_cnes}
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR src.mes_referencia >= CAST(:data_inicio AS DATE))
              AND UPPER(src.municipio_nome) = UPPER(:municipio_nome)
              AND (
                  :classificacao_vazia
                  OR src.classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT :top_n
            """
        )
    else:
        join_cnes_fato = join_cnes.replace("src.unidade_notificadora", f'f."{unidade_col}"')
        sql = text(
            f"""
            SELECT
                COALESCE(NULLIF(TRIM(f."{unidade_col}"), ''), 'NI') AS unidade_notificadora,
                {select_nome}
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            {join_cnes_fato}
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT :top_n
            """
        )
    return pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
            "top_n": top_n,
        },
    )


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_unidade_notificadora_mensal_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Serie mensal por unidade notificadora no municipio foco."""
    columns = load_fato_columns()
    unidade_col = _resolve_column(columns, ["id_unidade", "ID_UNIDADE"])
    has_mv = relation_exists("saude", "mv_painel2_mes_unidade_municipio_rj")
    if unidade_col is None and not has_mv:
        return pd.DataFrame()

    has_cnes_dim = table_exists("saude", "dim_cnes_estabelecimento")
    select_nome = (
        "COALESCE(NULLIF(TRIM(cn.nome_fantasia), ''), NULLIF(TRIM(cn.nome_empresarial), ''), NULLIF(TRIM(cn.razao_social), ''), '') AS unidade_nome,"
        if has_cnes_dim
        else "''::VARCHAR AS unidade_nome,"
    )
    join_cnes = (
        f"""
        LEFT JOIN (
            SELECT
                LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(cnes), ''), '0'), '[^0-9]', '', 'g'), 7, '0') AS cnes_norm,
                MAX(NULLIF(TRIM(nome_fantasia), '')) AS nome_fantasia,
                MAX(NULLIF(TRIM(nome_empresarial), '')) AS nome_empresarial,
                MAX(NULLIF(TRIM(razao_social), '')) AS razao_social
            FROM saude.dim_cnes_estabelecimento
            GROUP BY 1
        ) cn
            ON LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(src.unidade_notificadora), ''), '0'), '[^0-9]', '', 'g'), 7, '0')
             = cn.cnes_norm
        """
        if has_cnes_dim
        else ""
    )
    if has_mv:
        sql = text(
            f"""
            SELECT
                src.ano,
                src.mes_referencia,
                src.unidade_notificadora,
                {select_nome}
                SUM(src.total_casos)::BIGINT AS total_casos
            FROM saude.mv_painel2_mes_unidade_municipio_rj src
            {join_cnes}
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR src.mes_referencia >= CAST(:data_inicio AS DATE))
              AND UPPER(src.municipio_nome) = UPPER(:municipio_nome)
              AND (
                  :classificacao_vazia
                  OR src.classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4
            ORDER BY 2, 5 DESC
            """
        )
    else:
        join_cnes_fato = join_cnes.replace("src.unidade_notificadora", f'f."{unidade_col}"')
        sql = text(
            f"""
            SELECT
                EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
                DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
                COALESCE(NULLIF(TRIM(f."{unidade_col}"), ''), 'NI') AS unidade_notificadora,
                {select_nome}
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            {join_cnes_fato}
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4
            ORDER BY 2, 5 DESC
            """
        )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_semana_epidemiologica_media_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna semana epidemiologica media do recorte no municipio foco."""
    columns = load_fato_columns()
    semana_col = _resolve_column(columns, ["semana_epidemiologica", "sem_not", "sem_pri"])
    if semana_col is None:
        return pd.DataFrame(columns=["semana_media"])

    sql = text(
        f"""
        WITH base AS (
            SELECT
                CASE
                    WHEN f."{semana_col}" IS NULL THEN NULL
                    ELSE RIGHT(LPAD(REGEXP_REPLACE(f."{semana_col}"::TEXT, '[^0-9]', '', 'g'), 6, '0'), 2)::INTEGER
                END AS semana_epi
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
        )
        SELECT ROUND(AVG(semana_epi)::NUMERIC, 0)::INTEGER AS semana_media
        FROM base
        WHERE semana_epi BETWEEN 1 AND 53
        """
    )
    return pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_perfil_top_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna categoria mais frequente por campo de perfil no municipio foco."""
    columns = load_fato_columns()
    candidates = {
        "idade": ["nu_idade_n"],
        "gestante": ["cs_gestant"],
        "raca": ["cs_raca"],
        "escolaridade": ["cs_escol_n"],
    }
    where_categoria = {
        "idade": """NULLIF(TRIM(f."{col}"::TEXT), '') IS NOT NULL
                    AND TRIM(f."{col}"::TEXT) <> 'NI'""",
        "gestante": """COALESCE(NULLIF(TRIM(f."{col}"::TEXT), ''), 'NI') IN ('1','2','3','4')""",
        "raca": """COALESCE(NULLIF(TRIM(f."{col}"::TEXT), ''), 'NI') IN ('1','2','3','4','5')""",
        "escolaridade": """COALESCE(NULLIF(TRIM(f."{col}"::TEXT), ''), 'NI')
                           IN ('1','01','2','02','3','03','4','04','5','05','6','06','7','07','8','08')""",
    }

    rows: list[dict[str, object]] = []
    engine = get_engine()
    for campo, cands in candidates.items():
        col = _resolve_column(columns, cands)
        if col is None:
            rows.append({"campo": campo, "valor_raw": None, "total_casos": None})
            continue

        filtro_categoria = where_categoria[campo].replace("{col}", col)
        sql = text(
            f"""
            SELECT
                COALESCE(NULLIF(TRIM(f."{col}"::TEXT), ''), 'NI') AS valor_raw,
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            LEFT JOIN saude.dim_ibge_municipio d
                ON (
                    CASE
                        WHEN f.municipio ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                                WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                                ELSE LPAD(f.municipio, 6, '0')
                            END
                        ELSE NULL
                    END
                ) = d.cd_mun6
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
              AND (
                  UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
                  OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
              )
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
              AND ({filtro_categoria})
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1
            """
        )
        df = pd.read_sql(
            sql,
            engine,
            params={
                "municipio_nome": municipio_nome,
                "data_inicio": data_inicio,
                "classificacao_vazia": len(classificacoes) == 0,
                "classificacoes": list(classificacoes),
            },
        )
        if df.empty:
            rows.append({"campo": campo, "valor_raw": None, "total_casos": None})
        else:
            rows.append(
                {
                    "campo": campo,
                    "valor_raw": df.iloc[0]["valor_raw"],
                    "total_casos": int(df.iloc[0]["total_casos"]) if pd.notna(df.iloc[0]["total_casos"]) else None,
                }
            )

    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_municipio_ano_rj_enriquecido(
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna casos por municipio/mes no RJ com enriquecimento da dimensao IBGE."""
    if relation_exists("saude", "mv_painel1_2_mes_municipio_rj"):
        sql = text(
            """
            SELECT
                ano,
                mes_referencia,
                uf,
                municipio_codigo,
                municipio_nome,
                SUM(total_casos)::BIGINT AS total_casos,
                MAX(total_pessoas)::BIGINT AS total_pessoas,
                MAX(area_km2)::NUMERIC AS area_km2,
                CASE
                    WHEN MAX(total_pessoas) > 0 THEN (SUM(total_casos)::NUMERIC / MAX(total_pessoas)::NUMERIC) * 100000
                    ELSE NULL
                END AS incidencia_100k
            FROM saude.mv_painel1_2_mes_municipio_rj
            WHERE (CAST(:data_inicio AS DATE) IS NULL OR mes_referencia >= CAST(:data_inicio AS DATE))
              AND (
                  :classificacao_vazia
                  OR classificacao_final = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4, 5
            """
        )
    else:
        sql = text(
            """
            WITH base AS (
                SELECT
                    EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
                    DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
                    COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
                    COALESCE(NULLIF(TRIM(f.municipio), ''), 'Municipio nao informado') AS municipio_raw,
                    COUNT(*)::BIGINT AS total_casos
                FROM saude.fato_dengue_casos f
                WHERE f.data_notificacao IS NOT NULL
                  AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
                  AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
                  AND (
                      :classificacao_vazia
                      OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
                  )
                GROUP BY 1, 2, 3, 4
            ),
            base_norm AS (
                SELECT
                    b.*,
                    CASE
                        WHEN b.municipio_raw ~ '^[0-9]+$' THEN
                            CASE
                                WHEN LENGTH(b.municipio_raw) >= 7 THEN SUBSTRING(b.municipio_raw FROM 1 FOR 6)
                                WHEN LENGTH(b.municipio_raw) = 6 THEN b.municipio_raw
                                ELSE LPAD(b.municipio_raw, 6, '0')
                            END
                        ELSE NULL
                    END AS cd_mun6
                FROM base b
            )
            SELECT
                b.ano,
                b.mes_referencia,
                b.uf,
                b.municipio_raw AS municipio_codigo,
                COALESCE(d.nm_mun, b.municipio_raw) AS municipio_nome,
                b.total_casos,
                d.total_pessoas,
                d.area_km2,
                CASE
                    WHEN d.total_pessoas > 0 THEN (b.total_casos::NUMERIC / d.total_pessoas::NUMERIC) * 100000
                    ELSE NULL
                END AS incidencia_100k
            FROM base_norm b
            LEFT JOIN saude.dim_ibge_municipio d
                ON b.cd_mun6 = d.cd_mun6
            """
        )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_casos_municipio_ano_brasil_enriquecido(
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna casos por municipio/mes no Brasil com enriquecimento da dimensao IBGE."""
    sql = text(
        """
        WITH base AS (
            SELECT
                EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
                DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
                COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') AS uf,
                COALESCE(NULLIF(TRIM(f.municipio), ''), 'Municipio nao informado') AS municipio_raw,
                COUNT(*)::BIGINT AS total_casos
            FROM saude.fato_dengue_casos f
            WHERE f.data_notificacao IS NOT NULL
              AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
              AND (
                  :classificacao_vazia
                  OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
              )
            GROUP BY 1, 2, 3, 4
        ),
        base_norm AS (
            SELECT
                b.*,
                CASE
                    WHEN b.municipio_raw ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(b.municipio_raw) >= 7 THEN SUBSTRING(b.municipio_raw FROM 1 FOR 6)
                            WHEN LENGTH(b.municipio_raw) = 6 THEN b.municipio_raw
                            ELSE LPAD(b.municipio_raw, 6, '0')
                        END
                    ELSE NULL
                END AS cd_mun6
            FROM base b
        )
        SELECT
            b.ano,
            b.mes_referencia,
            b.uf,
            b.municipio_raw AS municipio_codigo,
            COALESCE(d.nm_mun, b.municipio_raw) AS municipio_nome,
            b.total_casos,
            d.total_pessoas,
            d.area_km2,
            CASE
                WHEN d.total_pessoas > 0 THEN (b.total_casos::NUMERIC / d.total_pessoas::NUMERIC) * 100000
                ELSE NULL
            END AS incidencia_100k
        FROM base_norm b
        LEFT JOIN saude.dim_ibge_municipio d
            ON b.cd_mun6 = d.cd_mun6
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_internacoes_mensal_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna internacoes mensais no municipio foco, se campo existir."""
    columns = load_fato_columns()
    intern_col = _resolve_column(columns, ["hospitalizacao", "hospitaliz"])
    if intern_col is None:
        return pd.DataFrame(columns=["mes_referencia", "internacoes"])

    sql = text(
        f"""
        SELECT
            DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
            COUNT(*) FILTER (
                WHERE COALESCE(NULLIF(TRIM(f."{intern_col}"), ''), 'NI') = '1'
            )::BIGINT AS internacoes
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1
        ORDER BY 1
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_perfil_demografico_mensal_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Serie mensal de perfil (faixa etaria, sexo, raca) com gravidade no municipio foco."""
    columns = load_fato_columns()
    idade_col = _resolve_column(columns, ["nu_idade_n"])
    sexo_col = _resolve_column(columns, ["cs_sexo", "sexo"])
    raca_col = _resolve_column(columns, ["cs_raca"])
    intern_col = _resolve_column(columns, ["hospitaliz", "hospitalizacao"])
    evol_col = _resolve_column(columns, ["evolucao_caso", "evolucao"])

    if idade_col is None and sexo_col is None and raca_col is None:
        return pd.DataFrame()

    idade_expr = (
        f"""
        CASE
            WHEN NULLIF(TRIM(f."{idade_col}"::TEXT), '') IS NULL THEN NULL
            WHEN f."{idade_col}"::TEXT !~ '^[0-9]+$' THEN NULL
            WHEN LEFT(f."{idade_col}"::TEXT, 1) = '4' THEN NULLIF(SUBSTRING(f."{idade_col}"::TEXT FROM 2), '')::INTEGER
            WHEN LEFT(f."{idade_col}"::TEXT, 1) = '3' THEN FLOOR(NULLIF(SUBSTRING(f."{idade_col}"::TEXT FROM 2), '')::NUMERIC / 12)::INTEGER
            WHEN LEFT(f."{idade_col}"::TEXT, 1) IN ('1','2') THEN 0
            ELSE NULL
        END
        """
        if idade_col is not None
        else "NULL::INTEGER"
    )

    faixa_expr = f"""
        CASE
            WHEN ({idade_expr}) IS NULL THEN 'Nao informado'
            WHEN ({idade_expr}) <= 9 THEN '0 a 9'
            WHEN ({idade_expr}) BETWEEN 10 AND 19 THEN '10 a 19'
            WHEN ({idade_expr}) BETWEEN 20 AND 39 THEN '20 a 39'
            WHEN ({idade_expr}) BETWEEN 40 AND 59 THEN '40 a 59'
            ELSE '60+'
        END
    """

    sexo_expr = (
        f"COALESCE(NULLIF(TRIM(f.\"{sexo_col}\"), ''), 'NI')"
        if sexo_col is not None
        else "'NI'"
    )
    raca_expr = (
        f"COALESCE(NULLIF(TRIM(f.\"{raca_col}\"), ''), 'NI')"
        if raca_col is not None
        else "'NI'"
    )
    intern_expr = (
        f"CASE WHEN COALESCE(NULLIF(TRIM(f.\"{intern_col}\"), ''), 'NI') = '1' THEN 1 ELSE 0 END"
        if intern_col is not None
        else "0"
    )
    obito_expr = (
        f"""
        CASE
            WHEN COALESCE(NULLIF(TRIM(f."{evol_col}"), ''), 'NI') IN ('2','3','4') THEN 1
            WHEN UPPER(COALESCE(NULLIF(TRIM(f."{evol_col}"), ''), 'NI')) LIKE '%OBITO%' THEN 1
            ELSE 0
        END
        """
        if evol_col is not None
        else "0"
    )

    sql = text(
        f"""
        SELECT
            EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
            DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
            {faixa_expr} AS faixa_etaria,
            {sexo_expr} AS sexo,
            {raca_expr} AS raca,
            COUNT(*)::BIGINT AS total_casos,
            SUM({intern_expr})::BIGINT AS internacoes,
            SUM({obito_expr})::BIGINT AS obitos
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 2
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_comorbidades_mensal_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Serie mensal de comorbidades no municipio foco."""
    columns = load_fato_columns()
    cand = {
        "diabetes": "Diabetes",
        "hematolog": "Doencas hematologicas",
        "hepatopat": "Hepatopatias",
        "renal": "Doenca renal cronica",
        "hipertensa": "Hipertensao arterial",
        "acido_pept": "Doenca acido-peptica",
        "auto_imune": "Doencas autoimunes",
    }
    present = {col: label for col, label in cand.items() if _resolve_column(columns, [col]) is not None}
    if not present:
        return pd.DataFrame()

    select_parts = []
    for col in present:
        real_col = _resolve_column(columns, [col])
        select_parts.append(
            f"SUM(CASE WHEN COALESCE(NULLIF(TRIM(f.\"{real_col}\"), ''), 'NI') = '1' THEN 1 ELSE 0 END)::BIGINT AS \"{col}\""
        )
    select_sql = ",\n            ".join(select_parts)

    sql = text(
        f"""
        SELECT
            EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
            DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
            {select_sql}
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1, 2
        ORDER BY 2
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if df.empty:
        return df
    df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    # long format
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        for col, label in present.items():
            rows.append(
                {
                    "ano": int(row["ano"]) if pd.notna(row["ano"]) else None,
                    "mes_referencia": row["mes_referencia"],
                    "comorbidade": label,
                    "total_casos": int(row[col]) if pd.notna(row[col]) else 0,
                }
            )
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_perfil_demografico_mensal_escopo(
    escopo: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Serie mensal de perfil (faixa etaria, sexo, raca) para escopo RJ ou BR."""
    columns = load_fato_columns()
    idade_col = _resolve_column(columns, ["nu_idade_n"])
    sexo_col = _resolve_column(columns, ["cs_sexo", "sexo"])
    raca_col = _resolve_column(columns, ["cs_raca"])
    intern_col = _resolve_column(columns, ["hospitaliz", "hospitalizacao"])
    evol_col = _resolve_column(columns, ["evolucao_caso", "evolucao"])

    if idade_col is None and sexo_col is None and raca_col is None:
        return pd.DataFrame()

    idade_expr = (
        f"""
        CASE
            WHEN NULLIF(TRIM(f."{idade_col}"::TEXT), '') IS NULL THEN NULL
            WHEN f."{idade_col}"::TEXT !~ '^[0-9]+$' THEN NULL
            WHEN LEFT(f."{idade_col}"::TEXT, 1) = '4' THEN NULLIF(SUBSTRING(f."{idade_col}"::TEXT FROM 2), '')::INTEGER
            WHEN LEFT(f."{idade_col}"::TEXT, 1) = '3' THEN FLOOR(NULLIF(SUBSTRING(f."{idade_col}"::TEXT FROM 2), '')::NUMERIC / 12)::INTEGER
            WHEN LEFT(f."{idade_col}"::TEXT, 1) IN ('1','2') THEN 0
            ELSE NULL
        END
        """
        if idade_col is not None
        else "NULL::INTEGER"
    )
    faixa_expr = f"""
        CASE
            WHEN ({idade_expr}) IS NULL THEN 'Nao informado'
            WHEN ({idade_expr}) <= 9 THEN '0 a 9'
            WHEN ({idade_expr}) BETWEEN 10 AND 19 THEN '10 a 19'
            WHEN ({idade_expr}) BETWEEN 20 AND 39 THEN '20 a 39'
            WHEN ({idade_expr}) BETWEEN 40 AND 59 THEN '40 a 59'
            ELSE '60+'
        END
    """
    sexo_expr = (
        f"COALESCE(NULLIF(TRIM(f.\"{sexo_col}\"), ''), 'NI')"
        if sexo_col is not None
        else "'NI'"
    )
    raca_expr = (
        f"COALESCE(NULLIF(TRIM(f.\"{raca_col}\"), ''), 'NI')"
        if raca_col is not None
        else "'NI'"
    )
    intern_expr = (
        f"CASE WHEN COALESCE(NULLIF(TRIM(f.\"{intern_col}\"), ''), 'NI') = '1' THEN 1 ELSE 0 END"
        if intern_col is not None
        else "0"
    )
    obito_expr = (
        f"""
        CASE
            WHEN COALESCE(NULLIF(TRIM(f."{evol_col}"), ''), 'NI') IN ('2','3','4') THEN 1
            WHEN UPPER(COALESCE(NULLIF(TRIM(f."{evol_col}"), ''), 'NI')) LIKE '%OBITO%' THEN 1
            ELSE 0
        END
        """
        if evol_col is not None
        else "0"
    )

    cond_escopo = "TRUE"
    escopo_norm = (escopo or "").strip().upper()
    if escopo_norm == "RJ":
        cond_escopo = "COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')"

    sql = text(
        f"""
        SELECT
            EXTRACT(YEAR FROM f.data_notificacao)::INTEGER AS ano,
            DATE_TRUNC('month', f.data_notificacao)::DATE AS mes_referencia,
            {faixa_expr} AS faixa_etaria,
            {sexo_expr} AS sexo,
            {raca_expr} AS raca,
            COUNT(*)::BIGINT AS total_casos,
            SUM({intern_expr})::BIGINT AS internacoes,
            SUM({obito_expr})::BIGINT AS obitos
        FROM saude.fato_dengue_casos f
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND ({cond_escopo})
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 2
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if not df.empty:
        df["mes_referencia"] = pd.to_datetime(df["mes_referencia"], errors="coerce")
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_clinico_exames_registros_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Registros clinicos/laboratoriais no municipio foco para montagem do painel clinico."""
    columns = load_fato_columns()
    base_cols = ["data_notificacao", "classificacao_final", "evolucao_caso", "hospitaliz", "dt_interna"]
    sintomas = [
        "febre",
        "mialgia",
        "cefaleia",
        "exantema",
        "vomito",
        "nausea",
        "dor_costas",
        "conjuntvit",
        "artrite",
        "artralgia",
        "petequia_n",
        "leucopenia",
        "laco",
        "dor_retro",
    ]
    comorbidades = [
        "diabetes",
        "hematolog",
        "hepatopat",
        "renal",
        "hipertensa",
        "acido_pept",
        "auto_imune",
    ]
    exames = [
        "dt_chik_s1",
        "dt_chik_s2",
        "dt_prnt",
        "res_chiks1",
        "res_chiks2",
        "resul_prnt",
        "dt_soro",
        "resul_soro",
        "dt_ns1",
        "resul_ns1",
        "dt_viral",
        "resul_vi_n",
        "dt_pcr",
        "resul_pcr_",
        "sorotipo",
        "histopa_n",
        "imunoh_n",
    ]

    selected = []
    for col in base_cols + sintomas + comorbidades + exames:
        real = _resolve_column(columns, [col])
        if real is not None:
            selected.append(real)

    if "data_notificacao" not in [c.lower() for c in selected]:
        return pd.DataFrame()

    # Remove duplicadas mantendo ordem
    seen = set()
    ordered_cols = []
    for c in selected:
        k = c.lower()
        if k in seen:
            continue
        seen.add(k)
        ordered_cols.append(c)

    select_sql = ", ".join([f'f."{c}" AS "{c.lower()}"' for c in ordered_cols])
    sql = text(
        f"""
        SELECT
            {select_sql}
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        """
    )
    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if df.empty:
        return df
    df["data_notificacao"] = pd.to_datetime(df["data_notificacao"], errors="coerce")
    if "dt_interna" in df.columns:
        df["dt_interna"] = pd.to_datetime(df["dt_interna"], errors="coerce")
    for dt_col in ["dt_chik_s1", "dt_chik_s2", "dt_prnt", "dt_soro", "dt_ns1", "dt_viral", "dt_pcr"]:
        if dt_col in df.columns:
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df["mes_referencia"] = df["data_notificacao"].dt.to_period("M").dt.to_timestamp()
    return df


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_avaliacao_registros_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Registros para painel de avaliacao (tempo, qualidade, exames e unidade)."""
    columns = load_fato_columns()
    has_cnes_dim = table_exists("saude", "dim_cnes_estabelecimento")

    wanted_cols = [
        "data_notificacao",
        "dt_sin_pri",
        "dt_encerra",
        "classificacao_final",
        "evolucao_caso",
        "hospitaliz",
        "id_unidade",
        "cs_sexo",
        "nu_idade_n",
        "cs_raca",
        "cs_escol_n",
        "cs_gestant",
        "resul_soro",
        "resul_ns1",
        "resul_vi_n",
        "resul_pcr_",
        "dt_soro",
        "dt_ns1",
        "dt_viral",
        "dt_pcr",
    ]

    selected: list[str] = []
    for col in wanted_cols:
        real = _resolve_column(columns, [col])
        if real is not None:
            selected.append(real)

    if "data_notificacao" not in [c.lower() for c in selected]:
        return pd.DataFrame()

    seen: set[str] = set()
    ordered_cols: list[str] = []
    for col in selected:
        key = col.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered_cols.append(col)

    select_sql = ", ".join([f'f."{c}" AS "{c.lower()}"' for c in ordered_cols])
    unidade_real = _resolve_column(columns, ["id_unidade"])
    join_cnes = ""
    unidade_nome_sql = "''::VARCHAR AS unidade_nome"
    if has_cnes_dim and unidade_real is not None:
        join_cnes = f"""
        LEFT JOIN (
            SELECT
                LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(cnes), ''), '0'), '[^0-9]', '', 'g'), 7, '0') AS cnes_norm,
                MAX(NULLIF(TRIM(nome_fantasia), '')) AS nome_fantasia,
                MAX(NULLIF(TRIM(nome_empresarial), '')) AS nome_empresarial,
                MAX(NULLIF(TRIM(razao_social), '')) AS razao_social
            FROM saude.dim_cnes_estabelecimento
            GROUP BY 1
        ) cn
            ON LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(f."{unidade_real}"), ''), '0'), '[^0-9]', '', 'g'), 7, '0')
             = cn.cnes_norm
        """
        unidade_nome_sql = (
            "COALESCE(NULLIF(TRIM(cn.nome_fantasia), ''), "
            "NULLIF(TRIM(cn.nome_empresarial), ''), "
            "NULLIF(TRIM(cn.razao_social), ''), '') AS unidade_nome"
        )

    sql = text(
        f"""
        SELECT
            {select_sql},
            {unidade_nome_sql}
        FROM saude.fato_dengue_casos f
        LEFT JOIN saude.dim_ibge_municipio d
            ON (
                CASE
                    WHEN f.municipio ~ '^[0-9]+$' THEN
                        CASE
                            WHEN LENGTH(f.municipio) >= 7 THEN SUBSTRING(f.municipio FROM 1 FOR 6)
                            WHEN LENGTH(f.municipio) = 6 THEN f.municipio
                            ELSE LPAD(f.municipio, 6, '0')
                        END
                    ELSE NULL
                END
            ) = d.cd_mun6
        {join_cnes}
        WHERE f.data_notificacao IS NOT NULL
          AND (CAST(:data_inicio AS DATE) IS NULL OR f.data_notificacao >= CAST(:data_inicio AS DATE))
          AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN ('RJ', '33')
          AND (
              UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
              OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
          )
          AND (
              :classificacao_vazia
              OR COALESCE(NULLIF(TRIM(f.classificacao_final), ''), 'NI') = ANY(:classificacoes)
          )
        """
    )

    df = pd.read_sql(
        sql,
        get_engine(),
        params={
            "municipio_nome": municipio_nome,
            "data_inicio": data_inicio,
            "classificacao_vazia": len(classificacoes) == 0,
            "classificacoes": list(classificacoes),
        },
    )
    if df.empty:
        return df

    for col in ["data_notificacao", "dt_sin_pri", "dt_encerra", "dt_soro", "dt_ns1", "dt_viral", "dt_pcr"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["mes_referencia"] = df["data_notificacao"].dt.to_period("M").dt.to_timestamp()
    return df


def get_classificacao_tuple(classificacao: Sequence[str] | None) -> tuple[str, ...]:
    """Normaliza filtro de classificacao para uso em cache."""
    return _norm_classificacoes(classificacao)

