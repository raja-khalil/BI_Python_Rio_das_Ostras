"""Acesso a dados analiticos para o dashboard."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.banco.database import get_engine


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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
def load_casos_mes_uf(classificacoes: tuple[str, ...] = tuple(), data_inicio: str | None = None) -> pd.DataFrame:
    """Retorna serie mensal por UF."""
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
def load_casos_mes_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna totais mensais do municipio foco no RJ."""
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
def load_status_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna distribuicao de classificacao/evolucao por mes no municipio foco."""
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
def load_unidade_notificadora_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    top_n: int = 10,
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Distribuicao por unidade notificadora no municipio foco, quando coluna existir."""
    columns = load_fato_columns()
    unidade_col = _resolve_column(columns, ["id_unidade", "ID_UNIDADE"])
    if unidade_col is None:
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
            ON LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(f."{unidade_col}"), ''), '0'), '[^0-9]', '', 'g'), 7, '0')
             = cn.cnes_norm
        """
        if has_cnes_dim
        else ""
    )

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


@st.cache_data(show_spinner=False, ttl=300)
def load_unidade_notificadora_mensal_municipio(
    municipio_nome: str,
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Serie mensal por unidade notificadora no municipio foco."""
    columns = load_fato_columns()
    unidade_col = _resolve_column(columns, ["id_unidade", "ID_UNIDADE"])
    if unidade_col is None:
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
            ON LPAD(REGEXP_REPLACE(COALESCE(NULLIF(TRIM(f."{unidade_col}"), ''), '0'), '[^0-9]', '', 'g'), 7, '0')
             = cn.cnes_norm
        """
        if has_cnes_dim
        else ""
    )

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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
def load_casos_municipio_ano_rj_enriquecido(
    classificacoes: tuple[str, ...] = tuple(),
    data_inicio: str | None = None,
) -> pd.DataFrame:
    """Retorna casos por municipio/mes no RJ com enriquecimento da dimensao IBGE."""
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


@st.cache_data(show_spinner=False, ttl=300)
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


@st.cache_data(show_spinner=False, ttl=300)
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


def get_classificacao_tuple(classificacao: Sequence[str] | None) -> tuple[str, ...]:
    """Normaliza filtro de classificacao para uso em cache."""
    return _norm_classificacoes(classificacao)
