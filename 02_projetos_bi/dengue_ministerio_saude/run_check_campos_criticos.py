"""Valida carga de campos criticos no banco para um recorte territorial.

Uso basico:
    python run_check_campos_criticos.py

Exemplo customizado:
    python run_check_campos_criticos.py --municipio "Rio das Ostras" --uf RJ --output-csv logs/check_campos_criticos.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.banco.database import get_engine
from src.config.settings import get_settings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check unico de campos criticos (sintomas, comorbidades, exames e perfil).")
    parser.add_argument("--municipio", default="Rio das Ostras", help="Nome do municipio para o recorte.")
    parser.add_argument("--uf", default="RJ", help="Sigla da UF (ex.: RJ).")
    parser.add_argument("--schema", default=None, help="Schema do banco (padrao: DB_SCHEMA do .env).")
    parser.add_argument("--output-csv", default=None, help="Caminho opcional para exportar resultado em CSV.")
    return parser.parse_args()


def _scope_where() -> str:
    return """
    f.data_notificacao IS NOT NULL
    AND COALESCE(NULLIF(TRIM(f.uf), ''), 'NI') IN (:uf_sigla, :uf_codigo)
    AND (
        UPPER(COALESCE(d.nm_mun, TRIM(f.municipio), '')) = UPPER(:municipio_nome)
        OR UPPER(TRIM(f.municipio)) = UPPER(:municipio_nome)
    )
    """


def _field_groups() -> dict[str, dict[str, str]]:
    return {
        "sintomas": {
            "febre": "IN ('1','2')",
            "mialgia": "IN ('1','2')",
            "cefaleia": "IN ('1','2')",
            "exantema": "IN ('1','2')",
            "vomito": "IN ('1','2')",
            "nausea": "IN ('1','2')",
            "dor_costas": "IN ('1','2')",
            "conjuntvit": "IN ('1','2')",
            "artrite": "IN ('1','2')",
            "artralgia": "IN ('1','2')",
            "petequia_n": "IN ('1','2')",
            "leucopenia": "IN ('1','2')",
            "laco": "IN ('1','2')",
            "dor_retro": "IN ('1','2')",
        },
        "comorbidades": {
            "diabetes": "IN ('1','2')",
            "hematolog": "IN ('1','2')",
            "hepatopat": "IN ('1','2')",
            "renal": "IN ('1','2')",
            "hipertensa": "IN ('1','2')",
            "acido_pept": "IN ('1','2')",
            "auto_imune": "IN ('1','2')",
        },
        "exames": {
            "resul_ns1": "IN ('1','2','3','4')",
            "resul_pcr_": "IN ('1','2','3','4')",
            "resul_soro": "IN ('1','2','3','4')",
            "resul_vi_n": "IN ('1','2','3','4')",
        },
        "perfil": {
            "cs_sexo": "IN ('M','F','I')",
            "nu_idade_n": "~ '^[1-4][0-9]+$'",
            "cs_gestant": "IN ('1','2','3','4','5','6','9')",
            "cs_raca": "IN ('1','2','3','4','5','9')",
            "cs_escol_n": "IN ('0','00','1','01','2','02','3','03','4','04','5','05','6','06','7','07','8','08','9','09','10','99')",
        },
    }


def _load_columns(schema: str) -> set[str]:
    sql = text(
        """
        SELECT LOWER(column_name) AS col
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = 'fato_dengue_casos'
        """
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"schema": schema})
    return set(df["col"].tolist())


def _load_total_scope(schema: str, municipio_nome: str, uf_sigla: str) -> int:
    sql = text(
        f"""
        SELECT COUNT(*)::BIGINT AS total
        FROM {schema}.fato_dengue_casos f
        LEFT JOIN {schema}.dim_ibge_municipio d
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
        WHERE {_scope_where()}
        """
    )
    uf_codigo = uf_sigla
    if uf_sigla.upper() == "RJ":
        uf_codigo = "33"
    with get_engine().connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "municipio_nome": municipio_nome,
                "uf_sigla": uf_sigla.upper(),
                "uf_codigo": uf_codigo,
            },
        )
    return int(df.iloc[0]["total"]) if not df.empty else 0


def _load_stats_all(
    schema: str,
    fields_with_pred: dict[str, str],
    municipio_nome: str,
    uf_sigla: str,
    columns: set[str],
) -> dict[str, dict[str, int]]:
    """Executa uma unica consulta agregada para todos os campos existentes."""
    existing_fields = [f for f in fields_with_pred if f.lower() in columns]
    if not existing_fields:
        return {}

    base_select = ["COALESCE(NULLIF(TRIM(f.\"{f}\"::TEXT), ''), 'NI') AS \"{f}\"".format(f=f) for f in existing_fields]
    agg_parts: list[str] = ["COUNT(*)::BIGINT AS total_scope"]
    for f in existing_fields:
        pred = fields_with_pred[f]
        agg_parts.extend(
            [
                f"COUNT(*) FILTER (WHERE \"{f}\" <> 'NI')::BIGINT AS \"{f}__preenchido\"",
                f"COUNT(*) FILTER (WHERE \"{f}\" = 'NI')::BIGINT AS \"{f}__vazio_ni\"",
                f"COUNT(*) FILTER (WHERE \"{f}\" {pred})::BIGINT AS \"{f}__valido\"",
                f"COUNT(*) FILTER (WHERE \"{f}\" <> 'NI' AND NOT (\"{f}\" {pred}))::BIGINT AS \"{f}__invalido\"",
            ]
        )

    sql = text(
        f"""
        WITH base AS (
            SELECT
                {", ".join(base_select)}
            FROM {schema}.fato_dengue_casos f
            LEFT JOIN {schema}.dim_ibge_municipio d
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
            WHERE {_scope_where()}
        )
        SELECT
            {", ".join(agg_parts)}
        FROM base
        """
    )
    uf_codigo = "33" if uf_sigla.upper() == "RJ" else uf_sigla.upper()
    with get_engine().connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "municipio_nome": municipio_nome,
                "uf_sigla": uf_sigla.upper(),
                "uf_codigo": uf_codigo,
            },
        )
    if df.empty:
        return {}

    row = df.iloc[0].to_dict()
    out: dict[str, dict[str, int]] = {}
    for f in existing_fields:
        out[f] = {
            "total_scope": int(row.get("total_scope", 0) or 0),
            "preenchido": int(row.get(f"{f}__preenchido", 0) or 0),
            "vazio_ni": int(row.get(f"{f}__vazio_ni", 0) or 0),
            "valido": int(row.get(f"{f}__valido", 0) or 0),
            "invalido": int(row.get(f"{f}__invalido", 0) or 0),
        }
    return out


def _status(total_scope: int, col_exists: bool, valido: int, preenchido: int) -> str:
    if total_scope == 0:
        return "SEM_REGISTROS_NO_RECORTE"
    if not col_exists:
        return "COLUNA_AUSENTE"
    if valido == 0 and preenchido == 0:
        return "SEM_DADO_INFORMADO"
    if valido == 0 and preenchido > 0:
        return "PREENCHIDO_INVALIDO"
    return "OK"


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    schema = args.schema or settings.db_schema
    municipio = args.municipio
    uf = args.uf.upper()

    columns = _load_columns(schema)
    total_scope = _load_total_scope(schema=schema, municipio_nome=municipio, uf_sigla=uf)
    groups = _field_groups()
    flat_fields = {field: pred for group in groups.values() for field, pred in group.items()}
    aggregated_stats = _load_stats_all(
        schema=schema,
        fields_with_pred=flat_fields,
        municipio_nome=municipio,
        uf_sigla=uf,
        columns=columns,
    )

    rows: list[dict[str, object]] = []
    for grupo, fields in groups.items():
        for field, valid_pred in fields.items():
            exists = field.lower() in columns
            if exists and field in aggregated_stats:
                stats = aggregated_stats[field]
            else:
                stats = {"total_scope": total_scope, "preenchido": 0, "vazio_ni": total_scope, "valido": 0, "invalido": 0}

            status = _status(total_scope=total_scope, col_exists=exists, valido=stats["valido"], preenchido=stats["preenchido"])
            pct_valido = (stats["valido"] / total_scope * 100.0) if total_scope > 0 else 0.0
            rows.append(
                {
                    "grupo": grupo,
                    "campo": field,
                    "coluna_existe": "SIM" if exists else "NAO",
                    "total_recorte": total_scope,
                    "valido": stats["valido"],
                    "preenchido": stats["preenchido"],
                    "vazio_ni": stats["vazio_ni"],
                    "invalido": stats["invalido"],
                    "pct_valido": round(pct_valido, 2),
                    "status": status,
                }
            )

    result = pd.DataFrame(rows).sort_values(["grupo", "campo"]).reset_index(drop=True)
    pendentes = result[result["status"] != "OK"].copy()

    print("=" * 88)
    print("CHECK UNICO DE CAMPOS CRITICOS")
    print(f"Recorte: municipio={municipio} | uf={uf} | schema={schema}")
    print(f"Total de registros no recorte: {total_scope}")
    print("=" * 88)
    print(result.to_string(index=False))
    print("-" * 88)
    if pendentes.empty:
        print("STATUS GERAL: OK - Nenhum campo critico pendente.")
    else:
        print(f"STATUS GERAL: PENDENTE - {len(pendentes)} campo(s) precisam de carga/ajuste.")
        print(pendentes[["grupo", "campo", "status"]].to_string(index=False))

    if args.output_csv:
        output = Path(args.output_csv)
        output.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output, index=False, encoding="utf-8-sig")
        print("-" * 88)
        print(f"Arquivo CSV gerado em: {output}")


if __name__ == "__main__":
    main()
