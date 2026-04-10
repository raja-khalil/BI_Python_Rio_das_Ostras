"""Auditoria de mapeamento: JSON -> transformador -> fato_dengue_casos.

Gera um diagnostico objetivo do que:
- existe no JSON normalizado;
- e usado pelo transformador (preparar_fato_dengue);
- sai do transformador;
- existe na tabela fato no banco.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.banco.database import get_engine
from src.config.settings import get_settings
from src.ingestao.reader_json_stream import iter_json_chunks
from src.transformacao.cleaning import pipeline_limpeza_padrao
from src.transformacao.dengue_fato import preparar_fato_dengue


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoria JSON x fato_dengue_casos")
    parser.add_argument("--schema", default="saude")
    parser.add_argument("--table", default="fato_dengue_casos")
    parser.add_argument("--year", type=int, default=None, help="Ano para selecionar arquivo JSON (opcional)")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument(
        "--output",
        default="data/processed/auditoria_json_fato.csv",
        help="CSV de saida com status por campo",
    )
    return parser.parse_args()


def _find_json_file(raw_dir: Path, year: int | None) -> Path:
    base = raw_dir / "json" / "portal_sus" / "extracted"
    files: list[Path] = []
    if year is not None:
        files = sorted((base / str(year)).glob("*.json"))
    else:
        for year_dir in sorted(base.glob("*")):
            if year_dir.is_dir():
                files.extend(sorted(year_dir.glob("*.json")))
    if not files:
        raise FileNotFoundError(f"Nenhum JSON encontrado em {base}")
    return files[-1]


def _extract_mapper_input_cols(transformer_file: Path) -> set[str]:
    content = transformer_file.read_text(encoding="utf-8", errors="ignore")
    # Captura colunas usadas como df.get("coluna", ...)
    matches = re.findall(r'df\.get\("([^"]+)"', content)
    return {m.strip() for m in matches if m.strip()}


def _load_db_columns(schema: str, table: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"schema": schema, "table": table}).fetchall()
    return [r[0] for r in rows]


def _load_not_null_counts(schema: str, table: str, cols: list[str]) -> dict[str, int]:
    if not cols:
        return {}
    engine = get_engine()
    exprs = ", ".join([f'COUNT("{c}") AS "{c}"' for c in cols])
    query = text(f'SELECT {exprs} FROM "{schema}"."{table}"')
    with engine.begin() as conn:
        row = conn.execute(query).mappings().first()
    return {c: int(row[c]) for c in cols} if row else {c: 0 for c in cols}


def main() -> None:
    args = _parse_args()
    settings = get_settings()

    json_path = _find_json_file(settings.raw_dir, args.year)
    raw_chunk = next(iter_json_chunks(path=json_path, chunk_size=args.chunk_size))
    cleaned = pipeline_limpeza_padrao(raw_chunk)
    transformed = preparar_fato_dengue(cleaned)

    json_cols = sorted(set(cleaned.columns))
    transformer_file = Path("src/transformacao/dengue_fato.py")
    mapper_input_cols = sorted(_extract_mapper_input_cols(transformer_file))
    output_cols = sorted(set(transformed.columns))
    db_cols = _load_db_columns(args.schema, args.table)
    db_cols_set = set(db_cols)
    db_technical_cols = {"id", "data_carga"}
    mapper_aliases = {
        # normalizacao remove underscore final
        "resul_pcr_": {"resul_pcr"},
    }

    relevant_not_null = _load_not_null_counts(args.schema, args.table, output_cols)

    universe = sorted(set(json_cols) | set(mapper_input_cols) | set(output_cols) | db_cols_set)
    rows: list[dict[str, object]] = []
    for col in universe:
        in_json = col in json_cols
        in_mapper_input = col in mapper_input_cols
        in_transformer_output = col in output_cols
        in_db = col in db_cols_set
        alias_in_json = any(a in json_cols for a in mapper_aliases.get(col, set()))

        if in_json and not in_mapper_input:
            status = "JSON_NAO_MAPEADO_NO_TRANSFORMADOR"
        elif in_mapper_input and not in_json and not alias_in_json:
            status = "MAPPER_REFERENCIA_CAMPO_AUSENTE_NO_JSON"
        elif in_transformer_output and not in_db:
            status = "SAIDA_TRANSFORMADOR_SEM_COLUNA_NA_FATO"
        elif in_db and not in_transformer_output and col not in db_technical_cols:
            status = "COLUNA_FATO_SEM_SAIDA_DO_TRANSFORMADOR"
        else:
            status = "OK"

        rows.append(
            {
                "campo": col,
                "json_normalizado": "SIM" if in_json else "NAO",
                "usado_no_transformador_entrada": "SIM" if in_mapper_input else "NAO",
                "saida_transformador": "SIM" if in_transformer_output else "NAO",
                "coluna_na_fato": "SIM" if in_db else "NAO",
                "not_null_na_fato": relevant_not_null.get(col, None),
                "status": status,
            }
        )

    out_df = pd.DataFrame(rows)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("=" * 96)
    print("AUDITORIA JSON x FATO_DENGUE_CASOS")
    print(f"Arquivo JSON analisado: {json_path}")
    print(f"Schema/Tabela: {args.schema}.{args.table}")
    print(f"CSV gerado: {output_path}")
    print("=" * 96)
    print(out_df["status"].value_counts(dropna=False).to_string())
    print("-" * 96)
    pendentes = out_df[out_df["status"] != "OK"]
    if pendentes.empty:
        print("STATUS GERAL: OK - sem lacunas de mapeamento estrutural.")
    else:
        print(f"STATUS GERAL: PENDENTE - {len(pendentes)} campo(s) com divergencia.")
        print(pendentes[["campo", "status"]].to_string(index=False))


if __name__ == "__main__":
    main()
