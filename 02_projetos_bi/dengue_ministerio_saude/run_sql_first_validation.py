"""Valida carga incremental SQL-first (anos recentes) e gera relatorio."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.banco.database import get_engine


LINE_RE = re.compile(r"Arquivo concluido:\s*(\d+)\s*linhas carregadas \(DENGBR(\d{2})\.json\)")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Valida carga SQL-first por ano com base no log incremental")
    parser.add_argument("--year-start", type=int, required=True)
    parser.add_argument("--year-end", type=int, required=True)
    parser.add_argument("--log-file", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument(
        "--tolerance-abs",
        type=int,
        default=0,
        help="Diferenca absoluta maxima aceitavel entre log e banco para status de tolerancia.",
    )
    parser.add_argument(
        "--tolerance-pct",
        type=float,
        default=0.0,
        help="Diferenca percentual maxima aceitavel (sobre loaded_rows_log) para status de tolerancia.",
    )
    return parser.parse_args()


def _read_loaded_rows_from_log(log_file: Path) -> dict[int, int]:
    if not log_file.exists():
        return {}
    raw = log_file.read_bytes()
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        txt = raw.decode("utf-16", errors="ignore")
    elif raw.startswith(b"\xef\xbb\xbf"):
        txt = raw.decode("utf-8-sig", errors="ignore")
    else:
        try:
            txt = raw.decode("utf-8")
        except UnicodeDecodeError:
            txt = raw.decode("latin-1", errors="ignore")
    out: dict[int, int] = {}
    for m in LINE_RE.finditer(txt):
        rows = int(m.group(1))
        year = 2000 + int(m.group(2))
        out[year] = rows
    return out


def _read_db_rows_data_notificacao(year_start: int, year_end: int) -> dict[int, int]:
    sql = text(
        """
        SELECT
            EXTRACT(YEAR FROM data_notificacao)::INT AS ano,
            COUNT(*)::BIGINT AS db_rows_data_notificacao
        FROM saude.fato_dengue_casos
        WHERE data_notificacao IS NOT NULL
          AND EXTRACT(YEAR FROM data_notificacao)::INT BETWEEN :year_start AND :year_end
        GROUP BY 1
        ORDER BY 1
        """
    )
    df = pd.read_sql(sql, get_engine(), params={"year_start": year_start, "year_end": year_end})
    if df.empty:
        return {}
    return {int(r["ano"]): int(r["db_rows_data_notificacao"]) for _, r in df.iterrows()}


def _read_db_rows_nu_ano(year_start: int, year_end: int) -> dict[int, int]:
    sql = text(
        """
        SELECT
            nu_ano::INT AS ano,
            COUNT(*)::BIGINT AS db_rows_nu_ano
        FROM saude.fato_dengue_casos
        WHERE NULLIF(TRIM(nu_ano), '') IS NOT NULL
          AND nu_ano ~ '^[0-9]{4}$'
          AND nu_ano::INT BETWEEN :year_start AND :year_end
        GROUP BY 1
        ORDER BY 1
        """
    )
    df = pd.read_sql(sql, get_engine(), params={"year_start": year_start, "year_end": year_end})
    if df.empty:
        return {}
    return {int(r["ano"]): int(r["db_rows_nu_ano"]) for _, r in df.iterrows()}


def main() -> None:
    args = _parse_args()
    if args.year_start > args.year_end:
        raise ValueError("--year-start nao pode ser maior que --year-end")

    loaded_by_year = _read_loaded_rows_from_log(args.log_file)
    db_by_data_notif = _read_db_rows_data_notificacao(args.year_start, args.year_end)
    db_by_nu_ano = _read_db_rows_nu_ano(args.year_start, args.year_end)

    rows = []
    all_ok = True
    for year in range(args.year_start, args.year_end + 1):
        loaded_rows = loaded_by_year.get(year)
        db_rows_data_notif = db_by_data_notif.get(year, 0)
        db_rows_nu_ano = db_by_nu_ano.get(year, 0)

        if loaded_rows is None:
            status = "SEM_LOG"
            motivo = "Ano nao encontrado no log de carga."
            all_ok = False
        else:
            diff_dn = abs(db_rows_data_notif - loaded_rows)
            diff_nu = abs(db_rows_nu_ano - loaded_rows)
            pct_dn = (diff_dn / loaded_rows * 100) if loaded_rows > 0 else 0.0
            pct_nu = (diff_nu / loaded_rows * 100) if loaded_rows > 0 else 0.0

            within_abs = args.tolerance_abs > 0 and diff_dn <= args.tolerance_abs and diff_nu <= args.tolerance_abs
            within_pct = args.tolerance_pct > 0 and pct_dn <= args.tolerance_pct and pct_nu <= args.tolerance_pct

            if db_rows_data_notif == loaded_rows and db_rows_nu_ano == loaded_rows:
                status = "OK_FULL"
                motivo = "Bate por data_notificacao e nu_ano."
            elif within_abs or within_pct:
                status = "OK_COM_TOLERANCIA"
                motivo = (
                    f"Divergencia pequena dentro da tolerancia "
                    f"(abs<={args.tolerance_abs} ou pct<={args.tolerance_pct}%)."
                )
            elif db_rows_data_notif == loaded_rows:
                status = "OK_DATA_NOTIFICACAO"
                motivo = "Bate por data_notificacao; nu_ano divergiu."
            elif db_rows_nu_ano == loaded_rows:
                status = "OK_NU_ANO"
                motivo = "Bate por nu_ano; data_notificacao divergiu."
            else:
                status = "DIVERGENTE"
                motivo = "Nao bate por nenhum criterio."
                all_ok = False

        rows.append(
            {
                "ano": year,
                "loaded_rows_log": loaded_rows if loaded_rows is not None else "",
                "db_rows_data_notificacao": db_rows_data_notif,
                "diff_data_notificacao_vs_log": (db_rows_data_notif - loaded_rows) if loaded_rows is not None else "",
                "db_rows_nu_ano": db_rows_nu_ano,
                "diff_nu_ano_vs_log": (db_rows_nu_ano - loaded_rows) if loaded_rows is not None else "",
                "status": status,
                "motivo": motivo,
            }
        )

    report = pd.DataFrame(rows)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.output_csv, index=False, encoding="utf-8-sig")

    print("=== VALIDACAO SQL-FIRST ===")
    print(report.to_string(index=False))
    print(f"RELATORIO: {args.output_csv}")

    if not all_ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
