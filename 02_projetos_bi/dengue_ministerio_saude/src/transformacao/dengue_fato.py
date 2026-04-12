"""Conformidade de esquema para carga da fato de dengue."""

from __future__ import annotations

import pandas as pd


def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _to_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64")


def preparar_fato_dengue(df: pd.DataFrame) -> pd.DataFrame:
    """Monta DataFrame final aderente ao schema saude.fato_dengue_casos."""
    output = pd.DataFrame(index=df.index)

    municipio = df.get("municipio")
    id_municip = df.get("id_municip")
    if municipio is None and id_municip is None:
        output["municipio"] = "NAO INFORMADO"
    elif municipio is None:
        output["municipio"] = id_municip.astype("string")
    else:
        output["municipio"] = municipio.astype("string")
        if id_municip is not None:
            mask_empty = output["municipio"].str.strip().fillna("") == ""
            output.loc[mask_empty, "municipio"] = id_municip.astype("string")

    output["municipio"] = output["municipio"].fillna("NAO INFORMADO")
    output["uf"] = df.get("sg_uf_not", df.get("sg_uf", "")).astype("string").str[:2]
    output["data_notificacao"] = _to_date(df.get("dt_notific", pd.Series(index=df.index)))
    output["dt_sin_pri"] = _to_date(df.get("dt_sin_pri", pd.Series(index=df.index)))
    output["semana_epidemiologica"] = _to_int(df.get("sem_not", pd.Series(index=df.index)))
    output["classificacao_final"] = df.get("classi_fin", pd.Series(index=df.index)).astype("string")
    output["evolucao_caso"] = df.get("evolucao", pd.Series(index=df.index)).astype("string")
    output["dt_encerra"] = _to_date(df.get("dt_encerra", pd.Series(index=df.index)))
    output["cs_sexo"] = df.get("cs_sexo", pd.Series(index=df.index)).astype("string").str[:1]
    output["nu_idade_n"] = df.get("nu_idade_n", pd.Series(index=df.index)).astype("string").str[:8]
    output["cs_gestant"] = df.get("cs_gestant", pd.Series(index=df.index)).astype("string").str[:2]
    output["cs_raca"] = df.get("cs_raca", pd.Series(index=df.index)).astype("string").str[:2]
    output["cs_escol_n"] = df.get("cs_escol_n", pd.Series(index=df.index)).astype("string").str[:4]
    output["id_unidade"] = df.get("id_unidade", pd.Series(index=df.index)).astype("string").str[:20]
    output["hospitaliz"] = df.get("hospitaliz", pd.Series(index=df.index)).astype("string").str[:2]
    output["febre"] = df.get("febre", pd.Series(index=df.index)).astype("string").str[:1]
    output["mialgia"] = df.get("mialgia", pd.Series(index=df.index)).astype("string").str[:1]
    output["cefaleia"] = df.get("cefaleia", pd.Series(index=df.index)).astype("string").str[:1]
    output["exantema"] = df.get("exantema", pd.Series(index=df.index)).astype("string").str[:1]
    output["vomito"] = df.get("vomito", pd.Series(index=df.index)).astype("string").str[:1]
    output["nausea"] = df.get("nausea", pd.Series(index=df.index)).astype("string").str[:1]
    output["dor_costas"] = df.get("dor_costas", pd.Series(index=df.index)).astype("string").str[:1]
    output["conjuntvit"] = df.get("conjuntvit", pd.Series(index=df.index)).astype("string").str[:1]
    output["artrite"] = df.get("artrite", pd.Series(index=df.index)).astype("string").str[:1]
    output["artralgia"] = df.get("artralgia", pd.Series(index=df.index)).astype("string").str[:1]
    output["petequia_n"] = df.get("petequia_n", pd.Series(index=df.index)).astype("string").str[:1]
    output["leucopenia"] = df.get("leucopenia", pd.Series(index=df.index)).astype("string").str[:1]
    output["laco"] = df.get("laco", pd.Series(index=df.index)).astype("string").str[:1]
    output["dor_retro"] = df.get("dor_retro", pd.Series(index=df.index)).astype("string").str[:1]
    output["resul_soro"] = df.get("resul_soro", pd.Series(index=df.index)).astype("string").str[:2]
    output["dt_chik_s1"] = _to_date(df.get("dt_chik_s1", pd.Series(index=df.index)))
    output["dt_chik_s2"] = _to_date(df.get("dt_chik_s2", pd.Series(index=df.index)))
    output["dt_prnt"] = _to_date(df.get("dt_prnt", pd.Series(index=df.index)))
    output["res_chiks1"] = df.get("res_chiks1", pd.Series(index=df.index)).astype("string").str[:2]
    output["res_chiks2"] = df.get("res_chiks2", pd.Series(index=df.index)).astype("string").str[:2]
    output["resul_prnt"] = df.get("resul_prnt", pd.Series(index=df.index)).astype("string").str[:2]
    output["dt_soro"] = _to_date(df.get("dt_soro", pd.Series(index=df.index)))
    output["resul_ns1"] = df.get("resul_ns1", pd.Series(index=df.index)).astype("string").str[:2]
    output["dt_ns1"] = _to_date(df.get("dt_ns1", pd.Series(index=df.index)))
    output["dt_viral"] = _to_date(df.get("dt_viral", pd.Series(index=df.index)))
    output["resul_vi_n"] = df.get("resul_vi_n", pd.Series(index=df.index)).astype("string").str[:2]
    output["dt_pcr"] = _to_date(df.get("dt_pcr", pd.Series(index=df.index)))
    # O nome original no JSON e "RESUL_PCR_", mas a normalizacao de colunas remove
    # underscore no final, resultando em "resul_pcr". Aceitar ambos evita perda de carga.
    output["resul_pcr_"] = (
        df.get("resul_pcr", df.get("resul_pcr_", pd.Series(index=df.index)))
        .astype("string")
        .str[:2]
    )
    output["sorotipo"] = df.get("sorotipo", pd.Series(index=df.index)).astype("string").str[:2]
    output["histopa_n"] = df.get("histopa_n", pd.Series(index=df.index)).astype("string").str[:2]
    output["imunoh_n"] = df.get("imunoh_n", pd.Series(index=df.index)).astype("string").str[:2]
    output["dt_interna"] = _to_date(df.get("dt_interna", pd.Series(index=df.index)))
    output["diabetes"] = df.get("diabetes", pd.Series(index=df.index)).astype("string").str[:1]
    output["hematolog"] = df.get("hematolog", pd.Series(index=df.index)).astype("string").str[:1]
    output["hepatopat"] = df.get("hepatopat", pd.Series(index=df.index)).astype("string").str[:1]
    output["renal"] = df.get("renal", pd.Series(index=df.index)).astype("string").str[:1]
    output["hipertensa"] = df.get("hipertensa", pd.Series(index=df.index)).astype("string").str[:1]
    output["acido_pept"] = df.get("acido_pept", pd.Series(index=df.index)).astype("string").str[:1]
    output["auto_imune"] = df.get("auto_imune", pd.Series(index=df.index)).astype("string").str[:1]

    # Estrutura completa do JSON (campos tecnicos adicionais para rastreabilidade SQL-first)
    raw_date_cols = ["dt_alrm", "dt_digita", "dt_grav", "dt_invest", "dt_notific", "dt_obito"]
    raw_text_cols = [
        "tp_not",
        "id_agravo",
        "sem_not",
        "nu_ano",
        "sg_uf_not",
        "id_municip",
        "id_regiona",
        "ano_nasc",
        "sem_pri",
        "sg_uf",
        "id_mn_resi",
        "id_rg_resi",
        "id_pais",
        "id_ocupa_n",
        "tpautocto",
        "coufinf",
        "copaisinf",
        "comuninf",
        "classi_fin",
        "criterio",
        "doenca_tra",
        "clinc_chik",
        "evolucao",
        "alrm_hipot",
        "alrm_plaq",
        "alrm_vom",
        "alrm_sang",
        "alrm_hemat",
        "alrm_abdom",
        "alrm_letar",
        "alrm_hepat",
        "alrm_liq",
        "grav_pulso",
        "grav_conv",
        "grav_ench",
        "grav_insuf",
        "grav_taqui",
        "grav_extre",
        "grav_hipot",
        "grav_hemat",
        "grav_melen",
        "grav_metro",
        "grav_sang",
        "grav_ast",
        "grav_mioc",
        "grav_consc",
        "grav_orgao",
        "mani_hemor",
        "epistaxe",
        "gengivo",
        "metro",
        "petequias",
        "hematura",
        "sangram",
        "laco_n",
        "plasmatico",
        "evidencia",
        "plaq_menor",
        "con_fhd",
        "complica",
        "tp_sistema",
        "nduplic_n",
        "cs_flxret",
        "flxrecebi",
        "migrado_w",
        "resul_pcr",
    ]
    for col in raw_date_cols:
        output[col] = _to_date(df.get(col, pd.Series(index=df.index)))
    for col in raw_text_cols:
        output[col] = df.get(col, pd.Series(index=df.index)).astype("string").str[:40]

    return output
