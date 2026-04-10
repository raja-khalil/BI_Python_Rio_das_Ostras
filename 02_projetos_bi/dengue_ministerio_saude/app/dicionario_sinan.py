"""Dicionario de dados (recorte utilizado no BI)."""

from __future__ import annotations

import pandas as pd


CLASSIFICACAO_FINAL_MAP = {
    "0": "Ignorado/Branco",
    "1": "Dengue",
    "2": "Descartado",
    "3": "Dengue com complicacoes",
    "4": "Febre Hemorragica da Dengue",
    "5": "Dengue classico",
    "6": "Sindrome do Choque da Dengue",
    "8": "Inconclusivo",
    "10": "Dengue",
    "11": "Dengue com sinais de alarme",
    "12": "Dengue grave",
}

EVOLUCAO_CASO_MAP = {
    "0": "Ignorado/Branco",
    "1": "Cura",
    "2": "Obito pelo agravo",
    "3": "Obito por outras causas",
    "4": "Obito em investigacao",
    "9": "Ignorado",
}


def decode_classificacao(value: object) -> str:
    if value is None:
        return "Nao informado"
    code = str(value).strip()
    if code in {"", "NI"}:
        return "Nao informado"
    return CLASSIFICACAO_FINAL_MAP.get(code, f"Codigo {code}")


def decode_evolucao(value: object) -> str:
    if value is None:
        return "Nao informado"
    code = str(value).strip()
    if code in {"", "NI"}:
        return "Nao informado"
    return EVOLUCAO_CASO_MAP.get(code, f"Codigo {code}")


def get_dicionario_df() -> pd.DataFrame:
    """Retorna dicionario dos principais campos exibidos no BI."""
    rows = [
        {
            "campo_tecnico": "data_notificacao",
            "nome_exibicao": "Data da Notificacao",
            "descricao": "Data de preenchimento da ficha de notificacao.",
        },
        {
            "campo_tecnico": "semana_epidemiologica",
            "nome_exibicao": "Semana Epidemiologica da Notificacao",
            "descricao": "Semana epidemiologica do caso (AAAASS).",
        },
        {
            "campo_tecnico": "uf / sg_uf_not",
            "nome_exibicao": "UF de Notificacao",
            "descricao": "Unidade federativa da notificacao (IBGE).",
        },
        {
            "campo_tecnico": "municipio / id_municip",
            "nome_exibicao": "Municipio de Notificacao",
            "descricao": "Municipio de notificacao (codigo IBGE).",
        },
        {
            "campo_tecnico": "classificacao_final / classi_fin",
            "nome_exibicao": "Classificacao Final",
            "descricao": "Classificacao final do caso apos investigacao.",
        },
        {
            "campo_tecnico": "evolucao_caso / evolucao",
            "nome_exibicao": "Evoluuo do Caso",
            "descricao": "Desfecho do caso (cura, obito etc.).",
        },
    ]
    return pd.DataFrame(rows)
