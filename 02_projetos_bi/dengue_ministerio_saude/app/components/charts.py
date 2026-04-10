"""Graficos do dashboard com foco narrativo."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.components.cards import make_card, render_kpi_cards
from app.theme import (
    BRAND_HIGHLIGHT,
    BRAND_BLUE,
    BRAND_YELLOW,
    GRID_SOFT,
    SURFACE_BG,
)

COLOR_RIO = BRAND_BLUE
COLOR_RJ = "#5F7489"
COLOR_BR = "#B7C8D8"
COLOR_ACCENT = BRAND_HIGHLIGHT
COLOR_NEUTRAL = "#1B496F"


def _as_int(séries: pd.Series) -> pd.Series:
    """Arredonda para inteiro sem casas decimais."""
    return pd.to_numeric(séries, errors="coerce").fillna(0).round(0).astype(int)


def _axis_max(séries: pd.Series, pad: float = 0.1, min_value: float = 1.0) -> float:
    """Retorna maximo de eixo com padding, evitando escalas distorcidas."""
    values = pd.to_numeric(séries, errors="coerce").dropna()
    if values.empty:
        return min_value
    vmax = float(values.max())
    if vmax <= 0:
        return min_value
    return vmax * (1.0 + max(0.0, pad))


def _adaptive_yaxis(
    séries: pd.Series,
    *,
    allow_log: bool = False,
    pad: float = 0.08,
    min_value: float = 1.0,
) -> dict[str, object]:
    """Define parametros de eixo Y com escala adaptativa ao recorte atual.

    Regras:
    - Padrao linear com range enxuto [0, max*(1+pad)].
    - Se permitido e houver grande discrepancia entre minimo positivo e maximo,
      aplica escala logaritmica para melhorar leitura das séries menores.
    """
    values = pd.to_numeric(séries, errors="coerce").dropna()
    if values.empty:
        return {"type": "linear", "range": [0, min_value]}

    vmax = float(values.max())
    if vmax <= 0:
        return {"type": "linear", "range": [0, min_value]}

    if allow_log:
        positive = values[values > 0]
        if not positive.empty:
            vmin_pos = float(positive.min())
            ratio = vmax / max(vmin_pos, 1.0)
            if ratio >= 25:
                return {"type": "log", "range": None}

    return {"type": "linear", "range": [0, _axis_max(values, pad=pad, min_value=min_value)]}


def _to_month_séries(df: pd.DataFrame, label: str, value_col: str) -> pd.DataFrame:
    serie = (
        df.groupby("mes_referencia", as_index=False)[value_col]
        .sum()
        .rename(columns={value_col: "valor"})
        .sort_values("mes_referencia")
    )
    serie["valor"] = _as_int(serie["valor"])
    serie["escopo"] = label
    return serie


def _ensure_last_n_months(serie: pd.DataFrame, n_months: int) -> pd.DataFrame:
    """Garante janela exata dos uUltimos N meses para cada serie/escopo."""
    if serie.empty or n_months <= 0:
        return serie
    ref = pd.to_datetime(serie["mes_referencia"], errors="coerce").max()
    if pd.isna(ref):
        return serie
    end = pd.Timestamp(year=ref.year, month=ref.month, day=1)
    start = end - pd.DateOffset(months=n_months - 1)
    months = pd.date_range(start=start, end=end, freq="MS")

    parts: list[pd.DataFrame] = []
    for escopo, grp in serie.groupby("escopo", as_index=False):
        base = grp.copy()
        base["mes_referencia"] = pd.to_datetime(base["mes_referencia"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        agg = base.groupby("mes_referencia", as_index=False)["valor"].sum()
        full = pd.DataFrame({"mes_referencia": months})
        full = full.merge(agg, on="mes_referencia", how="left")
        full["valor"] = _as_int(full["valor"])
        full["escopo"] = escopo
        parts.append(full)
    return pd.concat(parts, ignore_index=True)


def _período_label(df: pd.DataFrame) -> str:
    """Gera descricao de período com base no recorte filtrado."""
    if df.empty:
        return "Período: Sem dado informado"

    if "mes_referencia" in df.columns:
        dt = pd.to_datetime(df["mes_referencia"], errors="coerce").dropna()
        if not dt.empty:
            dt_min = dt.min()
            dt_max = dt.max()
            if dt_min.to_period("M") == dt_max.to_period("M"):
                return f"Período: {dt_max:%b/%Y}"
            return f"Período: {dt_min:%b/%Y} a {dt_max:%b/%Y}"

    if "ano" in df.columns:
        anos = pd.to_numeric(df["ano"], errors="coerce").dropna().astype(int)
        if not anos.empty:
            ano_min = int(anos.min())
            ano_max = int(anos.max())
            if ano_min == ano_max:
                return f"Período: {ano_max}"
            return f"Período: {ano_min} a {ano_max}"

    return "Período: recorte atual"


def _período_label_multi(dfs: list[pd.DataFrame]) -> str:
    valid = [d for d in dfs if d is not None and not d.empty]
    if not valid:
        return "Período: Sem dado informado"
    merged = pd.concat(valid, ignore_index=True)
    return _período_label(merged)


def render_time_séries(
    df_mes_brasil: pd.DataFrame,
    df_mes_rj: pd.DataFrame,
    df_mes_municipio: pd.DataFrame,
    comparacao: str,
    período_rapido: str,
    metrica_coluna: str,
    municipio_nome: str,
) -> None:
    """Grafico principal do painel situacional."""
    frames: list[pd.DataFrame] = []
    if comparacao in {"Rio das Ostras x RJ x Brasil", "Rio das Ostras x Brasil"} and not df_mes_brasil.empty:
        frames.append(_to_month_séries(df_mes_brasil, "Brasil", metrica_coluna))
    if comparacao in {"Rio das Ostras x RJ x Brasil", "Rio das Ostras x RJ"} and not df_mes_rj.empty:
        frames.append(_to_month_séries(df_mes_rj, "Rio de Janeiro", metrica_coluna))
    if not df_mes_municipio.empty:
        frames.append(_to_month_séries(df_mes_municipio, municipio_nome, metrica_coluna))

    if not frames:
        st.info("Sem dado informado")
        return

    serie = pd.concat(frames, ignore_index=True)
    months_map = {
        "Últimos 3 meses": 3,
        "Últimos 6 meses": 6,
        "Últimos 12 meses": 12,
    }
    n_months = months_map.get(período_rapido)
    if n_months:
        serie = _ensure_last_n_months(serie, n_months=n_months)
    período = _período_label(serie)
    if comparacao == "Rio das Ostras":
        titulo = f"Evolução mensal de casos em {municipio_nome}"
    elif comparacao == "Rio das Ostras x RJ":
        titulo = f"Evolução mensal de casos: {municipio_nome} em comparação com Rio de Janeiro"
    elif comparacao == "Rio das Ostras x Brasil":
        titulo = f"Evolução mensal de casos: {municipio_nome} em comparação com Brasil"
    else:
        titulo = f"Evolução mensal de casos: {municipio_nome} em comparação com Rio de Janeiro e Brasil"
    fig = px.line(
        serie,
        x="mes_referencia",
        y="valor",
        color="escopo",
        line_group="escopo",
        color_discrete_map={
            municipio_nome: COLOR_RIO,
            "Rio de Janeiro": COLOR_RJ,
            "Brasil": COLOR_BR,
        },
        title=f"{titulo}<br><sup>{período}</sup>",
    )
    fig.update_traces(
        mode="lines",
        line={"width": 2.8},
        hovertemplate="<b>%{fullData.name}</b><br>Período: %{x|%b/%Y}<br>Valor: %{y:,.0f}<extra></extra>",
    )
    fig.update_layout(
        plot_bgcolor=SURFACE_BG,
        paper_bgcolor=SURFACE_BG,
        margin={"l": 20, "r": 20, "t": 64, "b": 20},
        legend_title_text="",
    )
    y_cfg = _adaptive_yaxis(serie["valor"], allow_log=False, pad=0.12)
    fig.update_xaxes(
        title="",
        gridcolor=GRID_SOFT,
        tickformat="%b/%Y",
        dtick="M1",
    )
    fig.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
    st.plotly_chart(fig, width="stretch")


def render_territorial_map(df_municipio_rj: pd.DataFrame, metrica_visual: str) -> None:
    """Mapa territorial substituto em treemap (ate haver geometria municipal)."""
    if df_municipio_rj.empty:
        st.info("Sem dado informado")
        return

    metric_col = "total_casos" if metrica_visual == "Casos totais" else "incidencia_100k"
    período = _período_label(df_municipio_rj)
    base = (
        df_municipio_rj.groupby("municipio_nome", as_index=False)
        .agg(
            total_casos=("total_casos", "sum"),
            incidencia_100k=("incidencia_100k", "mean"),
        )
        .dropna(subset=[metric_col])
    )
    base["total_casos"] = _as_int(base["total_casos"])
    base["incidencia_100k"] = _as_int(base["incidencia_100k"])
    if base.empty:
        st.info("Sem dado informado")
        return

    fig = px.treemap(
        base,
        path=["municipio_nome"],
        values="total_casos",
        color=metric_col,
        color_continuous_scale=[[0, "#FFF5E8"], [0.5, BRAND_YELLOW], [1, BRAND_BLUE]],
        title=f"Distribuição territorial por município (visão de concentração)<br><sup>{período}</sup>",
    )
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>Valor: %{color:,.0f}<br>Casos: %{value:,.0f}<extra></extra>"
    )
    fig.update_layout(
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
        paper_bgcolor=SURFACE_BG,
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Mapa geoespacial municipal (coropletico real) sera ativado assim que a geometria dos municipios "
        "for incorporada ao projeto."
    )


def render_rankings(
    df_municipio_rj: pd.DataFrame,
    municipio_foco: str,
    top_n: int,
) -> None:
    """Rankings de casos notificados e incidencia."""
    if df_municipio_rj.empty:
        st.info("Sem dado informado")
        return

    base = (
        df_municipio_rj.groupby("municipio_nome", as_index=False)
        .agg(
            casos_notificados=("total_casos", "sum"),
            incidencia_100k=("incidencia_100k", "mean"),
        )
        .dropna(subset=["casos_notificados"])
    )
    base["casos_notificados"] = _as_int(base["casos_notificados"])
    base["incidencia_100k"] = _as_int(base["incidencia_100k"])

    top_cases = base.sort_values("casos_notificados", ascending=False).head(top_n).copy()
    top_cases = top_cases.sort_values("casos_notificados", ascending=True)
    top_cases["is_focus"] = top_cases["municipio_nome"].eq(municipio_foco)

    fig_cases = go.Figure()
    fig_cases.add_bar(
        x=top_cases["casos_notificados"],
        y=top_cases["municipio_nome"],
        orientation="h",
        marker_color=np.where(top_cases["is_focus"], COLOR_ACCENT, COLOR_NEUTRAL),
        hovertemplate="<b>%{y}</b><br>Casos notificados: %{x:,.0f}<extra></extra>",
    )
    período = _período_label(df_municipio_rj)
    fig_cases.update_layout(
        title=f"Municípios com maior número de casos notificados no estado<br><sup>{período}</sup>",
        margin={"l": 12, "r": 12, "t": 52, "b": 12},
        plot_bgcolor=SURFACE_BG,
        paper_bgcolor=SURFACE_BG,
    )
    y_cfg_cases = _adaptive_yaxis(top_cases["casos_notificados"], pad=0.06)
    fig_cases.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_cases["range"])
    fig_cases.update_yaxes(title="")

    top_inc = base.dropna(subset=["incidencia_100k"]).sort_values("incidencia_100k", ascending=False).head(top_n).copy()
    top_inc = top_inc.sort_values("incidencia_100k", ascending=True)
    top_inc["is_focus"] = top_inc["municipio_nome"].eq(municipio_foco)
    media_estadual = float(top_inc["incidencia_100k"].mean()) if not top_inc.empty else 0.0
    top_inc["delta_media_estadual"] = top_inc["incidencia_100k"] - media_estadual

    fig_inc = go.Figure()
    fig_inc.add_bar(
        x=top_inc["incidencia_100k"],
        y=top_inc["municipio_nome"],
        orientation="h",
        marker_color=np.where(top_inc["is_focus"], COLOR_ACCENT, "#36566F"),
        customdata=top_inc[["delta_media_estadual"]],
        hovertemplate="<b>%{y}</b><br>Incidência: %{x:,.0f}<br>Vs media estadual: %{customdata[0]:+,.0f}<extra></extra>",
    )
    fig_inc.update_layout(
        title=f"Municípios com maior incidência por 100 mil habitantes<br><sup>{período}</sup>",
        margin={"l": 12, "r": 12, "t": 52, "b": 12},
        plot_bgcolor=SURFACE_BG,
        paper_bgcolor=SURFACE_BG,
    )
    y_cfg_inc = _adaptive_yaxis(top_inc["incidencia_100k"], pad=0.06)
    fig_inc.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_inc["range"])
    fig_inc.update_yaxes(title="")

    c1, c2 = st.columns(2)
    c1.plotly_chart(fig_cases, width="stretch")
    c2.plotly_chart(fig_inc, width="stretch")


def render_classification_donut(df_status_municipio: pd.DataFrame) -> None:
    """Donut de classificacao dos casos."""
    if df_status_municipio.empty:
        st.info("Sem dado informado")
        return

    base = df_status_municipio.copy()
    if "classificacao_grupo" not in base.columns:
        if "classificacao_final" in base.columns:
            map_classif = {
                "1": "Confirmados",
                "3": "Confirmados",
                "4": "Confirmados",
                "5": "Confirmados",
                "6": "Confirmados",
                "10": "Confirmados",
                "11": "Confirmados",
                "12": "Confirmados",
                "2": "Descartados",
                "8": "Inconclusivos",
            }
            base["classificacao_grupo"] = (
                pd.to_numeric(base["classificacao_final"], errors="coerce")
                .fillna(-1)
                .astype(int)
                .astype(str)
                .map(map_classif)
                .fillna("Em investigação")
            )
        else:
            st.info("Sem dado informado")
            return

    classif = (
        base.groupby("classificacao_grupo", as_index=False)["total_casos"]
        .sum()
        .sort_values("total_casos", ascending=False)
    )
    classif["total_casos"] = _as_int(classif["total_casos"])
    if classif.empty:
        st.info("Sem dado informado")
        return
    total_abs = int(classif["total_casos"].sum())

    período = _período_label(df_status_municipio)
    fig = px.pie(
        classif,
        names="classificacao_grupo",
        values="total_casos",
        hole=0.58,
        color="classificacao_grupo",
        color_discrete_map={
            "Confirmados": BRAND_BLUE,
            "Descartados": "#9EB1C3",
            "Inconclusivos": BRAND_YELLOW,
            "Em investigação": "#36566F",
        },
        title=f"Distribuição dos casos por classificação<br><sup>{período}</sup>",
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>Casos: %{value:,.0f}<br>Percentual: %{percent}<extra></extra>",
    )
    fig.update_layout(
        margin={"l": 12, "r": 12, "t": 50, "b": 12},
        paper_bgcolor=SURFACE_BG,
        legend_title_text="",
    )
    fig.add_annotation(
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        text=f"<b>{total_abs:,.0f}</b><br><span style='font-size:11px;'>casos</span>",
        showarrow=False,
        font={"size": 22, "color": BRAND_BLUE},
        align="center",
    )
    st.plotly_chart(fig, width="stretch")


def render_sexo_chart(df_sexo: pd.DataFrame) -> None:
    """Grafico de distribuicao por sexo."""
    if df_sexo.empty:
        st.info("Sem dado informado")
        return
    label_map = {"M": "Masculino", "F": "Feminino", "I": "Ignorado", "NI": "Não informado"}
    base = df_sexo.copy()
    base["sexo_label"] = base["sexo"].map(label_map).fillna("Não informado")
    base["total_casos"] = _as_int(base["total_casos"])
    período = _período_label(df_sexo)
    fig = px.bar(
        base.sort_values("total_casos", ascending=True),
        x="total_casos",
        y="sexo_label",
        orientation="h",
        title=f"Casos por sexo no município<br><sup>{período}</sup>",
        color_discrete_sequence=[BRAND_BLUE],
    )
    fig.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
    fig.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
    y_cfg = _adaptive_yaxis(base["total_casos"], pad=0.06)
    fig.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
    fig.update_yaxes(title="")
    st.plotly_chart(fig, width="stretch")


def render_unidade_notificadora_chart(df_unidade: pd.DataFrame) -> None:
    """Grafico de unidades notificadoras do municipio."""
    if df_unidade.empty:
        st.info("Sem dado informado")
        return
    período = _período_label(df_unidade)
    base = df_unidade.copy()
    base["total_casos"] = _as_int(base["total_casos"])
    if "unidade_nome" in base.columns:
        nome = base["unidade_nome"].astype(str).str.strip()
        base["unidade_label"] = np.where(
            nome.fillna("").eq("") | nome.eq(""),
            "Unidade não identificada",
            nome,
        )
    else:
        base["unidade_label"] = "Unidade não identificada"
    base = base.sort_values("total_casos", ascending=True)
    fig = px.bar(
        base,
        x="total_casos",
        y="unidade_label",
        orientation="h",
        title=f"Top unidades notificadoras no município<br><sup>{período}</sup>",
        color_discrete_sequence=["#36566F"],
    )
    fig.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
    fig.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
    y_cfg = _adaptive_yaxis(base["total_casos"], pad=0.06)
    fig.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
    fig.update_yaxes(title="")
    st.plotly_chart(fig, width="stretch")


def render_territorio_risco_unidades(
    df_unidade_mensal: pd.DataFrame,
    top_n: int,
    comparacao: str,
    municipio_foco: str,
    df_municipio_rj_filtrado: pd.DataFrame | None = None,
    df_municipio_br_filtrado: pd.DataFrame | None = None,
) -> None:
    """Painel operacional por unidades: pressao, crescimento e risco."""
    st.markdown("### Território e Risco")
    if df_unidade_mensal.empty:
        st.info("Sem dado informado")
        return

    base = df_unidade_mensal.copy()
    base["total_casos"] = _as_int(base["total_casos"])
    base["mes_ref"] = pd.to_datetime(base["mes_referencia"], errors="coerce").dt.to_period("M")
    nome = base["unidade_nome"].astype(str).str.strip() if "unidade_nome" in base.columns else pd.Series([""] * len(base))
    base["unidade_label"] = np.where(
        nome.fillna("").eq("") | nome.eq(""),
        "Unidade não identificada",
        nome,
    )

    if base.empty or base["mes_ref"].isna().all():
        st.info("Sem dado informado")
        return

    meses_validos = sorted(base["mes_ref"].dropna().unique())
    if not meses_validos:
        st.info("Sem dado informado")
        return
    mes_inicio = meses_validos[0]
    mes_fim = meses_validos[-1]

    período_df = (
        base.groupby("unidade_label", as_index=False)["total_casos"]
        .sum()
        .rename(columns={"total_casos": "casos_período"})
    )
    ini_df = (
        base[base["mes_ref"] == mes_inicio]
        .groupby("unidade_label", as_index=False)["total_casos"]
        .sum()
        .rename(columns={"total_casos": "casos_inicio"})
    )
    fim_df = (
        base[base["mes_ref"] == mes_fim]
        .groupby("unidade_label", as_index=False)["total_casos"]
        .sum()
        .rename(columns={"total_casos": "casos_fim"})
    )
    risco = (
        período_df.merge(ini_df, on="unidade_label", how="left")
        .merge(fim_df, on="unidade_label", how="left")
        .fillna({"casos_inicio": 0, "casos_fim": 0})
    )
    risco["casos_período"] = _as_int(risco["casos_período"])
    risco["casos_inicio"] = _as_int(risco["casos_inicio"])
    risco["casos_fim"] = _as_int(risco["casos_fim"])

    def _growth(row: pd.Series) -> float:
        inicio = float(row["casos_inicio"])
        fim = float(row["casos_fim"])
        if inicio <= 0:
            return 100.0 if fim > 0 else 0.0
        return ((fim - inicio) / inicio) * 100.0

    risco["crescimento_pct"] = risco.apply(_growth, axis=1)
    risco["crescimento_pct"] = risco["crescimento_pct"].replace([np.inf, -np.inf], 0).fillna(0)

    if risco.empty:
        st.info("Sem dado informado")
        return

    vol_rank = risco["casos_período"].rank(pct=True, method="average").fillna(0)
    cresc_rank = risco["crescimento_pct"].clip(lower=0).rank(pct=True, method="average").fillna(0)
    risco["score_risco"] = (0.6 * vol_rank + 0.4 * cresc_rank).round(3)

    def _faixa(score: float) -> str:
        if score >= 0.75:
            return "Critico"
        if score >= 0.55:
            return "Alto"
        if score >= 0.35:
            return "Moderado"
        return "Baixo"

    risco["classificacao_risco"] = risco["score_risco"].apply(_faixa)
    risco = risco.sort_values(["score_risco", "casos_período"], ascending=[False, False]).reset_index(drop=True)

    top_pressao = (
        risco.sort_values("casos_período", ascending=False).head(top_n).copy().sort_values("casos_período", ascending=True)
    )
    top_crescimento = (
        risco[risco["casos_período"] > 0]
        .sort_values("crescimento_pct", ascending=False)
        .head(top_n)
        .copy()
        .sort_values("crescimento_pct", ascending=True)
    )

    unidade_pressao = risco.sort_values("casos_período", ascending=False).iloc[0]
    unidade_crescimento = risco.sort_values("crescimento_pct", ascending=False).iloc[0]
    unidade_risco = risco.sort_values("score_risco", ascending=False).iloc[0]

    def _fmt_abs(v: int | float | None) -> str:
        if v is None:
            return "-"
        try:
            return f"{int(round(float(v))):,}".replace(",", ".")
        except (TypeError, ValueError):
            return "-"

    def _build_text_focus_card(
        label: str,
        destaque: str | None,
        quantidade: int | float | None,
        extra: str | None = None,
    ) -> dict[str, object]:
        txt = str(destaque).strip() if destaque is not None else ""
        if not txt or txt.lower() in {"nan", "none", "-"}:
            txt = "Sem dado informado"
        help_txt = f"Quantidade: {_fmt_abs(quantidade)}"
        if extra:
            help_txt = f"{help_txt} | {extra}"
        return {
            "label": label,
            "value": txt,
            "value_mode": "text",
            "delta_text": "-",
            "delta_pct": None,
            "help": help_txt,
        }

    cards_territorio = [
        _build_text_focus_card(
            "Unidade com maior pressao (período filtrado)",
            str(unidade_pressao["unidade_label"]),
            int(unidade_pressao["casos_período"]),
        ),
        _build_text_focus_card(
            "Maior crescimento no período",
            str(unidade_crescimento["unidade_label"]),
            int(round(unidade_crescimento["crescimento_pct"])),
            f"{mes_inicio.strftime('%m/%Y')} -> {mes_fim.strftime('%m/%Y')} | crescimento (%)",
        ),
        _build_text_focus_card(
            "Prioridade de acao (score de risco)",
            f"{unidade_risco['classificacao_risco']} | {unidade_risco['unidade_label']}",
            int(round(float(unidade_risco["score_risco"]) * 100)),
            "Score de risco",
        ),
    ]
    # Regra visual do projeto: cards do painel 2 seguem exatamente o mesmo modelo do painel 1.
    render_kpi_cards(cards_territorio)

    d1, d2 = st.columns(2)
    with d1:
        fig_casos = px.bar(
            top_pressao,
            x="casos_período",
            y="unidade_label",
            orientation="h",
            title="Unidades com maior volume de casos",
            color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig_casos.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
        y_cfg_casos = _adaptive_yaxis(top_pressao["casos_período"], pad=0.08)
        fig_casos.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_casos["range"])
        fig_casos.update_yaxes(title="")
        fig_casos.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
        st.plotly_chart(fig_casos, width="stretch")

    with d2:
        if top_crescimento.empty:
            st.info("Sem dado informado")
        else:
            fig_cres = px.bar(
                top_crescimento,
                x="crescimento_pct",
                y="unidade_label",
                orientation="h",
                title="Unidades com maior crescimento mensal",
                color_discrete_sequence=[COLOR_ACCENT],
            )
            fig_cres.update_traces(hovertemplate="<b>%{y}</b><br>Crescimento: %{x:,.0f}%<extra></extra>")
            y_cfg_cres = _adaptive_yaxis(top_crescimento["crescimento_pct"], pad=0.08, min_value=5)
            fig_cres.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_cres["range"])
            fig_cres.update_yaxes(title="")
            fig_cres.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
            st.plotly_chart(fig_cres, width="stretch")

    tabela = risco.copy()
    tabela["casos_período"] = _as_int(tabela["casos_período"])
    tabela["casos_inicio"] = _as_int(tabela["casos_inicio"])
    tabela["casos_fim"] = _as_int(tabela["casos_fim"])
    tabela["crescimento_pct"] = tabela["crescimento_pct"].round(0).astype(int)
    tabela["score_risco"] = (tabela["score_risco"] * 100).round(0).astype(int)
    tabela = tabela.rename(
        columns={
            "unidade_label": "Unidade",
            "casos_período": "Casos (período)",
            "casos_inicio": f"Casos inicio ({mes_inicio.strftime('%m/%Y')})",
            "casos_fim": f"Casos fim ({mes_fim.strftime('%m/%Y')})",
            "crescimento_pct": "Crescimento (%)",
            "classificacao_risco": "Classificação de risco",
            "score_risco": "Score de risco",
        }
    )
    st.markdown("#### Prioridade de acao por unidade")
    st.dataframe(
        tabela[
            [
                "Unidade",
                "Casos (período)",
                f"Casos inicio ({mes_inicio.strftime('%m/%Y')})",
                f"Casos fim ({mes_fim.strftime('%m/%Y')})",
                "Crescimento (%)",
                "Classificação de risco",
                "Score de risco",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    # Comparação territorial (aplica filtro "Comparação territorial" tambem no painel 2).
    st.markdown("#### Contexto territorial (filtro de comparacao)")
    st.caption(f"Comparacao ativa: {comparacao}")

    foco_norm = str(municipio_foco).strip().casefold()

    def _render_contexto(df_ctx: pd.DataFrame, escopo: str) -> None:
        if df_ctx is None or df_ctx.empty:
            st.info(f"Sem dado informado para {escopo}.")
            return
        if "municipio_nome" not in df_ctx.columns or "total_casos" not in df_ctx.columns:
            st.info(f"Sem dado informado para {escopo}.")
            return
        base_ctx = (
            df_ctx.groupby("municipio_nome", as_index=False)
            .agg(total_casos=("total_casos", "sum"))
            .sort_values("total_casos", ascending=False)
        )
        if base_ctx.empty:
            st.info(f"Sem dado informado para {escopo}.")
            return
        base_ctx["total_casos"] = _as_int(base_ctx["total_casos"])
        base_ctx["posicao"] = np.arange(1, len(base_ctx) + 1)
        row_foco = base_ctx[
            base_ctx["municipio_nome"].astype(str).str.strip().str.casefold() == foco_norm
        ]
        posicao = int(row_foco.iloc[0]["posicao"]) if not row_foco.empty else None
        total_foco = int(row_foco.iloc[0]["total_casos"]) if not row_foco.empty else None

        c_ctx1, c_ctx2 = st.columns(2)
        with c_ctx1:
            render_kpi_cards(
                [
                    make_card(
                        f"Posicao de {municipio_foco} ({escopo})",
                        posicao,
                        "Ranking por casos no período filtrado",
                    )
                ]
            )
        with c_ctx2:
            render_kpi_cards(
                [
                    make_card(
                        f"Casos de {municipio_foco} ({escopo})",
                        total_foco,
                        "Total no período filtrado",
                    )
                ]
            )

        top_ctx = base_ctx.head(max(1, int(top_n))).copy().sort_values("total_casos", ascending=True)
        top_ctx["is_focus"] = top_ctx["municipio_nome"].eq(municipio_foco)
        fig_ctx = go.Figure()
        fig_ctx.add_bar(
            x=top_ctx["total_casos"],
            y=top_ctx["municipio_nome"],
            orientation="h",
            marker_color=np.where(top_ctx["is_focus"], COLOR_ACCENT, COLOR_NEUTRAL),
            hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>",
        )
        fig_ctx.update_layout(
            title=f"Ranking por casos - {escopo}",
            margin={"l": 12, "r": 12, "t": 52, "b": 12},
            plot_bgcolor=SURFACE_BG,
            paper_bgcolor=SURFACE_BG,
        )
        y_cfg_ctx = _adaptive_yaxis(top_ctx["total_casos"], pad=0.08)
        fig_ctx.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_ctx["range"])
        fig_ctx.update_yaxes(title="")
        st.plotly_chart(fig_ctx, width="stretch")

    if comparacao in {"Rio das Ostras", "Rio das Ostras x RJ", "Rio das Ostras x RJ x Brasil"}:
        _render_contexto(df_municipio_rj_filtrado if df_municipio_rj_filtrado is not None else pd.DataFrame(), "RJ")
    if comparacao in {"Rio das Ostras x Brasil", "Rio das Ostras x RJ x Brasil"}:
        _render_contexto(df_municipio_br_filtrado if df_municipio_br_filtrado is not None else pd.DataFrame(), "Brasil")

    # Comparacao direta com outro municipio do estado (RJ).
    if df_municipio_rj_filtrado is not None and not df_municipio_rj_filtrado.empty:
        st.markdown("#### Comparacao com outro municipio do estado (RJ)")
        base_cmp = df_municipio_rj_filtrado.copy()
        base_cmp["municipio_nome"] = base_cmp["municipio_nome"].astype(str).str.strip()
        municipios_rj = sorted([m for m in base_cmp["municipio_nome"].dropna().unique().tolist() if m and m.casefold() != foco_norm])
        if not municipios_rj:
            st.info("Sem dado informado para comparar com outro municipio do RJ.")
            return

        cidade_cmp = st.selectbox(
            "Município para comparar com Rio das Ostras",
            options=municipios_rj,
            index=0,
            key="painel2_municipio_comparacao_rj",
        )

        serie_cmp = (
            base_cmp[base_cmp["municipio_nome"].isin([municipio_foco, cidade_cmp])]
            .groupby(["mes_referencia", "municipio_nome"], as_index=False)["total_casos"]
            .sum()
            .sort_values("mes_referencia")
        )
        if serie_cmp.empty:
            st.info("Sem dado informado para a comparacao selecionada.")
            return
        serie_cmp["total_casos"] = _as_int(serie_cmp["total_casos"])

        total_foco = int(serie_cmp[serie_cmp["municipio_nome"] == municipio_foco]["total_casos"].sum())
        total_cmp = int(serie_cmp[serie_cmp["municipio_nome"] == cidade_cmp]["total_casos"].sum())
        diff_abs = total_foco - total_cmp
        diff_txt = f"Diferenca no período: {diff_abs:+,}".replace(",", ".")

        def _growth_city(nome_cidade: str) -> int | None:
            tmp = serie_cmp[serie_cmp["municipio_nome"] == nome_cidade].sort_values("mes_referencia")
            if tmp.empty:
                return None
            ini = int(tmp.iloc[0]["total_casos"])
            fim = int(tmp.iloc[-1]["total_casos"])
            if ini <= 0:
                return 100 if fim > 0 else 0
            return int(round(((fim - ini) / ini) * 100.0))

        cresc_foco = _growth_city(municipio_foco)
        cresc_cmp = _growth_city(cidade_cmp)

        render_kpi_cards(
            [
                make_card(
                    f"Casos no período ({municipio_foco})",
                    total_foco,
                    diff_txt,
                    delta_pct=float(cresc_foco) if cresc_foco is not None else None,
                ),
                make_card(
                    f"Casos no período ({cidade_cmp})",
                    total_cmp,
                    "Comparacao direta no recorte filtrado",
                    delta_pct=float(cresc_cmp) if cresc_cmp is not None else None,
                ),
            ]
        )

        fig_cmp = px.line(
            serie_cmp,
            x="mes_referencia",
            y="total_casos",
            color="municipio_nome",
            color_discrete_map={
                municipio_foco: COLOR_ACCENT,
                cidade_cmp: COLOR_NEUTRAL,
            },
            title=f"Evoluuo mensal: {municipio_foco} x {cidade_cmp}",
        )
        fig_cmp.update_traces(
            mode="lines+markers",
            line={"width": 3},
            marker={"size": 6},
            hovertemplate="<b>%{fullData.name}</b><br>Período: %{x|%b/%Y}<br>Casos: %{y:,.0f}<extra></extra>",
        )
        y_cfg_cmp = _adaptive_yaxis(serie_cmp["total_casos"], pad=0.10)
        fig_cmp.update_xaxes(title="", gridcolor=GRID_SOFT, tickformat="%b/%Y", dtick="M1")
        fig_cmp.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_cmp["range"])
        fig_cmp.update_layout(
            plot_bgcolor=SURFACE_BG,
            paper_bgcolor=SURFACE_BG,
            legend_title_text="",
            margin={"l": 12, "r": 12, "t": 52, "b": 12},
        )
        st.plotly_chart(fig_cmp, width="stretch")


def render_perfil_epidemiologico(
    df_perfil_mensal: pd.DataFrame,
    df_comorb_mensal: pd.DataFrame,
    comparacao: str = "Rio das Ostras",
    df_perfil_rj: pd.DataFrame | None = None,
    df_perfil_br: pd.DataFrame | None = None,
    df_clinico: pd.DataFrame | None = None,
) -> None:
    """Dashboard 3 - Perfil Epidemiologico."""
    st.markdown("### Perfil Epidemiologico")
    if df_perfil_mensal.empty:
        st.info("Sem dado informado")
        return

    base = df_perfil_mensal.copy()
    base["total_casos"] = _as_int(base["total_casos"])
    base["internacoes"] = _as_int(base["internacoes"])
    base["obitos"] = _as_int(base["obitos"])

    faixa_order = ["0 a 9", "10 a 19", "20 a 39", "40 a 59", "60+", "Não informado"]
    faixa_mid = {"0 a 9": 5, "10 a 19": 15, "20 a 39": 30, "40 a 59": 50, "60+": 70, "Não informado": np.nan}
    sexo_map = {"M": "Masculino", "F": "Feminino", "I": "Ignorado", "NI": "Não informado"}
    raca_map = {
        "1": "Branca",
        "2": "Preta",
        "3": "Amarela",
        "4": "Parda",
        "5": "Indigena",
        "9": "Ignorado",
        "NI": "Não informado",
    }
    base["sexo_label"] = base["sexo"].astype(str).str.strip().map(sexo_map).fillna("Não informado")
    base["raca_label"] = base["raca"].astype(str).str.strip().map(raca_map).fillna("Não informado")

    total_casos = int(base["total_casos"].sum())
    faixa_total = (
        base.groupby("faixa_etaria", as_index=False)[["total_casos", "internacoes", "obitos"]]
        .sum()
    )
    faixa_total["faixa_etaria"] = pd.Categorical(faixa_total["faixa_etaria"], categories=faixa_order, ordered=True)
    faixa_total = faixa_total.sort_values("faixa_etaria")
    sexo_total = base.groupby("sexo_label", as_index=False)["total_casos"].sum().sort_values("total_casos", ascending=False)
    raca_total = base.groupby("raca_label", as_index=False)["total_casos"].sum().sort_values("total_casos", ascending=False)

    faixa_top = faixa_total.sort_values("total_casos", ascending=False).iloc[0] if not faixa_total.empty else None
    sexo_top = sexo_total.iloc[0] if not sexo_total.empty else None
    faixa_total["taxa_intern"] = np.where(faixa_total["total_casos"] > 0, faixa_total["internacoes"] / faixa_total["total_casos"], 0.0)
    risco_top = faixa_total.sort_values("taxa_intern", ascending=False).iloc[0] if not faixa_total.empty else None

    # Idade media aproximada por ponto medio da faixa etaria.
    faixa_total["mid"] = faixa_total["faixa_etaria"].astype(str).map(faixa_mid)
    denom = float(faixa_total[faixa_total["mid"].notna()]["total_casos"].sum())
    idade_media = (
        float((faixa_total["mid"] * faixa_total["total_casos"]).fillna(0).sum()) / denom
        if denom > 0
        else None
    )

    comorb_pct = None
    if not df_comorb_mensal.empty and total_casos > 0:
        comorb_sum = int(_as_int(df_comorb_mensal["total_casos"]).sum())
        comorb_pct = (comorb_sum / total_casos) * 100.0

    def _pct(v: int | float | None) -> str:
        if v is None:
            return "-"
        return f"{int(round(float(v)))}%"

    cards = [
        make_card("Idade media", int(round(idade_media)) if idade_media is not None else None, "anos (estimativa por faixa)"),
        make_card(
            "Grupo mais afetado",
            int(faixa_top["total_casos"]) if faixa_top is not None else None,
            (
                f"{faixa_top['faixa_etaria']} | {_pct((faixa_top['total_casos'] / total_casos) * 100)} dos casos"
                if faixa_top is not None and total_casos > 0
                else "Sem dado informado"
            ),
        ),
        make_card(
            "Sexo predominante",
            int(sexo_top["total_casos"]) if sexo_top is not None else None,
            (
                f"{sexo_top['sexo_label']} | {_pct((sexo_top['total_casos'] / total_casos) * 100)} dos casos"
                if sexo_top is not None and total_casos > 0
                else "Sem dado informado"
            ),
        ),
        make_card(
            "Grupo de maior risco (internacao)",
            int(round((float(risco_top['taxa_intern']) * 100.0))) if risco_top is not None else None,
            (f"{risco_top['faixa_etaria']} | taxa de internacao" if risco_top is not None else "Sem dado informado"),
            suffix="%",
        ),
        make_card("Comorbidades (frequencia)", int(round(comorb_pct)) if comorb_pct is not None else None, "Percentual relativo no período", suffix="%"),
    ]
    render_kpi_cards(cards)

    # B2 Faixa etaria
    faixa_bar = faixa_total.sort_values("total_casos", ascending=True).copy()
    faixa_destaque = faixa_top["faixa_etaria"] if faixa_top is not None else None
    fig_faixa = go.Figure()
    fig_faixa.add_bar(
        x=faixa_bar["total_casos"],
        y=faixa_bar["faixa_etaria"].astype(str),
        orientation="h",
        marker_color=np.where(faixa_bar["faixa_etaria"].astype(str).eq(str(faixa_destaque)), COLOR_ACCENT, COLOR_NEUTRAL),
        customdata=np.where(total_casos > 0, (faixa_bar["total_casos"] / total_casos) * 100.0, 0.0),
        hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<br>Percentual: %{customdata:,.0f}%<extra></extra>",
    )
    fig_faixa.update_layout(title="Distribuição de casos por faixa etaria", plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
    y_cfg_fx = _adaptive_yaxis(faixa_bar["total_casos"], pad=0.08)
    fig_faixa.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_fx["range"])
    fig_faixa.update_yaxes(title="")

    # B3 Sexo donut
    fig_sexo = px.pie(
        sexo_total,
        names="sexo_label",
        values="total_casos",
        hole=0.58,
        color="sexo_label",
        color_discrete_sequence=[BRAND_BLUE, COLOR_ACCENT, "#9EB1C3", "#D8E0E8"],
        title="Distribuição dos casos por sexo",
    )
    fig_sexo.update_traces(textinfo="percent", hovertemplate="<b>%{label}</b><br>Casos: %{value:,.0f}<br>%{percent}<extra></extra>")
    fig_sexo.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")

    c1, c2 = st.columns(2)
    c1.plotly_chart(fig_faixa, width="stretch")
    c2.plotly_chart(fig_sexo, width="stretch")

    # B4 Raca/cor
    fig_raca = px.bar(
        raca_total.sort_values("total_casos", ascending=True),
        x="total_casos",
        y="raca_label",
        orientation="h",
        title="Distribuição dos casos por raca/cor",
        color_discrete_sequence=[COLOR_NEUTRAL],
    )
    fig_raca.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
    y_cfg_r = _adaptive_yaxis(raca_total["total_casos"], pad=0.08)
    fig_raca.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_r["range"])
    fig_raca.update_yaxes(title="")
    fig_raca.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
    st.plotly_chart(fig_raca, width="stretch")

    # B5 Gravidade por faixa
    grav = faixa_total.copy()
    grav = grav[grav["faixa_etaria"].notna()]
    fig_grav = go.Figure()
    fig_grav.add_bar(name="Internações", x=grav["faixa_etaria"].astype(str), y=grav["internacoes"], marker_color=COLOR_ACCENT, hovertemplate="<b>%{x}</b><br>Internações: %{y:,.0f}<extra></extra>")
    fig_grav.add_bar(name="Óbitos", x=grav["faixa_etaria"].astype(str), y=grav["obitos"], marker_color="#C73E3E", hovertemplate="<b>%{x}</b><br>Óbitos: %{y:,.0f}<extra></extra>")
    fig_grav.update_layout(barmode="stack", title="Internações e Óbitos por faixa etaria", plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
    st.plotly_chart(fig_grav, width="stretch")

    # B6 Comorbidades
    if df_comorb_mensal.empty:
        st.info("Comorbidades associadas aos casos: Sem dado informado")
    else:
        comorb = (
            df_comorb_mensal.groupby("comorbidade", as_index=False)["total_casos"]
            .sum()
            .sort_values("total_casos", ascending=False)
        )
        comorb["total_casos"] = _as_int(comorb["total_casos"])
        if len(comorb) > 6:
            top = comorb.head(5).copy()
            outras = int(comorb.iloc[5:]["total_casos"].sum())
            top = pd.concat([top, pd.DataFrame([{"comorbidade": "Outras", "total_casos": outras}])], ignore_index=True)
            comorb = top
        fig_com = px.bar(
            comorb.sort_values("total_casos", ascending=True),
            x="total_casos",
            y="comorbidade",
            orientation="h",
            title="Comorbidades associadas aos casos",
            color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig_com.update_traces(hovertemplate="<b>%{y}</b><br>Frequencia: %{x:,.0f}<extra></extra>")
        y_cfg_com = _adaptive_yaxis(comorb["total_casos"], pad=0.08)
        fig_com.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_com["range"])
        fig_com.update_yaxes(title="")
        fig_com.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
        st.plotly_chart(fig_com, width="stretch")

    # B7 Perfil etario ao longo do tempo (top 4 faixas)
    serie_faixa = (
        base.groupby(["mes_referencia", "faixa_etaria"], as_index=False)["total_casos"]
        .sum()
        .sort_values("mes_referencia")
    )
    top_faixas = (
        serie_faixa.groupby("faixa_etaria", as_index=False)["total_casos"]
        .sum()
        .sort_values("total_casos", ascending=False)
        .head(4)["faixa_etaria"]
        .tolist()
    )
    serie_plot = serie_faixa[serie_faixa["faixa_etaria"].isin(top_faixas)].copy()
    fig_serie = px.line(
        serie_plot,
        x="mes_referencia",
        y="total_casos",
        color="faixa_etaria",
        title="Mudanca do perfil etario ao longo do tempo",
    )
    fig_serie.update_traces(hovertemplate="<b>%{fullData.name}</b><br>Período: %{x|%b/%Y}<br>Casos: %{y:,.0f}<extra></extra>")
    fig_serie.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
    fig_serie.update_xaxes(title="", gridcolor=GRID_SOFT, tickformat="%b/%Y", dtick="M1")
    y_cfg_ser = _adaptive_yaxis(serie_plot["total_casos"] if not serie_plot.empty else pd.Series([0]), pad=0.08)
    fig_serie.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_ser["range"])
    st.plotly_chart(fig_serie, width="stretch")

    # B8 Heatmap idade x sexo
    heat = (
        base.groupby(["sexo_label", "faixa_etaria"], as_index=False)["total_casos"]
        .sum()
    )
    if heat.empty:
        st.info("Risco por idade e sexo: Sem dado informado")
        return
    heat["faixa_etaria"] = pd.Categorical(heat["faixa_etaria"], categories=faixa_order, ordered=True)
    heat = heat.sort_values(["sexo_label", "faixa_etaria"])
    pivot = heat.pivot(index="sexo_label", columns="faixa_etaria", values="total_casos").fillna(0)
    fig_heat = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=[str(i) for i in pivot.index],
            colorscale=[[0, "#EAF0F6"], [1, BRAND_BLUE]],
            hovertemplate="Sexo: %{y}<br>Faixa etaria: %{x}<br>Casos: %{z:,.0f}<extra></extra>",
        )
    )
    fig_heat.update_layout(
        title="Risco por idade e sexo (casos)",
        plot_bgcolor=SURFACE_BG,
        paper_bgcolor=SURFACE_BG,
        margin={"l": 12, "r": 12, "t": 52, "b": 12},
    )
    st.plotly_chart(fig_heat, width="stretch")

    # Blocos comparativos quando filtro territorial incluir RJ/BR.
    def _normalize_comp(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        tmp = df.copy()
        tmp["escopo"] = label
        tmp["total_casos"] = _as_int(tmp["total_casos"])
        tmp["sexo_label"] = tmp["sexo"].astype(str).str.strip().map(sexo_map).fillna("Não informado")
        return tmp

    comp_parts: list[pd.DataFrame] = []
    base_local = base.copy()
    base_local["escopo"] = "Rio das Ostras"
    comp_parts.append(base_local)
    if comparacao in {"Rio das Ostras x RJ", "Rio das Ostras x RJ x Brasil"}:
        comp_parts.append(_normalize_comp(df_perfil_rj if df_perfil_rj is not None else pd.DataFrame(), "RJ"))
    if comparacao in {"Rio das Ostras x Brasil", "Rio das Ostras x RJ x Brasil"}:
        comp_parts.append(_normalize_comp(df_perfil_br if df_perfil_br is not None else pd.DataFrame(), "Brasil"))

    comp_parts = [p for p in comp_parts if p is not None and not p.empty]
    if len(comp_parts) <= 1:
        return

    st.markdown("#### Comparacao de perfil (Rio das Ostras x escopos selecionados)")
    comp_df = pd.concat(comp_parts, ignore_index=True)

    # Comparacao por faixa etaria (%)
    faixa_cmp = comp_df.groupby(["escopo", "faixa_etaria"], as_index=False)["total_casos"].sum()
    total_escopo = faixa_cmp.groupby("escopo", as_index=False)["total_casos"].sum().rename(columns={"total_casos": "total_escopo"})
    faixa_cmp = faixa_cmp.merge(total_escopo, on="escopo", how="left")
    faixa_cmp["pct"] = np.where(faixa_cmp["total_escopo"] > 0, (faixa_cmp["total_casos"] / faixa_cmp["total_escopo"]) * 100.0, 0.0)
    faixa_cmp["faixa_etaria"] = pd.Categorical(faixa_cmp["faixa_etaria"], categories=faixa_order, ordered=True)
    faixa_cmp = faixa_cmp.sort_values(["faixa_etaria", "escopo"])

    fig_cmp_faixa = px.bar(
        faixa_cmp,
        x="faixa_etaria",
        y="pct",
        color="escopo",
        barmode="group",
        title="Comparacao por faixa etaria (% dos casos)",
        color_discrete_map={"Rio das Ostras": COLOR_ACCENT, "RJ": COLOR_NEUTRAL, "Brasil": BRAND_BLUE},
    )
    fig_cmp_faixa.update_traces(hovertemplate="<b>%{fullData.name}</b><br>Faixa: %{x}<br>Percentual: %{y:,.0f}%<extra></extra>")
    fig_cmp_faixa.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
    fig_cmp_faixa.update_xaxes(title="", categoryorder="array", categoryarray=faixa_order)
    fig_cmp_faixa.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=[0, max(100, float(faixa_cmp["pct"].max() * 1.15))])

    # Comparacao por sexo (%)
    sexo_cmp = comp_df.groupby(["escopo", "sexo_label"], as_index=False)["total_casos"].sum()
    total_escopo_s = sexo_cmp.groupby("escopo", as_index=False)["total_casos"].sum().rename(columns={"total_casos": "total_escopo"})
    sexo_cmp = sexo_cmp.merge(total_escopo_s, on="escopo", how="left")
    sexo_cmp["pct"] = np.where(sexo_cmp["total_escopo"] > 0, (sexo_cmp["total_casos"] / sexo_cmp["total_escopo"]) * 100.0, 0.0)
    fig_cmp_sexo = px.bar(
        sexo_cmp,
        x="sexo_label",
        y="pct",
        color="escopo",
        barmode="group",
        title="Comparacao por sexo (% dos casos)",
        color_discrete_map={"Rio das Ostras": COLOR_ACCENT, "RJ": COLOR_NEUTRAL, "Brasil": BRAND_BLUE},
    )
    fig_cmp_sexo.update_traces(hovertemplate="<b>%{fullData.name}</b><br>Sexo: %{x}<br>Percentual: %{y:,.0f}%<extra></extra>")
    fig_cmp_sexo.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
    fig_cmp_sexo.update_xaxes(title="")
    fig_cmp_sexo.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=[0, max(100, float(sexo_cmp["pct"].max() * 1.15))])

    cc1, cc2 = st.columns(2)
    cc1.plotly_chart(fig_cmp_faixa, width="stretch")
    cc2.plotly_chart(fig_cmp_sexo, width="stretch")

    # Sintomas mais frequentes no Painel 3, usando sinais clinicos oficiais.
    st.markdown("#### Sintomas mais frequentes (sinais clinicos)")
    if df_clinico is None or df_clinico.empty:
        st.info("Sem dado informado")
        return

    sintomas_map = {
        "febre": "Febre",
        "mialgia": "Mialgia",
        "cefaleia": "Cefaleia",
        "exantema": "Exantema",
        "vomito": "Vomito",
        "nausea": "Nausea",
        "dor_costas": "Dor nas costas",
        "conjuntvit": "Conjuntivite",
        "artrite": "Artrite",
        "artralgia": "Artralgia intensa",
        "petequia_n": "Petequias",
        "leucopenia": "Leucopenia",
        "laco": "Prova do laco",
        "dor_retro": "Dor retroorbital",
    }
    base_cli = df_clinico.copy()
    total_cli = len(base_cli)
    cols_presentes = [c for c in sintomas_map if c in base_cli.columns]
    if total_cli <= 0 or not cols_presentes:
        st.info("Sem dado informado")
        return

    def _is_yes(séries: pd.Series) -> pd.Series:
        return séries.astype(str).str.strip().isin(["1", "S", "SIM"])

    rows = []
    for col in cols_presentes:
        sim = int(_is_yes(base_cli[col]).sum())
        pct = (sim / total_cli) * 100.0 if total_cli > 0 else 0.0
        rows.append({"sintoma": sintomas_map[col], "casos": sim, "percentual": pct})
    df_sint = pd.DataFrame(rows).sort_values("percentual", ascending=False)
    if df_sint.empty:
        st.info("Sem dado informado")
        return

    fig_sint = px.bar(
        df_sint.sort_values("percentual", ascending=True),
        x="percentual",
        y="sintoma",
        orientation="h",
        title="Sintomas mais frequentes nos casos notificados",
        color_discrete_sequence=[COLOR_NEUTRAL],
    )
    fig_sint.update_traces(
        customdata=df_sint.sort_values("percentual", ascending=True)[["casos"]],
        hovertemplate="<b>%{y}</b><br>Percentual: %{x:,.0f}%<br>Casos: %{customdata[0]:,.0f}<extra></extra>",
    )
    y_cfg_s = _adaptive_yaxis(df_sint["percentual"], pad=0.10, min_value=5)
    fig_sint.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg_s["range"])
    fig_sint.update_yaxes(title="")
    fig_sint.update_layout(
        plot_bgcolor=SURFACE_BG,
        paper_bgcolor=SURFACE_BG,
        margin={"l": 12, "r": 12, "t": 52, "b": 12},
    )
    st.plotly_chart(fig_sint, width="stretch")


def render_clinico_exames_dashboard(df_clinico: pd.DataFrame) -> None:
    """Dashboard 4 - Clínico e Exames."""
    st.markdown("### Clínico e Exames")
    if df_clinico.empty:
        st.info("Sem dado informado")
        return

    base = df_clinico.copy()
    base["classificacao_final"] = base.get("classificacao_final", pd.Series(dtype="object")).astype(str).str.strip()
    base["evolucao_caso"] = base.get("evolucao_caso", pd.Series(dtype="object")).astype(str).str.strip()
    base["hospitaliz"] = base.get("hospitaliz", pd.Series(dtype="object")).astype(str).str.strip()
    base["mes_referencia"] = pd.to_datetime(base.get("mes_referencia"), errors="coerce")
    base = base[base["mes_referencia"].notna()].copy()
    if base.empty:
        st.info("Sem dado informado")
        return

    confirm_codes = {"1", "3", "4", "5", "6", "10", "11", "12"}
    sintomas_map = {
        "febre": "Febre",
        "cefaleia": "Cefaleia",
        "mialgia": "Mialgia",
        "exantema": "Exantema",
        "vomito": "Vomito",
        "nausea": "Nausea",
        "dor_costas": "Dor nas costas",
        "conjuntvit": "Conjuntivite",
        "artrite": "Artrite",
        "artralgia": "Artralgia intensa",
        "petequia_n": "Petequias",
        "leucopenia": "Leucopenia",
        "laco": "Prova do laco",
        "dor_retro": "Dor retroorbital",
    }
    exam_defs = {
        "res_chiks1": {"label": "Chikungunya IgM Soro 1", "date_col": "dt_chik_s1"},
        "res_chiks2": {"label": "Chikungunya IgM Soro 2", "date_col": "dt_chik_s2"},
        "resul_prnt": {"label": "PRNT", "date_col": "dt_prnt"},
        "resul_soro": {"label": "Dengue IgM", "date_col": "dt_soro"},
        "resul_ns1": {"label": "NS1", "date_col": "dt_ns1"},
        "resul_vi_n": {"label": "Isolamento viral", "date_col": "dt_viral"},
        "resul_pcr_": {"label": "RT-PCR", "date_col": "dt_pcr"},
        "histopa_n": {"label": "Histopatologia", "date_col": None},
        "imunoh_n": {"label": "Imunohistoquimica", "date_col": None},
    }
    comorb_map = {
        "diabetes": "Diabetes",
        "hematolog": "Doenças hematologicas",
        "hepatopat": "Hepatopatias",
        "renal": "Doenca renal cronica",
        "hipertensa": "Hipertensao arterial",
        "acido_pept": "Doenca acido-peptica",
        "auto_imune": "Doenças autoimunes",
    }
    sint_cols = [c for c in sintomas_map if c in base.columns]
    comorb_cols = [c for c in comorb_map if c in base.columns]
    exam_cols = [c for c in exam_defs if c in base.columns]
    exam_date_cols = [v["date_col"] for c, v in exam_defs.items() if c in exam_cols and v["date_col"] in base.columns]

    total = len(base)
    if total == 0:
        st.info("Sem dado informado")
        return

    # Helpers
    def _is_yes(séries: pd.Series) -> pd.Series:
        return séries.astype(str).str.strip().isin(["1", "S", "SIM"])

    def _fmt_abs(v: int | float | None) -> str:
        if v is None:
            return "-"
        try:
            return f"{int(round(float(v))):,}".replace(",", ".")
        except (TypeError, ValueError):
            return "-"

    def _build_text_focus_card(label: str, destaque: str | None, quantidade: int | float | None, extra: str | None = None) -> dict[str, object]:
        txt = str(destaque).strip() if destaque is not None else ""
        if not txt or txt.lower() in {"nan", "none", "-"}:
            txt = "Sem dado informado"
        help_txt = f"Casos: {_fmt_abs(quantidade)}"
        if extra:
            help_txt = f"{help_txt} | {extra}"
        return {
            "label": label,
            "value": txt,
            "value_mode": "text",
            "delta_text": "-",
            "delta_pct": None,
            "help": help_txt,
        }

    # Bloco 1 - cards
    sint_data = []
    for col in sint_cols:
        sim = int(_is_yes(base[col]).sum())
        pct = (sim / total) * 100 if total > 0 else 0
        sint_data.append({"sintoma": sintomas_map[col], "sim": sim, "pct": pct, "col": col})
    df_sint = pd.DataFrame(sint_data).sort_values("sim", ascending=False) if sint_data else pd.DataFrame()
    sint_top = df_sint.iloc[0] if not df_sint.empty else None

    combo_cols = [c for c in ["febre", "cefaleia", "mialgia"] if c in base.columns]
    if combo_cols:
        def _combo(row: pd.Series) -> str:
            labels = [sintomas_map[c] for c in combo_cols if str(row.get(c, "")).strip() == "1"]
            return " + ".join(labels[:3]) if labels else "Sem padrao clnico"
        base["padrao_combo"] = base.apply(_combo, axis=1)
        combo_counts = base.groupby("padrao_combo", as_index=False).size().sort_values("size", ascending=False)
        combo_top = combo_counts.iloc[0] if not combo_counts.empty else None
    else:
        combo_counts = pd.DataFrame()
        combo_top = None

    if exam_cols:
        def _has_date(séries: pd.Series) -> pd.Series:
            return pd.to_datetime(séries, errors="coerce").notna()

        exam_any_realizado = pd.Series(False, index=base.index)
        exam_any_positivo = pd.Series(False, index=base.index)
        for c in exam_cols:
            vals = base[c].astype(str).str.strip()
            date_col = exam_defs[c]["date_col"]
            via_result = vals.isin(["1", "2", "3"])
            via_data = _has_date(base[date_col]) if date_col and date_col in base.columns else pd.Series(False, index=base.index)
            exam_any_realizado = exam_any_realizado | via_result | via_data
            exam_any_positivo = exam_any_positivo | vals.eq("1")
        pct_exam = (float(exam_any_realizado.sum()) / total) * 100 if total > 0 else 0.0
        positivos = int((exam_any_realizado & exam_any_positivo).sum())
        positividade = (positivos / float(exam_any_realizado.sum())) * 100 if int(exam_any_realizado.sum()) > 0 else 0.0
        pct_sem_exam = 100.0 - pct_exam
    else:
        pct_exam = None
        positividade = None
        pct_sem_exam = None

    cards = [
        _build_text_focus_card(
            "Sintoma predominante",
            str(sint_top["sintoma"]) if sint_top is not None else "Sem dado informado",
            int(sint_top["sim"]) if sint_top is not None else None,
            f"{int(round(float(sint_top['pct'])))}% dos casos" if sint_top is not None else None,
        ),
        _build_text_focus_card(
            "Padrao clinico tipico",
            str(combo_top["padrao_combo"]) if combo_top is not None else "Sem dado informado",
            int(combo_top["size"]) if combo_top is not None else None,
            None,
        ),
        make_card("Exames realizados", int(round(float(pct_exam))) if pct_exam is not None else None, "Percentual de casos com exame", suffix="%"),
        make_card("Taxa de positividade", int(round(float(positividade))) if positividade is not None else None, "Confirmacao laboratorial", suffix="%"),
        make_card("Casos sem exame", int(round(float(pct_sem_exam))) if pct_sem_exam is not None else None, "Percentual sem testagem", suffix="%"),
    ]
    render_kpi_cards(cards)

    # Bloco 2 - sintomas frequentes
    if df_sint.empty:
        st.info("Sintomas mais frequentes: Sem dado informado")
    else:
        fig_sint = px.bar(
            df_sint.sort_values("pct", ascending=True),
            x="pct",
            y="sintoma",
            orientation="h",
            title="Sintomas mais frequentes nos casos notificados",
            color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig_sint.update_traces(customdata=df_sint.sort_values("pct", ascending=True)[["sim"]], hovertemplate="<b>%{y}</b><br>Percentual: %{x:,.0f}%<br>Casos: %{customdata[0]:,.0f}<extra></extra>")
        y_cfg = _adaptive_yaxis(df_sint["pct"], pad=0.10, min_value=5)
        fig_sint.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
        fig_sint.update_yaxes(title="")
        fig_sint.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
        st.plotly_chart(fig_sint, width="stretch")

    # Bloco 3 - combinacoes
    if combo_counts.empty:
        st.info("Combinacoes de sintomas: Sem dado informado")
    else:
        top_combo = combo_counts.head(10).sort_values("size", ascending=True)
        fig_combo = px.bar(
            top_combo,
            x="size",
            y="padrao_combo",
            orientation="h",
            title="Combinacoes de sintomas mais comuns",
            color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig_combo.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
        y_cfg = _adaptive_yaxis(top_combo["size"], pad=0.10)
        fig_combo.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
        fig_combo.update_yaxes(title="")
        fig_combo.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
        st.plotly_chart(fig_combo, width="stretch")

    # Bloco 4 - sintomas x confirmacao
    if df_sint.empty:
        st.info("Relação sintomas x confirmacao: Sem dado informado")
    else:
        rows = []
        cls = base["classificacao_final"].astype(str)
        confirmado_mask = cls.isin(confirm_codes)
        descartado_mask = cls.eq("2")
        for _, row in df_sint.iterrows():
            col = row["col"]
            mask = _is_yes(base[col])
            rows.append(
                {
                    "sintoma": row["sintoma"],
                    "Confirmados": int((mask & confirmado_mask).sum()),
                    "Descartados": int((mask & descartado_mask).sum()),
                }
            )
        sx = pd.DataFrame(rows)
        sx_long = sx.melt(id_vars="sintoma", value_vars=["Confirmados", "Descartados"], var_name="status", value_name="casos")
        fig_sx = px.bar(
            sx_long,
            x="sintoma",
            y="casos",
            color="status",
            barmode="group",
            title="Relação entre sintomas e confirmacao dos casos",
            color_discrete_map={"Confirmados": BRAND_BLUE, "Descartados": "#9EB1C3"},
        )
        fig_sx.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:,.0f}<extra></extra>")
        fig_sx.update_xaxes(title="")
        fig_sx.update_yaxes(title="", gridcolor=GRID_SOFT)
        fig_sx.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
        st.plotly_chart(fig_sx, width="stretch")

    # Bloco 5 - doencas preexistentes (nome + codigo)
    if not comorb_cols:
        st.info("Doenças preexistentes: Sem dado informado")
    else:
        comorb_rows = []
        for col in comorb_cols:
            vals = base[col].astype(str).str.strip()
            positivos = int(vals.eq("1").sum())
            pct = (positivos / total) * 100.0 if total > 0 else 0.0
            comorb_rows.append(
                {
                    "doenca_codigo": col,
                    "doenca_label": comorb_map[col],
                    "casos": positivos,
                    "percentual": pct,
                }
            )
        comorb_df = pd.DataFrame(comorb_rows).sort_values("casos", ascending=False)
        if comorb_df["casos"].sum() <= 0:
            st.info("Doenças preexistentes: Sem dado informado")
        else:
            comorb_plot = comorb_df.sort_values("casos", ascending=True).copy()
            fig_comorb = px.bar(
                comorb_plot,
                x="casos",
                y="doenca_label",
                orientation="h",
                title="Doenças preexistentes nos casos notificados",
                color_discrete_sequence=[COLOR_NEUTRAL],
            )
            fig_comorb.update_traces(
                customdata=comorb_plot[["doenca_codigo", "percentual"]],
                hovertemplate="<b>%{y}</b><br>Codigo: %{customdata[0]}<br>Casos: %{x:,.0f}<br>Percentual: %{customdata[1]:,.0f}%<extra></extra>",
            )
            y_cfg = _adaptive_yaxis(comorb_plot["casos"], pad=0.10, min_value=5)
            fig_comorb.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
            fig_comorb.update_yaxes(title="")
            fig_comorb.update_layout(
                plot_bgcolor=SURFACE_BG,
                paper_bgcolor=SURFACE_BG,
                margin={"l": 12, "r": 12, "t": 52, "b": 12},
            )
            st.plotly_chart(fig_comorb, width="stretch")

    # Bloco 6 - exames realizados resultados
    if not exam_cols:
        st.info("Exames realizados e resultados: Sem dado informado")
    else:
        exam_rows = []
        for col in exam_cols:
            vals = base[col].astype(str).str.strip()
            date_col = exam_defs[col]["date_col"]
            via_result = vals.isin(["1", "2", "3"])
            via_data = pd.to_datetime(base[date_col], errors="coerce").notna() if date_col and date_col in base.columns else pd.Series(False, index=base.index)
            realizado = int((via_result | via_data).sum())
            positivo = int(vals.eq("1").sum())
            negativo = int(vals.eq("2").sum())
            não_real = int((vals.eq("4") | vals.eq("") | vals.eq("nan") | vals.eq("None")).sum())
            exam_rows.append(
                {
                    "exame": exam_defs[col]["label"],
                    "Realizados": realizado,
                    "Positivos": positivo,
                    "Negativos": negativo,
                    "Não realizados": não_real,
                }
            )
        exam_df = pd.DataFrame(exam_rows)
        exam_long = exam_df.melt(id_vars="exame", var_name="metrica", value_name="valor")
        fig_exam = px.bar(
            exam_long,
            x="exame",
            y="valor",
            color="metrica",
            barmode="group",
            title="Exames realizados e resultados",
            color_discrete_map={
                "Realizados": BRAND_BLUE,
                "Positivos": COLOR_ACCENT,
                "Negativos": "#5F7489",
                "Não realizados": "#B7C8D8",
            },
        )
        fig_exam.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:,.0f}<extra></extra>")
        fig_exam.update_xaxes(title="")
        fig_exam.update_yaxes(title="", gridcolor=GRID_SOFT)
        fig_exam.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
        st.plotly_chart(fig_exam, width="stretch")

    # Bloco 7 - evolucao exames no tempo
    if not exam_cols:
        st.info("Evoluuo da realizacao de exames: Sem dado informado")
    else:
        tmp = base.copy()
        exame_realizado = pd.Series(False, index=tmp.index)
        for col in exam_cols:
            vals = tmp[col].astype(str).str.strip()
            date_col = exam_defs[col]["date_col"]
            via_result = vals.isin(["1", "2", "3"])
            via_data = pd.to_datetime(tmp[date_col], errors="coerce").notna() if date_col and date_col in tmp.columns else pd.Series(False, index=tmp.index)
            exame_realizado = exame_realizado | via_result | via_data
        tmp["exame_realizado"] = exame_realizado.astype(int)
        evo = tmp.groupby("mes_referencia", as_index=False).agg(total=("exame_realizado", "size"), realizados=("exame_realizado", "sum"))
        evo["pct"] = np.where(evo["total"] > 0, (evo["realizados"] / evo["total"]) * 100.0, 0.0)
        fig_evo = px.line(
            evo,
            x="mes_referencia",
            y="pct",
            title="Evoluuo da realizacao de exames ao longo do tempo",
            color_discrete_sequence=[BRAND_BLUE],
        )
        fig_evo.update_traces(mode="lines+markers", hovertemplate="Período: %{x|%b/%Y}<br>Percentual: %{y:,.0f}%<extra></extra>")
        y_cfg = _adaptive_yaxis(evo["pct"], pad=0.10, min_value=10)
        fig_evo.update_xaxes(title="", gridcolor=GRID_SOFT, tickformat="%b/%Y", dtick="M1")
        fig_evo.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
        fig_evo.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
        st.plotly_chart(fig_evo, width="stretch")

    # Bloco 8 - classificacao final
    map_classif = {
        "1": "Confirmados",
        "3": "Confirmados",
        "4": "Confirmados",
        "5": "Confirmados",
        "6": "Confirmados",
        "10": "Confirmados",
        "11": "Confirmados",
        "12": "Confirmados",
        "2": "Descartados",
        "8": "Inconclusivos",
    }
    cls = base["classificacao_final"].astype(str).str.strip().map(map_classif).fillna("Em investigação")
    donut = cls.value_counts(dropna=False).reset_index()
    donut.columns = ["classificacao", "casos"]
    donut["casos"] = _as_int(donut["casos"])
    fig_donut = px.pie(
        donut,
        names="classificacao",
        values="casos",
        hole=0.58,
        title="Classificação dos casos apos investigação",
        color="classificacao",
        color_discrete_map={
            "Confirmados": BRAND_BLUE,
            "Descartados": "#9EB1C3",
            "Inconclusivos": BRAND_YELLOW,
            "Em investigação": "#36566F",
        },
    )
    fig_donut.update_traces(textinfo="percent", hovertemplate="<b>%{label}</b><br>Casos: %{value:,.0f}<br>%{percent}<extra></extra>")
    fig_donut.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
    st.plotly_chart(fig_donut, width="stretch")

    # Bloco 9 - sorotipo
    if "sorotipo" not in base.columns:
        st.info("Sorotipo: Sem dado informado")
    else:
        soro = base["sorotipo"].astype(str).str.strip()
        soro = soro[soro.isin(["1", "2", "3", "4"])]
        if soro.empty:
            st.info("Sorotipo: Sem dado informado")
        else:
            map_soro = {"1": "DEN 1", "2": "DEN 2", "3": "DEN 3", "4": "DEN 4"}
            soro_df = soro.map(map_soro).value_counts().reset_index()
            soro_df.columns = ["sorotipo", "casos"]
            soro_df["casos"] = _as_int(soro_df["casos"])
            fig_soro = px.bar(
                soro_df.sort_values("casos", ascending=True),
                x="casos",
                y="sorotipo",
                orientation="h",
                title="Sorotipo informado nos casos",
                color_discrete_sequence=[COLOR_NEUTRAL],
            )
            fig_soro.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
            y_cfg = _adaptive_yaxis(soro_df["casos"], pad=0.10, min_value=5)
            fig_soro.update_xaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
            fig_soro.update_yaxes(title="")
            fig_soro.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
            st.plotly_chart(fig_soro, width="stretch")

    # Bloco 10 - exames x gravidade
    if not exam_cols:
        st.info("Internações e Óbitos por resultado de exame: Sem dado informado")
    else:
        tmp = base.copy()
        is_obito = tmp["evolucao_caso"].isin(["2", "3", "4"]) | tmp["evolucao_caso"].str.upper().str.contains("OBITO", na=False)
        is_intern = tmp["hospitaliz"].isin(["1"])

        def _resultado_global(row: pd.Series) -> str:
            vals = [str(row.get(c, "")).strip() for c in exam_cols]
            datas = []
            for c in exam_cols:
                dcol = exam_defs[c]["date_col"]
                if dcol and dcol in row.index:
                    datas.append(pd.notna(pd.to_datetime(row.get(dcol), errors="coerce")))
            if any(v == "1" for v in vals):
                return "Positivo"
            if any(v == "2" for v in vals):
                return "Negativo"
            if any(v == "3" for v in vals):
                return "Inconclusivo"
            if any(datas):
                return "Com coleta (sem resultado)"
            return "Sem exame"

        tmp["resultado_exame"] = tmp.apply(_resultado_global, axis=1)
        tmp["internacao"] = is_intern.astype(int)
        tmp["obito"] = is_obito.astype(int)
        eg = tmp.groupby("resultado_exame", as_index=False)[["internacao", "obito"]].sum()
        eg_long = eg.melt(id_vars="resultado_exame", value_vars=["internacao", "obito"], var_name="metrica", value_name="valor")
        eg_long["metrica"] = eg_long["metrica"].map({"internacao": "Internações", "obito": "Óbitos"})
        fig_eg = px.bar(
            eg_long,
            x="resultado_exame",
            y="valor",
            color="metrica",
            barmode="group",
            title="Internações e Óbitos por resultado de exame",
            color_discrete_map={"Internações": COLOR_ACCENT, "Óbitos": "#C73E3E"},
        )
        fig_eg.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:,.0f}<extra></extra>")
        fig_eg.update_xaxes(title="")
        fig_eg.update_yaxes(title="", gridcolor=GRID_SOFT)
        fig_eg.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12}, legend_title_text="")
        st.plotly_chart(fig_eg, width="stretch")


def render_year_comparison(df_mes_municipio: pd.DataFrame, metrica_coluna: str, municipio_nome: str) -> None:
    """Compara anos quando o filtro seleciona mais de um ano."""
    if df_mes_municipio.empty or "ano" not in df_mes_municipio.columns:
        return
    anos = sorted(df_mes_municipio["ano"].dropna().astype(int).unique().tolist())
    if len(anos) < 2:
        return
    base = (
        df_mes_municipio.groupby("ano", as_index=False)[metrica_coluna]
        .sum()
        .rename(columns={metrica_coluna: "valor"})
        .sort_values("ano")
    )
    base["valor"] = _as_int(base["valor"])
    período = _período_label(df_mes_municipio)
    fig = px.bar(
        base,
        x="ano",
        y="valor",
        title=f"Comparação anual ({municipio_nome})<br><sup>{período}</sup>",
        text_auto=True,
        color_discrete_sequence=[BRAND_BLUE],
    )
    fig.update_traces(
        texttemplate="%{y:,.0f}",
        hovertemplate="<b>Ano %{x}</b><br>Valor: %{y:,.0f}<extra></extra>",
    )
    fig.update_layout(plot_bgcolor=SURFACE_BG, paper_bgcolor=SURFACE_BG, margin={"l": 12, "r": 12, "t": 52, "b": 12})
    fig.update_xaxes(title="")
    y_cfg = _adaptive_yaxis(base["valor"], pad=0.06)
    fig.update_yaxes(title="", gridcolor=GRID_SOFT, type="linear", range=y_cfg["range"])
    st.plotly_chart(fig, width="stretch")
