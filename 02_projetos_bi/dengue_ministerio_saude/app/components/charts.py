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


def _as_int(series: pd.Series) -> pd.Series:
    """Arredonda para inteiro sem casas decimais."""
    return pd.to_numeric(series, errors="coerce").fillna(0).round(0).astype(int)


def _axis_max(series: pd.Series, pad: float = 0.1, min_value: float = 1.0) -> float:
    """Retorna maximo de eixo com padding, evitando escalas distorcidas."""
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return min_value
    vmax = float(values.max())
    if vmax <= 0:
        return min_value
    return vmax * (1.0 + max(0.0, pad))


def _adaptive_yaxis(
    series: pd.Series,
    *,
    allow_log: bool = False,
    pad: float = 0.08,
    min_value: float = 1.0,
) -> dict[str, object]:
    """Define parametros de eixo Y com escala adaptativa ao recorte atual.

    Regras:
    - Padrao linear com range enxuto [0, max*(1+pad)].
    - Se permitido e houver grande discrepancia entre minimo positivo e maximo,
      aplica escala logaritmica para melhorar leitura das series menores.
    """
    values = pd.to_numeric(series, errors="coerce").dropna()
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


def _to_month_series(df: pd.DataFrame, label: str, value_col: str) -> pd.DataFrame:
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
    """Garante janela exata dos ultimos N meses para cada serie/escopo."""
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


def _periodo_label(df: pd.DataFrame) -> str:
    """Gera descricao de periodo com base no recorte filtrado."""
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


def _periodo_label_multi(dfs: list[pd.DataFrame]) -> str:
    valid = [d for d in dfs if d is not None and not d.empty]
    if not valid:
        return "Período: Sem dado informado"
    merged = pd.concat(valid, ignore_index=True)
    return _periodo_label(merged)


def render_time_series(
    df_mes_brasil: pd.DataFrame,
    df_mes_rj: pd.DataFrame,
    df_mes_municipio: pd.DataFrame,
    comparacao: str,
    periodo_rapido: str,
    metrica_coluna: str,
    municipio_nome: str,
) -> None:
    """Grafico principal do painel situacional."""
    frames: list[pd.DataFrame] = []
    if comparacao in {"Rio das Ostras x RJ x Brasil", "Rio das Ostras x Brasil"} and not df_mes_brasil.empty:
        frames.append(_to_month_series(df_mes_brasil, "Brasil", metrica_coluna))
    if comparacao in {"Rio das Ostras x RJ x Brasil", "Rio das Ostras x RJ"} and not df_mes_rj.empty:
        frames.append(_to_month_series(df_mes_rj, "Rio de Janeiro", metrica_coluna))
    if not df_mes_municipio.empty:
        frames.append(_to_month_series(df_mes_municipio, municipio_nome, metrica_coluna))

    if not frames:
        st.info("Sem dado informado")
        return

    serie = pd.concat(frames, ignore_index=True)
    months_map = {
        "Ultimos 3 meses": 3,
        "Ultimos 6 meses": 6,
        "Ultimos 12 meses": 12,
    }
    n_months = months_map.get(periodo_rapido)
    if n_months:
        serie = _ensure_last_n_months(serie, n_months=n_months)
    periodo = _periodo_label(serie)
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
        title=f"{titulo}<br><sup>{periodo}</sup>",
    )
    fig.update_traces(
        mode="lines",
        line={"width": 2.8},
        hovertemplate="<b>%{fullData.name}</b><br>Periodo: %{x|%b/%Y}<br>Valor: %{y:,.0f}<extra></extra>",
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
    periodo = _periodo_label(df_municipio_rj)
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
        title=f"Distribuição territorial por município (visão de concentração)<br><sup>{periodo}</sup>",
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
    periodo = _periodo_label(df_municipio_rj)
    fig_cases.update_layout(
        title=f"Municípios com maior número de casos notificados no estado<br><sup>{periodo}</sup>",
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
        hovertemplate="<b>%{y}</b><br>Incidencia: %{x:,.0f}<br>Vs media estadual: %{customdata[0]:+,.0f}<extra></extra>",
    )
    fig_inc.update_layout(
        title=f"Municípios com maior incidência por 100 mil habitantes<br><sup>{periodo}</sup>",
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
                .fillna("Em investigacao")
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

    periodo = _periodo_label(df_status_municipio)
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
            "Em investigacao": "#36566F",
        },
        title=f"Distribuição dos casos por classificação<br><sup>{periodo}</sup>",
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
    label_map = {"M": "Masculino", "F": "Feminino", "I": "Ignorado", "NI": "Nao informado"}
    base = df_sexo.copy()
    base["sexo_label"] = base["sexo"].map(label_map).fillna("Nao informado")
    base["total_casos"] = _as_int(base["total_casos"])
    periodo = _periodo_label(df_sexo)
    fig = px.bar(
        base.sort_values("total_casos", ascending=True),
        x="total_casos",
        y="sexo_label",
        orientation="h",
        title=f"Casos por sexo no município<br><sup>{periodo}</sup>",
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
    periodo = _periodo_label(df_unidade)
    base = df_unidade.copy()
    base["total_casos"] = _as_int(base["total_casos"])
    if "unidade_nome" in base.columns:
        nome = base["unidade_nome"].astype(str).str.strip()
        base["unidade_label"] = np.where(
            nome.fillna("").eq("") | nome.eq(""),
            "Unidade nao identificada",
            nome,
        )
    else:
        base["unidade_label"] = "Unidade nao identificada"
    base = base.sort_values("total_casos", ascending=True)
    fig = px.bar(
        base,
        x="total_casos",
        y="unidade_label",
        orientation="h",
        title=f"Top unidades notificadoras no município<br><sup>{periodo}</sup>",
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
    st.markdown("### Territorio e Risco")
    if df_unidade_mensal.empty:
        st.info("Sem dado informado")
        return

    base = df_unidade_mensal.copy()
    base["total_casos"] = _as_int(base["total_casos"])
    base["mes_ref"] = pd.to_datetime(base["mes_referencia"], errors="coerce").dt.to_period("M")
    nome = base["unidade_nome"].astype(str).str.strip() if "unidade_nome" in base.columns else pd.Series([""] * len(base))
    base["unidade_label"] = np.where(
        nome.fillna("").eq("") | nome.eq(""),
        "Unidade nao identificada",
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

    periodo_df = (
        base.groupby("unidade_label", as_index=False)["total_casos"]
        .sum()
        .rename(columns={"total_casos": "casos_periodo"})
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
        periodo_df.merge(ini_df, on="unidade_label", how="left")
        .merge(fim_df, on="unidade_label", how="left")
        .fillna({"casos_inicio": 0, "casos_fim": 0})
    )
    risco["casos_periodo"] = _as_int(risco["casos_periodo"])
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

    vol_rank = risco["casos_periodo"].rank(pct=True, method="average").fillna(0)
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
    risco = risco.sort_values(["score_risco", "casos_periodo"], ascending=[False, False]).reset_index(drop=True)

    top_pressao = (
        risco.sort_values("casos_periodo", ascending=False).head(top_n).copy().sort_values("casos_periodo", ascending=True)
    )
    top_crescimento = (
        risco[risco["casos_periodo"] > 0]
        .sort_values("crescimento_pct", ascending=False)
        .head(top_n)
        .copy()
        .sort_values("crescimento_pct", ascending=True)
    )

    unidade_pressao = risco.sort_values("casos_periodo", ascending=False).iloc[0]
    unidade_crescimento = risco.sort_values("crescimento_pct", ascending=False).iloc[0]
    unidade_risco = risco.sort_values("score_risco", ascending=False).iloc[0]

    cards_territorio = [
        make_card(
            "Unidade com maior pressao (periodo filtrado)",
            int(unidade_pressao["casos_periodo"]),
            str(unidade_pressao["unidade_label"]),
        ),
        make_card(
            "Maior crescimento no periodo",
            int(round(unidade_crescimento["crescimento_pct"])),
            f"{unidade_crescimento['unidade_label']} | {mes_inicio.strftime('%m/%Y')} -> {mes_fim.strftime('%m/%Y')}",
            suffix="%",
        ),
        make_card(
            "Prioridade de acao (score de risco)",
            int(round(float(unidade_risco["score_risco"]) * 100)),
            f"{unidade_risco['classificacao_risco']} | {unidade_risco['unidade_label']}",
        ),
    ]
    # Regra visual do projeto: cards do painel 2 seguem exatamente o mesmo modelo do painel 1.
    render_kpi_cards(cards_territorio)

    d1, d2 = st.columns(2)
    with d1:
        fig_casos = px.bar(
            top_pressao,
            x="casos_periodo",
            y="unidade_label",
            orientation="h",
            title="Unidades com maior volume de casos",
            color_discrete_sequence=[COLOR_NEUTRAL],
        )
        fig_casos.update_traces(hovertemplate="<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>")
        y_cfg_casos = _adaptive_yaxis(top_pressao["casos_periodo"], pad=0.08)
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
    tabela["casos_periodo"] = _as_int(tabela["casos_periodo"])
    tabela["casos_inicio"] = _as_int(tabela["casos_inicio"])
    tabela["casos_fim"] = _as_int(tabela["casos_fim"])
    tabela["crescimento_pct"] = tabela["crescimento_pct"].round(0).astype(int)
    tabela["score_risco"] = (tabela["score_risco"] * 100).round(0).astype(int)
    tabela = tabela.rename(
        columns={
            "unidade_label": "Unidade",
            "casos_periodo": "Casos (periodo)",
            "casos_inicio": f"Casos inicio ({mes_inicio.strftime('%m/%Y')})",
            "casos_fim": f"Casos fim ({mes_fim.strftime('%m/%Y')})",
            "crescimento_pct": "Crescimento (%)",
            "classificacao_risco": "Classificacao de risco",
            "score_risco": "Score de risco",
        }
    )
    st.markdown("#### Prioridade de acao por unidade")
    st.dataframe(
        tabela[
            [
                "Unidade",
                "Casos (periodo)",
                f"Casos inicio ({mes_inicio.strftime('%m/%Y')})",
                f"Casos fim ({mes_fim.strftime('%m/%Y')})",
                "Crescimento (%)",
                "Classificacao de risco",
                "Score de risco",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    # Comparacao territorial (aplica filtro "Comparacao territorial" tambem no painel 2).
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
                        "Ranking por casos no periodo filtrado",
                    )
                ]
            )
        with c_ctx2:
            render_kpi_cards(
                [
                    make_card(
                        f"Casos de {municipio_foco} ({escopo})",
                        total_foco,
                        "Total no periodo filtrado",
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
            "Municipio para comparar com Rio das Ostras",
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
        diff_txt = f"Diferenca no periodo: {diff_abs:+,}".replace(",", ".")

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
                    f"Casos no periodo ({municipio_foco})",
                    total_foco,
                    diff_txt,
                    delta_pct=float(cresc_foco) if cresc_foco is not None else None,
                ),
                make_card(
                    f"Casos no periodo ({cidade_cmp})",
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
            title=f"Evolucao mensal: {municipio_foco} x {cidade_cmp}",
        )
        fig_cmp.update_traces(
            mode="lines+markers",
            line={"width": 3},
            marker={"size": 6},
            hovertemplate="<b>%{fullData.name}</b><br>Periodo: %{x|%b/%Y}<br>Casos: %{y:,.0f}<extra></extra>",
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
    periodo = _periodo_label(df_mes_municipio)
    fig = px.bar(
        base,
        x="ano",
        y="valor",
        title=f"Comparação anual ({municipio_nome})<br><sup>{periodo}</sup>",
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
