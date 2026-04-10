"""Aplicacao Streamlit do BI de dengue."""

from __future__ import annotations

import base64
from datetime import date
from pathlib import Path
import time
import pandas as pd
import streamlit as st

from app.business_rules import TARGET_MUNICIPIO_NOME, TARGET_UF_CODIGO, TARGET_UF_SIGLA, mask_uf_rj
from app.components.cards import make_card, render_kpi_cards
from app.components.charts import (
    render_classification_donut,
    render_clinico_exames_dashboard,
    render_perfil_epidemiologico,
    render_rankings,
    render_territorio_risco_unidades,
    render_time_séries,
)
from app.components.filters import render_filters_sidebar
from app.data_access import (
    load_clinico_exames_registros_municipio,
    load_comorbidades_mensal_municipio,
    get_classificacao_tuple,
    load_casos_ano,
    load_casos_municipio_ano_brasil_enriquecido,
    load_casos_mes_municipio,
    load_casos_mes_uf,
    load_casos_municipio_ano_rj_enriquecido,
    load_internacoes_mensal_municipio,
    load_populacao_refs,
    load_perfil_demografico_mensal_municipio,
    load_perfil_demografico_mensal_escopo,
    load_perfil_top_municipio,
    load_semana_epidemiologica_media_municipio,
    load_sexo_municipio,
    load_status_municipio,
    load_unidade_notificadora_mensal_municipio,
    load_unidade_notificadora_municipio,
)
from app.theme import (
    BORDER_SOFT,
    BRAND_BLUE,
    BRAND_HIGHLIGHT,
    FONT_FAMILY,
    SURFACE_BG,
    SURFACE_CARD,
    TEXT_PRIMARY,
)


st.set_page_config(page_title="Monitoramento da Dengue | Rio das Ostras - RJ", page_icon=":bar_chart:", layout="wide")

CONFIRM_CODES = {"1", "3", "4", "5", "6", "10", "11", "12"}
SLOW_LOADING_SECONDS = 60


def _image_data_uri(image_path: Path) -> str | None:
    """Converte imagem para data URI."""
    if not image_path.exists():
        return None
    try:
        encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return f"data:image/png;base64,{encoded}"
    except OSError:
        return None


def _logo_data_uri() -> str | None:
    """Converte logo oficial para data URI (uso no overlay)."""
    logo_path = Path("assets/branding/Logo_prefeitura-RO/Logo_PMRO_Slogan_azul.png")
    return _image_data_uri(logo_path)


def _start_loading_overlay(blocking: bool = True) -> dict[str, st.delta_generator.DeltaGenerator | int | float]:
    """Mostra carregamento com barra; overlay modal opcional."""
    overlay = st.empty()
    progress_slot = st.empty()

    logo_uri = _logo_data_uri()
    logo_html = (
        f'<img src="{logo_uri}" class="pmro-loading-logo" alt="Prefeitura Rio das Ostras"/>' if logo_uri else ""
    )
    if blocking:
        overlay.markdown(
            f"""
            <style>
            .pmro-loading-overlay {{
                position: fixed;
                inset: 0;
                background: rgba(7, 20, 33, 0.30);
                z-index: 9998;
                display: flex;
                align-items: center;
                justify-content: center;
                pointer-events: none;
            }}
            .pmro-loading-card {{
                background: rgba(255, 255, 255, 0.92);
                border: 1px solid {BORDER_SOFT};
                border-radius: 16px;
                box-shadow: 0 12px 30px rgba(15, 47, 75, 0.2);
                min-width: 250px;
                max-width: 88vw;
                text-align: center;
                padding: 14px 16px;
            }}
            .pmro-loading-logo {{
                width: 180px;
                max-width: 62vw;
                animation: pmroPulse 1.3s ease-in-out infinite;
                margin-bottom: 6px;
            }}
            .pmro-loading-title {{
                color: {TEXT_PRIMARY};
                font-size: 0.92rem;
                font-weight: 700;
                margin: 2px 0 0 0;
            }}
            @keyframes pmroPulse {{
                0% {{ opacity: 0.55; transform: scale(0.98); }}
                50% {{ opacity: 1; transform: scale(1.0); }}
                100% {{ opacity: 0.55; transform: scale(0.98); }}
            }}
            </style>
            <div class="pmro-loading-overlay">
                <div class="pmro-loading-card">
                    {logo_html}
                    <p class="pmro-loading-title">Carregando painel oficial</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        overlay.empty()
    progress_bar = progress_slot.progress(0, text="Carregando dados... 0%")
    now = time.perf_counter()
    return {
        "overlay": overlay,
        "progress_slot": progress_slot,
        "progress_bar": progress_bar,
        "last_pct": 0,
        "last_update_ts": now,
    }


def _update_loading_overlay(
    loading_refs: dict[str, st.delta_generator.DeltaGenerator | int | float] | None,
    step: int,
    total_steps: int,
    label: str,
    started_at: float | None = None,
) -> None:
    """Atualiza percentual e texto da barra de carregamento."""
    if not loading_refs:
        return
    pct = int(round((step / max(total_steps, 1)) * 100))
    pct = max(0, min(100, pct))
    now = time.perf_counter()
    last_pct = int(loading_refs.get("last_pct", 0))
    last_update_ts = float(loading_refs.get("last_update_ts", now))
    if pct != last_pct:
        loading_refs["last_pct"] = pct
        loading_refs["last_update_ts"] = now
        last_update_ts = now
    texto = f"{label} ({pct}%)"
    if started_at is not None and pct < 100:
        sem_atualizacao = now - last_update_ts
        if sem_atualizacao >= SLOW_LOADING_SECONDS:
            texto = f"{texto} | Demorando mais que o esperado..."
    loading_refs["progress_bar"].progress(pct, text=texto)


def _finish_loading_overlay(loading_refs: dict[str, st.delta_generator.DeltaGenerator | int | float] | None) -> None:
    """Remove overlay ao final da carga."""
    if not loading_refs:
        return
    try:
        loading_refs["progress_bar"].empty()
    except Exception:
        pass
    loading_refs["progress_slot"].empty()
    loading_refs["overlay"].empty()


def _filters_signature(filtros: dict[str, object]) -> tuple:
    """Assinatura hashable dos filtros relevantes para detectar recarga por mudanca."""
    return (
        str(filtros.get("período_rapido", "")),
        tuple(sorted(int(v) for v in filtros.get("anos", []) or [])),
        tuple(sorted(int(v) for v in filtros.get("meses", []) or [])),
        str(filtros.get("comparacao", "")),
        str(filtros.get("metrica_visual", "")),
        str(filtros.get("estado", "")),
        str(filtros.get("municipio", "")),
        tuple(sorted(str(v) for v in filtros.get("classificacao", []) or [])),
        bool(filtros.get("carregar_histórico_completo", False)),
        int(filtros.get("top_n", 10) or 10),
        str(filtros.get("secao", st.session_state.get("filtro_secao", ""))),
    )


def _inject_mobile_first_styles() -> None:
    """Estilos globais mobile-first para o painel Streamlit."""
    st.markdown(
        f"""
        <style>
        :root {{
            --pmro-blue: {BRAND_BLUE};
            --pmro-highlight: {BRAND_HIGHLIGHT};
            --pmro-bg: {SURFACE_BG};
            --pmro-card: {SURFACE_CARD};
            --pmro-border: {BORDER_SOFT};
            --pmro-text: {TEXT_PRIMARY};
            --pmro-font: {FONT_FAMILY};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        /* Base mobile-first */
        .stApp {
            font-size: 15px;
            font-family: var(--pmro-font);
            background-color: var(--pmro-bg);
            color: var(--pmro-text);
        }
        .block-container {
            padding-top: 0.8rem;
            padding-bottom: 1rem;
            padding-left: 0.7rem;
            padding-right: 0.7rem;
            max-width: 100%;
        }
        h1, h2, h3 {
            line-height: 1.2;
            color: var(--pmro-text);
        }
        h1 {
            font-size: 1.55rem;
        }
        h2 {
            font-size: 1.25rem;
        }
        h3 {
            font-size: 1.08rem;
        }
        .stSidebar .block-container {
            padding-top: 1rem;
            padding-left: 0.6rem;
            padding-right: 0.6rem;
        }
        [data-testid="stSidebar"] {
            background: #eef3f8;
            border-right: 1px solid var(--pmro-border);
        }
        [data-testid="stAlert"] {
            border-radius: 12px;
            border: 1px solid var(--pmro-border);
        }
        .pmro-header-block {
            margin: 0;
        }
        .pmro-sticky-anchor {
            display: block;
            width: 1px;
            height: 1px;
            margin: 0;
            padding: 0;
        }
        div[data-testid="stVerticalBlock"] > div:has(.pmro-sticky-anchor) {
            position: sticky;
            top: 0.35rem;
            z-index: 1000;
            background: var(--pmro-bg);
            padding-top: 0.1rem;
            padding-bottom: 0.15rem;
        }
        .pmro-header-logo-wrap {
            display: flex;
            align-items: center;
            justify-content: flex-start;
            margin: 0;
            padding: 0;
        }
        .pmro-header-logo {
            width: clamp(120px, 12vw, 185px);
            max-width: 100%;
            height: auto;
            display: block;
        }
        .pmro-header-right-logos {
            display: flex;
            align-items: center;
            justify-content: flex-start;
            gap: 0.28rem;
            width: 100%;
            flex-wrap: nowrap;
            overflow: visible;
            padding-left: 0.08rem;
            padding-right: 0;
            padding-bottom: 0;
        }
        .pmro-header-right-logo {
            display: block;
            width: auto;
            height: auto;
            max-height: 58px;
            max-width: 42%;
            opacity: 0.98;
            object-fit: contain;
            flex: 0 1 auto;
        }
        .pmro-header-right-logo-govtic {
            width: auto;
            height: auto;
            max-height: 64px;
            max-width: 56%;
            filter: drop-shadow(0 2px 6px rgba(15, 47, 75, 0.16));
            opacity: 1;
            object-fit: contain;
        }
        .pmro-header-title {
            margin: 0;
            line-height: 1.15;
            font-weight: 700;
            font-size: 1.15rem;
            color: var(--pmro-text);
        }
        .pmro-header-subtitle {
            margin: 0.16rem 0 0 0;
            color: #5f7287;
            font-size: 0.88rem;
            line-height: 1.2;
        }
        [data-testid="stSegmentedControl"] {
            margin-top: 0.28rem;
            margin-bottom: 0;
            width: fit-content;
            max-width: 100%;
        }
        [data-testid="stSegmentedControl"] + div {
            margin-top: 0;
        }
        [data-testid="stSegmentedControl"] > div {
            width: fit-content;
            max-width: 100%;
        }
        [data-testid="stSegmentedControl"] [role="radiogroup"] {
            width: fit-content;
            max-width: 100%;
        }
        /* Fallback para radio horizontal */
        div[role="radiogroup"] {
            width: fit-content;
            max-width: 100%;
        }
        [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div,
        .stTextInput input {
            border-color: var(--pmro-border);
        }
        .stMultiSelect [data-baseweb="tag"] {
            background-color: var(--pmro-highlight) !important;
            border: 1px solid var(--pmro-highlight) !important;
            color: #102a43 !important;
        }
        .stMultiSelect [data-baseweb="tag"] span {
            color: #102a43 !important;
            font-weight: 700;
        }
        .stMultiSelect [data-baseweb="tag"] svg {
            color: #102a43 !important;
        }
        [data-baseweb="radio"] input:checked + div,
        [data-baseweb="checkbox"] input:checked + div {
            background-color: var(--pmro-highlight) !important;
            border-color: var(--pmro-highlight) !important;
        }
        [data-baseweb="radio"] input:focus + div,
        [data-baseweb="checkbox"] input:focus + div,
        .stSelectbox [data-baseweb="select"] > div:focus-within,
        .stMultiSelect [data-baseweb="select"] > div:focus-within {
            box-shadow: 0 0 0 2px rgba(223, 162, 48, 0.25) !important;
            border-color: var(--pmro-highlight) !important;
        }
        /* Plotly container */
        [data-testid="stPlotlyChart"] {
            border-radius: 10px;
            overflow: hidden;
            background: var(--pmro-card);
            border: 1px solid var(--pmro-border);
        }
        /* Desktop/tablet refinements */
        @media (min-width: 768px) {
            .block-container {
                padding-top: 1rem;
                padding-bottom: 1.4rem;
                padding-left: 1.2rem;
                padding-right: 1.2rem;
            }
            .pmro-header-title { font-size: 1.35rem; }
            .pmro-header-subtitle { font-size: 0.95rem; }
            h1 { font-size: 1.9rem; }
            h2 { font-size: 1.45rem; }
            h3 { font-size: 1.18rem; }
            .pmro-header-right-logos {
                justify-content: flex-start;
                gap: 0.28rem;
                overflow: visible;
                padding-bottom: 0;
            }
            .pmro-header-right-logo {
                max-height: 56px;
                max-width: 42%;
            }
            .pmro-header-right-logo-govtic {
                max-height: 62px;
                max-width: 56%;
            }
        }
        @media (min-width: 992px) {
            .pmro-header-right-logos {
                gap: 0.25rem;
            }
            .pmro-header-right-logo {
                max-height: 60px;
                max-width: 42%;
            }
            .pmro-header-right-logo-govtic {
                max-height: 68px;
                max-width: 56%;
            }
        }
        @media (min-width: 1280px) {
            .pmro-header-right-logos { gap: 0.24rem; }
            .pmro-header-right-logo {
                max-height: 64px;
                max-width: 42%;
            }
            .pmro-header-right-logo-govtic {
                max-height: 72px;
                max-width: 56%;
            }
        }
        @media (max-width: 767px) {
            div[data-testid="stVerticalBlock"] > div:has(.pmro-sticky-anchor) {
                top: 0.2rem;
            }
            [data-testid="stSegmentedControl"],
            [data-testid="stSegmentedControl"] > div,
            [data-testid="stSegmentedControl"] [role="radiogroup"],
            div[role="radiogroup"] {
                width: 100%;
            }
            .pmro-header-right-logos {
                justify-content: center;
                gap: 0.2rem;
                overflow-x: visible;
                padding-bottom: 1px;
            }
            .pmro-header-right-logo {
                max-height: 42px;
                max-width: 44%;
            }
            .pmro-header-right-logo-govtic {
                max-height: 48px;
                max-width: 56%;
            }
        }
        @media (min-width: 1200px) {
            .block-container {
                padding-left: 1.6rem;
                padding-right: 1.6rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_portal_header() -> str:
    """Cabecalho institucional do painel oficial com navegacao global."""
    logo_path = Path("assets/branding/Logo_prefeitura-RO/Logo_PMRO.png")
    logo_uri = _image_data_uri(logo_path)
    govtic_path = Path("assets/branding/parceiros/logo_govtic.png")
    govtic_uri = _image_data_uri(govtic_path)
    digital_path = Path("assets/branding/parceiros/logo_rio_das_ostras_digital.png")
    digital_uri = _image_data_uri(digital_path)
    with st.container(border=True):
        st.markdown('<span class="pmro-sticky-anchor"></span>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([0.82, 2.88, 2.30], vertical_alignment="center", gap="small")
        with c1:
            if logo_uri:
                st.markdown(
                    f"""
                    <div class="pmro-header-logo-wrap">
                        <img src="{logo_uri}" class="pmro-header-logo" alt="Prefeitura Municipal de Rio das Ostras"/>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            elif logo_path.exists():
                st.image(str(logo_path), width=160)
        with c2:
            st.markdown(
                """
                <div class="pmro-header-block">
                    <p class="pmro-header-title">Prefeitura Municipal de Rio das Ostras</p>
                    <p class="pmro-header-subtitle">Secretaria Municipal de Governança e Transformação Digital</p>
                    <p class="pmro-header-subtitle"><strong>Painel Oficial de Monitoramento da Dengue</strong></p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            secao = _render_top_menu(show_title=False)
        with c3:
            logos_right: list[str] = []
            if govtic_uri:
                logos_right.append(
                    f'<img src="{govtic_uri}" class="pmro-header-right-logo pmro-header-right-logo-govtic" alt="GOVTIC"/>'
                )
            if digital_uri:
                logos_right.append(
                    f'<img src="{digital_uri}" class="pmro-header-right-logo" alt="Rio das Ostras Digital"/>'
                )
            if logos_right:
                st.markdown(
                    f'<div class="pmro-header-right-logos">{"".join(logos_right)}</div>',
                    unsafe_allow_html=True,
                )
    return secao


def _render_portal_footer() -> None:
    """Rodape com fontes oficiais utilizadas no painel."""
    with st.container(border=True):
        with st.expander("Fontes de dados", expanded=False):
            st.caption(
                "Ministerio da Saude (SINAN - Dengue): "
                "https://dadosabertos.saude.gov.br/dataset/arboviroses-dengue"
            )
            st.caption(
                "CNES / DATASUS (Base de estabelecimentos): "
                "https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp"
            )
            st.caption("IBGE (Base territorial e populacional): acervo interno da Prefeitura, origem Censo 2022 / IBGE.")
            st.caption("Manual da marca Prefeitura de Rio das Ostras: arquivo institucional interno (PDF de referencia visual).")
            st.caption("Ambiente analitico local: PostgreSQL schema `saude`.")


def _render_top_menu(show_title: bool = True) -> str:
    """Navegacao principal em botoes no cabecalho."""
    options = [
        "Situação Geral",
        "Território e Risco",
        "Perfil dos Casos",
        "Clínico e Exames",
        "Avaliação",
    ]
    if show_title:
        st.markdown("##### Menu")
    if hasattr(st, "segmented_control"):
        selected = st.segmented_control(
            "Secao do painel",
            options=options,
            key="filtro_secao",
            label_visibility="collapsed",
            selection_mode="single",
        )
        return selected or st.session_state.get("filtro_secao", "Situação Geral")
    selected = st.radio(
        "Secao do painel",
        options=options,
        key="filtro_secao",
        horizontal=True,
        label_visibility="collapsed",
    )
    return selected or st.session_state.get("filtro_secao", "Situação Geral")


def _filtrar_df(df: pd.DataFrame, anos: list[int]) -> pd.DataFrame:
    if df.empty or not anos or "ano" not in df.columns:
        return df
    return df[df["ano"].isin(anos)].copy()


def _filtrar_por_meses(df: pd.DataFrame, meses: list[int]) -> pd.DataFrame:
    if df.empty or not meses or "mes_referencia" not in df.columns:
        return df
    out = df.copy()
    return out[out["mes_referencia"].dt.month.isin(meses)]


def _aplicar_período_rapido(df: pd.DataFrame, período_rapido: str) -> pd.DataFrame:
    if df.empty or "mes_referencia" not in df.columns or período_rapido == "Personalizado (ano/mês)":
        return df
    ref = pd.to_datetime(df["mes_referencia"]).max()
    if pd.isna(ref):
        return df

    if período_rapido == "Últimos 3 meses":
        inicio = ref - pd.DateOffset(months=2)
    elif período_rapido == "Últimos 6 meses":
        inicio = ref - pd.DateOffset(months=5)
    elif período_rapido == "Últimos 12 meses":
        inicio = ref - pd.DateOffset(months=11)
    else:
        inicio = pd.Timestamp(year=ref.year, month=1, day=1)
    return df[df["mes_referencia"] >= inicio].copy()


def _delta_pct(atual: float | int | None, anterior: float | int | None) -> float | None:
    if atual is None or anterior is None or anterior == 0:
        return None
    return ((float(atual) - float(anterior)) / float(anterior)) * 100


def _sum_mes(df: pd.DataFrame, mes: pd.Period, col: str) -> int:
    if df.empty:
        return 0
    return int(df[df["mes_referencia"].dt.to_period("M") == mes][col].sum())


def _start_date_for_fast_window(
    período_rapido: str,
    anos_selecionados: list[int],
    carregar_histórico_completo: bool,
    referencia_base: pd.Timestamp | None = None,
) -> str | None:
    """Define janela de dados carregada conforme estrategia progressiva de cache."""
    ref_ts = pd.Timestamp(referencia_base) if referencia_base is not None and not pd.isna(referencia_base) else pd.Timestamp(date.today())
    first_day_current_month = pd.Timestamp(year=ref_ts.year, month=ref_ts.month, day=1)
    if período_rapido == "Últimos 3 meses":
        return (first_day_current_month - pd.DateOffset(months=2)).date().isoformat()
    if período_rapido == "Últimos 6 meses":
        return (first_day_current_month - pd.DateOffset(months=5)).date().isoformat()
    if período_rapido == "Últimos 12 meses":
        return (first_day_current_month - pd.DateOffset(months=11)).date().isoformat()
    if período_rapido == "Ano atual":
        return date(ref_ts.year, 1, 1).isoformat()

    # Personalizado: se no houver ano selecionado, carrega apenas uUltimos 5 anos por padrao.
    if anos_selecionados:
        return date(min(anos_selecionados), 1, 1).isoformat()
    if carregar_histórico_completo:
        return None
    return date(ref_ts.year - 4, 1, 1).isoformat()


def _preload_next_windows(
    classificacao_tuple: tuple[str, ...],
    municipio_foco: str,
    top_n: int,
) -> bool:
    """Preaquece cache em etapas: 6m -> 12m -> 5 anos."""
    stage = int(st.session_state.get("preload_stage", 0))
    if stage == 0:
        start = (pd.Timestamp(date.today().replace(day=1)) - pd.DateOffset(months=5)).date().isoformat()
    elif stage == 1:
        start = date(date.today().year, 1, 1).isoformat()
    elif stage == 2:
        start = date(date.today().year - 4, 1, 1).isoformat()
    else:
        return False

    load_casos_mes_uf(classificacoes=classificacao_tuple, data_inicio=start)
    load_casos_municipio_ano_rj_enriquecido(classificacoes=classificacao_tuple, data_inicio=start)
    load_casos_mes_municipio(municipio_nome=municipio_foco, classificacoes=classificacao_tuple, data_inicio=start)
    load_status_municipio(municipio_nome=municipio_foco, classificacoes=classificacao_tuple, data_inicio=start)
    load_internacoes_mensal_municipio(
        municipio_nome=municipio_foco,
        classificacoes=classificacao_tuple,
        data_inicio=start,
    )
    load_sexo_municipio(municipio_nome=municipio_foco, classificacoes=classificacao_tuple, data_inicio=start)
    load_unidade_notificadora_municipio(
        municipio_nome=municipio_foco,
        classificacoes=classificacao_tuple,
        top_n=top_n,
        data_inicio=start,
    )
    load_unidade_notificadora_mensal_municipio(
        municipio_nome=municipio_foco,
        classificacoes=classificacao_tuple,
        data_inicio=start,
    )
    st.session_state["preload_stage"] = stage + 1
    return True


def _build_cards_período(
    df_mes_municipio: pd.DataFrame,
    df_status_municipio: pd.DataFrame,
    df_internacoes_mensal: pd.DataFrame,
    pop_municipio: int | None,
) -> list[dict[str, str | float | int | None]]:
    total_notificados = int(df_mes_municipio["total_casos"].sum()) if not df_mes_municipio.empty else 0
    total_confirmados = int(
        df_status_municipio.loc[df_status_municipio["classificacao_final"].isin(CONFIRM_CODES), "total_casos"].sum()
    )
    total_descartados = int(df_status_municipio.loc[df_status_municipio["classificacao_final"] == "2", "total_casos"].sum())
    total_inconclusivos = int(df_status_municipio.loc[df_status_municipio["classificacao_final"] == "8", "total_casos"].sum())
    total_obitos = int(df_status_municipio.loc[df_status_municipio["evolucao_caso"] == "2", "total_casos"].sum())
    total_internacoes = int(df_internacoes_mensal["internacoes"].sum()) if not df_internacoes_mensal.empty else None
    total_investig = max(total_notificados - total_confirmados - total_descartados - total_inconclusivos, 0)
    incidencia = (total_confirmados / pop_municipio * 100000) if pop_municipio and pop_municipio > 0 else None
    letalidade = (total_obitos / total_confirmados * 100) if total_confirmados > 0 else None

    return [
        make_card("Casos confirmados", total_confirmados, "Total no período filtrado"),
        make_card("Casos notificados", total_notificados, "Total no período filtrado"),
        make_card("Óbitos", total_obitos, "Evolução do caso = óbito pelo agravo"),
        make_card("Internações", total_internacoes, "Hospitalização = sim"),
        make_card("Incidência", incidencia, "Por 100 mil habitantes", absolute=False),
        make_card("Letalidade", letalidade, "Percentual sobre casos confirmados", suffix="%", absolute=False),
        make_card("Casos em investigação", total_investig, "Notificados sem encerramento conclusivo"),
    ]


def _build_cards_ultimo_mes(
    df_mes_municipio: pd.DataFrame,
    df_status_municipio: pd.DataFrame,
    df_internacoes_mensal: pd.DataFrame,
) -> list[dict[str, str | float | int | None]]:
    if df_mes_municipio.empty:
        return [
            make_card("Confirmados (último mês)", None, "Sem dado informado"),
            make_card("Notificados (último mês)", None, "Sem dado informado"),
            make_card("Óbitos (último mês)", None, "Sem dado informado"),
            make_card("Internações (último mês)", None, "Sem dado informado"),
            make_card("Em investigação (último mês)", None, "Sem dado informado"),
        ]

    ref = df_mes_municipio["mes_referencia"].max().to_period("M")
    ant = (ref.to_timestamp() - pd.DateOffset(months=1)).to_period("M")

    conf_ref = _sum_mes(df_status_municipio[df_status_municipio["classificacao_final"].isin(CONFIRM_CODES)], ref, "total_casos")
    conf_ant = _sum_mes(df_status_municipio[df_status_municipio["classificacao_final"].isin(CONFIRM_CODES)], ant, "total_casos")
    not_ref = _sum_mes(df_mes_municipio, ref, "total_casos")
    not_ant = _sum_mes(df_mes_municipio, ant, "total_casos")
    ob_ref = _sum_mes(df_status_municipio[df_status_municipio["evolucao_caso"] == "2"], ref, "total_casos")
    ob_ant = _sum_mes(df_status_municipio[df_status_municipio["evolucao_caso"] == "2"], ant, "total_casos")
    int_ref = _sum_mes(df_internacoes_mensal, ref, "internacoes") if not df_internacoes_mensal.empty else None
    int_ant = _sum_mes(df_internacoes_mensal, ant, "internacoes") if not df_internacoes_mensal.empty else None
    inv_ref = max(not_ref - conf_ref, 0)
    inv_ant = max(not_ant - conf_ant, 0)

    return [
        make_card("Confirmados (último mês)", conf_ref, "Comparado ao mês anterior", _delta_pct(conf_ref, conf_ant)),
        make_card("Notificados (último mês)", not_ref, "Comparado ao mês anterior", _delta_pct(not_ref, not_ant)),
        make_card("Óbitos (último mês)", ob_ref, "Comparado ao mês anterior", _delta_pct(ob_ref, ob_ant)),
        make_card("Internações (último mês)", int_ref, "Comparado ao mês anterior", _delta_pct(int_ref, int_ant)),
        make_card("Em investigação (último mês)", inv_ref, "Comparado ao mês anterior", _delta_pct(inv_ref, inv_ant)),
    ]


def _build_scope_frames(
    df_mes_filtrado: pd.DataFrame,
    df_mes_municipio: pd.DataFrame,
    pop_brasil: int | None,
    pop_rj: int | None,
    pop_municipio: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    br = (
        df_mes_filtrado.groupby("mes_referencia", as_index=False)["total_casos"].sum().assign(
            incidencia_100k=lambda d: (d["total_casos"] / pop_brasil * 100000) if pop_brasil else None
        )
        if not df_mes_filtrado.empty
        else pd.DataFrame(columns=["mes_referencia", "total_casos", "incidencia_100k"])
    )
    rj_df = df_mes_filtrado[mask_uf_rj(df_mes_filtrado["uf"])] if not df_mes_filtrado.empty else df_mes_filtrado
    rj = (
        rj_df.groupby("mes_referencia", as_index=False)["total_casos"].sum().assign(
            incidencia_100k=lambda d: (d["total_casos"] / pop_rj * 100000) if pop_rj else None
        )
        if not rj_df.empty
        else pd.DataFrame(columns=["mes_referencia", "total_casos", "incidencia_100k"])
    )
    mun = (
        df_mes_municipio.groupby("mes_referencia", as_index=False)["total_casos"].sum().assign(
            incidencia_100k=lambda d: (d["total_casos"] / pop_municipio * 100000) if pop_municipio else None
        )
        if not df_mes_municipio.empty
        else pd.DataFrame(columns=["mes_referencia", "total_casos", "incidencia_100k"])
    )
    return br, rj, mun


def _mes_ano_descritivo(ts: pd.Timestamp | None) -> str:
    if ts is None or pd.isna(ts):
        return "-"
    meses = {
        1: "janeiro",
        2: "fevereiro",
        3: "março",
        4: "abril",
        5: "maio",
        6: "junho",
        7: "julho",
        8: "agosto",
        9: "setembro",
        10: "outubro",
        11: "novembro",
        12: "dezembro",
    }
    return f"{meses.get(int(ts.month), str(ts.month))}/{int(ts.year)}"


def _render_dashboard_situacao_geral(
    df_mes_filtrado: pd.DataFrame,
    df_municipio_rj_filtrado: pd.DataFrame,
    df_municipio_br_filtrado: pd.DataFrame,
    df_mes_municipio: pd.DataFrame,
    df_status_municipio: pd.DataFrame,
    df_internacoes_mensal: pd.DataFrame,
    df_sexo_municipio: pd.DataFrame,
    df_unidade_municipio: pd.DataFrame,
    df_perfil_top: pd.DataFrame,
    semana_epi_media: int | None,
    comparacao: str,
    período_rapido: str,
    metrica_visual: str,
    municipio_foco: str,
    top_n: int,
    pop_refs: pd.DataFrame,
) -> None:
    pop_brasil = int(pop_refs.iloc[0]["pop_brasil"]) if not pop_refs.empty and pd.notna(pop_refs.iloc[0]["pop_brasil"]) else None
    pop_rj = int(pop_refs.iloc[0]["pop_rj"]) if not pop_refs.empty and pd.notna(pop_refs.iloc[0]["pop_rj"]) else None
    pop_municipio = int(pop_refs.iloc[0]["pop_municipio"]) if not pop_refs.empty and pd.notna(pop_refs.iloc[0]["pop_municipio"]) else None

    st.markdown("### Situação Geral da Dengue")
    render_kpi_cards(
        _build_cards_período(
            df_mes_municipio=df_mes_municipio,
            df_status_municipio=df_status_municipio,
            df_internacoes_mensal=df_internacoes_mensal,
            pop_municipio=pop_municipio,
        )
    )

    mes_ref_txt = _mes_ano_descritivo(df_mes_municipio["mes_referencia"].max() if not df_mes_municipio.empty else None)
    st.markdown(f"#### Dados do último mês ({mes_ref_txt})")
    render_kpi_cards(
        _build_cards_ultimo_mes(
            df_mes_municipio=df_mes_municipio,
            df_status_municipio=df_status_municipio,
            df_internacoes_mensal=df_internacoes_mensal,
        )
    )

    metric_col = "total_casos" if metrica_visual == "Casos totais" else "incidencia_100k"
    df_br, df_rj, df_mun = _build_scope_frames(
        df_mes_filtrado=df_mes_filtrado,
        df_mes_municipio=df_mes_municipio,
        pop_brasil=pop_brasil,
        pop_rj=pop_rj,
        pop_municipio=pop_municipio,
    )
    render_time_séries(
        df_mes_brasil=df_br,
        df_mes_rj=df_rj,
        df_mes_municipio=df_mun,
        comparacao=comparacao,
        período_rapido=período_rapido,
        metrica_coluna=metric_col,
        municipio_nome=municipio_foco,
    )

    def _build_posicao_card(df_base: pd.DataFrame, escopo_label: str) -> dict[str, str | float | int | None]:
        if df_base.empty:
            return make_card(f"Posição no {escopo_label}", None, "Sem dado informado")
        ranking = (
            df_base.groupby("municipio_nome", as_index=False)["total_casos"]
            .sum()
            .sort_values("total_casos", ascending=False)
            .reset_index(drop=True)
        )
        ranking["posicao"] = ranking["total_casos"].rank(method="min", ascending=False).astype(int)
        mask = ranking["municipio_nome"].astype(str).str.upper().eq(municipio_foco.upper())
        if not mask.any():
            return make_card(f"Posição no {escopo_label}", None, "Sem dado informado")
        row = ranking.loc[mask].iloc[0]
        pos = int(row["posicao"])
        total = int(ranking["municipio_nome"].nunique())
        return make_card(
            f"Posição no {escopo_label}",
            pos,
            f"{municipio_foco} está na posição {pos} de {total} municípios",
        )

    c_donut, c_pos = st.columns(2)
    with c_donut:
        render_classification_donut(df_status_municipio=df_status_municipio)
    with c_pos:
        def _perfil_row(campo: str) -> tuple[int | None, str]:
            if df_perfil_top.empty:
                return None, "Sem dado informado"
            row = df_perfil_top[df_perfil_top["campo"] == campo]
            if row.empty:
                return None, "Sem dado informado"
            valor_raw = row.iloc[0].get("valor_raw")
            total = row.iloc[0].get("total_casos")
            total = int(total) if pd.notna(total) else None
            if valor_raw is None or (isinstance(valor_raw, float) and pd.isna(valor_raw)):
                return total, "Sem dado informado"
            raw = str(valor_raw).strip()

            if campo == "idade":
                digits = "".join(ch for ch in raw if ch.isdigit())
                if len(digits) >= 2:
                    unidade = digits[0]
                    numero = int(digits[1:] or "0")
                    map_u = {"1": "hora(s)", "2": "dia(s)", "3": "mes(es)", "4": "ano(s)"}
                    return total, f"{numero} {map_u.get(unidade, 'unidade')}"
                return total, raw

            if campo == "gestante":
                gest_map = {
                    "1": "1o trimestre",
                    "2": "2o trimestre",
                    "3": "3o trimestre",
                    "4": "idade gestacional ignorada",
                    "5": "no",
                    "6": "no se aplica",
                    "9": "ignorado",
                    "NI": "no informado",
                }
                return total, gest_map.get(raw, raw)

            if campo == "raca":
                raca_map = {
                    "1": "branca",
                    "2": "preta",
                    "3": "amarela",
                    "4": "parda",
                    "5": "indigena",
                    "9": "ignorado",
                    "NI": "no informado",
                }
                return total, raca_map.get(raw, raw)

            if campo == "escolaridade":
                d = "".join(ch for ch in raw if ch.isdigit())
                esc_map = {
                    "0": "analfabeto",
                    "00": "analfabeto",
                    "1": "1a a 4a serie incompleta EF",
                    "01": "1a a 4a serie incompleta EF",
                    "2": "4a serie completa EF",
                    "02": "4a serie completa EF",
                    "3": "5a a 8a serie incompleta EF",
                    "03": "5a a 8a serie incompleta EF",
                    "4": "fundamental completo",
                    "04": "fundamental completo",
                    "5": "medio incompleto",
                    "05": "medio incompleto",
                    "6": "medio completo",
                    "06": "medio completo",
                    "7": "superior incompleta",
                    "07": "superior incompleta",
                    "8": "superior completa",
                    "08": "superior completa",
                    "9": "ignorado",
                    "09": "ignorado",
                    "10": "no se aplica",
                    "99": "ignorado",
                    "NI": "no informado",
                }
                key = d if d else raw
                return total, esc_map.get(key, raw)

            return total, raw

        unidade_top_nome = None
        unidade_top_casos = None
        sexo_top_nome = None
        sexo_top_casos = None
        if not df_unidade_municipio.empty:
            top_row = df_unidade_municipio.sort_values("total_casos", ascending=False).iloc[0]
            nome = str(top_row.get("unidade_nome", "")).strip() if "unidade_nome" in df_unidade_municipio.columns else ""
            unidade_top_nome = nome if nome else "Unidade no identificada"
            unidade_top_casos = int(top_row.get("total_casos", 0))
        if not df_sexo_municipio.empty:
            sexo_map = {"F": "Feminino", "M": "Masculino", "I": "Ignorado", "NI": "Não informado"}
            top_sexo = df_sexo_municipio.sort_values("total_casos", ascending=False).iloc[0]
            sexo_raw = str(top_sexo.get("sexo", "NI")).strip().upper()
            sexo_top_nome = sexo_map.get(sexo_raw, sexo_raw if sexo_raw else "Não informado")
            sexo_top_casos = int(top_sexo.get("total_casos", 0))

        idade_top_casos, idade_top_txt = _perfil_row("idade")
        gest_top_casos, gest_top_txt = _perfil_row("gestante")
        raca_top_casos, raca_top_txt = _perfil_row("raca")
        esc_top_casos, esc_top_txt = _perfil_row("escolaridade")

        def _fmt_abs(v: int | float | None) -> str:
            if v is None:
                return "-"
            try:
                return f"{int(round(float(v))):,}".replace(",", ".")
            except (TypeError, ValueError):
                return "-"

        def _build_text_focus_card(label: str, destaque: str | None, quantidade: int | float | None) -> dict[str, object]:
            txt = str(destaque).strip() if destaque is not None else ""
            if not txt or txt.lower() in {"nan", "none", "-"}:
                txt = "Sem dado informado"
            return {
                "label": label,
                "value": txt,
                "value_mode": "text",
                "delta_text": "-",
                "delta_pct": None,
                "help": f"Casos: {_fmt_abs(quantidade)}",
            }

        render_kpi_cards(
            [
                _build_posicao_card(df_municipio_rj_filtrado, "Estado (RJ)"),
                _build_posicao_card(df_municipio_br_filtrado, "Brasil"),
                make_card(
                    "Semana epidemiológica média",
                    semana_epi_media,
                    "Média da semana epidemiológica no período filtrado",
                ),
                _build_text_focus_card("Unidade que mais notificou", (unidade_top_nome or "Sem dado informado"), unidade_top_casos),
                _build_text_focus_card("Sexo mais notificado", (sexo_top_nome or "Sem dado informado"), sexo_top_casos),
                _build_text_focus_card("Idade mais notificada", idade_top_txt, idade_top_casos),
                _build_text_focus_card("Gestante (situacao mais frequente)", gest_top_txt, gest_top_casos),
                _build_text_focus_card("Raca/Cor mais notificada", raca_top_txt, raca_top_casos),
                _build_text_focus_card("Escolaridade mais notificada", esc_top_txt, esc_top_casos),
            ]
        )

    render_rankings(df_municipio_rj=df_municipio_rj_filtrado, municipio_foco=municipio_foco, top_n=top_n)


def main() -> None:
    _inject_mobile_first_styles()
    if "filtro_secao" not in st.session_state:
        st.session_state["filtro_secao"] = "Situação Geral"
    if "last_rendered_secao" not in st.session_state:
        st.session_state["last_rendered_secao"] = st.session_state.get("filtro_secao", "Situação Geral")
    initial_panel_ready = bool(st.session_state.get("initial_panel_ready", False))
    last_filter_signature = st.session_state.get("last_filter_signature")
    loading = _start_loading_overlay() if not initial_panel_ready else None
    started_at = time.perf_counter()
    total_steps = 14
    step = 0
    try:
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Iniciando painel...", started_at=started_at)

        secao = _render_portal_header()
        if not secao:
            secao = "Situação Geral"
        secao_anterior = str(st.session_state.get("last_rendered_secao", secao))
        secao_changed = bool(initial_panel_ready and secao_anterior != secao)
        if secao_changed and not loading:
            loading = _start_loading_overlay(blocking=True)
            _update_loading_overlay(
                loading,
                step,
                total_steps,
                f"Abrindo painel: {secao}...",
                started_at=started_at,
            )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Preparando identidade visual...", started_at=started_at)

        df_ano_base = load_casos_ano()
        if df_ano_base.empty:
            st.error("Não foi possível carregar dados analíticos. Verifique as views no schema 'saude'.")
            st.stop()
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Conectando base de dados...", started_at=started_at)

        anos_disponíveis = sorted(df_ano_base["ano"].dropna().astype(int).tolist())
        df_mes_uf_base = load_casos_mes_uf()
        meses_disponíveis = sorted(df_mes_uf_base["mes_referencia"].dt.month.dropna().astype(int).unique().tolist())
        referencia_base = pd.to_datetime(df_mes_uf_base["mes_referencia"], errors="coerce").max()
        df_municipio_base = load_casos_municipio_ano_rj_enriquecido()
        municipios_disponíveis = sorted(
            [m for m in df_municipio_base["municipio_nome"].dropna().astype(str).unique().tolist() if m]
        )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Lendo períodos disponíveis...", started_at=started_at)

        filtros = render_filters_sidebar(
            anos_disponíveis=anos_disponíveis,
            meses_disponíveis=meses_disponíveis,
            municipios_disponíveis=municipios_disponíveis,
        )
        current_signature = _filters_signature(filtros)
        filter_changed = bool(initial_panel_ready and last_filter_signature is not None and current_signature != last_filter_signature)
        if filter_changed and not loading:
            # Regra UX: sempre exibir overlay oficial (logo + card) em qualquer carregamento.
            loading = _start_loading_overlay(blocking=True)
            if secao_changed:
                msg_reload = f"Abrindo painel: {secao}..."
            else:
                msg_reload = "Recarregando painel com novos filtros..."
            _update_loading_overlay(loading, step, total_steps, msg_reload, started_at=started_at)
        step += 1
        label_filtro = "Aplicando filtros..."
        if str(filtros.get("período_rapido", "")) == "Personalizado (ano/mês)":
            label_filtro = "Aplicando filtros (consulta historica sob demanda)..."
        _update_loading_overlay(loading, step, total_steps, label_filtro, started_at=started_at)

        comparacao = str(filtros["comparacao"])
        período_rapido = str(filtros["período_rapido"])
        metrica_visual = str(filtros["metrica_visual"])
        municipio_foco = str(filtros["municipio"]) if filtros["municipio"] else TARGET_MUNICIPIO_NOME
        anos_selecionados = filtros["anos"] if período_rapido == "Personalizado (ano/mês)" else []
        meses_selecionados = filtros["meses"]
        classificacao_tuple = get_classificacao_tuple(filtros["classificacao"])
        carregar_histórico_completo = bool(filtros["carregar_histórico_completo"])
        if not bool(filtros.get("personalizado_valido", True)):
            st.warning("Selecione ao menos 1 ano no filtro 'Personalizado (ano/mês)'. O mes e opcional.")
            _render_portal_footer()
            st.session_state["last_filter_signature"] = current_signature
            return
        data_inicio = _start_date_for_fast_window(
            período_rapido=período_rapido,
            anos_selecionados=anos_selecionados if período_rapido == "Personalizado (ano/mês)" else [],
            carregar_histórico_completo=carregar_histórico_completo,
            referencia_base=referencia_base,
        )

        df_mes_uf = load_casos_mes_uf(classificacoes=classificacao_tuple, data_inicio=data_inicio)
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Montando séries temporais...", started_at=started_at)

        df_municipio_rj = load_casos_municipio_ano_rj_enriquecido(classificacoes=classificacao_tuple, data_inicio=data_inicio)
        df_municipio_br = load_casos_municipio_ano_brasil_enriquecido(
            classificacoes=classificacao_tuple, data_inicio=data_inicio
        )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Carregando dados territoriais...", started_at=started_at)

        df_mes_municipio = load_casos_mes_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Carregando recorte do municipio...", started_at=started_at)

        df_status_municipio = load_status_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        step += 1
        _update_loading_overlay(
            loading, step, total_steps, "Atualizando classificacao de casos...", started_at=started_at
        )

        df_internacoes_mensal = load_internacoes_mensal_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Consolidando internacoes...", started_at=started_at)

        df_sexo_municipio = load_sexo_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        step += 1
        _update_loading_overlay(loading, step, total_steps, "Consolidando perfil dos casos...", started_at=started_at)

        df_unidade_municipio = load_unidade_notificadora_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            top_n=int(filtros["top_n"]),
            data_inicio=data_inicio,
        )
        df_unidade_mensal = load_unidade_notificadora_mensal_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        step += 1
        _update_loading_overlay(
            loading, step, total_steps, "Consolidando unidades notificadoras...", started_at=started_at
        )
        df_perfil_top = load_perfil_top_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        df_clinico_exames = load_clinico_exames_registros_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        df_perfil_demografico_mensal = load_perfil_demografico_mensal_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        df_comorbidades_mensal = load_comorbidades_mensal_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        df_perfil_demografico_rj = load_perfil_demografico_mensal_escopo(
            escopo="RJ",
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        df_perfil_demografico_br = load_perfil_demografico_mensal_escopo(
            escopo="BR",
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )

        df_semana = load_semana_epidemiologica_media_municipio(
            municipio_nome=municipio_foco,
            classificacoes=classificacao_tuple,
            data_inicio=data_inicio,
        )
        semana_epi_media = (
            int(df_semana.iloc[0]["semana_media"])
            if not df_semana.empty and pd.notna(df_semana.iloc[0].get("semana_media"))
            else None
        )

        pop_refs = load_populacao_refs(municipio_nome=municipio_foco)
        step += 1
        _update_loading_overlay(
            loading, step, total_steps, "Calculando indicadores de referencia...", started_at=started_at
        )

        step += 1
        _update_loading_overlay(loading, step, total_steps, "Finalizando painel...", started_at=started_at)

        st.title("Monitoramento da Dengue | Rio das Ostras - RJ")

        df_mes_filtrado = _filtrar_df(df_mes_uf, anos=anos_selecionados)
        df_municipio_rj_filtrado = _filtrar_df(df_municipio_rj, anos=anos_selecionados)
        df_municipio_br_filtrado = _filtrar_df(df_municipio_br, anos=anos_selecionados)
        df_mes_municipio = _filtrar_df(df_mes_municipio, anos=anos_selecionados)
        df_status_municipio = _filtrar_df(df_status_municipio, anos=anos_selecionados)
        df_internacoes_mensal = _filtrar_df(df_internacoes_mensal, anos=anos_selecionados)
        df_unidade_mensal = _filtrar_df(df_unidade_mensal, anos=anos_selecionados)
        df_perfil_demografico_mensal = _filtrar_df(df_perfil_demografico_mensal, anos=anos_selecionados)
        df_comorbidades_mensal = _filtrar_df(df_comorbidades_mensal, anos=anos_selecionados)
        df_perfil_demografico_rj = _filtrar_df(df_perfil_demografico_rj, anos=anos_selecionados)
        df_perfil_demografico_br = _filtrar_df(df_perfil_demografico_br, anos=anos_selecionados)
        df_clinico_exames = _filtrar_df(df_clinico_exames, anos=anos_selecionados)

        df_mes_filtrado = _filtrar_por_meses(df_mes_filtrado, meses=meses_selecionados)
        df_municipio_rj_filtrado = _filtrar_por_meses(df_municipio_rj_filtrado, meses=meses_selecionados)
        df_municipio_br_filtrado = _filtrar_por_meses(df_municipio_br_filtrado, meses=meses_selecionados)
        df_mes_municipio = _filtrar_por_meses(df_mes_municipio, meses=meses_selecionados)
        df_status_municipio = _filtrar_por_meses(df_status_municipio, meses=meses_selecionados)
        df_internacoes_mensal = _filtrar_por_meses(df_internacoes_mensal, meses=meses_selecionados)
        df_unidade_mensal = _filtrar_por_meses(df_unidade_mensal, meses=meses_selecionados)
        df_perfil_demografico_mensal = _filtrar_por_meses(df_perfil_demografico_mensal, meses=meses_selecionados)
        df_comorbidades_mensal = _filtrar_por_meses(df_comorbidades_mensal, meses=meses_selecionados)
        df_perfil_demografico_rj = _filtrar_por_meses(df_perfil_demografico_rj, meses=meses_selecionados)
        df_perfil_demografico_br = _filtrar_por_meses(df_perfil_demografico_br, meses=meses_selecionados)
        df_clinico_exames = _filtrar_por_meses(df_clinico_exames, meses=meses_selecionados)

        df_mes_filtrado = _aplicar_período_rapido(df_mes_filtrado, período_rapido=período_rapido)
        df_municipio_rj_filtrado = _aplicar_período_rapido(df_municipio_rj_filtrado, período_rapido=período_rapido)
        df_municipio_br_filtrado = _aplicar_período_rapido(df_municipio_br_filtrado, período_rapido=período_rapido)
        df_mes_municipio = _aplicar_período_rapido(df_mes_municipio, período_rapido=período_rapido)
        df_status_municipio = _aplicar_período_rapido(df_status_municipio, período_rapido=período_rapido)
        df_internacoes_mensal = _aplicar_período_rapido(df_internacoes_mensal, período_rapido=período_rapido)
        df_unidade_mensal = _aplicar_período_rapido(df_unidade_mensal, período_rapido=período_rapido)
        df_perfil_demografico_mensal = _aplicar_período_rapido(df_perfil_demografico_mensal, período_rapido=período_rapido)
        df_comorbidades_mensal = _aplicar_período_rapido(df_comorbidades_mensal, período_rapido=período_rapido)
        df_perfil_demografico_rj = _aplicar_período_rapido(df_perfil_demografico_rj, período_rapido=período_rapido)
        df_perfil_demografico_br = _aplicar_período_rapido(df_perfil_demografico_br, período_rapido=período_rapido)
        df_clinico_exames = _aplicar_período_rapido(df_clinico_exames, período_rapido=período_rapido)

        if secao == "Situação Geral":
            _render_dashboard_situacao_geral(
                df_mes_filtrado=df_mes_filtrado,
                df_municipio_rj_filtrado=df_municipio_rj_filtrado,
                df_municipio_br_filtrado=df_municipio_br_filtrado,
                df_mes_municipio=df_mes_municipio,
                df_status_municipio=df_status_municipio,
                df_internacoes_mensal=df_internacoes_mensal,
                df_sexo_municipio=df_sexo_municipio,
                df_unidade_municipio=df_unidade_municipio,
                df_perfil_top=df_perfil_top,
                semana_epi_media=semana_epi_media,
                comparacao=comparacao,
                período_rapido=período_rapido,
                metrica_visual=metrica_visual,
                municipio_foco=municipio_foco,
                top_n=int(filtros["top_n"]),
                pop_refs=pop_refs,
            )
        elif secao == "Território e Risco":
            render_territorio_risco_unidades(
                df_unidade_mensal=df_unidade_mensal,
                top_n=int(filtros["top_n"]),
                comparacao=comparacao,
                municipio_foco=municipio_foco,
                df_municipio_rj_filtrado=df_municipio_rj_filtrado,
                df_municipio_br_filtrado=df_municipio_br_filtrado,
            )
        elif secao == "Perfil dos Casos":
            render_perfil_epidemiologico(
                df_perfil_mensal=df_perfil_demografico_mensal,
                df_comorb_mensal=df_comorbidades_mensal,
                comparacao=comparacao,
                df_perfil_rj=df_perfil_demografico_rj,
                df_perfil_br=df_perfil_demografico_br,
                df_clinico=df_clinico_exames,
            )
        elif secao == "Clínico e Exames":
            render_clinico_exames_dashboard(df_clinico=df_clinico_exames)
        else:
            st.markdown("### Avaliação")
            st.warning("Dependente de colunas de datas clinicas e encerramento para calculo final dos tempos.")

        _render_portal_footer()
        st.session_state["last_filter_signature"] = current_signature
        st.session_state["last_rendered_secao"] = secao
        # Libera a interface ao usuario antes do pre-aquecimento em segundo plano.
        _finish_loading_overlay(loading)
        loading = None

        # Regra de UX: libera visualizacao rapida primeiro (3 meses),
        # e so depois inicia aquecimento progressivo do cache.
        if not initial_panel_ready:
            st.session_state["initial_panel_ready"] = True
            st.session_state["_cache_auto_refresh_last"] = time.perf_counter()
            _finish_loading_overlay(loading)
            loading = None
            st.rerun()
        else:
            _preload_next_windows(
                classificacao_tuple=classificacao_tuple,
                municipio_foco=municipio_foco,
                top_n=int(filtros["top_n"]),
            )
    except Exception as exc:
        st.error(
            "Não foi possível concluir o carregamento do painel. "
            "Se o problema persistir, recarregue a pagina com F5."
        )
        st.info("Dica: pressione F5 para tentar novamente.")
        with st.expander("Detalhes tecnicos do erro"):
            st.exception(exc)
    finally:
        _finish_loading_overlay(loading)


if __name__ == "__main__":
    main()
