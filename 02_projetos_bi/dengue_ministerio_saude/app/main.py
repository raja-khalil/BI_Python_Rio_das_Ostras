"""Aplicacao Streamlit inicial do BI de dengue."""

from __future__ import annotations

import streamlit as st

from app.components.cards import render_kpi_cards
from app.components.charts import render_comparative_placeholder, render_time_series_placeholder
from app.components.filters import render_filters_sidebar


st.set_page_config(
    page_title="BI Dengue - Rio das Ostras",
    page_icon=":bar_chart:",
    layout="wide",
)


def main() -> None:
    """Renderiza pagina principal do dashboard."""
    filtros = render_filters_sidebar()

    st.title("BI Dengue | Rio das Ostras")
    st.caption(
        "Painel analitico inicial para monitoramento de dengue com foco em leitura rapida e decisao."
    )

    st.markdown("### Contexto")
    st.write(
        "Este painel esta preparado para integrar dados do Ministerio da Saude e evoluir para"
        " comparacoes historicas, territoriais e operacionais."
    )

    st.markdown("### Indicadores")
    render_kpi_cards()

    st.markdown("### Serie Temporal")
    render_time_series_placeholder()

    st.markdown("### Analise Comparativa")
    render_comparative_placeholder()

    st.caption(f"Filtros ativos: {filtros}")


if __name__ == "__main__":
    main()
