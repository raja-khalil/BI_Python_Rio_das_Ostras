"""Componentes de filtros do dashboard."""

from __future__ import annotations

import streamlit as st


def render_filters_sidebar() -> dict[str, str]:
    """Renderiza filtros iniciais de exemplo na barra lateral."""
    st.sidebar.header("Filtros")
    periodo = st.sidebar.selectbox("Periodo", ["Ultimos 30 dias", "Ano atual", "Historico"])
    territorio = st.sidebar.selectbox("Territorio", ["Rio das Ostras", "Estado", "Brasil"])
    return {"periodo": periodo, "territorio": territorio}
