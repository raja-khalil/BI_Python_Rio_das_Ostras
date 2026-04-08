"""Cards de indicadores do dashboard."""

from __future__ import annotations

import streamlit as st


def render_kpi_cards() -> None:
    """Renderiza KPIs placeholder."""
    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Casos", "-", "-")
    col2.metric("Incidencia (100k)", "-", "-")
    col3.metric("Variacao vs periodo anterior", "-", "-")
