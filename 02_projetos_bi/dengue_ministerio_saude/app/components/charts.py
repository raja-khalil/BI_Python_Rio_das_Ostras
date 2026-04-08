"""Graficos base do dashboard."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render_time_series_placeholder() -> None:
    """Renderiza grafico de serie temporal placeholder."""
    sample = pd.DataFrame(
        {
            "periodo": ["2026-01", "2026-02", "2026-03", "2026-04"],
            "casos": [12, 18, 14, 22],
        }
    )
    fig = px.line(sample, x="periodo", y="casos", markers=True, title="Evolucao de Casos")
    st.plotly_chart(fig, use_container_width=True)


def render_comparative_placeholder() -> None:
    """Renderiza grafico comparativo placeholder."""
    sample = pd.DataFrame(
        {
            "grupo": ["Periodo Atual", "Periodo Anterior"],
            "casos": [22, 14],
        }
    )
    fig = px.bar(sample, x="grupo", y="casos", title="Comparativo de Periodos")
    st.plotly_chart(fig, use_container_width=True)
