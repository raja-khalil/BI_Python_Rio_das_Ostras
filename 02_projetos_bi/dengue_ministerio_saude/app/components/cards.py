"""Cards visuais do dashboard."""

from __future__ import annotations

import math

import streamlit as st

from app.theme import (
    BORDER_SOFT,
    BRAND_BLUE,
    BRAND_WHITE,
    SEMANTIC_DANGER,
    SEMANTIC_NEUTRAL,
    SEMANTIC_SUCCESS,
)


def _fmt_num(value: float | int | None, suffix: str = "", absolute: bool = True) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    # Regra global dos cards: arredondar por proximidade e nunca exibir casas decimais.
    text = f"{int(round(float(value))):,}".replace(",", ".")
    return f"{text}{suffix}"


def _delta_color(delta_pct: float | None) -> str:
    if delta_pct is None:
        return SEMANTIC_NEUTRAL
    if delta_pct > 0:
        return SEMANTIC_DANGER
    if delta_pct < 0:
        return SEMANTIC_SUCCESS
    return SEMANTIC_NEUTRAL


def render_kpi_cards(card_items: list[dict[str, str | float | int | None]]) -> None:
    """Renderiza cards premium (visual portal)."""
    css = """
        <style>
        .kpi-grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px;
            margin-bottom: 12px;
        }
        .kpi-card {
            background: linear-gradient(145deg, __BRAND_BLUE__ 0%, #0b5f97 52%, #063d65 100%);
            border-radius: 14px;
            border: 1px solid __BORDER_SOFT__;
            box-shadow: 0 8px 18px rgba(2, 16, 30, 0.30);
            padding: 12px 12px 10px 12px;
            min-height: 96px;
        }
        .kpi-label {
            color: #e2edf8;
            font-size: 0.78rem;
            font-weight: 600;
            margin-bottom: 5px;
        }
        .kpi-value {
            color: __BRAND_WHITE__;
            font-size: 1.42rem;
            line-height: 1.15;
            font-weight: 800;
            margin-bottom: 4px;
        }
        .kpi-delta {
            font-size: 0.80rem;
            font-weight: 700;
            margin-bottom: 2px;
        }
        .kpi-help {
            color: #d4e5f5;
            font-size: 0.70rem;
            line-height: 1.2;
        }
        @media (min-width: 680px) {
            .kpi-grid {
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
                margin-bottom: 14px;
            }
            .kpi-card {
                padding: 14px 14px 12px 14px;
                min-height: 108px;
            }
            .kpi-label { font-size: 0.83rem; }
            .kpi-value { font-size: 1.62rem; }
            .kpi-help { font-size: 0.73rem; }
        }
        @media (min-width: 1200px) {
            .kpi-grid {
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            }
            .kpi-value { font-size: 1.75rem; }
        }
        </style>
        """
    css = (
        css.replace("__BRAND_BLUE__", BRAND_BLUE)
        .replace("__BORDER_SOFT__", BORDER_SOFT)
        .replace("__BRAND_WHITE__", BRAND_WHITE)
    )
    st.markdown(css, unsafe_allow_html=True)

    blocks: list[str] = []
    for item in card_items:
        delta_pct = item.get("delta_pct")
        delta_txt = str(item.get("delta_text", ""))
        delta_color = _delta_color(float(delta_pct)) if delta_pct is not None else SEMANTIC_NEUTRAL

        blocks.append(
            (
                '<div class="kpi-card">'
                f'<div class="kpi-label">{item.get("label", "-")}</div>'
                f'<div class="kpi-value">{item.get("value", "-")}</div>'
                f'<div class="kpi-delta" style="color: {delta_color};">{delta_txt}</div>'
                f'<div class="kpi-help">{item.get("help", "")}</div>'
                "</div>"
            )
        )

    st.markdown(f'<div class="kpi-grid">{"".join(blocks)}</div>', unsafe_allow_html=True)


def make_card(
    label: str,
    value: float | int | None,
    help_text: str,
    delta_pct: float | None = None,
    suffix: str = "",
    absolute: bool = True,
) -> dict[str, str | float | int | None]:
    if delta_pct is None:
        delta_text = "-"
    else:
        signal = "+" if delta_pct > 0 else ""
        delta_text = f"{signal}{int(round(float(delta_pct)))}% vs mes anterior"
    return {
        "label": label,
        "value": _fmt_num(value, suffix=suffix, absolute=absolute),
        "delta_text": delta_text,
        "delta_pct": delta_pct,
        "help": help_text,
    }
