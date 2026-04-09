"""Tema visual oficial do painel (Prefeitura de Rio das Ostras)."""

from __future__ import annotations


# Paleta oficial extraida do manual de aplicacao da marca.
BRAND_BLUE = "#004F80"
BRAND_YELLOW = "#DFA230"
BRAND_ORANGE = BRAND_YELLOW
BRAND_HIGHLIGHT = BRAND_ORANGE
BRAND_WHITE = "#FFFFFF"

# Cores de apoio para interface e leitura.
SURFACE_BG = "#F6FAFE"
SURFACE_CARD = "#FFFFFF"
TEXT_PRIMARY = "#0F2F4B"
TEXT_SECONDARY = "#3E5E79"
BORDER_SOFT = "#D6E3EF"
GRID_SOFT = "#E2EAF2"

# Cores semanticas para variacao (analitico).
# Separacao explicita:
# - BRAND_HIGHLIGHT: destaque visual institucional (laranja da marca)
# - SEMANTIC_RISK_HIGH: alerta epidemiologico (vermelho)
SEMANTIC_RISK_HIGH = "#C0392B"
SEMANTIC_DANGER = SEMANTIC_RISK_HIGH
SEMANTIC_SUCCESS = "#1F8A5B"
SEMANTIC_NEUTRAL = "#7A8CA1"

# Tipografia institucional.
# A fonte oficial da marca e "Amsi Font Family".
# Como fallback web-safe, mantemos Segoe UI / Arial / sans-serif.
FONT_FAMILY = '"Amsi", "Segoe UI", "Helvetica Neue", Arial, sans-serif'
