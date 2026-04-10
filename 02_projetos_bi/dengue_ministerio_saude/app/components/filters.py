"""Componentes de filtros do dashboard."""

from __future__ import annotations

import streamlit as st


def render_filters_sidebar(
    anos_disponíveis: list[int],
    meses_disponíveis: list[int],
    municipios_disponíveis: list[str],
) -> dict[str, list | int | str]:
    """Renderiza filtros globais com persistência na sessão."""
    meses_nomes = {
        1: "Jan",
        2: "Fev",
        3: "Mar",
        4: "Abr",
        5: "Mai",
        6: "Jun",
        7: "Jul",
        8: "Ago",
        9: "Set",
        10: "Out",
        11: "Nov",
        12: "Dez",
    }

    defaults = {
        "filtro_secao": "Situação Geral",
        "filtro_comparacao": "Rio das Ostras",
        "filtro_anos": [],
        "filtro_meses": [],
        "filtro_classificacao": [],
        "filtro_período_rapido": "Últimos 3 meses",
        "filtro_uf": "RJ",
        "filtro_municipio": ("Rio das Ostras" if "Rio das Ostras" in municipios_disponíveis else (municipios_disponíveis[0] if municipios_disponíveis else "")),
        "filtro_top_n": 10,
        "filtro_carregar_histórico_completo": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    def _clear_filters() -> None:
        st.session_state["filtro_anos"] = defaults["filtro_anos"]
        st.session_state["filtro_meses"] = []
        st.session_state["filtro_comparacao"] = defaults["filtro_comparacao"]
        st.session_state["filtro_classificacao"] = defaults["filtro_classificacao"]
        st.session_state["filtro_período_rapido"] = defaults["filtro_período_rapido"]
        st.session_state["filtro_uf"] = defaults["filtro_uf"]
        st.session_state["filtro_municipio"] = defaults["filtro_municipio"]
        st.session_state["filtro_top_n"] = defaults["filtro_top_n"]
        st.session_state["filtro_carregar_histórico_completo"] = defaults["filtro_carregar_histórico_completo"]

    # Regra de UX: se houver anos selecionados, período deve ser personalizado.
    anos_preselecionados = st.session_state.get("filtro_anos", [])
    if anos_preselecionados:
        st.session_state["filtro_período_rapido"] = "Personalizado (ano/mês)"

    st.sidebar.header("Filtros")
    período_rapido = st.sidebar.selectbox(
        "Período",
        options=[
            "Personalizado (ano/mês)",
            "Últimos 3 meses",
            "Últimos 6 meses",
            "Últimos 12 meses",
            "Ano atual",
        ],
        key="filtro_período_rapido",
    )
    comparacao = st.sidebar.selectbox(
        "Comparação territorial",
        options=["Rio das Ostras", "Rio das Ostras x RJ x Brasil", "Rio das Ostras x RJ", "Rio das Ostras x Brasil"],
        key="filtro_comparacao",
    )
    metrica_visual = "Casos totais"
    # Escopo fixo do painel (sem filtro de seleção)
    st.session_state["filtro_uf"] = "RJ"
    st.session_state["filtro_municipio"] = "Rio das Ostras"
    uf = st.session_state["filtro_uf"]
    municipio = st.session_state["filtro_municipio"]
    st.sidebar.markdown("**Estado:** Rio de Janeiro (RJ)")
    st.sidebar.markdown("**Município:** Rio das Ostras")

    ano_atual = max(anos_disponíveis) if anos_disponíveis else None
    anos_recentes = [a for a in anos_disponíveis if ano_atual is not None and a >= (ano_atual - 4)]
    carregar_histórico_completo = st.sidebar.checkbox(
        "Carregar histórico completo (mais lento)",
        key="filtro_carregar_histórico_completo",
        help="Quando desmarcado, o filtro de ano mostra apenas os últimos 5 anos para navegação mais rápida.",
    )
    anos_options = anos_disponíveis if carregar_histórico_completo else anos_recentes
    anos_options = sorted(anos_options, reverse=True)
    st.session_state["filtro_anos"] = [a for a in st.session_state.get("filtro_anos", []) if a in anos_options]
    anos = st.sidebar.multiselect(
        "Ano",
        options=anos_options,
        key="filtro_anos",
        help="Selecione um ou mais anos. Ao selecionar, o período muda automaticamente para Personalizado (ano/mês).",
    )
    meses_options = [m for m in range(1, 13) if m in meses_disponíveis]
    meses = st.sidebar.multiselect(
        "Mês",
        options=meses_options,
        format_func=lambda m: f"{m:02d} - {meses_nomes.get(m, str(m))}",
        key="filtro_meses",
        help="Se vazio, considera todos os meses.",
    )
    if período_rapido == "Personalizado (ano/mês)":
        if not anos:
            st.sidebar.warning("No modo Personalizado (ano/mês), selecione pelo menos 1 ano. O mês é opcional.")
        else:
            st.sidebar.caption("Modo Personalizado: ano obrigatório selecionado; mês opcional.")
            st.sidebar.info(
                "Consulta histórica sob demanda: esse modo pode demorar um pouco mais para carregar."
            )
    classificacao_opcoes = {
        "1": "Confirmado (1)",
        "2": "Descartado (2)",
        "8": "Inconclusivo (8)",
        "9": "Ignorado (9)",
        "11": "Dengue com sinais de alarme (11)",
        "12": "Dengue grave (12)",
    }
    classificacao = st.sidebar.multiselect(
        "Classificação do caso",
        options=list(classificacao_opcoes.keys()),
        default=st.session_state["filtro_classificacao"],
        format_func=lambda c: classificacao_opcoes.get(c, c),
        key="filtro_classificacao",
        help="Se vazio, considera todas as classificações.",
    )
    top_n = st.sidebar.slider("Top municípios", min_value=5, max_value=30, step=1, key="filtro_top_n")

    st.sidebar.button("Limpar filtros", on_click=_clear_filters, use_container_width=True)

    resumo_meses = "todos" if not meses else ", ".join([f"{m:02d}" for m in meses])
    st.sidebar.caption(
        "Resumo de filtros ativos | "
        f"anos={anos if anos else 'todos'} | "
        f"meses={resumo_meses} | "
        f"período={período_rapido} | "
        f"uf={uf} | municipio={municipio} | "
        f"classificação={classificacao if classificacao else 'todas'} | "
        f"comparação={comparacao} | "
        f"histórico={'completo' if carregar_histórico_completo else 'últimos 5 anos'}"
    )

    stage = int(st.session_state.get("preload_stage", 0))
    stage = max(0, min(stage, 3))
    progress = (stage + 1) / 4
    stage_labels = {
        0: "Base inicial pronta: 3 meses",
        1: "Cache aquecido: 6 meses",
        2: "Cache aquecido: 12 meses",
        3: "Cache aquecido: 5 anos",
    }
    st.sidebar.markdown("**Desempenho de cache**")
    st.sidebar.progress(progress)
    st.sidebar.caption(stage_labels.get(stage, "Inicializando cache"))
    if stage < 3:
        st.sidebar.caption("Aquecimento automático em andamento... não precisa clicar para atualizar.")
    else:
        st.sidebar.caption("Cache até 5 anos concluído.")
    if carregar_histórico_completo:
        st.sidebar.caption("Histórico completo: carregamento sob demanda (mais lento).")

    return {
        "secao": st.session_state.get("filtro_secao", defaults["filtro_secao"]),
        "comparacao": comparacao,
        "período_rapido": período_rapido,
        "metrica_visual": metrica_visual,
        "uf": uf,
        "municipio": municipio,
        "anos": anos,
        "meses": meses,
        "classificacao": classificacao,
        "top_n": top_n,
        "carregar_histórico_completo": carregar_histórico_completo,
        "personalizado_valido": (período_rapido != "Personalizado (ano/mês)") or bool(anos),
    }
