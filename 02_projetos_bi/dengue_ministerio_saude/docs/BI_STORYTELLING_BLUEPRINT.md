# BI Dengue - Estrutura Narrativa (Storytelling com Dados)

## Perguntas norteadoras
1. Como esta a situacao?
2. Onde esta o problema?
3. O que exige decisao?

## Camadas do BI

### 1) Analise situacional
- Cards de leitura rapida:
  - Total de casos notificados
  - Casos confirmados
  - Casos descartados
  - Casos em investigacao
  - Obitos
  - Internacoes
  - Incidencia por 100 mil habitantes
  - Letalidade
- Evolucao temporal:
  - Rio das Ostras em destaque
  - RJ e Brasil como contexto
- Territorio:
  - Ranking por casos
  - Ranking por incidencia
  - Mapa por municipio (casos/incidencia)

### 2) Tomada de decisao
- Priorizacao territorial:
  - Municipios com maior crescimento
  - Municipios com maior incidencia
  - Municipios com mais casos absolutos
- Pressao assistencial:
  - Internacoes
  - Obitos
  - Classificacao de risco
- Tendencia recente:
  - Serie atual x periodo anterior
  - Aceleracao/desaceleracao

### 3) Avaliativo
- Efetividade operacional:
  - Tempo sintomas->notificacao
  - Tempo notificacao->encerramento
  - Percentual encerrado
- Qualidade:
  - Completude de campos
  - Registros incompletos
- Diagnostico:
  - Exames realizados
  - Positivos, negativos, inconclusivos

## Regras visuais
- Destacar Rio das Ostras com cor primaria.
- Manter RJ e Brasil em tons neutros.
- Evitar codigos e siglas tecnicas no texto principal.
- Titulo deve trazer mensagem, nao so tipo do grafico.
- Tooltip limpo: local, periodo, valor principal e 1 indicador complementar.

## Filtros globais (prioridade)
- Periodo (atalhos):
  - Ultimos 3 meses
  - Ultimos 6 meses
  - Ultimos 12 meses
  - Ano atual
- Ano
- Mes
- Comparacao territorial:
  - Rio das Ostras x RJ
  - Rio das Ostras x Brasil
  - Rio das Ostras x RJ x Brasil
- Classificacao do caso
- Botao Limpar filtros
- Resumo de filtros ativos

## Roadmap de implementacao
1. Analise situacional completa (cards + serie + ranking + incidencia + mapa)
2. Tomada de decisao (crescimento + risco + pressao assistencial)
3. Perfil dos casos (sexo, idade, raca/cor, gestante, comorbidades)
4. Avaliativo (resposta, encerramento, completude, exames)
