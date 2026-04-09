# Especificacao Funcional Completa - BI Dengue

## 1. Objetivo
Disponibilizar um BI narrativo para vigilancia epidemiologica de dengue com foco em Rio das Ostras, suportando leitura executiva, tomada de decisao e avaliacao operacional.

## 2. Perguntas de negocio
1. Como esta a situacao?
2. Onde esta o problema?
3. O que exige decisao?

## 3. Arquitetura de dados (modelo logico)

### 3.1 Fato principal (camada analitica)
Entidade: `saude.fato_dengue_analitica` (view derivada da base operacional).

Campos esperados:
- `id_caso`
- `data_notificacao`
- `data_sintomas`
- `ano`
- `mes`
- `semana_epidemiologica`
- `municipio`
- `uf`
- `regiao`
- `rgi`
- `rgint`
- `classificacao_final`
- `evolucao`
- `hospitalizacao`
- `idade`
- `faixa_etaria`
- `sexo`
- `raca`
- `escolaridade`
- `comorbidade`
- `sintoma_febre`
- `sintoma_cefaleia`
- `exame_ns1`
- `exame_pcr`
- `exame_sorologia`
- `dt_encerramento`
- `dt_digitacao`

Observacao: parte dos campos ainda depende de ampliacao da ingestao para leitura completa do dicionario SINAN.

### 3.2 Dimensao territorial
Entidade fisica: `saude.dim_ibge_municipio`.
Entidade de consumo: `saude.dim_territorio` (view amigavel).

Campos minimos:
- `municipio`
- `codigo_municipio`
- `uf`
- `regiao`
- `rgi`
- `rgint`
- `populacao`

### 3.3 Join principal
`fato_dengue_analitica.codigo_municipio = dim_territorio.codigo_municipio`

## 4. Metricas padronizadas

### 4.1 Base
- `casos_total = count(id_caso)`
- `casos_confirmados = count(classificacao_final confirmado)`
- `casos_descartados = count(classificacao_final descartado)`
- `obitos = count(evolucao = obito)`
- `internacoes = count(hospitalizacao = sim)`

### 4.2 Derivadas
- `incidencia = (casos_confirmados / populacao) * 100000`
- `letalidade = (obitos / casos_confirmados) * 100`
- `taxa_internacao = (internacoes / casos_confirmados) * 100`

### 4.3 Crescimento
- `crescimento = (valor_atual - valor_anterior) / valor_anterior`

## 5. Dashboards (escopo funcional)

### 5.1 Dashboard 1 - Situacao Geral
- Cards principais:
  - Casos confirmados
  - Casos notificados
  - Obitos
  - Internacoes
  - Incidencia
  - Letalidade
- Serie temporal comparativa:
  - Rio das Ostras (destaque)
  - RJ (secundario)
  - Brasil (contexto)
- Mapa (casos/incidencia)
- Ranking por casos
- Ranking por incidencia

### 5.2 Dashboard 2 - Territorio e Risco
- Cards:
  - maior crescimento
  - maior incidencia
  - maior volume de casos
- Crescimento por municipio
- Mapa de risco por incidencia
- Tabela gerencial (municipio, casos, incidencia, crescimento, risco)

### 5.3 Dashboard 3 - Perfil dos Casos
- Faixa etaria
- Sexo
- Raca/cor
- Gravidade (internacao/obito)

### 5.4 Dashboard 4 - Clinico e Exames
- Frequencia de sintomas
- Exames realizados/positivos/negativos
- Confirmacao por classificacao final

### 5.5 Dashboard 5 - Avaliativo
- Tempo medio sintomas->notificacao
- Tempo medio notificacao->encerramento
- Percentual de casos encerrados
- Percentual de preenchimento/completude

## 6. Filtros (comportamento tecnico)

### 6.1 Filtros globais
- periodo
- ano
- mes
- municipio
- estado
- comparacao territorial
- classificacao do caso

### 6.2 Regras
1. Persistencia entre paginas/sessoes da navegacao.
2. Hierarquia Estado -> Municipio.
3. Multiselecao para mes e classificacao.
4. Acao explicita de limpeza de filtros.

## 7. UX obrigatoria
- Tooltips sem codigos tecnicos.
- Rio das Ostras em destaque visual; demais referencias em tons neutros.
- Titulos com mensagem analitica.
- Alternancia entre casos totais e incidencia.
- Drill-down territorial Brasil -> Estado -> Municipio.
- Interacao cruzada entre componentes.

## 8. Atualizacao e performance
- Frequencia recomendada: diaria.
- Camada agregada obrigatoria: `saude.agg_dengue_mensal`.
- Objetivo: reduzir custo computacional no dashboard e garantir baixa latencia.

## 9. Criterios de aceite (MVP)
1. Dashboard Situacao Geral com cards + serie + ranking + incidencia operando em ate 3s em recorte mensal.
2. Filtros globais persistentes e botao limpar filtros funcionando.
3. Metricas padronizadas com mesma definicao em todos os paineis.
4. Dimensao territorial enriquecendo nome/populacao/incidencia corretamente.
