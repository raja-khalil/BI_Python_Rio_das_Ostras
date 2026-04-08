# Projeto BI - Dengue Ministerio da Saude (Rio das Ostras)

## Objetivo
Estrutura base padronizada para projetos de BI do municipio de Rio das Ostras, iniciando pelo tema dengue com fontes do Ministerio da Saude.

## Escopo inicial
- Ingestao de dados via API, JSON, CSV e XML
- Organizacao de dados em camadas (`raw`, `staging`, `processed`)
- Pipeline com etapas de ingestao, transformacao, validacao e carga no PostgreSQL
- Dashboard analitico inicial em Streamlit
- Base reutilizavel para futuros projetos de BI municipais

## Arquitetura resumida
- `src/ingestao`: conectores e leitores de fontes
- `src/transformacao`: limpeza e padronizacao
- `src/validacao`: regras de qualidade
- `src/banco`: conexao e carga para PostgreSQL
- `src/indicadores`: regras analiticas e KPIs
- `app/`: camada de visualizacao em Streamlit
- `sql/`: DDL, views e consultas analiticas

## Stack
- Python 3.12+
- PostgreSQL
- Streamlit
- pandas, numpy, pyarrow
- requests, lxml
- SQLAlchemy, psycopg
- plotly
- python-dotenv
- pytest, black, ruff

## Estrutura de pastas
```text
dengue_ministerio_saude/
|-- data/
|   |-- raw/
|   |   |-- api/
|   |   |-- json/
|   |   |-- csv/
|   |   `-- xml/
|   |-- staging/
|   |-- processed/
|   `-- external/
|-- src/
|   |-- ingestao/
|   |-- transformacao/
|   |-- validacao/
|   |-- banco/
|   |-- indicadores/
|   |-- config/
|   `-- utils/
|-- sql/
|   |-- ddl/
|   |-- views/
|   `-- queries/
|-- app/
|   |-- pages/
|   |-- components/
|   `-- main.py
|-- docs/
|-- logs/
|-- notebooks/
|-- tests/
|-- .env.example
|-- requirements.txt
|-- pyproject.toml
|-- run_pipeline.py
`-- README.md
```

## Fluxo do pipeline
1. Selecionar fonte e modo de carga (historica ou incremental)
2. Ingerir e opcionalmente persistir bruto em `data/raw`
3. Aplicar limpeza e padronizacao
4. Validar integridade minima
5. Carregar no PostgreSQL
6. Consumir tabelas e views no dashboard

## Como usar (basico)
1. Criar e ativar ambiente virtual:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```
2. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
3. Instalar o projeto em modo editavel (recomendado para imports estaveis):
   ```bash
   pip install -e .
   ```
4. Criar arquivo `.env` com base em `.env.example`
5. Executar pipeline:
   ```bash
   python run_pipeline.py
   ```
6. Executar dashboard:
   ```bash
   streamlit run app/main.py
   ```

## Operacao de producao
- `PIPELINE_MODE=historical`: executa carga historica completa por lotes de anos.
- `PIPELINE_MODE=incremental`: executa carga incremental diaria com base em watermark no PostgreSQL.
- Tabelas de metadados (criacao automatica): `saude.bi_pipeline_watermark` e `saude.bi_pipeline_execucoes`.
- Parametros principais em `.env`:
  `HISTORICAL_START_YEAR`, `HISTORICAL_END_YEAR`, `HISTORICAL_BATCH_YEARS`,
  `INCREMENTAL_OVERLAP_DAYS`, `INCREMENTAL_FALLBACK_YEAR`.

### Exemplos
- Carga historica 2000-2026:
  ```bash
  $env:PIPELINE_MODE="historical"
  python run_pipeline.py
  ```
- Incremental diario:
  ```bash
  $env:PIPELINE_MODE="incremental"
  python run_pipeline.py
  ```

## Execucao assistida
- Sempre que houver necessidade de executar comandos locais (instalacao, teste, lint, pipeline ou app), a orientacao sera avisada antes da execucao.
- Fluxo recomendado com suporte: alinhar o comando, executar no terminal, compartilhar o resultado e aplicar o proximo passo.

## Replicabilidade
Este projeto foi estruturado para funcionar como modelo padrao para novos BIs do municipio, mantendo a mesma organizacao de camadas, modulos e boas praticas.
