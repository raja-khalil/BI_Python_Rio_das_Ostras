# Projeto BI - Dengue Ministerio da Saude (Rio das Ostras)

## Objetivo
Estrutura base padronizada para projetos de BI do municipio de Rio das Ostras, iniciando pelo tema dengue com fontes do Ministerio da Saude.

## Regra de design dos paineis
- O **Painel 1 (Situacao Geral)** e a referencia visual oficial do projeto.
- Todo novo painel deve seguir o mesmo padrao:
  - cards premium no topo (mesmo componente/estilo);
  - proporcao de layout em blocos (cards -> graficos lado a lado -> tabela/lista);
  - mesma hierarquia tipografica, espacamento e paleta.
- Alteracoes visuais em paineis secundarios devem manter consistencia com o Painel 1.

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

## Downloader do Portal SUS
- Script: `run_downloader.py`
- Fonte: dataset `Sinan/Dengue` no portal de dados abertos.
- Destino dos arquivos:
  - JSON: `data/raw/json/portal_sus/`
  - CSV: `data/raw/csv/portal_sus/`
  - XML: `data/raw/xml/portal_sus/`
- Arquivos `.zip` podem ser extraidos automaticamente para `extracted/<ano>/`.

### Execucao unica
```bash
python run_downloader.py --formats json,csv,xml --year-start 2000 --year-end 2026 --extract-zip
```

### Busca periodica por atualizacoes
```bash
python run_downloader.py --periodic --interval-minutes 60 --formats json,csv,xml --year-start 2000 --year-end 2026 --extract-zip
```

O downloader usa `metadata_modified` do portal para evitar re-download de arquivos sem alteracao.
Por padrao, o `.zip` e removido apos extracao (`PORTAL_KEEP_ZIP=false`).

## Downloader CNES (base de dados)
- Script: `run_cnes_downloader.py`
- Fonte: `https://cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp`
- Regra: baixa apenas o arquivo mais recente da lista CNES.
- Destino dos arquivos:
  - Zip: `data/raw/external/cnes/`
  - Extraido: `data/raw/external/cnes/extracted/<yyyymm>/`
- Por padrao, o zip e removido apos extracao (`CNES_KEEP_ZIP=false`).

### Execucao unica
```bash
python run_cnes_downloader.py --extract-zip --check-interval-days 40
```

### Forcar verificacao imediata
```bash
python run_cnes_downloader.py --force --extract-zip --check-interval-days 40
```

### Modo periodico
```bash
python run_cnes_downloader.py --periodic --interval-minutes 60 --check-interval-days 40 --extract-zip
```

### Carga da dimensao CNES no banco (nomes das unidades)
```bash
python run_load_cnes_dim.py
```

Ou informando arquivo especifico:
```bash
python run_load_cnes_dim.py --file-path "C:\\caminho\\arquivo_cnes.csv"
```

Tabela criada/carregada:
- `saude.dim_cnes_estabelecimento`

## Backfill de JSON extraido (sem estourar memoria)
- Script: `run_json_backfill.py`
- Leitura streaming por chunks com `ijson`.
- Recomendado para arquivos grandes (ex.: `DENGBR25.json`).

Exemplo:
```bash
python run_json_backfill.py --year-start 2000 --year-end 2026 --chunk-size 100000
```

Por padrao, o script remove no banco os registros do ano antes de recarregar.

## Views analiticas para BI
Crie/atualize as views antes de abrir o dashboard:

```sql
\i sql/ddl/004_bi_views.sql
```

Views disponibilizadas:
- `saude.vw_dengue_ano`
- `saude.vw_dengue_mes_uf`
- `saude.vw_dengue_municipio_ano`

## Dimensao IBGE (municipios)
- Base compartilhada recomendada:
  `C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras\01_bases_compartilhadas\ibge\ibge_censo_2022_municipios_basico_br.csv`
- Copia local do projeto:
  `data/external/ibge/ibge_censo_2022_municipios_basico_br.csv`

Carregar dimensao:
```bash
python run_load_ibge_dim.py
```

DDL da dimensao:
- `sql/ddl/005_dim_ibge_municipio.sql`

## Build da camada analitica
Atualiza objetos analiticos (views + agregados):
```bash
python run_build_analytics.py
```

Objetos principais:
- `saude.fato_dengue_analitica` (view)
- `saude.dim_territorio` (view)
- `saude.agg_dengue_mensal` (tabela agregada)

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

## Identidade visual oficial (PMRO)
- Referencia: `docs/IDENTIDADE_VISUAL_PMRO.md`
- Logos oficiais no projeto: `assets/branding/Logo_prefeitura-RO/`
- Copia em base compartilhada: `../01_bases_compartilhadas/identidade_visual/Logo_prefeitura-RO/`

Paleta oficial aplicada no tema:
- Azul: `#004F80`
- Amarelo: `#DFA230`
- Branco: `#FFFFFF`
