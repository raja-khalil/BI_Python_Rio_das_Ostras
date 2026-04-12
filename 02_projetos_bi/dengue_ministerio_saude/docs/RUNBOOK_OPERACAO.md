# Runbook Operacional - BI Dengue (Rio das Ostras)

## 1) Rotina normal
- Incremental 10 dias: `ops/run_incremental_10dias.ps1`
- Build analitico (views, agregados, MVs): `run_build_analytics.py`
- Backup diario schema `saude`: `ops/run_backup_schema_saude.ps1`

## 2) Se falhar X, faca Y

### X: Falha de download no MS (rede/temporario)
- Sintoma: `failed > 0` no log do downloader.
- Y:
1. Reexecutar com forca:  
`python run_downloader.py --formats json --year-start 2025 --year-end 2026 --extract-zip --force`
2. Se sucesso, rodar backfill do ano afetado.

### X: Falha no backfill JSON -> SQL
- Sintoma: erro no `run_json_backfill.py` ou divergencia SQL-first.
- Y:
1. Validar arquivo local em `data/raw/json/portal_sus/extracted/<ano>/`.
2. Reprocessar somente o ano:  
`python run_json_backfill.py --year-start 2026 --year-end 2026 --continue-on-error`
3. Validar:  
`python run_sql_first_validation.py --year-start 2026 --year-end 2026`

### X: Falha em agg_dengue_mensal por chave duplicada
- Sintoma: `UniqueViolation agg_dengue_mensal_pkey`.
- Y:
1. Nao bloquear operacao do painel (MVs continuam atualizando).
2. Abrir incidente de dados para deduplicacao de `fato_dengue_analitica`.
3. Reprocessar agregacao apos correcao.

### X: Painel lento em filtros
- Sintoma: consultas demoradas no Streamlit.
- Y:
1. Rodar build analitico para refrescar MVs:  
`python run_build_analytics.py`
2. Verificar se MVs existem em `saude`:
   - `mv_painel1_mes_uf_classif`
   - `mv_painel1_2_mes_municipio_rj`
   - `mv_painel2_mes_unidade_municipio_rj`

### X: Email de status nao envia
- Sintoma: SMTP `Authentication Required`.
- Y:
1. Confirmar credenciais SMTP institucionais.
2. Testar isolado:  
`powershell -ExecutionPolicy Bypass -File ops/test_smtp.ps1`
3. Se relay sem auth: `BI_SMTP_AUTH=false`.

## 3) Backups
- Script: `ops/run_backup_schema_saude.ps1`
- Politica:
  - ate 7 dias: diarios
  - de 8 a 30 dias: somente domingo
  - >30 dias: remove

## 4) Agendamento recomendado
- Incremental 10 dias (com retry no Task Scheduler: 30 min, 3 tentativas).
- Healthcheck diario curto.
- Backup diario do schema `saude`.

## 5) Verificacao rapida de saude
1. Ultimo log incremental em `logs/ops`.
2. Ultimo CSV SQL-first em `logs/ops/validacao_sql_first_*.csv`.
3. Abrir dashboard e validar cards principais do painel 1.
