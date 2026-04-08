# Checklist Producao - Dengue BI

## 1) Validacoes basicas
- Banco `dengue_bi` online e schema `saude` existente.
- `.venv` ativa e dependencias instaladas.
- Arquivos JSON extraidos em `data/raw/json/portal_sus/extracted/<ano>/`.

## 2) Rotina diaria de ingestao
- Script pronto: `ops/run_daily_incremental.ps1`.
- Ele executa:
  - `run_downloader.py` (json, ano atual e anterior).
  - `run_json_backfill.py` (janela recente, em chunks).
  - log em `logs/ops/daily_incremental_*.log`.

## 3) Agendamento no Windows Task Scheduler
1. Abrir `Task Scheduler` -> `Create Task`.
2. Nome: `DengueBI_DailyIncremental`.
3. Trigger: Daily (ex.: 06:00).
4. Action:
   - Program/script: `powershell.exe`
   - Arguments:
     `-ExecutionPolicy Bypass -File "C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras\02_projetos_bi\dengue_ministerio_saude\ops\run_daily_incremental.ps1"`
5. Marcar "Run whether user is logged on or not".
6. Marcar "Run with highest privileges".

## 4) Monitoramento de falhas
- Revisar `logs/ops/` diariamente.
- Consultas de saude no banco:
```sql
SELECT EXTRACT(YEAR FROM data_notificacao) AS ano, COUNT(*) AS qtd
FROM saude.fato_dengue_casos
GROUP BY 1 ORDER BY 1;

SELECT COUNT(*) AS datas_nulas
FROM saude.fato_dengue_casos
WHERE data_notificacao IS NULL;
```

## 5) Versionamento automatico (opcional)
- Script pronto: `ops/git_autopush.ps1`.
- Recomendacao: agendar apos a carga diaria (ex.: 06:45).
- Cuidado: isso envia tudo que mudou para `main`. Use so se esse fluxo for desejado.

## 6) Dashboard
- Start manual:
  `.\.venv\Scripts\python.exe -m streamlit run app\main.py`
- URL local (porta dinamica): exibida no terminal.

