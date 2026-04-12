# Checklist Producao - Dengue BI

## 1) Validacoes basicas
- Banco `dengue_bi` online e schema `saude` existente.
- `.venv` ativa e dependencias instaladas.
- Arquivos JSON extraidos em `data/raw/json/portal_sus/extracted/<ano>/`.

## 2) Rotina incremental (a cada 10 dias)
- Script pronto: `ops/run_daily_incremental.ps1` (runner usado pelo agendamento de 10 dias).
- Wrapper opcional: `ops/run_incremental_10dias.ps1`.
- Ele executa:
  - `run_downloader.py` (json, ano atual e anterior).
  - O downloader compara `metadata_modified` do portal e baixa apenas arquivos alterados.
  - `run_json_backfill.py` (janela recente, em chunks).
  - Remove JSON local apos carga bem-sucedida (mantem dados no banco e manifesto de controle).
  - log em `logs/ops/incremental_*.log`.

## 3) Agendamento no Windows Task Scheduler
1. Abrir `Task Scheduler` -> `Create Task`.
2. Nome: `DengueBI_DailyIncremental`.
3. Trigger: Daily + Recur every `10` days (ex.: 06:00).
4. Action:
   - Program/script: `powershell.exe`
   - Arguments:
     `-ExecutionPolicy Bypass -File "C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras\02_projetos_bi\dengue_ministerio_saude\ops\run_incremental_10dias.ps1"`
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

## 7) SMTP institucional (no-reply)
- Variaveis recomendadas:
  - `BI_NOTIFY_EMAIL=true`
  - `BI_NOTIFY_TO=raja.pmro@gmail.com;anjos.mauricio.ro@gmail.com`
  - `BI_SMTP_HOST=<smtp_institucional>`
  - `BI_SMTP_PORT=587`
  - `BI_SMTP_USE_SSL=true`
  - `BI_SMTP_AUTH=true` (ou `false` para relay interno sem usuario/senha)
  - `BI_SMTP_USER=<conta_smtp>`
  - `BI_SMTP_PASS=<senha>`
  - `BI_MAIL_FROM=no-reply@seu-dominio`
  - `BI_MAIL_REPLYTO=no-reply@seu-dominio`
- Teste isolado de envio:
  - `powershell.exe -ExecutionPolicy Bypass -File "...\ops\test_smtp.ps1"`
