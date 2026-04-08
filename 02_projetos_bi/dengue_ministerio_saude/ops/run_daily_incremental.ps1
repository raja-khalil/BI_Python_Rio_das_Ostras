$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $projectRoot "logs\ops"
$logFile = Join-Path $logDir "daily_incremental_$timestamp.log"

if (!(Test-Path $pythonExe)) {
    throw "Python da venv nao encontrado em: $pythonExe"
}

if (!(Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory | Out-Null
}

Push-Location $projectRoot
try {
    "[$(Get-Date -Format s)] Inicio execucao diaria incremental" | Tee-Object -FilePath $logFile -Append

    $yearStart = (Get-Date).Year - 1
    $yearEnd = (Get-Date).Year

    # 1) Atualiza artefatos JSON recentes do portal (ano atual e anterior).
    & $pythonExe run_downloader.py --formats json --year-start $yearStart --year-end $yearEnd --extract-zip 2>&1 `
        | Tee-Object -FilePath $logFile -Append

    # 2) Recarrega banco para janela recente (ano atual e anterior).
    & $pythonExe run_json_backfill.py --year-start $yearStart --year-end $yearEnd --chunk-size 10000 --db-chunksize 200 --continue-on-error 2>&1 `
        | Tee-Object -FilePath $logFile -Append

    "[$(Get-Date -Format s)] Fim execucao diaria incremental (sucesso)" | Tee-Object -FilePath $logFile -Append
}
finally {
    Pop-Location
}
