$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$checkScript = Join-Path $projectRoot "run_check_campos_criticos.py"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $projectRoot "logs\ops"
$logFile = Join-Path $logDir "daily_incremental_$timestamp.log"
$checkCsv = Join-Path $logDir "check_campos_criticos_rio_das_ostras_$timestamp.csv"

if (!(Test-Path $pythonExe)) {
    throw "Python da venv nao encontrado em: $pythonExe"
}
if (!(Test-Path $checkScript)) {
    throw "Script de check nao encontrado em: $checkScript"
}

if (!(Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory | Out-Null
}

function Invoke-PythonCommand {
    param(
        [string]$PythonExe,
        [string]$Arguments,
        [string]$LogFile
    )

    $stdoutFile = [System.IO.Path]::GetTempFileName()
    $stderrFile = [System.IO.Path]::GetTempFileName()

    try {
        $proc = Start-Process -FilePath $PythonExe `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -Wait `
            -PassThru `
            -RedirectStandardOutput $stdoutFile `
            -RedirectStandardError $stderrFile

        if (Test-Path $stdoutFile) {
            Get-Content $stdoutFile | Tee-Object -FilePath $LogFile -Append | Out-Null
        }
        if (Test-Path $stderrFile) {
            Get-Content $stderrFile | Tee-Object -FilePath $LogFile -Append | Out-Null
        }

        return [int]$proc.ExitCode
    }
    finally {
        Remove-Item $stdoutFile, $stderrFile -ErrorAction SilentlyContinue
    }
}

Push-Location $projectRoot
try {
    "[$(Get-Date -Format s)] Inicio execucao diaria incremental" | Tee-Object -FilePath $logFile -Append

    $yearStart = (Get-Date).Year - 1
    $yearEnd = (Get-Date).Year

    # 1) Atualiza artefatos JSON recentes do portal (ano atual e anterior).
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_downloader.py --formats json --year-start $yearStart --year-end $yearEnd --extract-zip" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no downloader (exit code: $exitCode)"
    }

    # 2) Recarrega banco para janela recente (ano atual e anterior).
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_json_backfill.py --year-start $yearStart --year-end $yearEnd --chunk-size 10000 --db-chunksize 200 --continue-on-error" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no backfill (exit code: $exitCode)"
    }

    # 3) Check unico de campos criticos e exportacao CSV para auditoria.
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_check_campos_criticos.py --municipio ""Rio das Ostras"" --uf RJ --output-csv ""$checkCsv""" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no check de campos criticos (exit code: $exitCode)"
    }
    "[$(Get-Date -Format s)] Check de campos criticos concluido: $checkCsv" | Tee-Object -FilePath $logFile -Append

    if (Test-Path $checkCsv) {
        $checkRows = Import-Csv -Path $checkCsv
        $okCount = ($checkRows | Where-Object { $_.status -eq "OK" } | Measure-Object).Count
        $pendingCount = ($checkRows | Where-Object { $_.status -ne "OK" } | Measure-Object).Count
        $totalCount = ($checkRows | Measure-Object).Count
        "[$(Get-Date -Format s)] Resumo check campos criticos: total=$totalCount ok=$okCount pendente=$pendingCount" | Tee-Object -FilePath $logFile -Append
    }
    else {
        "[$(Get-Date -Format s)] Aviso: CSV do check nao encontrado para resumo ($checkCsv)." | Tee-Object -FilePath $logFile -Append
    }

    "[$(Get-Date -Format s)] Fim execucao diaria incremental (sucesso)" | Tee-Object -FilePath $logFile -Append
}
finally {
    Pop-Location
}
