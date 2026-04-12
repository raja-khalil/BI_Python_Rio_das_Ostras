param(
    [int[]]$Years = @(2021, 2020, 2019, 2018, 2015, 2014, 2013),
    [int]$ChunkSize = 100000,
    [int]$DbChunkSize = 1000
)

$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$logsDir = Join-Path $projectRoot "logs\ops"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$totalYears = $Years.Count
$doneYears = 0
$scriptStart = Get-Date

if (!(Test-Path $pythonExe)) {
    throw "Python nao encontrado em: $pythonExe"
}
if (!(Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

Set-Location $projectRoot

foreach ($year in $Years) {
    $yearStartAt = Get-Date
    $logFile = Join-Path $logsDir ("reprocess_year_{0}_{1}.log" -f $year, $timestamp)
    $validationCsv = Join-Path $logsDir ("validacao_sql_first_year_{0}_{1}.csv" -f $year, $timestamp)

    "[$(Get-Date -Format s)] INICIO ano $year" | Tee-Object -FilePath $logFile -Append

    & $pythonExe "run_downloader.py" --formats json --year-start $year --year-end $year --extract-zip --force 2>&1 |
        Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        "[$(Get-Date -Format s)] ERRO downloader ano $year" | Tee-Object -FilePath $logFile -Append
        continue
    }

    & $pythonExe "run_json_backfill.py" --year-start $year --year-end $year --chunk-size $ChunkSize --db-chunksize $DbChunkSize 2>&1 |
        Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        "[$(Get-Date -Format s)] ERRO backfill ano $year" | Tee-Object -FilePath $logFile -Append
        continue
    }

    & $pythonExe "run_sql_first_validation.py" --year-start $year --year-end $year --log-file $logFile --output-csv $validationCsv 2>&1 |
        Tee-Object -FilePath $logFile -Append
    $validationExit = $LASTEXITCODE

    if ($validationExit -eq 0 -and (Test-Path $validationCsv)) {
        $row = Import-Csv -Path $validationCsv | Select-Object -First 1
        if ($row.status -like "OK*") {
            $yy = $year.ToString().Substring(2, 2)
            $jsonPath = Join-Path $projectRoot ("data\raw\json\portal_sus\extracted\{0}\DENGBR{1}.json" -f $year, $yy)
            if (Test-Path $jsonPath) {
                Remove-Item $jsonPath -Force
                "[$(Get-Date -Format s)] LIMPEZA OK ano ${year}: $jsonPath" | Tee-Object -FilePath $logFile -Append
            }
            "[$(Get-Date -Format s)] RESULTADO ano ${year}: OK" | Tee-Object -FilePath $logFile -Append
        }
        else {
            "[$(Get-Date -Format s)] RESULTADO ano ${year}: DIVERGENTE (json mantido)" | Tee-Object -FilePath $logFile -Append
        }
    }
    else {
        "[$(Get-Date -Format s)] RESULTADO ano ${year}: FALHA VALIDACAO (json mantido)" | Tee-Object -FilePath $logFile -Append
    }

    "[$(Get-Date -Format s)] FIM ano $year" | Tee-Object -FilePath $logFile -Append
    $doneYears += 1
    $elapsedMin = [Math]::Round(((Get-Date) - $scriptStart).TotalMinutes, 1)
    $yearElapsedMin = [Math]::Round(((Get-Date) - $yearStartAt).TotalMinutes, 1)
    $avgPerYear = if ($doneYears -gt 0) { $elapsedMin / $doneYears } else { 0 }
    $remaining = $totalYears - $doneYears
    $etaMin = [Math]::Round(($remaining * $avgPerYear), 1)
    "[$(Get-Date -Format s)] PROGRESSO anos=$doneYears/$totalYears | duracao_ano_min=$yearElapsedMin | eta_restante_min=$etaMin" | Tee-Object -FilePath $logFile -Append
}

"[$(Get-Date -Format s)] FIM reprocessamento divergentes" | Write-Output
