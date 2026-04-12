param(
    [int[]]$Years = @(2008, 2013, 2014, 2015, 2018, 2019, 2020, 2021),
    [int]$ToleranceAbs = 7000,
    [double]$TolerancePct = 0.5
)

$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$logsDir = Join-Path $projectRoot "logs\ops"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

if (!(Test-Path $pythonExe)) {
    throw "Python da venv nao encontrado em: $pythonExe"
}
if (!(Test-Path $logsDir)) {
    New-Item -Path $logsDir -ItemType Directory | Out-Null
}

Set-Location $projectRoot

$summary = @()

foreach ($year in $Years) {
    $log = Get-ChildItem $logsDir -File -Filter ("reprocess_year_{0}_*.log" -f $year) -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($null -eq $log) {
        $log = Get-ChildItem $logsDir -File -Filter ("backfill_bloco_*_*.log") -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
    }

    if ($null -eq $log) {
        $summary += [PSCustomObject]@{
            ano = $year
            status = "SEM_LOG"
            acao = "MANTIDO"
            csv = ""
            log = ""
        }
        continue
    }

    $validationCsv = Join-Path $logsDir ("validacao_sql_first_tol_year_{0}_{1}.csv" -f $year, $timestamp)
    & $pythonExe "run_sql_first_validation.py" `
        --year-start $year `
        --year-end $year `
        --log-file $log.FullName `
        --output-csv $validationCsv `
        --tolerance-abs $ToleranceAbs `
        --tolerance-pct $TolerancePct

    $row = Import-Csv $validationCsv | Select-Object -First 1
    $status = [string]$row.status
    $acao = "MANTIDO"

    if ($status -like "OK*") {
        $yy = $year.ToString().Substring(2, 2)
        $jsonPath = Join-Path $projectRoot ("data\raw\json\portal_sus\extracted\{0}\DENGBR{1}.json" -f $year, $yy)
        if (Test-Path $jsonPath) {
            Remove-Item $jsonPath -Force -ErrorAction SilentlyContinue
        }

        $rawRoot = Join-Path $projectRoot "data\raw\json\portal_sus"
        if (Test-Path $rawRoot) {
            Get-ChildItem $rawRoot -File -Filter ("dengue_{0}_*" -f $year) -ErrorAction SilentlyContinue | ForEach-Object {
                Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
            }
        }

        $downloadsYear = Join-Path $projectRoot ("data\raw\json\portal_sus\downloads\{0}" -f $year)
        if (Test-Path $downloadsYear) {
            Get-ChildItem $downloadsYear -File -ErrorAction SilentlyContinue | ForEach-Object {
                Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
            }
        }
        $acao = "DELETADO_LOCAL"
    }

    $summary += [PSCustomObject]@{
        ano = $year
        status = $status
        acao = $acao
        csv = $validationCsv
        log = $log.FullName
    }
}

$summaryPath = Join-Path $logsDir ("cleanup_tolerancia_resumo_{0}.csv" -f $timestamp)
$summary | Export-Csv -Path $summaryPath -NoTypeInformation -Encoding UTF8
$summary | Format-Table -AutoSize
Write-Host "RESUMO: $summaryPath"
