param(
    [int]$StartYear = 2000,
    [int]$EndYear = (Get-Date).Year,
    [int]$BlockSize = 5,
    [int]$ChunkSize = 100000,
    [int]$DbChunkSize = 1000
)

$ErrorActionPreference = "Continue"

if ($StartYear -gt $EndYear) {
    throw "StartYear nao pode ser maior que EndYear."
}
if ($BlockSize -lt 1) {
    throw "BlockSize deve ser >= 1."
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$validationScript = Join-Path $projectRoot "run_sql_first_validation.py"
$logsDir = Join-Path $projectRoot "logs\ops"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$summaryCsv = Join-Path $logsDir "backfill_5anos_resumo_$timestamp.csv"
$summaryRows = @()
$totalBlocks = [Math]::Ceiling((($EndYear - $StartYear + 1) / [double]$BlockSize))
$completedBlocks = 0
$scriptStart = Get-Date

if (!(Test-Path $pythonExe)) {
    throw "Python da venv nao encontrado em: $pythonExe"
}
if (!(Test-Path $validationScript)) {
    throw "Script de validacao SQL-first nao encontrado: $validationScript"
}
if (!(Test-Path $logsDir)) {
    New-Item -Path $logsDir -ItemType Directory | Out-Null
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
    for ($blockEnd = $EndYear; $blockEnd -ge $StartYear; $blockEnd -= $BlockSize) {
        $blockStartAt = Get-Date
        $blockStart = [Math]::Max($StartYear, $blockEnd - $BlockSize + 1)
        $blockTag = "{0}_{1}" -f $blockStart, $blockEnd
        $blockLog = Join-Path $logsDir "backfill_bloco_$blockTag`_$timestamp.log"
        $blockValidationCsv = Join-Path $logsDir "validacao_sql_first_bloco_$blockTag`_$timestamp.csv"

        $blockStatus = "OK"
        $blockMessage = "Sucesso"

        $progressPct = [Math]::Round((($completedBlocks / [double]$totalBlocks) * 100), 1)
        "[$(Get-Date -Format s)] INICIO bloco $blockStart-$blockEnd ($($completedBlocks + 1)/$totalBlocks, $progressPct% concluido)" | Tee-Object -FilePath $blockLog -Append

        # 1) Downloader (JSON), com tentativa forcada se faltar extracao local.
        $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
            -Arguments "run_downloader.py --formats json --year-start $blockStart --year-end $blockEnd --extract-zip" `
            -LogFile $blockLog
        if ($exitCode -ne 0) {
            throw "Falha no downloader (bloco $blockTag, exit code $exitCode)"
        }

        $baseExtracted = Join-Path $projectRoot "data\raw\json\portal_sus\extracted"
        $missingExtracted = @()
        foreach ($year in $blockStart..$blockEnd) {
            $yearDir = Join-Path $baseExtracted $year
            $jsonCount = 0
            if (Test-Path $yearDir) {
                $jsonCount = (Get-ChildItem -Path $yearDir -File -Filter "*.json" -ErrorAction SilentlyContinue | Measure-Object).Count
            }
            if ($jsonCount -le 0) {
                $missingExtracted += $year
            }
        }
        if ($missingExtracted.Count -gt 0) {
            "[$(Get-Date -Format s)] Extracao ausente no bloco $blockTag para ano(s): $($missingExtracted -join ', '). Forcando re-download." | Tee-Object -FilePath $blockLog -Append
            $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
                -Arguments "run_downloader.py --formats json --year-start $blockStart --year-end $blockEnd --extract-zip --force" `
                -LogFile $blockLog
            if ($exitCode -ne 0) {
                throw "Falha no downloader forcado (bloco $blockTag, exit code $exitCode)"
            }
        }

        # 2) Backfill do bloco.
        $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
            -Arguments "run_json_backfill.py --year-start $blockStart --year-end $blockEnd --chunk-size $ChunkSize --db-chunksize $DbChunkSize --continue-on-error" `
            -LogFile $blockLog
        if ($exitCode -ne 0) {
            $blockStatus = "PARCIAL"
            $blockMessage = "Backfill com erro/parcial (exit code $exitCode)."
        }

        # 3) Validacao SQL-first por ano do bloco.
        $validationExit = Invoke-PythonCommand -PythonExe $pythonExe `
            -Arguments "run_sql_first_validation.py --year-start $blockStart --year-end $blockEnd --log-file ""$blockLog"" --output-csv ""$blockValidationCsv""" `
            -LogFile $blockLog
        if ($validationExit -ne 0) {
            $blockStatus = "PARCIAL"
            $blockMessage = "Validacao SQL-first com divergencia em pelo menos um ano."
        }

        # 4) Remove JSON/ZIP apenas dos anos com status OK na validacao.
        $deletedYears = @()
        $notDeletedYears = @()
        if (Test-Path $blockValidationCsv) {
            $validationRows = Import-Csv -Path $blockValidationCsv
            foreach ($row in $validationRows) {
                $year = [int]$row.ano
                $status = [string]$row.status
                if ($status -like "OK*") {
                    $yearDir = Join-Path $baseExtracted $year
                    if (Test-Path $yearDir) {
                        Get-ChildItem -Path $yearDir -File -Filter "*.json" -ErrorAction SilentlyContinue | ForEach-Object {
                            Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                        }
                    }
                    # Remove zips na raiz e na pasta downloads\{ano} (se existir).
                    $zipRoot = Join-Path $projectRoot "data\raw\json\portal_sus"
                    if (Test-Path $zipRoot) {
                        Get-ChildItem -Path $zipRoot -File -Filter ("dengue_{0}_*.json.zip" -f $year) -ErrorAction SilentlyContinue | ForEach-Object {
                            Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                        }
                    }
                    $zipYearDir = Join-Path $projectRoot ("data\raw\json\portal_sus\downloads\{0}" -f $year)
                    if (Test-Path $zipYearDir) {
                        Get-ChildItem -Path $zipYearDir -File -Filter "*.zip" -ErrorAction SilentlyContinue | ForEach-Object {
                            Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                        }
                    }
                    $deletedYears += $year
                }
                else {
                    $notDeletedYears += $year
                }
            }
        }
        else {
            $blockStatus = "PARCIAL"
            $blockMessage = "Relatorio de validacao nao encontrado."
        }

        $completedBlocks += 1
        $blockElapsedMin = [Math]::Round(((Get-Date) - $blockStartAt).TotalMinutes, 1)
        $elapsedMin = [Math]::Round(((Get-Date) - $scriptStart).TotalMinutes, 1)
        $avgPerBlock = if ($completedBlocks -gt 0) { $elapsedMin / $completedBlocks } else { 0 }
        $remainingBlocks = $totalBlocks - $completedBlocks
        $etaMin = [Math]::Round(($remainingBlocks * $avgPerBlock), 1)
        "[$(Get-Date -Format s)] FIM bloco $blockStart-$blockEnd | status=$blockStatus | duracao_min=$blockElapsedMin | progresso=$completedBlocks/$totalBlocks | eta_restante_min=$etaMin | deletados=$($deletedYears -join ',') | mantidos=$($notDeletedYears -join ',')" | Tee-Object -FilePath $blockLog -Append

        $summaryRows += [PSCustomObject]@{
            bloco              = "$blockStart-$blockEnd"
            status             = $blockStatus
            mensagem           = $blockMessage
            anos_deletados_ok  = ($deletedYears -join ";")
            anos_mantidos      = ($notDeletedYears -join ";")
            log_file           = $blockLog
            validation_csv     = $blockValidationCsv
            executado_em       = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        }
    }
}
finally {
    Pop-Location
}

$summaryRows | Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding UTF8
Write-Host "Resumo geral: $summaryCsv"
