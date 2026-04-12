$ErrorActionPreference = "Continue"

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
$checkScript = Join-Path $projectRoot "run_check_campos_criticos.py"
$validationScript = Join-Path $projectRoot "run_sql_first_validation.py"
$buildAnalyticsScript = Join-Path $projectRoot "run_build_analytics.py"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $projectRoot "logs\ops"
$logFile = Join-Path $logDir "incremental_$timestamp.log"
$checkCsv = Join-Path $logDir "check_campos_criticos_rio_das_ostras_$timestamp.csv"
$validationCsv = Join-Path $logDir "validacao_sql_first_$timestamp.csv"
$cleanupAfterValidatedLoad = $true
$notifyEmailEnabled = ($env:BI_NOTIFY_EMAIL -eq "1" -or $env:BI_NOTIFY_EMAIL -eq "true")
$notifyTo = if ($env:BI_NOTIFY_TO) { $env:BI_NOTIFY_TO } else { "raja.pmro@gmail.com;anjos.mauricio.ro@gmail.com" }
$runStatus = "SUCESSO"
$runMessage = ""
$downloaderSummary = ""

if (!(Test-Path $pythonExe)) {
    throw "Python da venv nao encontrado em: $pythonExe"
}
if (!(Test-Path $checkScript)) {
    throw "Script de check nao encontrado em: $checkScript"
}
if (!(Test-Path $validationScript)) {
    throw "Script de validacao SQL-first nao encontrado em: $validationScript"
}
if (!(Test-Path $buildAnalyticsScript)) {
    throw "Script de build analitico nao encontrado em: $buildAnalyticsScript"
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

function Send-StatusEmail {
    param(
        [string]$Status,
        [string]$Message,
        [string]$LogFile,
        [string]$CheckCsv,
        [string]$DownloaderSummary,
        [string]$ValidationCsv
    )

    if (-not $notifyEmailEnabled) {
        return
    }

    $smtpHost = $env:BI_SMTP_HOST
    $smtpPort = if ($env:BI_SMTP_PORT) { [int]$env:BI_SMTP_PORT } else { 587 }
    $smtpUser = $env:BI_SMTP_USER
    $smtpPass = $env:BI_SMTP_PASS
    $mailFrom = if ($env:BI_MAIL_FROM) { $env:BI_MAIL_FROM } else { $smtpUser }
    $mailReplyTo = if ($env:BI_MAIL_REPLYTO) { $env:BI_MAIL_REPLYTO } else { $mailFrom }
    $smtpUseSsl = if ($env:BI_SMTP_USE_SSL) { $env:BI_SMTP_USE_SSL } else { "true" }
    $enableSsl = ($smtpUseSsl -eq "1" -or $smtpUseSsl -eq "true")
    $smtpAuth = if ($env:BI_SMTP_AUTH) { $env:BI_SMTP_AUTH } else { "true" }
    $useAuth = ($smtpAuth -eq "1" -or $smtpAuth -eq "true")

    if ([string]::IsNullOrWhiteSpace($smtpHost) -or [string]::IsNullOrWhiteSpace($mailFrom)) {
        "[$(Get-Date -Format s)] Notificacao por email habilitada, mas SMTP nao configurado." | Tee-Object -FilePath $logFile -Append
        return
    }
    if ($useAuth -and ([string]::IsNullOrWhiteSpace($smtpUser) -or [string]::IsNullOrWhiteSpace($smtpPass))) {
        "[$(Get-Date -Format s)] SMTP com autenticacao exige BI_SMTP_USER e BI_SMTP_PASS." | Tee-Object -FilePath $logFile -Append
        return
    }
    if ($useAuth -and $smtpUser -ne $mailFrom) {
        "[$(Get-Date -Format s)] Aviso SMTP: BI_SMTP_USER e BI_MAIL_FROM diferentes. Alguns provedores bloqueiam envio." | Tee-Object -FilePath $logFile -Append
    }

    $toList = $notifyTo -split "[;,]" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
    if ($toList.Count -eq 0) {
        "[$(Get-Date -Format s)] Notificacao por email sem destinatarios validos." | Tee-Object -FilePath $logFile -Append
        return
    }

    $subject = "[Dengue BI] Atualizacao incremental - $Status - $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
    $body = @"
Status: $Status
Data/Hora: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
Projeto: dengue_ministerio_saude

Resumo downloader:
$DownloaderSummary

Mensagem:
$Message

Log:
$LogFile

CSV check campos criticos:
$CheckCsv

CSV validacao SQL-first:
$ValidationCsv

Aviso: este e um email automatico. Nao responda.
"@

    try {
        $mail = $null
        $client = $null
        try {
            $mail = New-Object System.Net.Mail.MailMessage
            $mail.From = New-Object System.Net.Mail.MailAddress($mailFrom)
            foreach ($to in $toList) {
                $mail.To.Add($to) | Out-Null
            }
            if (-not [string]::IsNullOrWhiteSpace($mailReplyTo)) {
                $mail.ReplyToList.Add($mailReplyTo) | Out-Null
            }
            $mail.Subject = $subject
            $mail.SubjectEncoding = [System.Text.Encoding]::UTF8
            $mail.Body = $body
            $mail.BodyEncoding = [System.Text.Encoding]::UTF8
            $mail.IsBodyHtml = $false
            $mail.Headers.Add("X-Auto-Response-Suppress", "All")

            $client = New-Object System.Net.Mail.SmtpClient($smtpHost, $smtpPort)
            $client.EnableSsl = $enableSsl
            if ($useAuth) {
                $client.Credentials = New-Object System.Net.NetworkCredential($smtpUser, $smtpPass)
            } else {
                $client.UseDefaultCredentials = $false
            }
            $client.Send($mail)
        }
        finally {
            if ($mail -ne $null) { $mail.Dispose() }
            if ($client -ne $null) { $client.Dispose() }
        }
        "[$(Get-Date -Format s)] Notificacao por email enviada para: $($toList -join ', ')" | Tee-Object -FilePath $logFile -Append
    }
    catch {
        "[$(Get-Date -Format s)] Falha ao enviar email: $($_.Exception.Message)" | Tee-Object -FilePath $logFile -Append
    }
}

Push-Location $projectRoot
try {
    "[$(Get-Date -Format s)] Inicio execucao incremental (janela recente)" | Tee-Object -FilePath $logFile -Append

    $yearStart = (Get-Date).Year - 1
    $yearEnd = (Get-Date).Year

    # 1) Atualiza artefatos JSON recentes do portal (ano atual e anterior).
    # O downloader valida metadata_modified e baixa apenas alterados.
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_downloader.py --formats json --year-start $yearStart --year-end $yearEnd --extract-zip" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no downloader (exit code: $exitCode)"
    }
    $downloaderLine = Select-String -Path $logFile -Pattern "Resumo downloader:" | Select-Object -Last 1
    if ($downloaderLine) {
        $downloaderSummary = $downloaderLine.Line
        $failedMatch = [regex]::Match($downloaderSummary, "failed['""]?\s*:\s*(\d+)")
        if ($failedMatch.Success -and [int]$failedMatch.Groups[1].Value -gt 0) {
            $runStatus = "PARCIAL"
        }
    }

    # 1.1) SQL-first: se extracao local estiver ausente, forca re-download.
    $baseExtracted = Join-Path $projectRoot "data\raw\json\portal_sus\extracted"
    $missingExtracted = @()
    foreach ($year in @($yearStart, $yearEnd)) {
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
        "[$(Get-Date -Format s)] Extracao ausente para ano(s): $($missingExtracted -join ', '). Forcando re-download." | Tee-Object -FilePath $logFile -Append
        $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
            -Arguments "run_downloader.py --formats json --year-start $yearStart --year-end $yearEnd --extract-zip --force" `
            -LogFile $logFile
        if ($exitCode -ne 0) {
            throw "Falha no downloader forcado (exit code: $exitCode)"
        }
    }

    # 2) Recarrega banco para janela recente (ano atual e anterior).
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_json_backfill.py --year-start $yearStart --year-end $yearEnd --chunk-size 10000 --db-chunksize 200 --continue-on-error" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no backfill (exit code: $exitCode)"
    }

    # 2.0) Atualiza camada analitica e MVs de performance (paineis 1 e 2).
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_build_analytics.py" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        throw "Falha no build analitico/materialized views (exit code: $exitCode)"
    }

    # 2.1) Validacao SQL-first: compara linhas carregadas (log) x linhas no banco por ano.
    $exitCode = Invoke-PythonCommand -PythonExe $pythonExe `
        -Arguments "run_sql_first_validation.py --year-start $yearStart --year-end $yearEnd --log-file ""$logFile"" --output-csv ""$validationCsv""" `
        -LogFile $logFile
    if ($exitCode -ne 0) {
        $runStatus = "PARCIAL"
        "[$(Get-Date -Format s)] Aviso: validacao SQL-first apontou divergencia. Limpeza local bloqueada." | Tee-Object -FilePath $logFile -Append
    }

    # 2.2) SQL-first: remove locais somente se validacao estiver OK.
    if ($cleanupAfterValidatedLoad -and $exitCode -eq 0) {
        foreach ($year in @($yearStart, $yearEnd)) {
            $yearDir = Join-Path $baseExtracted $year
            if (Test-Path $yearDir) {
                Get-ChildItem -Path $yearDir -File -Filter "*.json" -ErrorAction SilentlyContinue | ForEach-Object {
                    Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                }
                "[$(Get-Date -Format s)] JSONs locais removidos apos validacao SQL-first: $yearDir" | Tee-Object -FilePath $logFile -Append
            }
            $zipDir = Join-Path $projectRoot "data\raw\json\portal_sus"
            if (Test-Path $zipDir) {
                Get-ChildItem -Path $zipDir -File -Filter ("dengue_{0}_*.json.zip" -f $year) -ErrorAction SilentlyContinue | ForEach-Object {
                    Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
                    "[$(Get-Date -Format s)] Zip local removido apos validacao SQL-first: $($_.FullName)" | Tee-Object -FilePath $logFile -Append
                }
            }
        }
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
        if ($pendingCount -gt 0 -and $runStatus -eq "SUCESSO") {
            $runStatus = "PARCIAL"
        }
    }
    else {
        "[$(Get-Date -Format s)] Aviso: CSV do check nao encontrado para resumo ($checkCsv)." | Tee-Object -FilePath $logFile -Append
    }

    if (Test-Path $validationCsv) {
        "[$(Get-Date -Format s)] Validacao SQL-first concluida: $validationCsv" | Tee-Object -FilePath $logFile -Append
    } else {
        $runStatus = "PARCIAL"
        "[$(Get-Date -Format s)] Aviso: relatorio de validacao SQL-first nao encontrado ($validationCsv)." | Tee-Object -FilePath $logFile -Append
    }

    "[$(Get-Date -Format s)] Fim execucao incremental (sucesso)" | Tee-Object -FilePath $logFile -Append
    $runMessage = "Rotina concluida. Status final: $runStatus."
}
catch {
    $runStatus = "ERRO"
    $runMessage = $_.Exception.Message
    "[$(Get-Date -Format s)] ERRO na execucao incremental: $runMessage" | Tee-Object -FilePath $logFile -Append
}
finally {
    Send-StatusEmail -Status $runStatus -Message $runMessage -LogFile $logFile -CheckCsv $checkCsv -DownloaderSummary $downloaderSummary -ValidationCsv $validationCsv
    Pop-Location
}

if ($runStatus -eq "ERRO") {
    throw "Execucao incremental finalizada com erro. Consulte o log: $logFile"
}
