$ErrorActionPreference = "Stop"

$smtpHost = $env:BI_SMTP_HOST
$smtpPort = if ($env:BI_SMTP_PORT) { [int]$env:BI_SMTP_PORT } else { 587 }
$smtpUser = $env:BI_SMTP_USER
$smtpPass = $env:BI_SMTP_PASS
$mailFrom = if ($env:BI_MAIL_FROM) { $env:BI_MAIL_FROM } else { $smtpUser }
$mailReplyTo = if ($env:BI_MAIL_REPLYTO) { $env:BI_MAIL_REPLYTO } else { $mailFrom }
$notifyTo = if ($env:BI_NOTIFY_TO) { $env:BI_NOTIFY_TO } else { "" }
$smtpUseSsl = if ($env:BI_SMTP_USE_SSL) { $env:BI_SMTP_USE_SSL } else { "true" }
$enableSsl = ($smtpUseSsl -eq "1" -or $smtpUseSsl -eq "true")
$smtpAuth = if ($env:BI_SMTP_AUTH) { $env:BI_SMTP_AUTH } else { "true" }
$useAuth = ($smtpAuth -eq "1" -or $smtpAuth -eq "true")

if ([string]::IsNullOrWhiteSpace($smtpHost) -or [string]::IsNullOrWhiteSpace($mailFrom)) {
    throw "SMTP nao configurado. Defina BI_SMTP_HOST e BI_MAIL_FROM."
}
if ($useAuth -and ([string]::IsNullOrWhiteSpace($smtpUser) -or [string]::IsNullOrWhiteSpace($smtpPass))) {
    throw "SMTP com autenticacao exige BI_SMTP_USER e BI_SMTP_PASS."
}

$toList = $notifyTo -split "[;,]" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
if ($toList.Count -eq 0) {
    throw "Sem destinatarios. Defina BI_NOTIFY_TO com 1 ou mais emails."
}

$subject = "[Dengue BI] TESTE SMTP - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
$body = @"
Teste SMTP executado com sucesso.

Servidor: $smtpHost:$smtpPort
SSL: $enableSsl
Autenticacao: $useAuth
From: $mailFrom
Reply-To: $mailReplyTo
To: $($toList -join ', ')

Aviso: este e um email automatico. Nao responda.
"@

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
    Write-Output "OK: email de teste enviado para $($toList -join ', ')"
}
finally {
    if ($mail -ne $null) { $mail.Dispose() }
    if ($client -ne $null) { $client.Dispose() }
}
