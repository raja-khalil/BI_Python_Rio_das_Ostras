$ErrorActionPreference = "Stop"

param(
    [string[]]$TaskNames = @(
        "BI_Dengue_Incremental_10dias",
        "BI_Dengue_Healthcheck_Diario",
        "BI_Dengue_Backup_Diario"
    ),
    [int]$RetryCount = 3,
    [int]$RetryMinutes = 30
)

foreach ($taskName in $TaskNames) {
    try {
        $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
        $settings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -RestartCount $RetryCount `
            -RestartInterval (New-TimeSpan -Minutes $RetryMinutes)

        Set-ScheduledTask -TaskName $taskName -Settings $settings | Out-Null
        Write-Host "Retry configurado: $taskName (tentativas=$RetryCount, intervalo=${RetryMinutes}min)"
    }
    catch {
        Write-Warning "Nao foi possivel atualizar tarefa '$taskName': $($_.Exception.Message)"
    }
}
