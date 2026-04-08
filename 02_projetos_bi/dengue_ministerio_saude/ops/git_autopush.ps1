$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Push-Location $projectRoot
try {
    git add -A
    $hasChanges = git diff --cached --name-only
    if (-not $hasChanges) {
        Write-Host "Sem alteracoes para commit."
        exit 0
    }

    git commit -m "chore: atualizacao automatica pipeline ($timestamp)"
    git push origin main
    Write-Host "Commit/push automatico concluido."
}
finally {
    Pop-Location
}

