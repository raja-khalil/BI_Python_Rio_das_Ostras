$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot "run_daily_incremental.ps1"

if (!(Test-Path $runner)) {
    throw "Runner incremental nao encontrado: $runner"
}

Push-Location $projectRoot
try {
    & $runner
}
finally {
    Pop-Location
}

