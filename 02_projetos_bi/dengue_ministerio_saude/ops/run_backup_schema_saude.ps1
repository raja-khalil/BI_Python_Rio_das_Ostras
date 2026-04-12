$ErrorActionPreference = "Stop"

param(
    [string]$Database = "dengue_bi",
    [string]$Host = "localhost",
    [int]$Port = 5432,
    [string]$User = "postgres",
    [string]$Schema = "saude",
    [int]$KeepDailyDays = 7,
    [int]$KeepExtendedDays = 30
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupRoot = Join-Path $projectRoot "backups"
$dailyDir = Join-Path $backupRoot "daily"
$logDir = Join-Path $projectRoot "logs\ops"
$logFile = Join-Path $logDir "backup_schema_saude_$timestamp.log"

if (!(Test-Path $dailyDir)) { New-Item -Path $dailyDir -ItemType Directory -Force | Out-Null }
if (!(Test-Path $logDir)) { New-Item -Path $logDir -ItemType Directory -Force | Out-Null }

function Write-Log([string]$msg) {
    "[$(Get-Date -Format s)] $msg" | Tee-Object -FilePath $logFile -Append
}

function Resolve-PgDumpPath {
    if ($env:BI_PGDUMP_PATH -and (Test-Path $env:BI_PGDUMP_PATH)) {
        return $env:BI_PGDUMP_PATH
    }
    $cmd = Get-Command pg_dump -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $defaultPaths = @(
        "C:\Program Files\PostgreSQL\16\bin\pg_dump.exe",
        "C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
        "C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
        "C:\Program Files\PostgreSQL\13\bin\pg_dump.exe"
    )
    foreach ($path in $defaultPaths) {
        if (Test-Path $path) { return $path }
    }
    throw "pg_dump nao encontrado. Configure BI_PGDUMP_PATH ou adicione PostgreSQL\\bin ao PATH."
}

$pgDumpExe = Resolve-PgDumpPath
$backupFile = Join-Path $dailyDir ("{0}_{1}.dump" -f $Schema, $timestamp)
$shaFile = "$backupFile.sha256"

Write-Log "Inicio backup schema=$Schema database=$Database host=$Host port=$Port"

if ($env:BI_DB_PASSWORD) {
    $env:PGPASSWORD = $env:BI_DB_PASSWORD
}

try {
    & $pgDumpExe `
        --host $Host `
        --port $Port `
        --username $User `
        --format custom `
        --schema $Schema `
        --file $backupFile `
        $Database 2>&1 | Tee-Object -FilePath $logFile -Append | Out-Null

    if (!(Test-Path $backupFile)) {
        throw "Backup nao foi gerado: $backupFile"
    }

    $hash = Get-FileHash -Path $backupFile -Algorithm SHA256
    "{0}  {1}" -f $hash.Hash, (Split-Path $backupFile -Leaf) | Set-Content -Path $shaFile -Encoding UTF8
    Write-Log "Backup criado: $backupFile"
    Write-Log "Hash SHA256: $($hash.Hash)"

    $now = Get-Date
    $allBackups = Get-ChildItem -Path $dailyDir -File -Filter "*.dump" -ErrorAction SilentlyContinue

    foreach ($file in $allBackups) {
        $ageDays = ($now - $file.LastWriteTime).TotalDays
        if ($ageDays -gt $KeepExtendedDays) {
            Remove-Item $file.FullName -Force -ErrorAction SilentlyContinue
            Remove-Item "$($file.FullName).sha256" -Force -ErrorAction SilentlyContinue
            Write-Log "Removido (> ${KeepExtendedDays} dias): $($file.Name)"
            continue
        }
        if ($ageDays -gt $KeepDailyDays) {
            # Entre 8 e 30 dias: manter apenas snapshots de domingo.
            if ($file.LastWriteTime.DayOfWeek -ne [System.DayOfWeek]::Sunday) {
                Remove-Item $file.FullName -Force -ErrorAction SilentlyContinue
                Remove-Item "$($file.FullName).sha256" -Force -ErrorAction SilentlyContinue
                Write-Log "Removido (retencao 7/30): $($file.Name)"
            }
        }
    }

    Write-Log "Fim backup com sucesso."
}
catch {
    Write-Log "ERRO backup: $($_.Exception.Message)"
    throw
}
finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}
