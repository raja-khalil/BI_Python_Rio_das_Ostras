param(
    [string]$RepoPath = "C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras",
    [string]$Branch = "main",
    [string]$MessagePrefix = "chore: auto-sync"
)

Set-Location $RepoPath

if (-not (Test-Path ".git")) {
    Write-Error "Repositorio git nao encontrado em $RepoPath"
    exit 1
}

git add -A
$status = git status --porcelain

if (-not $status) {
    Write-Output "Sem alteracoes para commit."
    exit 0
}

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$message = "$MessagePrefix ($timestamp)"

git commit -m $message
if ($LASTEXITCODE -ne 0) {
    Write-Error "Falha ao criar commit."
    exit 1
}

git push origin $Branch
if ($LASTEXITCODE -ne 0) {
    Write-Error "Falha no push. Verifique autenticacao no GitHub."
    exit 1
}

Write-Output "Commit e push concluídos com sucesso."
