# Versionamento no GitHub

## Repositorio
- URL: `https://github.com/raja-khalil/BI_Python_Rio_das_Ostras.git`
- Raiz versionada: `C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras`

## Fluxo base
1. Criar/alterar arquivos.
2. Commit e push manual:
   ```powershell
   cd C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras
   git add -A
   git commit -m "feat: descricao da alteracao"
   git push origin main
   ```

## Automacao local (opcional)
Script: `00_admin/scripts/auto_commit_push.ps1`

Execucao manual:
```powershell
powershell -ExecutionPolicy Bypass -File C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras\00_admin\scripts\auto_commit_push.ps1
```

## Agendamento automatico no Windows
Exemplo (a cada 15 minutos):
```powershell
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-ExecutionPolicy Bypass -File C:\Users\Administrador\Documents\BI_Python\Rio-das-Ostras\00_admin\scripts\auto_commit_push.ps1"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 15)
Register-ScheduledTask -TaskName "BI_RioDasOstras_AutoGitPush" -Action $action -Trigger $trigger -Description "Auto commit e push do projeto BI Rio das Ostras"
```

Para remover:
```powershell
Unregister-ScheduledTask -TaskName "BI_RioDasOstras_AutoGitPush" -Confirm:$false
```

## Observacoes
- O push requer autenticacao no GitHub (PAT ou Git Credential Manager).
- Evite auto-push de arquivos sensiveis. O `.gitignore` ja bloqueia `.env` e dados brutos/processados.
