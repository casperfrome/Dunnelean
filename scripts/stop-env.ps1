$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
docker compose --env-file (Join-Path $root 'deploy/.env') -f (Join-Path $root 'deploy/compose.yml') --profile test stop
if ($LASTEXITCODE -ne 0) { throw 'Stopping environment failed' }
Write-Host 'Containers stopped; persistent volumes retained.'
