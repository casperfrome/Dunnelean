param([string]$Config = 'dunnelean.toml', [switch]$DebugBuild)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$root = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $root 'deploy/.env'
if (Test-Path -LiteralPath $envFile) {
    Get-Content -LiteralPath $envFile -Encoding UTF8 | ForEach-Object {
        if ($_ -match '^DUNNELEAN_TEST_PASSWORD=(.*)$') { $env:DUNNELEAN_TEST_PASSWORD = $Matches[1] }
    }
}
$profile = if ($DebugBuild) { 'debug' } else { 'release' }
$binary = Join-Path $root "target/$profile/dunnelean.exe"
if (-not (Test-Path -LiteralPath $binary)) { throw "Build first: cargo build --release (or cargo build for -DebugBuild)" }
Push-Location $root
try { & $binary serve --config $Config } finally { Pop-Location }
