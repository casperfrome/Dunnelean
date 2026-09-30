param([switch]$WithoutTestMysql)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$root = Split-Path $PSScriptRoot -Parent
$envFile = Join-Path $root 'deploy/.env'
if (-not (Test-Path -LiteralPath $envFile)) {
    $random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $secretBytes = New-Object byte[] 24
        $random.GetBytes($secretBytes)
        $rootPassword = [BitConverter]::ToString($secretBytes).Replace('-', '')
        $random.GetBytes($secretBytes)
        $testPassword = [BitConverter]::ToString($secretBytes).Replace('-', '')
    } finally { $random.Dispose() }
    [System.IO.File]::WriteAllText($envFile, "MYSQL_TEST_ROOT_PASSWORD=$rootPassword`nDUNNELEAN_TEST_PASSWORD=$testPassword`n", [System.Text.UTF8Encoding]::new($false))
}
$info = docker info --format '{{json .}}' | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw 'Docker is unavailable' }
if ($info.MemTotal -lt 11GB) { throw 'Docker needs at least 11 GiB memory for this development topology' }
if ($info.NCPU -lt 6) { throw 'Docker needs at least 6 logical CPUs for this topology' }
$beImage = 'apache/doris:be-4.1.4@sha256:08c139f3d0ec45090a5214fd76f19f84b871d05f1a2db83b5e35a85c193617b2'
$mapCount = docker run --rm --entrypoint cat $beImage /proc/sys/vm/max_map_count
if ([long]$mapCount -lt 2000000) { throw "vm.max_map_count=$mapCount; configure Docker/WSL vm.max_map_count >= 2000000 first" }
$disk = docker run --rm --entrypoint df $beImage -Pk / | Select-Object -Last 1
if ($LASTEXITCODE -ne 0) { throw 'Could not check Docker disk space' }
$availableKiB = [long](($disk.Trim() -split '\s+')[3])
if ($availableKiB -lt 8MB) { throw 'Docker needs at least 8 GiB of free disk space for test data and logs' }
$compose = @('compose', '--env-file', $envFile, '-f', (Join-Path $root 'deploy/compose.yml'))
if (-not $WithoutTestMysql) { $compose += @('--profile', 'test') }
& docker @compose up -d --wait --wait-timeout 300
if ($LASTEXITCODE -ne 0) { throw 'Doris environment failed to become healthy' }
Write-Host 'Doris FE/BE 4.1.4 ready. Existing MySQL containers were not changed.'
