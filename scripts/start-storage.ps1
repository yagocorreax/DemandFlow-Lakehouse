param(
    [switch]$Bootstrap
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    Write-Host "Iniciando somente AIStor e aguardando prontidao..."
    docker compose up -d --no-deps --wait --wait-timeout 120 minio
    if ($LASTEXITCODE -ne 0) {
        throw "AIStor nao ficou pronto. Nenhum processamento foi iniciado."
    }
    if ($Bootstrap) {
        & "$PSScriptRoot\bootstrap-s3.ps1"
    }
}
finally {
    Pop-Location
}
