$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    # Reuses the server container and its mc client; no helper container.
    docker compose exec -T minio sh /opt/demandflow/bootstrap.sh
    if ($LASTEXITCODE -ne 0) {
        throw "Falha no bootstrap S3. Confira saude, licenca e credenciais do AIStor."
    }
}
finally {
    Pop-Location
}
