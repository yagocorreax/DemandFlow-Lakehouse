$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host ""
    Write-Host "====================================="
    Write-Host " DemandFlow Lakehouse - Rebuild"
    Write-Host "====================================="

    # --------------------------------------------------
    # LIMPEZA DE RECURSOS
    # --------------------------------------------------

    Write-Host ""
    Write-Host "[0/4] Parando serviços desnecessários..."

    docker compose --profile cdc stop `
        debezium kafka postgres

    docker compose --profile query stop `
        trino hive-metastore

    # --------------------------------------------------
    # LOCALSTACK
    # --------------------------------------------------

    Write-Host ""
    Write-Host "Iniciando LocalStack..."

    docker compose up -d localstack

    Start-Sleep -Seconds 10

    & "$PSScriptRoot\bootstrap-localstack.ps1"

    # --------------------------------------------------
    # RAW
    # --------------------------------------------------

    Write-Host ""
    Write-Host "====================================="
    Write-Host "[1/4] RAW"
    Write-Host "====================================="

    docker compose --profile cdc up -d `
        postgres kafka debezium

    Start-Sleep -Seconds 20

    & "$PSScriptRoot\run-raw-ingestion.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na camada Raw."
    }

    Write-Host "Raw concluída."

    Write-Host "Liberando memória..."

    docker compose --profile cdc stop `
        debezium kafka postgres

    Start-Sleep -Seconds 5

    # --------------------------------------------------
    # BRONZE
    # --------------------------------------------------

    Write-Host ""
    Write-Host "====================================="
    Write-Host "[2/4] BRONZE"
    Write-Host "====================================="

    & "$PSScriptRoot\run-bronze.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na camada Bronze."
    }

    & "$PSScriptRoot\validate-bronze.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na validação Bronze."
    }

    # --------------------------------------------------
    # SILVER
    # --------------------------------------------------

    Write-Host ""
    Write-Host "====================================="
    Write-Host "[3/4] SILVER"
    Write-Host "====================================="

    & "$PSScriptRoot\run-silver.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na camada Silver."
    }

    & "$PSScriptRoot\validate-silver.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na validação Silver."
    }

    # --------------------------------------------------
    # GOLD
    # --------------------------------------------------

    Write-Host ""
    Write-Host "====================================="
    Write-Host "[4/4] GOLD"
    Write-Host "====================================="

    & "$PSScriptRoot\run-gold.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na camada Gold."
    }

    & "$PSScriptRoot\validate-gold.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na validação Gold."
    }

    Write-Host ""
    Write-Host "====================================="
    Write-Host " LAKEHOUSE RECONSTRUÍDO COM SUCESSO"
    Write-Host "====================================="
}
finally {
    Write-Host ""
    Write-Host "Garantindo que serviços pesados fiquem parados..."

    docker compose --profile cdc stop `
        debezium kafka postgres

    docker compose --profile query stop `
        trino hive-metastore

    Pop-Location
}