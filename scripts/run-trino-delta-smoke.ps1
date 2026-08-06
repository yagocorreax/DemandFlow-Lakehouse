$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host "Criando a tabela Delta com Spark..."

    & "$PSScriptRoot\run-spark-delta-smoke.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao criar a tabela Delta."
    }

    Write-Host "Iniciando o Hive Metastore..."

    docker compose `
        --profile query `
        up -d hive-metastore

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar o Hive Metastore."
    }

    Write-Host "Aguardando a porta 9083..."

    $metastoreReady = $false

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $metastoreReady = Test-NetConnection `
            -ComputerName localhost `
            -Port 9083 `
            -InformationLevel Quiet `
            -WarningAction SilentlyContinue

        if ($metastoreReady) {
            break
        }

        Start-Sleep -Seconds 2
    }

    if (-not $metastoreReady) {
        throw "O Hive Metastore não ficou disponível."
    }

    Write-Host "Iniciando o Trino..."

    docker compose `
        --profile query `
        up -d trino

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar o Trino."
    }

    Write-Host "Aguardando o Trino..."

    $trinoReady = $false

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            Invoke-RestMethod `
                -Uri "http://localhost:8080/v1/info" `
                -TimeoutSec 2 | Out-Null

            $trinoReady = $true
            break
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    if (-not $trinoReady) {
        throw "O Trino não ficou disponível."
    }

    Write-Host "Criando o schema Bronze..."

    docker exec demandflow-trino `
        trino `
        --execute `
        "CREATE SCHEMA IF NOT EXISTS delta.bronze;"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao criar o schema Bronze."
    }

    $tableCount = docker exec demandflow-trino `
        trino `
        --output-format CSV `
        --execute `
        "SELECT COUNT(*) FROM delta.information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'products_smoke';"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao verificar a tabela."
    }

    $normalizedCount = (
        $tableCount |
        Select-Object -Last 1
    ).Trim().Replace('"', '')

    if ($normalizedCount -eq "0") {
        Write-Host "Registrando a tabela Delta..."

        docker exec demandflow-trino `
            trino `
            --execute `
            "CALL delta.system.register_table(schema_name => 'bronze', table_name => 'products_smoke', table_location => 's3://demandflow-dev-bronze/smoke/products_delta');"

        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao registrar a tabela Delta."
        }
    }
    else {
        Write-Host "Tabela já registrada no catálogo."
    }

    Write-Host "Consultando a tabela pelo Trino..."

    docker exec demandflow-trino `
        trino `
        --execute `
        "SELECT product_id, sku, unit_price, is_active FROM delta.bronze.products_smoke ORDER BY product_id;"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao consultar a tabela pelo Trino."
    }

    $rowCount = docker exec demandflow-trino `
        trino `
        --output-format CSV `
        --execute `
        "SELECT COUNT(*) FROM delta.bronze.products_smoke;"

    $normalizedRowCount = (
        $rowCount |
        Select-Object -Last 1
    ).Trim().Replace('"', '')

    if ($normalizedRowCount -ne "4") {
        throw "Quantidade inesperada de registros: $normalizedRowCount"
    }

    Write-Host ""
    Write-Host "SMOKE TEST TRINO + DELTA CONCLUÍDO COM SUCESSO."
}
finally {
    Pop-Location
}
