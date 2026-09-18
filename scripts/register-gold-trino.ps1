$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host "Iniciando infraestrutura de consulta..."

    docker compose up -d localstack

    docker compose `
        --profile query `
        up -d hive-metastore trino

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar Hive Metastore/Trino."
    }

    Write-Host "Aguardando Trino..."

    $trinoReady = $false

    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            Invoke-RestMethod `
                -Uri "http://localhost:8080/v1/info" `
                -TimeoutSec 2 |
                Out-Null

            $trinoReady = $true
            break
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    if (-not $trinoReady) {
        throw "Trino não ficou disponível."
    }

    Write-Host "Criando schema Gold..."

    docker exec demandflow-trino `
        trino `
        --execute `
        "CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3://demandflow-gold/analytics/');"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao criar schema Gold."
    }

$tables = [ordered]@{
    "daily_sales" =
        "s3a://demandflow-gold/analytics/daily_sales"

    "product_sales" =
        "s3a://demandflow-gold/analytics/product_sales"

    "inventory_health" =
        "s3a://demandflow-gold/analytics/inventory_health"

    "forecast_accuracy" =
        "s3a://demandflow-gold/analytics/forecast_accuracy"
}

    foreach ($tableName in $tables.Keys) {

        Write-Host ""
        Write-Host "Verificando $tableName..."

        $existing = docker exec demandflow-trino `
            trino `
            --output-format TSV `
            --execute `
            "SELECT table_name
             FROM delta.information_schema.tables
             WHERE table_schema = 'gold'
               AND table_name = '$tableName';"

        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao consultar $tableName."
        }

        $existingText = (
            $existing |
            Out-String
        ).Trim()

        if ($existingText -eq $tableName) {
            Write-Host "[EXISTENTE] $tableName"
            continue
        }

        $location = $tables[$tableName]

        Write-Host "[REGISTRANDO] $tableName"

        docker exec demandflow-trino `
            trino `
            --execute `
            "CALL delta.system.register_table(
                schema_name => 'gold',
                table_name => '$tableName',
                table_location => '$location'
            );"

        if ($LASTEXITCODE -ne 0) {
            throw (
                "Falha ao registrar tabela: " +
                $tableName
            )
        }

        Write-Host "[OK] $tableName"
    }

    Write-Host ""
    Write-Host "Tabelas Gold registradas:"

    docker exec demandflow-trino `
        trino `
        --execute "SHOW TABLES FROM delta.gold;"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao listar tabelas Gold."
    }

    Write-Host ""
    Write-Host "REGISTRO GOLD NO TRINO CONCLUÍDO."
}
finally {
    Pop-Location
}