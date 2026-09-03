$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host "Preparando CDC..."

    & "$PSScriptRoot\configure-postgres-cdc.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na configuração do PostgreSQL."
    }

    & "$PSScriptRoot\register-debezium-connector.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao registrar o Debezium."
    }

    Write-Host ""
    Write-Host "Iniciando LocalStack..."

    docker compose up -d localstack

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar o LocalStack."
    }

    $localStackReady = $false

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            Invoke-RestMethod `
                -Uri "http://localhost:4566/_localstack/health" `
                -TimeoutSec 2 |
                Out-Null

            $localStackReady = $true
            break
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    if (-not $localStackReady) {
        throw "LocalStack não ficou disponível."
    }

    Write-Host "Preparando buckets..."

    & "$PSScriptRoot\bootstrap-localstack.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao preparar os buckets."
    }

    Write-Host ""
    Write-Host "Executando ingestão Kafka -> Raw..."

    docker compose `
        --profile processing `
        --profile cdc `
        run `
        --rm `
        spark `
        /opt/spark/bin/spark-submit `
        --master "local[2]" `
        --driver-memory "1g" `
        --packages `
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4" `
        --conf "spark.jars.ivy=/tmp/.ivy2" `
        /opt/demandflow/src/spark/raw/kafka_to_raw.py

    if ($LASTEXITCODE -ne 0) {
        throw "A ingestão Raw falhou."
    }

    Write-Host ""
    Write-Host "PIPELINE RAW CONCLUÍDO COM SUCESSO."
}
finally {
    Pop-Location
}
