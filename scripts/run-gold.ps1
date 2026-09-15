$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host "Iniciando LocalStack..."

    docker compose up -d localstack

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar LocalStack."
    }

    Write-Host "Preparando buckets..."

    & "$PSScriptRoot\bootstrap-localstack.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao preparar buckets."
    }

    Write-Host ""
    Write-Host "Executando Silver -> Gold..."

    docker compose `
        --profile processing `
        run `
        --rm `
        spark `
        /opt/spark/bin/spark-submit `
        --master "local[2]" `
        --driver-memory "1g" `
        --packages `
        "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4" `
        --conf "spark.jars.ivy=/tmp/.ivy2" `
        /opt/demandflow/src/spark/gold/silver_to_gold.py

    if ($LASTEXITCODE -ne 0) {
        throw "A camada Gold falhou."
    }

    Write-Host ""
    Write-Host "PIPELINE GOLD CONCLUÍDO."
}
finally {
    Pop-Location
}