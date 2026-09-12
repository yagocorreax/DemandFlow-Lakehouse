$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    docker compose up -d localstack

    Write-Host "Validando Bronze..."

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
        /opt/demandflow/src/spark/bronze/validate_bronze.py

    if ($LASTEXITCODE -ne 0) {
        throw "Validação da Bronze falhou."
    }
}
finally {
    Pop-Location
}
