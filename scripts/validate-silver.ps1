$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    docker compose up -d localstack

    Write-Host "Validando Silver..."

    docker compose `
        --profile processing `
        run `
        --rm `
        spark `
        /opt/spark/bin/spark-submit `
        --master "local[2]" `
        --driver-memory "2g" `
        --packages `
        "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4" `
        --conf "spark.jars.ivy=/tmp/.ivy2" `
        /opt/demandflow/src/spark/silver/validate_silver.py

    if ($LASTEXITCODE -ne 0) {
        throw "Validação da Silver falhou."
    }
}
finally {
    Pop-Location
}
