$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    & "$PSScriptRoot\start-storage.ps1"

    Write-Host "Validando Gold..."

    docker compose `
        --profile processing `
        run `
        --rm `
        --no-deps `
        spark `
        /opt/spark/bin/spark-submit `
        --master "local[2]" `
        --driver-memory "2g" `
        --packages `
        "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4" `
        --conf "spark.jars.ivy=/opt/demandflow/.ivy2" `
        /opt/demandflow/src/spark/gold/validate_gold.py

    if ($LASTEXITCODE -ne 0) {
        throw "Validação da Gold falhou."
    }
}
finally {
    Pop-Location
}
