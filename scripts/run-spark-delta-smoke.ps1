$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    & "$PSScriptRoot\start-storage.ps1" -Bootstrap

    Write-Host "Executando Spark + Delta Lake..."

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
        --conf "spark.jars.ivy=/opt/demandflow/.ivy2" `
        /opt/demandflow/src/spark/smoke/delta_s3.py

    if ($LASTEXITCODE -ne 0) {
        throw "O smoke test do Spark falhou."
    }
}
finally {
    Pop-Location
}
