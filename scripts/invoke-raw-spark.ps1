$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    docker compose `
        --profile processing `
        run `
        --rm `
        spark `
        /opt/spark/bin/spark-submit `
        --master "local[2]" `
        --driver-memory "1g" `
        --packages `
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4" `
        --conf "spark.jars.ivy=/opt/demandflow/.ivy2" `
        /opt/demandflow/src/spark/raw/kafka_to_raw.py

    if ($LASTEXITCODE -ne 0) {
        throw "A execução Spark Kafka -> Raw falhou."
    }
}
finally {
    Pop-Location
}
