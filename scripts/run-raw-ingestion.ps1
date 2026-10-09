$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot

try {
    Write-Host "Preparando CDC..."

    # A configuração PostgreSQL exige isolamento. O armazenamento é retomado
    # logo depois, com o volume persistente preservado.
    & docker compose stop -t 30 minio
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao isolar o PostgreSQL do armazenamento."
    }

    & "$PSScriptRoot\configure-postgres-cdc.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha na configuração do PostgreSQL."
    }

    & "$PSScriptRoot\start-storage.ps1" -Bootstrap

    & "$PSScriptRoot\register-debezium-connector.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao registrar o Debezium."
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
