$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$localStackHealthUrl = "http://localhost:4566/_localstack/health"

Push-Location $projectRoot

try {
    Write-Host "Iniciando o LocalStack..."

    docker compose up -d localstack

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar o LocalStack."
    }

    Write-Host "Aguardando o LocalStack ficar disponível..."

    $localStackReady = $false

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            Invoke-RestMethod `
                -Uri $localStackHealthUrl `
                -TimeoutSec 2 | Out-Null

            $localStackReady = $true
            break
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    if (-not $localStackReady) {
        throw "O LocalStack não ficou disponível dentro do tempo esperado."
    }

    Write-Host "LocalStack disponível."

    Write-Host "Preparando os buckets..."

    & "$PSScriptRoot\bootstrap-localstack.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao preparar os buckets."
    }

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
        --conf "spark.jars.ivy=/tmp/.ivy2" `
        /opt/demandflow/src/spark/smoke/delta_s3.py

    if ($LASTEXITCODE -ne 0) {
        throw "O smoke test do Spark falhou."
    }
}
finally {
    Pop-Location
}
