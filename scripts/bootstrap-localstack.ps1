$ErrorActionPreference = "Stop"

$endpoint = "http://localhost:4566"
$region = "us-east-1"

$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = $region

$buckets = @(
    "demandflow-raw",
    "demandflow-bronze",
    "demandflow-silver",
    "demandflow-gold",
    "demandflow-quarantine",
    "demandflow-checkpoints"
)

Write-Host "Verificando conexão com o LocalStack..."

aws `
    --endpoint-url=$endpoint `
    sts get-caller-identity | Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível conectar ao LocalStack."
}

$existingBuckets = aws `
    --endpoint-url=$endpoint `
    s3api list-buckets `
    --query "Buckets[].Name" `
    --output text

foreach ($bucket in $buckets) {
    $bucketExists = $existingBuckets -split "\s+" -contains $bucket

    if ($bucketExists) {
        Write-Host "[EXISTENTE] $bucket"
        continue
    }

    aws `
        --endpoint-url=$endpoint `
        s3api create-bucket `
        --bucket $bucket `
        --region $region | Out-Null

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao criar o bucket $bucket."
    }

    Write-Host "[CRIADO] $bucket"
}

Write-Host ""
Write-Host "Buckets disponíveis:"

aws `
    --endpoint-url=$endpoint `
    s3api list-buckets `
    --query "Buckets[].Name" `
    --output table
