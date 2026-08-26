$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$sqlFile = Join-Path $projectRoot "infra\postgres\cdc\setup.sql"

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory)]
        [string]$Name
    )

    $line = Get-Content $envFile |
        Where-Object {
            $_ -match "^\s*$([regex]::Escape($Name))\s*="
        } |
        Select-Object -Last 1

    if (-not $line) {
        throw "Variável $Name não encontrada no .env."
    }

    return (($line -split "=", 2)[1]).Trim().Trim('"').Trim("'")
}

$postgresUser = Get-DotEnvValue "POSTGRES_USER"
$database = Get-DotEnvValue "POSTGRES_DATABASE"
$cdcUser = Get-DotEnvValue "DEBEZIUM_POSTGRES_USER"
$cdcPassword = Get-DotEnvValue "DEBEZIUM_POSTGRES_PASSWORD"

Push-Location $projectRoot

try {
    docker compose up -d postgres

    Write-Host "Aguardando PostgreSQL..."

    for ($i = 1; $i -le 30; $i++) {
        $status = docker inspect `
            demandflow-postgres `
            --format "{{.State.Health.Status}}" `
            2>$null

        if ($status -eq "healthy") {
            break
        }

        Start-Sleep -Seconds 2
    }

    Write-Host "Configurando CDC..."

    Get-Content $sqlFile -Raw |
        docker exec -i demandflow-postgres `
            psql `
            -v ON_ERROR_STOP=1 `
            -v "cdc_user=$cdcUser" `
            -v "cdc_password=$cdcPassword" `
            -v "database_name=$database" `
            -U $postgresUser `
            -d $database

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao configurar o PostgreSQL."
    }

    Write-Host ""
    Write-Host "CDC PostgreSQL configurado."
}
finally {
    Pop-Location
}
