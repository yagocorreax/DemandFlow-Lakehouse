$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"

$configFile = Join-Path `
    $projectRoot `
    "config\debezium\postgres-connector.json"

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
        throw "Variável $Name não encontrada."
    }

    return (($line -split "=", 2)[1]).Trim().Trim('"').Trim("'")
}

$connectUrl = Get-DotEnvValue "KAFKA_CONNECT_URL"

$cdcUser = Get-DotEnvValue "DEBEZIUM_POSTGRES_USER"
$cdcPassword = Get-DotEnvValue "DEBEZIUM_POSTGRES_PASSWORD"
$database = Get-DotEnvValue "POSTGRES_DATABASE"

Push-Location $projectRoot

try {
    docker compose `
        --profile cdc `
        up -d postgres kafka debezium

    Write-Host "Aguardando Kafka Connect..."

    $ready = $false

    for ($i = 1; $i -le 60; $i++) {
        try {
            Invoke-RestMethod `
                -Uri "$connectUrl/connector-plugins" `
                -TimeoutSec 2 |
                Out-Null

            $ready = $true
            break
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }

    if (-not $ready) {
        throw "Kafka Connect não ficou disponível."
    }

    $payload = Get-Content $configFile -Raw |
        ConvertFrom-Json

    $payload.config.'database.user' = $cdcUser
    $payload.config.'database.password' = $cdcPassword
    $payload.config.'database.dbname' = $database

    $connectorName = $payload.name

    $existing = @(
        Invoke-RestMethod `
            -Uri "$connectUrl/connectors"
    )

    if ($existing -contains $connectorName) {

        Write-Host "Atualizando conector..."

        $body = $payload.config |
            ConvertTo-Json -Depth 20

        Invoke-RestMethod `
            -Uri "$connectUrl/connectors/$connectorName/config" `
            -Method Put `
            -ContentType "application/json" `
            -Body $body |
            Out-Null
    }
    else {

        Write-Host "Criando conector..."

        $body = $payload |
            ConvertTo-Json -Depth 20

        Invoke-RestMethod `
            -Uri "$connectUrl/connectors" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body |
            Out-Null
    }

    Start-Sleep -Seconds 5

    $status = Invoke-RestMethod `
        -Uri "$connectUrl/connectors/$connectorName/status"

    $status |
        ConvertTo-Json -Depth 10

    if ($status.connector.state -ne "RUNNING") {
        throw "O conector não está RUNNING."
    }

    $failed = @(
        $status.tasks |
        Where-Object {
            $_.state -eq "FAILED"
        }
    )

    if ($failed.Count -gt 0) {
        throw $failed[0].trace
    }

    Write-Host ""
    Write-Host "Conector Debezium funcionando."
}
finally {
    Pop-Location
}
