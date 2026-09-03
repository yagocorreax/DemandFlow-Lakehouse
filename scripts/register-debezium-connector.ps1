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
        throw "Variável $Name não encontrada no .env."
    }

    return (($line -split "=", 2)[1]).Trim().Trim('"').Trim("'")
}

$connectUrl = Get-DotEnvValue "KAFKA_CONNECT_URL"

$cdcUser = Get-DotEnvValue "DEBEZIUM_POSTGRES_USER"
$cdcPassword = Get-DotEnvValue "DEBEZIUM_POSTGRES_PASSWORD"
$database = Get-DotEnvValue "POSTGRES_DATABASE"

Push-Location $projectRoot

try {
    Write-Host "Iniciando infraestrutura CDC..."

    docker compose `
        --profile cdc `
        up -d postgres kafka debezium

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar infraestrutura CDC."
    }

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

    Write-Host "Kafka Connect disponível."

    $payload = Get-Content $configFile -Raw |
        ConvertFrom-Json

    $payload.config.'database.user' = $cdcUser
    $payload.config.'database.password' = $cdcPassword
    $payload.config.'database.dbname' = $database

    $connectorName = $payload.name

    Write-Host "Configurando connector $connectorName..."

    #
    # PUT é usado tanto para criar quanto para atualizar.
    # Isso torna o script idempotente.
    #
    $body = $payload.config |
        ConvertTo-Json -Depth 20

    Invoke-RestMethod `
        -Uri "$connectUrl/connectors/$connectorName/config" `
        -Method Put `
        -ContentType "application/json" `
        -Body $body |
        Out-Null

    Write-Host "Aguardando connector ficar RUNNING..."

    $running = $false

    for ($i = 1; $i -le 60; $i++) {
        try {
            $status = Invoke-RestMethod `
                -Uri "$connectUrl/connectors/$connectorName/status"

            $connectorRunning = (
                $status.connector.state -eq "RUNNING"
            )

            $tasks = @($status.tasks)

            $tasksRunning = (
                $tasks.Count -gt 0 -and
                @(
                    $tasks |
                    Where-Object {
                        $_.state -ne "RUNNING"
                    }
                ).Count -eq 0
            )

            $failedTask = $tasks |
                Where-Object {
                    $_.state -eq "FAILED"
                } |
                Select-Object -First 1

            if ($failedTask) {
                throw (
                    "Task do connector falhou:`n" +
                    $failedTask.trace
                )
            }

            if ($connectorRunning -and $tasksRunning) {
                $running = $true
                break
            }
        }
        catch {
            if ($_.Exception.Message -like "*Task do connector falhou*") {
                throw
            }
        }

        Start-Sleep -Seconds 2
    }

    if (-not $running) {
        throw "O connector não ficou RUNNING."
    }

    Write-Host ""
    Write-Host "Connector Debezium configurado com sucesso."
    Write-Host "Nome: $connectorName"
    Write-Host "Status: RUNNING"
}
finally {
    Pop-Location
}