$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"

$configFile = Join-Path `
    $projectRoot `
    "config\debezium\postgres-connector.json"

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory)]
        [string]$ContainerName,

        [Parameter(Mandatory)]
        [string]$ServiceName,

        [int]$Attempts = 60,

        [int]$IntervalSeconds = 2
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $health = & docker inspect `
            --format "{{.State.Health.Status}}" `
            $ContainerName `
            2>$null

        if ($LASTEXITCODE -eq 0 -and $health -eq "healthy") {
            Write-Host "$ServiceName saudável."
            return
        }

        Start-Sleep -Seconds $IntervalSeconds
    }

    throw "$ServiceName não ficou saudável dentro do prazo."
}

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory)]
        [string]$Name
    )

    if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
        throw "Arquivo .env não encontrado."
    }

    $lines = @(
        Get-Content -LiteralPath $envFile -Encoding UTF8 |
        Where-Object {
            $_ -match "^\s*$([regex]::Escape($Name))\s*="
        } |
        ForEach-Object { $_ }
    )

    if ($lines.Count -eq 0) {
        throw "Variável $Name não encontrada no .env."
    }
    if ($lines.Count -ne 1) {
        throw "Variável $Name está duplicada no .env."
    }

    $value = (($lines[0] -split "=", 2)[1]).Trim()
    if ($value.Length -ge 2) {
        $first = $value[0]
        $last = $value[$value.Length - 1]
        if (($first -eq '"' -and $last -eq '"') -or
            ($first -eq "'" -and $last -eq "'")) {
            $value = $value.Substring(1, $value.Length - 2)
        }
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Variável $Name está vazia no .env."
    }

    return $value
}

$connectUrl = Get-DotEnvValue "KAFKA_CONNECT_URL"

try {
    $connectUri = [Uri]$connectUrl
}
catch {
    throw "KAFKA_CONNECT_URL não é uma URL válida."
}

$allowedConnectHosts = @("localhost", "127.0.0.1", "::1")
if ($connectUri.Scheme -ne "http" -or
    $connectUri.Port -ne 8083 -or
    $connectUri.Host -notin $allowedConnectHosts -or
    -not [string]::IsNullOrEmpty($connectUri.UserInfo) -or
    -not [string]::IsNullOrEmpty($connectUri.Query) -or
    -not [string]::IsNullOrEmpty($connectUri.Fragment)) {
    throw (
        "KAFKA_CONNECT_URL deve apontar para o Kafka Connect local em " +
        "http://localhost:8083 (ou loopback equivalente)."
    )
}
# Compose publica o Connect somente em 127.0.0.1. Canonicalizar evita que
# "localhost" seja resolvido como ::1 de forma intermitente no Windows.
$connectUrl = "http://127.0.0.1:8083"

$cdcUser = Get-DotEnvValue "DEBEZIUM_POSTGRES_USER"
$cdcPassword = Get-DotEnvValue "DEBEZIUM_POSTGRES_PASSWORD"
$database = Get-DotEnvValue "POSTGRES_DATABASE"

Push-Location $projectRoot

try {
    Write-Host "Iniciando PostgreSQL isoladamente..."

    & docker compose up -d --no-deps postgres

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar PostgreSQL."
    }

    Wait-ContainerHealthy `
        -ContainerName "demandflow-postgres" `
        -ServiceName "PostgreSQL" `
        -Attempts 30

    Write-Host "Iniciando Kafka após PostgreSQL estar saudável..."

    & docker compose `
        --profile cdc `
        up -d --no-deps kafka

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar Kafka."
    }

    Wait-ContainerHealthy `
        -ContainerName "demandflow-kafka" `
        -ServiceName "Kafka"

    Write-Host "Protegendo os tópicos CDC antes de iniciar Debezium..."

    & "$PSScriptRoot\configure-kafka-cdc-topics.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao configurar os tópicos Kafka do CDC."
    }

    Write-Host "Iniciando Debezium após Kafka estar saudável..."

    & docker compose `
        --profile cdc `
        up -d --no-deps debezium

    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar Debezium."
    }

    Write-Host "Aguardando Kafka Connect..."

    $ready = $false

    # Nesta máquina local, a recuperação dos tópicos internos já levou 124 s.
    # A janela de 180 s evita um falso timeout sem aumentar CPU ou memória.
    for ($i = 1; $i -le 90; $i++) {
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

    if (-not (Test-Path -LiteralPath $configFile -PathType Leaf)) {
        throw "Template do connector Debezium não encontrado."
    }

    $payload = Get-Content -LiteralPath $configFile -Raw -Encoding UTF8 |
        ConvertFrom-Json

    if (
        $payload.config.'database.password' -ne
        "__DEMANDFLOW_RUNTIME_SECRET__"
    ) {
        throw (
            "O template Debezium não pode conter senha literal. " +
            "Use o marcador de injeção em runtime."
        )
    }

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
    $body = $null
    $payload = $null
    $cdcPassword = $null
    Pop-Location
}
