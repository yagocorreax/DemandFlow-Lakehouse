$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$pythonExecutable = Join-Path $projectRoot ".venv\Scripts\python.exe"
$generatorFile = Join-Path $projectRoot "src\generator\main.py"
$postgresContainer = "demandflow-postgres"
$kafkaContainer = "demandflow-kafka"
$connectorName = "demandflow-postgres-connector"
$promotionsTopic = "demandflow.public.promotions"
$stackStartAttempted = $false
$validationCompleted = $false
$cudMarker = $null
$restartMarker = $null
$cudPromotionId = $null
$restartPromotionId = $null
$generatorOutput = $null
$generatorText = $null
$primaryFailure = $null
$cleanupSummary = $null

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory)]
        [string]$Text,

        [Parameter(Mandatory)]
        [string]$Name
    )

    $pattern = (
        "(?m)^[\t ]*" +
        [regex]::Escape($Name) +
        "[\t ]*=(?<value>[^\r\n]*)$"
    )
    $entryMatches = [regex]::Matches($Text, $pattern)

    if ($entryMatches.Count -eq 0) {
        throw "Variável $Name não encontrada no .env."
    }
    if ($entryMatches.Count -ne 1) {
        throw "Variável $Name está duplicada no .env."
    }

    $value = $entryMatches[0].Groups["value"].Value.Trim()
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

function ConvertTo-SqlLiteral {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    return "'" + $Value.Replace("'", "''") + "'"
}

function Get-RunningServices {
    $services = @(
        & docker compose ps --status running --services 2>$null |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.Trim() }
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível consultar o estado do Docker Compose."
    }

    return @($services)
}

function Assert-OnlyCdcServices {
    $expected = @("debezium", "kafka", "postgres") | Sort-Object
    $actual = @(Get-RunningServices) | Sort-Object
    $difference = @(Compare-Object $expected $actual)

    if ($difference.Count -ne 0) {
        throw "O isolamento CDC falhou: o conjunto de serviços ativos divergiu."
    }
}

function Test-ContainerRunning {
    param(
        [Parameter(Mandatory)]
        [string]$ContainerName
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $running = & docker inspect `
            --format "{{.State.Running}}" `
            $ContainerName `
            2>$null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    return (
        $exitCode -eq 0 -and
        ([string]::Join("", @($running))).Trim() -eq "true"
    )
}

function Wait-ConnectorRunning {
    param(
        [int]$Attempts = 90,

        [int]$IntervalSeconds = 2
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $status = Invoke-RestMethod `
                -Uri "$script:connectUrl/connectors/$script:connectorName/status" `
                -TimeoutSec 3

            $tasks = @($status.tasks)
            $failedTaskCount = @(
                $tasks | Where-Object { $_.state -eq "FAILED" }
            ).Count

            if ($status.connector.state -eq "FAILED" -or
                $failedTaskCount -ne 0) {
                throw "O connector ou uma task entrou no estado FAILED."
            }

            $nonRunningTaskCount = @(
                $tasks | Where-Object { $_.state -ne "RUNNING" }
            ).Count

            if ($status.connector.state -eq "RUNNING" -and
                $tasks.Count -gt 0 -and
                $nonRunningTaskCount -eq 0) {
                return
            }
        }
        catch {
            if ($_.Exception.Message -like "*estado FAILED*") {
                throw
            }
        }

        Start-Sleep -Seconds $IntervalSeconds
    }

    throw "O connector Debezium não ficou RUNNING dentro do prazo."
}

function Invoke-PostgresCommand {
    param(
        [Parameter(Mandatory)]
        [string]$Sql
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $Sql |
            & docker exec -i $script:postgresContainer `
                psql `
                -X `
                -q `
                -v ON_ERROR_STOP=1 `
                -U $script:postgresUser `
                -d $script:database `
                1>$null `
                2>$null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "O PostgreSQL recusou uma operação da validação CDC."
    }
}

function Invoke-PostgresScalar {
    param(
        [Parameter(Mandatory)]
        [string]$Sql
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            $Sql |
                & docker exec -i $script:postgresContainer `
                    psql `
                    -X `
                    -qAt `
                    -v ON_ERROR_STOP=1 `
                    -U $script:postgresUser `
                    -d $script:database `
                    2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Uma consulta de validação CDC falhou no PostgreSQL."
    }

    return ([string]::Join("", $output)).Trim()
}

function Get-TopicMessages {
    param(
        [Parameter(Mandatory)]
        [string]$Topic
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-console-consumer.sh `
                --bootstrap-server kafka:29092 `
                --topic $Topic `
                --from-beginning `
                --timeout-ms 3000 `
                2>$null
        )
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    return @($output)
}

function Get-RecordProperty {
    param(
        [AllowNull()]
        [object]$Record,

        [Parameter(Mandatory)]
        [string]$Name
    )

    if ($null -eq $Record) {
        return $null
    }

    $property = $Record.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Get-PromotionEvents {
    param(
        [Parameter(Mandatory)]
        [string]$PromotionId
    )

    $events = New-Object System.Collections.Generic.List[object]

    foreach ($line in @(Get-TopicMessages $script:promotionsTopic)) {
        if ([string]::IsNullOrWhiteSpace([string]$line)) {
            continue
        }

        try {
            $message = [string]$line | ConvertFrom-Json
        }
        catch {
            continue
        }

        $wrappedPayload = Get-RecordProperty $message "payload"
        if ($null -ne $wrappedPayload) {
            $message = $wrappedPayload
        }

        $before = Get-RecordProperty $message "before"
        $after = Get-RecordProperty $message "after"
        $beforeId = Get-RecordProperty $before "promotion_id"
        $afterId = Get-RecordProperty $after "promotion_id"

        if ([string]$beforeId -cne $PromotionId -and
            [string]$afterId -cne $PromotionId) {
            continue
        }

        $operation = Get-RecordProperty $message "op"
        $source = Get-RecordProperty $message "source"
        $events.Add(
            [pscustomobject]@{
                operation = [string]$operation
                before = $before
                after = $after
                source = $source
            }
        )
    }

    return $events.ToArray()
}

function Wait-PromotionEvents {
    param(
        [Parameter(Mandatory)]
        [string]$PromotionId,

        [Parameter(Mandatory)]
        [string[]]$ExpectedOperations,

        [int]$Attempts = 10
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $events = @(Get-PromotionEvents $PromotionId)
        $operations = @($events | ForEach-Object { $_.operation })

        if ($events.Count -eq $ExpectedOperations.Count -and
            [string]::Join(",", $operations) -ceq
            [string]::Join(",", $ExpectedOperations)) {
            return @($events)
        }

        Start-Sleep -Seconds 2
    }

    throw (
        "O tópico não apresentou a sequência CDC esperada para a chave " +
        "de validação."
    )
}

function Assert-CudPayload {
    param(
        [Parameter(Mandatory)]
        [object[]]$Events,

        [Parameter(Mandatory)]
        [string]$Marker,

        [Parameter(Mandatory)]
        [string]$PromotionId
    )

    if ($Events.Count -ne 3) {
        throw "A validação C/U/D recebeu uma quantidade inesperada de eventos."
    }

    $create = $Events[0]
    $update = $Events[1]
    $delete = $Events[2]

    if ($null -ne $create.before -or
        $null -eq $create.after -or
        [string](Get-RecordProperty $create.after "promotion_id") -cne
            $PromotionId -or
        (Get-RecordProperty $create.after "promotion_name") -cne $Marker -or
        (Get-RecordProperty $create.after "discount_percentage") -cne "11.00") {
        throw "O payload de criação CDC está incorreto."
    }

    # REPLICA IDENTITY DEFAULT não garante pré-imagem em UPDATE. Quando ela
    # existir, a chave deve coincidir; o estado novo sempre deve estar completo.
    $updateBeforeId = Get-RecordProperty $update.before "promotion_id"
    if ($null -eq $update.after -or
        ($null -ne $update.before -and
            [string]$updateBeforeId -cne $PromotionId) -or
        [string](Get-RecordProperty $update.after "promotion_id") -cne
            $PromotionId -or
        (Get-RecordProperty $update.after "promotion_name") -cne $Marker -or
        (Get-RecordProperty $update.after "discount_percentage") -cne "12.00") {
        throw "O payload de atualização CDC está incorreto."
    }

    if ($null -eq $delete.before -or
        [string](Get-RecordProperty $delete.before "promotion_id") -cne
            $PromotionId -or
        $null -ne $delete.after) {
        throw "O payload de exclusão CDC está incorreto."
    }

    foreach ($event in $Events) {
        if ($null -eq (Get-RecordProperty $event.source "lsn")) {
            throw "Um evento CDC não contém LSN de origem."
        }
    }
}

function Get-TopicEndOffset {
    param(
        [Parameter(Mandatory)]
        [string]$Topic
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-get-offsets.sh `
                --bootstrap-server kafka:29092 `
                --topic $Topic `
                --time -1 `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Não foi possível consultar offsets do tópico $Topic."
    }

    [long]$total = 0
    $partitionCount = 0
    foreach ($line in $output) {
        if ([string]$line -match ":(?<offset>[0-9]+)$") {
            $total += [long]$Matches["offset"]
            $partitionCount++
        }
    }

    if ($partitionCount -eq 0) {
        throw "O tópico $Topic não apresentou partições consultáveis."
    }

    return $total
}

function Wait-TopicOffsetGreaterThan {
    param(
        [Parameter(Mandatory)]
        [string]$Topic,

        [Parameter(Mandatory)]
        [long]$Baseline,

        [int]$Attempts = 45
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $current = Get-TopicEndOffset $Topic
        if ($current -gt $Baseline) {
            return $current
        }

        Start-Sleep -Seconds 2
    }

    throw "O offset do tópico $Topic não avançou dentro do prazo."
}

function Wait-GeneratorOffsets {
    param(
        [Parameter(Mandatory)]
        [hashtable]$Before,

        [int]$MinimumDelta = 4,

        [int]$Attempts = 20
    )

    [long]$beforeTotal = 0
    foreach ($value in $Before.Values) {
        $beforeTotal += [long]$value
    }

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        [long]$afterTotal = 0
        foreach ($topic in $Before.Keys) {
            $afterTotal += Get-TopicEndOffset ([string]$topic)
        }

        if (($afterTotal - $beforeTotal) -ge $MinimumDelta) {
            return $afterTotal - $beforeTotal
        }

        Start-Sleep -Seconds 2
    }

    throw "Os tópicos CDC não refletiram a transação do Generator no prazo."
}

function Wait-SlotState {
    param(
        [Parameter(Mandatory)]
        [ValidateSet("active", "inactive")]
        [string]$ExpectedState,

        [int]$Attempts = 20
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $state = Invoke-PostgresScalar @"
SELECT CASE WHEN active THEN 'active' ELSE 'inactive' END
FROM pg_replication_slots
WHERE slot_name = 'demandflow_slot';
"@

        if ($state -eq $ExpectedState) {
            return
        }

        Start-Sleep -Seconds 1
    }

    throw "O slot lógico não entrou no estado $ExpectedState."
}

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Arquivo .env não encontrado."
}
if (-not (Test-Path -LiteralPath $pythonExecutable -PathType Leaf)) {
    throw "Python do ambiente virtual não encontrado."
}
if (-not (Test-Path -LiteralPath $generatorFile -PathType Leaf)) {
    throw "Generator não encontrado."
}

$envText = [System.IO.File]::ReadAllText($envFile)
$configuredConnectUrl = Get-DotEnvValue $envText "KAFKA_CONNECT_URL"
try {
    $configuredConnectUri = [Uri]$configuredConnectUrl
}
catch {
    throw "KAFKA_CONNECT_URL não é uma URL válida."
}
if ($configuredConnectUri.Scheme -ne "http" -or
    $configuredConnectUri.Port -ne 8083 -or
    $configuredConnectUri.Host -notin @("localhost", "127.0.0.1", "::1")) {
    throw "KAFKA_CONNECT_URL não aponta para o Kafka Connect local."
}
$connectUrl = "http://127.0.0.1:8083"
$postgresUser = Get-DotEnvValue $envText "POSTGRES_USER"
$database = Get-DotEnvValue $envText "POSTGRES_DATABASE"

Push-Location $projectRoot

try {
    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "Há serviços ativos; a validação CDC isolada foi cancelada."
    }

    Write-Host "Iniciando e registrando a infraestrutura CDC em sequência..."
    $stackStartAttempted = $true
    & "$PSScriptRoot\register-debezium-connector.ps1"

    Assert-OnlyCdcServices
    Wait-ConnectorRunning
    Write-Host "PostgreSQL, Kafka e Debezium estão saudáveis e isolados."

    $cudMarker = "__demandflow_cdc_cud_{0}" -f (
        [guid]::NewGuid().ToString("N")
    )
    $quotedCudMarker = ConvertTo-SqlLiteral $cudMarker

    Write-Host "Validando criação, atualização e exclusão no Kafka..."
    $cudPromotionId = Invoke-PostgresScalar @"
INSERT INTO promotions (
    promotion_name,
    discount_percentage,
    start_date,
    end_date
)
VALUES ($quotedCudMarker, 11.00, CURRENT_DATE, CURRENT_DATE + 1)
RETURNING promotion_id
\gset

UPDATE promotions
SET discount_percentage = 12.00
WHERE promotion_id = :promotion_id;

DELETE FROM promotions
WHERE promotion_id = :promotion_id;

SELECT :promotion_id;
"@
    if ($cudPromotionId -cnotmatch "^[0-9]+$") {
        throw "A mutação C/U/D não retornou uma chave primária válida."
    }

    $cudEvents = @(
        Wait-PromotionEvents `
            -PromotionId $cudPromotionId `
            -ExpectedOperations @("c", "u", "d")
    )
    Assert-CudPayload `
        -Events $cudEvents `
        -Marker $cudMarker `
        -PromotionId $cudPromotionId
    $cudMarker = $null
    $cudPromotionId = $null
    Write-Host "Eventos c/u/d e seus payloads foram validados."

    $slotLsnBeforeRestart = Invoke-PostgresScalar @"
SELECT confirmed_flush_lsn::text
FROM pg_replication_slots
WHERE slot_name = 'demandflow_slot';
"@
    if ($slotLsnBeforeRestart -cnotmatch "^[0-9A-Fa-f]+/[0-9A-Fa-f]+$") {
        throw "O slot lógico não apresentou um LSN válido antes do reinício."
    }

    $offsetBeforeRestart = Get-TopicEndOffset "demandflow-connect-offsets"

    Write-Host "Parando somente Debezium para testar retomada de offset..."
    & docker compose --profile cdc stop -t 30 debezium
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível parar Debezium para o teste de retomada."
    }
    Wait-SlotState "inactive"

    $restartMarker = "__demandflow_cdc_restart_{0}" -f (
        [guid]::NewGuid().ToString("N")
    )
    $quotedRestartMarker = ConvertTo-SqlLiteral $restartMarker
    $restartPromotionId = Invoke-PostgresScalar @"
INSERT INTO promotions (
    promotion_name,
    discount_percentage,
    start_date,
    end_date
)
VALUES ($quotedRestartMarker, 13.00, CURRENT_DATE, CURRENT_DATE + 1)
RETURNING promotion_id;
"@
    if ($restartPromotionId -cnotmatch "^[0-9]+$") {
        throw "A mutação durante a parada não retornou uma chave válida."
    }

    Write-Host "Reiniciando somente Debezium..."
    & docker compose --profile cdc up -d --no-deps debezium
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível reiniciar Debezium."
    }

    Wait-ConnectorRunning
    Wait-SlotState "active"
    $restartEvents = @(
        Wait-PromotionEvents `
            -PromotionId $restartPromotionId `
            -ExpectedOperations @("c")
    )

    $snapshotValue = Get-RecordProperty $restartEvents[0].source "snapshot"
    if ($snapshotValue -eq $true -or
        ([string]$snapshotValue).ToLowerInvariant() -eq "true") {
        throw "Debezium repetiu snapshot em vez de retomar o offset."
    }

    $null = Wait-TopicOffsetGreaterThan `
        -Topic "demandflow-connect-offsets" `
        -Baseline $offsetBeforeRestart

    $quotedLsn = ConvertTo-SqlLiteral $slotLsnBeforeRestart
    $lsnAdvanced = Invoke-PostgresScalar @"
SELECT CASE
    WHEN pg_wal_lsn_diff(confirmed_flush_lsn, $quotedLsn::pg_lsn) > 0
    THEN 'ok'
    ELSE 'invalid'
END
FROM pg_replication_slots
WHERE slot_name = 'demandflow_slot';
"@
    if ($lsnAdvanced -ne "ok") {
        throw "O LSN confirmado não avançou após a retomada do Debezium."
    }
    Write-Host "Reinício, slot lógico e retomada de offset foram validados."

    $generatorTopics = @(
        "demandflow.public.orders",
        "demandflow.public.order_items",
        "demandflow.public.inventory",
        "demandflow.public.inventory_movements"
    )
    $offsetsBeforeGenerator = @{}
    foreach ($topic in $generatorTopics) {
        $offsetsBeforeGenerator[$topic] = Get-TopicEndOffset $topic
    }

    Write-Host "Executando uma venda determinística pelo Generator..."
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $generatorOutput = @(
            & $pythonExecutable `
                -B `
                $generatorFile `
                --iterations 1 `
                --interval-seconds 0 `
                --action sale `
                2>&1
        )
        $generatorExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    $generatorText = [string]::Join("`n", @($generatorOutput))
    if ($generatorExitCode -ne 0 -or
        $generatorText -notmatch "1/1 confirmada" -or
        $generatorText -match "revertida") {
        throw "O Generator não confirmou a venda de validação."
    }

    $generatorDelta = Wait-GeneratorOffsets $offsetsBeforeGenerator
    if ($generatorDelta -lt 4) {
        throw "A venda não produziu todos os eventos CDC esperados."
    }
    Write-Host "Generator confirmado em quatro tópicos transacionais."

    Invoke-PostgresCommand @"
DELETE FROM promotions
WHERE promotion_id = $restartPromotionId;
"@
    $null = Wait-PromotionEvents `
        -PromotionId $restartPromotionId `
        -ExpectedOperations @("c", "d")
    $restartMarker = $null
    $restartPromotionId = $null

    $validationCompleted = $true
    Write-Host "Validação CDC funcional concluída com sucesso."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
    $envText = $null
    $generatorOutput = $null
    $generatorText = $null
    $cleanupFailures = New-Object System.Collections.Generic.List[string]

    if ((Test-ContainerRunning $postgresContainer) -and
        ($null -ne $cudPromotionId -or $null -ne $restartPromotionId)) {
        try {
            $promotionIds = @(
                @($cudPromotionId, $restartPromotionId) |
                    Where-Object { [string]$_ -match "^[0-9]+$" }
            )

            if ($promotionIds.Count -gt 0) {
                Invoke-PostgresCommand (
                    "DELETE FROM promotions WHERE promotion_id IN ({0});" -f
                    [string]::Join(",", $promotionIds)
                )
            }
        }
        catch {
            $cleanupFailures.Add("marcadores SQL")
        }
    }

    if ($stackStartAttempted) {
        foreach ($service in @("debezium", "kafka", "postgres")) {
            Write-Host "Parando $service..."
            & docker compose --profile cdc stop -t 30 $service
            if ($LASTEXITCODE -ne 0) {
                $cleanupFailures.Add("serviço $service")
            }
        }

        try {
            $runningAfter = @(Get-RunningServices)
            if ($runningAfter.Count -ne 0) {
                $cleanupFailures.Add("confirmação de isolamento final")
            }
        }
        catch {
            $cleanupFailures.Add("consulta do estado final")
        }
    }

    Pop-Location

    if ($cleanupFailures.Count -ne 0) {
        $cleanupSummary = [string]::Join(", ", $cleanupFailures)
    }

    if ($stackStartAttempted) {
        Write-Host "Todos os serviços CDC foram parados."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação CDC falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }

    throw "A validação CDC falhou: $primaryFailure"
}

if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação CDC exige revisão em: $cleanupSummary"
}

if (-not $validationCompleted) {
    throw "A validação CDC não foi concluída."
}
