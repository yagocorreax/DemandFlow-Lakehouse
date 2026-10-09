param(
    [switch]$CreateSnapshot,

    [switch]$ResumePublishedSnapshot,

    [long]$PreviouslyMeasuredKafkaSizeKb = -1
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$postgresContainer = "demandflow-postgres"
$kafkaContainer = "demandflow-kafka"
$connectorName = "demandflow-postgres-connector"
$connectUrl = "http://127.0.0.1:8083"
$bootstrapServer = "kafka:29092"
$signalTopic = "demandflow-signal"
$topicPrefix = "demandflow"
$tables = [ordered]@{
    stores = "public.stores"
    products = "public.products"
    promotions = "public.promotions"
    orders = "public.orders"
    order_items = "public.order_items"
    inventory = "public.inventory"
    inventory_movements = "public.inventory_movements"
    demand_forecasts = "public.demand_forecasts"
}
$stackStartAttempted = $false
$recoveryCompleted = $false
$primaryFailure = $null
$cleanupSummary = $null

if ($CreateSnapshot -eq $ResumePublishedSnapshot) {
    throw (
        "Informe exatamente uma ação: -CreateSnapshot para uma nova " +
        "recuperação ou -ResumePublishedSnapshot para retomar um lote " +
        "já publicado."
    )
}

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
    $matches = [regex]::Matches($Text, $pattern)
    if ($matches.Count -eq 0) {
        throw "Variável $Name não encontrada no .env."
    }
    if ($matches.Count -ne 1) {
        throw "Variável $Name está duplicada no .env."
    }

    $value = $matches[0].Groups["value"].Value.Trim()
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
    if (@(Compare-Object $expected $actual).Count -ne 0) {
        throw "O isolamento falhou: somente PostgreSQL, Kafka e Debezium deveriam estar ativos."
    }
}

function Assert-OnlyRecoveryCoreServices {
    $expected = @("kafka", "postgres") | Sort-Object
    $actual = @(Get-RunningServices) | Sort-Object
    if (@(Compare-Object $expected $actual).Count -ne 0) {
        throw "A retomada deve manter somente PostgreSQL e Kafka ativos."
    }
}

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

function Start-RecoveryCoreServices {
    Write-Host "Iniciando PostgreSQL para conferir a fonte..."
    & docker compose up -d --no-deps postgres
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar PostgreSQL na retomada."
    }
    Wait-ContainerHealthy `
        -ContainerName $script:postgresContainer `
        -ServiceName "PostgreSQL" `
        -Attempts 30

    Write-Host "Iniciando Kafka após PostgreSQL estar saudável..."
    & docker compose --profile cdc up -d --no-deps kafka
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao iniciar Kafka na retomada."
    }
    Wait-ContainerHealthy `
        -ContainerName $script:kafkaContainer `
        -ServiceName "Kafka"

    & "$PSScriptRoot\configure-kafka-cdc-topics.ps1"
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao reconfirmar a retenção dos tópicos CDC."
    }
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
            $failed = @(
                $tasks | Where-Object { $_.state -eq "FAILED" }
            )
            if ($status.connector.state -eq "FAILED" -or $failed.Count -ne 0) {
                throw "O connector Debezium entrou no estado FAILED."
            }
            $notRunning = @(
                $tasks | Where-Object { $_.state -ne "RUNNING" }
            )
            if ($status.connector.state -eq "RUNNING" -and
                $tasks.Count -gt 0 -and
                $notRunning.Count -eq 0) {
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

function Get-SourceCounts {
    $queries = @()
    foreach ($tableName in $script:tables.Keys) {
        $qualifiedName = $script:tables[$tableName]
        $queries += "SELECT '$tableName', COUNT(*)::bigint FROM $qualifiedName"
    }
    $sql = [string]::Join(" UNION ALL`n", $queries) + ";"

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            $sql |
                & docker exec -i $script:postgresContainer `
                    psql `
                    -X `
                    -qAt `
                    -F "|" `
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
        throw "Não foi possível contar as linhas atuais no PostgreSQL."
    }

    $counts = @{}
    foreach ($line in $output) {
        if ([string]$line -match "^(?<table>[a-z_]+)[|](?<count>[0-9]+)$") {
            $counts[$Matches["table"]] = [long]$Matches["count"]
        }
    }
    foreach ($tableName in $script:tables.Keys) {
        if (-not $counts.ContainsKey($tableName)) {
            throw "A contagem da tabela $tableName não foi retornada."
        }
    }

    return $counts
}

function Advance-LogicalWalForSnapshot {
    $sql = @"
SELECT pg_logical_emit_message(
    true,
    'demandflow-snapshot-resume',
    'snapshot-recovery'
);
"@
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            $sql |
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

    $lsn = ([string]::Join("", @($output))).Trim()
    if ($exitCode -ne 0 -or $lsn -cnotmatch "^[0-9A-Fa-f]+/[0-9A-Fa-f]+$") {
        throw "Não foi possível avançar o WAL lógico antes do snapshot."
    }

    Write-Host "WAL lógico avançado sem alteração de tabela."
}

function Get-TopicOffsets {
    param(
        [Parameter(Mandatory)]
        [string]$Topic,

        [long]$Time = -1
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-get-offsets.sh `
                --bootstrap-server $script:bootstrapServer `
                --topic $Topic `
                --time $Time `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Não foi possível consultar os offsets de $Topic."
    }

    $offsets = @{}
    $pattern = (
        "^" + [regex]::Escape($Topic) +
        ":(?<partition>[0-9]+):(?<offset>[0-9]+)$"
    )
    foreach ($line in $output) {
        if ([string]$line -match $pattern) {
            $offsets[[int]$Matches["partition"]] = [long]$Matches["offset"]
        }
    }
    if ($offsets.Count -ne 3) {
        throw "O tópico $Topic não apresentou exatamente três partições."
    }

    return $offsets
}

function Get-AllTopicOffsets {
    param(
        [long]$Time = -1
    )

    $result = @{}
    foreach ($tableName in $script:tables.Keys) {
        $topic = "$script:topicPrefix.public.$tableName"
        $result[$topic] = Get-TopicOffsets -Topic $topic -Time $Time
    }
    return $result
}

function Get-OffsetDelta {
    param(
        [Parameter(Mandatory)]
        [hashtable]$Before,

        [Parameter(Mandatory)]
        [hashtable]$After
    )

    [long]$delta = 0
    foreach ($partition in $Before.Keys) {
        if (-not $After.ContainsKey($partition)) {
            throw "Uma partição desapareceu durante a recuperação."
        }
        $partitionDelta = [long]$After[$partition] - [long]$Before[$partition]
        if ($partitionDelta -lt 0) {
            throw "Um offset Kafka retrocedeu durante a recuperação."
        }
        $delta += $partitionDelta
    }
    return $delta
}

function Wait-SnapshotOffsets {
    param(
        [Parameter(Mandatory)]
        [hashtable]$BeforeByTopic,

        [Parameter(Mandatory)]
        [hashtable]$SourceCounts,

        [int]$Attempts = 120
    )

    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $currentByTopic = Get-AllTopicOffsets
        $complete = $true

        foreach ($tableName in $script:tables.Keys) {
            $topic = "$script:topicPrefix.public.$tableName"
            $delta = Get-OffsetDelta `
                -Before $BeforeByTopic[$topic] `
                -After $currentByTopic[$topic]
            $expected = [long]$SourceCounts[$tableName]

            if ($delta -gt $expected) {
                throw (
                    "O tópico $topic avançou $delta registros; eram esperados " +
                    "$expected. A recuperação foi interrompida para evitar " +
                    "aceitar escrita concorrente ou snapshot duplicado."
                )
            }
            if ($delta -ne $expected) {
                $complete = $false
            }
        }

        if ($complete) {
            Wait-ConnectorRunning
            return $currentByTopic
        }

        Start-Sleep -Seconds 2
    }

    throw "O snapshot bloqueante não publicou todas as linhas dentro do prazo."
}

function Send-BlockingSnapshotSignal {
    $collections = @(
        $script:tables.Values | ForEach-Object { [string]$_ }
    )
    $value = [ordered]@{
        type = "execute-snapshot"
        data = [ordered]@{
            type = "blocking"
            "data-collections" = $collections
        }
    } | ConvertTo-Json -Depth 10 -Compress
    $record = "$script:topicPrefix|$value"

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $record |
            & docker exec -i $script:kafkaContainer `
                /opt/kafka/bin/kafka-console-producer.sh `
                --bootstrap-server $script:bootstrapServer `
                --topic $script:signalTopic `
                --reader-property "parse.key=true" `
                --reader-property "key.separator=|"
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
        $value = $null
        $record = $null
    }

    if ($exitCode -ne 0) {
        throw "Não foi possível publicar o sinal de snapshot bloqueante."
    }
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

function Read-TopicPartitionRange {
    param(
        [Parameter(Mandatory)]
        [string]$Topic,

        [Parameter(Mandatory)]
        [int]$Partition,

        [Parameter(Mandatory)]
        [long]$StartOffset,

        [Parameter(Mandatory)]
        [long]$Count
    )

    if ($Count -le 0) {
        return @()
    }

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $lines = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-console-consumer.sh `
                --bootstrap-server $script:bootstrapServer `
                --topic $Topic `
                --partition $Partition `
                --offset $StartOffset `
                --max-messages $Count `
                --timeout-ms 30000 `
                --formatter-property "print.offset=true" `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0 -or $lines.Count -ne $Count) {
        throw "Não foi possível inspecionar o intervalo $Topic/$Partition."
    }

    $records = New-Object System.Collections.Generic.List[object]
    foreach ($line in $lines) {
        if ([string]$line -cnotmatch "^Offset:(?<offset>[0-9]+)\t(?<json>.+)$") {
            throw "O consumidor Kafka não publicou offset e JSON de forma inequívoca."
        }
        $offset = [long]$Matches["offset"]
        try {
            $message = $Matches["json"] | ConvertFrom-Json
        }
        catch {
            throw "Foi encontrado JSON inválido ao reconstruir o snapshot publicado."
        }
        $payload = Get-RecordProperty $message "payload"
        if ($null -ne $payload) {
            $message = $payload
        }
        $source = Get-RecordProperty $message "source"
        $records.Add(
            [pscustomobject]@{
                Topic = $Topic
                Partition = $Partition
                Offset = $offset
                Operation = [string](Get-RecordProperty $message "op")
                SourceTable = [string](Get-RecordProperty $source "table")
                Snapshot = ([string](
                    Get-RecordProperty $source "snapshot"
                )).ToLowerInvariant()
                SourceTimestamp = [long](
                    Get-RecordProperty $source "ts_ms"
                )
            }
        )
    }

    return $records.ToArray()
}

function Find-LatestPublishedSnapshot {
    param(
        [Parameter(Mandatory)]
        [hashtable]$SourceCounts
    )

    $afterByTopic = Get-AllTopicOffsets -Time -1
    $earliestByTopic = Get-AllTopicOffsets -Time -2
    $candidates = New-Object System.Collections.Generic.List[object]
    $blockingSnapshotMarkers = @(
        "true",
        "first",
        "first_in_data_collection",
        "last_in_data_collection",
        "last"
    )

    foreach ($tableName in $script:tables.Keys) {
        $topic = "$script:topicPrefix.public.$tableName"
        $scanWindow = [long]$SourceCounts[$tableName]
        foreach ($partition in @($afterByTopic[$topic].Keys | Sort-Object)) {
            $endOffset = [long]$afterByTopic[$topic][$partition]
            $earliestOffset = [long]$earliestByTopic[$topic][$partition]
            $startOffset = [Math]::Max(
                $earliestOffset,
                $endOffset - $scanWindow
            )
            $records = @(
                Read-TopicPartitionRange `
                    -Topic $topic `
                    -Partition $partition `
                    -StartOffset $startOffset `
                    -Count ($endOffset - $startOffset)
            )
            foreach ($record in $records) {
                if ($record.Operation -ceq "r" -and
                    $record.SourceTable -ceq $tableName -and
                    $record.Snapshot -in $blockingSnapshotMarkers) {
                    $candidates.Add($record)
                }
            }
        }
    }

    if ($candidates.Count -eq 0) {
        throw "Nenhum snapshot bloqueante publicado foi encontrado para retomar."
    }
    $latestTimestamp = [long](
        $candidates | Measure-Object -Property SourceTimestamp -Maximum
    ).Maximum
    if ($latestTimestamp -le 0) {
        throw "O snapshot publicado não contém timestamp de origem válido."
    }

    $latestRecords = @(
        $candidates |
            Where-Object { $_.SourceTimestamp -eq $latestTimestamp }
    )
    $beforeByTopic = @{}
    [long]$total = 0

    foreach ($tableName in $script:tables.Keys) {
        $topic = "$script:topicPrefix.public.$tableName"
        $beforeByTopic[$topic] = @{}
        [long]$tableCount = 0

        foreach ($partition in @($afterByTopic[$topic].Keys | Sort-Object)) {
            $endOffset = [long]$afterByTopic[$topic][$partition]
            $partitionRecords = @(
                $latestRecords |
                    Where-Object {
                        $_.Topic -ceq $topic -and
                        $_.Partition -eq $partition
                    } |
                    Sort-Object Offset
            )

            if ($partitionRecords.Count -eq 0) {
                $beforeByTopic[$topic][$partition] = $endOffset
                continue
            }

            $baseline = [long]$partitionRecords[0].Offset
            $expectedCount = $endOffset - $baseline
            $lastOffset = [long]$partitionRecords[-1].Offset
            if ($partitionRecords.Count -ne $expectedCount -or
                $lastOffset -ne ($endOffset - 1)) {
                throw (
                    "Há eventos posteriores ou lacunas após o snapshot em " +
                    "$topic/$partition; a retomada foi recusada."
                )
            }

            $beforeByTopic[$topic][$partition] = $baseline
            $tableCount += $partitionRecords.Count
        }

        $expectedTableCount = [long]$SourceCounts[$tableName]
        if ($tableCount -ne $expectedTableCount) {
            throw (
                "O snapshot mais recente contém $tableCount leituras de " +
                "$tableName; eram esperadas $expectedTableCount."
            )
        }
        $total += $tableCount
    }

    return [pscustomobject]@{
        BeforeByTopic = $beforeByTopic
        AfterByTopic = $afterByTopic
        SourceTimestamp = $latestTimestamp
        RecordCount = $total
    }
}

function Assert-NewRecordsAreSnapshotReads {
    param(
        [Parameter(Mandatory)]
        [hashtable]$BeforeByTopic,

        [Parameter(Mandatory)]
        [hashtable]$AfterByTopic
    )

    [long]$validated = 0
    foreach ($tableName in $script:tables.Keys) {
        $topic = "$script:topicPrefix.public.$tableName"
        foreach ($partition in @($BeforeByTopic[$topic].Keys | Sort-Object)) {
            $startOffset = [long]$BeforeByTopic[$topic][$partition]
            $endOffset = [long]$AfterByTopic[$topic][$partition]
            $messageCount = $endOffset - $startOffset
            if ($messageCount -eq 0) {
                continue
            }

            $previousErrorAction = $ErrorActionPreference
            try {
                $ErrorActionPreference = "Continue"
                $messages = @(
                    & docker exec $script:kafkaContainer `
                        /opt/kafka/bin/kafka-console-consumer.sh `
                        --bootstrap-server $script:bootstrapServer `
                        --topic $topic `
                        --partition $partition `
                        --offset $startOffset `
                        --max-messages $messageCount `
                        --timeout-ms 30000 `
                        2>$null
                )
                $exitCode = $LASTEXITCODE
            }
            finally {
                $ErrorActionPreference = $previousErrorAction
            }

            if ($exitCode -ne 0 -or $messages.Count -ne $messageCount) {
                throw "Não foi possível reler integralmente o novo snapshot em $topic/$partition."
            }

            foreach ($line in $messages) {
                try {
                    $message = [string]$line | ConvertFrom-Json
                }
                catch {
                    throw "O snapshot publicou JSON inválido em $topic/$partition."
                }
                $payload = Get-RecordProperty $message "payload"
                if ($null -ne $payload) {
                    $message = $payload
                }
                $operation = [string](Get-RecordProperty $message "op")
                $source = Get-RecordProperty $message "source"
                $sourceTable = [string](Get-RecordProperty $source "table")
                $snapshot = Get-RecordProperty $source "snapshot"
                $snapshotText = ([string]$snapshot).ToLowerInvariant()

                $blockingSnapshotMarkers = @(
                    "true",
                    "first",
                    "first_in_data_collection",
                    "last_in_data_collection",
                    "last"
                )
                if ($operation -cne "r" -or
                    $sourceTable -cne $tableName -or
                    ($snapshot -ne $true -and
                        $snapshotText -notin $blockingSnapshotMarkers)) {
                    throw "Um registro novo em $topic não é uma leitura de snapshot válida."
                }
                $validated++
            }
        }
    }

    return $validated
}

function Get-KafkaDataSizeKb {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                du -sk /tmp/kraft-combined-logs `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }
    $text = [string]::Join(" ", @($output))
    if ($exitCode -ne 0 -or $text -notmatch "^\s*(?<size>[0-9]+)\s") {
        throw "Não foi possível medir o volume persistente do Kafka."
    }
    return [long]$Matches["size"]
}

function Invoke-RawAndGetInputCount {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\invoke-raw-spark.ps1")
        $succeeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }
    foreach ($line in $output) {
        Write-Host ([string]$line)
    }
    if (-not $succeeded -or $exitCode -ne 0) {
        throw "A ingestão Kafka -> Raw falhou."
    }
    $text = [string]::Join("`n", @($output | ForEach-Object { [string]$_ }))
    $matches = [regex]::Matches(
        $text,
        "(?m)^RAW_INPUT_ROWS_TOTAL=(?<count>[0-9]+)[\r ]*$"
    )
    if ($matches.Count -ne 1) {
        throw "A ingestão Raw não publicou uma contagem inequívoca."
    }
    return [long]$matches[0].Groups["count"].Value
}

function Invoke-RawValidation {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\validate-raw.ps1")
        $succeeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }
    foreach ($line in $output) {
        Write-Host ([string]$line)
    }
    if (-not $succeeded -or $exitCode -ne 0) {
        throw "A validação fail-closed da Raw falhou."
    }

    $text = [string]::Join("`n", @($output | ForEach-Object { [string]$_ }))
    $eventMatch = [regex]::Match(
        $text,
        "(?m)^RAW_EVENT_COUNT=(?<count>[0-9]+)[\r ]*$"
    )
    $snapshotMatch = [regex]::Match(
        $text,
        "(?m)^RAW_SNAPSHOT_EVENT_COUNT=(?<count>[0-9]+)[\r ]*$"
    )
    if (-not $eventMatch.Success -or -not $snapshotMatch.Success) {
        throw "A validação Raw não publicou as métricas de completude."
    }

    $snapshotCounts = @{}
    foreach ($tableName in $script:tables.Keys) {
        $match = [regex]::Match(
            $text,
            "(?m)^RAW_SNAPSHOT_COUNT_$tableName=(?<count>[0-9]+)[\r ]*$"
        )
        if (-not $match.Success) {
            throw "A validação Raw não publicou a contagem de $tableName."
        }
        $snapshotCounts[$tableName] = [long]$match.Groups["count"].Value
    }

    return [pscustomobject]@{
        EventCount = [long]$eventMatch.Groups["count"].Value
        SnapshotEventCount = [long]$snapshotMatch.Groups["count"].Value
        SnapshotCounts = $snapshotCounts
    }
}

function Stop-ComposeService {
    param(
        [Parameter(Mandatory)]
        [string]$Service
    )

    Write-Host "Parando $Service..."
    & docker compose --profile cdc stop -t 30 $Service
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível parar $Service."
    }
}

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Arquivo .env não encontrado."
}
$envText = [System.IO.File]::ReadAllText($envFile)
$postgresUser = Get-DotEnvValue $envText "POSTGRES_USER"
$database = Get-DotEnvValue $envText "POSTGRES_DATABASE"

Push-Location $projectRoot
try {
    if (@(Get-RunningServices).Count -ne 0) {
        throw "A recuperação exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true
    if ($ResumePublishedSnapshot) {
        Write-Host "Retomando o snapshot já publicado sem iniciar Debezium..."
        Start-RecoveryCoreServices
        Assert-OnlyRecoveryCoreServices
    }
    else {
        Write-Host "Iniciando a infraestrutura CDC em sequência..."
        & "$PSScriptRoot\register-debezium-connector.ps1"
        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao iniciar a infraestrutura CDC."
        }
        Assert-OnlyCdcServices
        Wait-ConnectorRunning
    }

    $sourceCounts = Get-SourceCounts
    [long]$sourceTotal = 0
    foreach ($tableName in $tables.Keys) {
        $count = [long]$sourceCounts[$tableName]
        $sourceTotal += $count
        Write-Host "Fonte ${tableName}: $count linhas."
    }
    if ($sourceTotal -eq 0) {
        throw "A fonte PostgreSQL está vazia; o snapshot foi cancelado."
    }

    if ($ResumePublishedSnapshot) {
        if ($PreviouslyMeasuredKafkaSizeKb -lt 0) {
            throw (
                "A retomada exige -PreviouslyMeasuredKafkaSizeKb para " +
                "preservar a medição anterior ao snapshot."
            )
        }
        $kafkaSizeBeforeKb = $PreviouslyMeasuredKafkaSizeKb
        $publishedSnapshot = Find-LatestPublishedSnapshot `
            -SourceCounts $sourceCounts
        $beforeByTopic = $publishedSnapshot.BeforeByTopic
        $afterByTopic = $publishedSnapshot.AfterByTopic
        $validatedSnapshotRecords = [long]$publishedSnapshot.RecordCount
        Write-Host (
            "Snapshot já publicado reconstruído pelo timestamp de origem " +
            "$($publishedSnapshot.SourceTimestamp)."
        )
    }
    else {
        # Em uma origem sem escrita recente, o pgoutput pode permanecer
        # procurando o LSN salvo. A mensagem lógica libera a posição sem
        # modificar tabelas e seu prefixo é excluído do fluxo pelo connector.
        Advance-LogicalWalForSnapshot
        $beforeByTopic = Get-AllTopicOffsets
        $kafkaSizeBeforeKb = Get-KafkaDataSizeKb
        Write-Host "Volume Kafka antes do snapshot: $kafkaSizeBeforeKb KiB."

        Write-Host "Solicitando snapshot bloqueante das oito tabelas..."
        Send-BlockingSnapshotSignal
        $afterByTopic = Wait-SnapshotOffsets `
            -BeforeByTopic $beforeByTopic `
            -SourceCounts $sourceCounts

        $validatedSnapshotRecords = Assert-NewRecordsAreSnapshotReads `
            -BeforeByTopic $beforeByTopic `
            -AfterByTopic $afterByTopic
    }
    if ($validatedSnapshotRecords -ne $sourceTotal) {
        throw (
            "Foram validadas $validatedSnapshotRecords leituras de snapshot; " +
            "eram esperadas $sourceTotal."
        )
    }

    $kafkaSizeAfterKb = Get-KafkaDataSizeKb
    Write-Host "Volume Kafka após o snapshot: $kafkaSizeAfterKb KiB."
    Write-Host "As $validatedSnapshotRecords mensagens novas são operações r."

    # A partir daqui os dados já estão persistidos no Kafka. Liberar banco e
    # Connect reduz a pressão local antes dos jobs Spark.
    if (-not $ResumePublishedSnapshot) {
        Stop-ComposeService "debezium"
    }
    Stop-ComposeService "postgres"

    Write-Host "Iniciando somente o armazenamento para a recuperação Raw..."
    & "$PSScriptRoot\start-storage.ps1" -Bootstrap

    Write-Host "Consumindo o snapshot recuperado no checkpoint existente..."
    $firstInputCount = Invoke-RawAndGetInputCount
    if ($firstInputCount -ne $sourceTotal) {
        throw (
            "A Raw consumiu $firstInputCount eventos novos; eram esperados " +
            "$sourceTotal."
        )
    }

    $firstValidation = Invoke-RawValidation
    foreach ($tableName in $tables.Keys) {
        $actual = [long]$firstValidation.SnapshotCounts[$tableName]
        $minimum = [long]$sourceCounts[$tableName]
        if ($actual -lt $minimum) {
            throw "A Raw contém somente $actual snapshots de $tableName; esperava ao menos $minimum."
        }
    }

    Write-Host "Repetindo Kafka -> Raw com o mesmo checkpoint..."
    $secondInputCount = Invoke-RawAndGetInputCount
    if ($secondInputCount -ne 0) {
        throw "A repetição consumiu $secondInputCount eventos; esperava zero."
    }

    Stop-ComposeService "kafka"
    $secondValidation = Invoke-RawValidation
    if ($secondValidation.EventCount -ne $firstValidation.EventCount -or
        $secondValidation.SnapshotEventCount -ne
            $firstValidation.SnapshotEventCount) {
        throw "As contagens Raw mudaram durante a repetição idempotente."
    }

    Write-Host ""
    Write-Host "RAW_SNAPSHOT_RECOVERY_SOURCE_ROWS=$sourceTotal"
    Write-Host "RAW_SNAPSHOT_RECOVERY_KAFKA_READS=$validatedSnapshotRecords"
    Write-Host "RAW_SNAPSHOT_RECOVERY_FIRST_INPUT=$firstInputCount"
    Write-Host "RAW_SNAPSHOT_RECOVERY_SECOND_INPUT=$secondInputCount"
    Write-Host "RAW_SNAPSHOT_RECOVERY_FINAL_EVENTS=$($secondValidation.EventCount)"
    Write-Host "RAW_SNAPSHOT_RECOVERY_KAFKA_KIB_BEFORE=$kafkaSizeBeforeKb"
    Write-Host "RAW_SNAPSHOT_RECOVERY_KAFKA_KIB_AFTER=$kafkaSizeAfterKb"

    $recoveryCompleted = $true
    Write-Host "RECUPERAÇÃO DO SNAPSHOT RAW CONCLUÍDA COM SUCESSO."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
    $envText = $null
    $cleanupFailures = New-Object System.Collections.Generic.List[string]
    if ($stackStartAttempted) {
        foreach ($service in @("debezium", "kafka", "postgres", "minio")) {
            Write-Host "Parando $service..."
            & docker compose --profile cdc stop -t 30 $service
            if ($LASTEXITCODE -ne 0) {
                $cleanupFailures.Add("serviço $service")
            }
        }
        try {
            if (@(Get-RunningServices).Count -ne 0) {
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
        Write-Host "Todos os serviços usados na recuperação foram parados."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A recuperação Raw falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }
    throw "A recuperação Raw falhou: $primaryFailure"
}
if ($null -ne $cleanupSummary) {
    throw "A limpeza da recuperação Raw exige revisão em: $cleanupSummary"
}
if (-not $recoveryCompleted) {
    throw "A recuperação Raw não foi concluída."
}
