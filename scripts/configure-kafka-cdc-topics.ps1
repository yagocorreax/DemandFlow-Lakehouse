$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$kafkaContainer = "demandflow-kafka"
$bootstrapServer = "kafka:29092"
$signalTopic = "demandflow-signal"
$sourceTopics = @(
    "demandflow.public.stores",
    "demandflow.public.products",
    "demandflow.public.promotions",
    "demandflow.public.orders",
    "demandflow.public.order_items",
    "demandflow.public.inventory",
    "demandflow.public.inventory_movements",
    "demandflow.public.demand_forecasts"
)

function Invoke-KafkaCommand {
    param(
        [Parameter(Mandatory)]
        [string]$Executable,

        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter(Mandatory)]
        [string]$FailureMessage
    )

    & docker exec $script:kafkaContainer $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
}

function Get-TopicDescription {
    param(
        [Parameter(Mandatory)]
        [string]$Topic
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-topics.sh `
                --bootstrap-server $script:bootstrapServer `
                --describe `
                --topic $Topic `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Não foi possível descrever o tópico $Topic."
    }

    return [string]::Join("`n", @($output | ForEach-Object { [string]$_ }))
}

function Get-TopicConfig {
    param(
        [Parameter(Mandatory)]
        [string]$Topic
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec $script:kafkaContainer `
                /opt/kafka/bin/kafka-configs.sh `
                --bootstrap-server $script:bootstrapServer `
                --entity-type topics `
                --entity-name $Topic `
                --describe `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Não foi possível consultar a retenção do tópico $Topic."
    }

    return [string]::Join("`n", @($output | ForEach-Object { [string]$_ }))
}

$running = & docker inspect `
    --format "{{.State.Running}}" `
    $kafkaContainer `
    2>$null
if ($LASTEXITCODE -ne 0 -or
    ([string]::Join("", @($running))).Trim() -ne "true") {
    throw "Kafka precisa estar em execução antes de configurar os tópicos CDC."
}

Write-Host "Garantindo tópico de sinalização com uma única partição..."
Invoke-KafkaCommand `
    -Executable "/opt/kafka/bin/kafka-topics.sh" `
    -Arguments @(
        "--bootstrap-server", $bootstrapServer,
        "--create", "--if-not-exists",
        "--topic", $signalTopic,
        "--partitions", "1",
        "--replication-factor", "1"
    ) `
    -FailureMessage "Falha ao criar o tópico de sinalização do Debezium."

$signalDescription = Get-TopicDescription $signalTopic
if ($signalDescription -notmatch "PartitionCount:\s*1(?:\s|$)") {
    throw "O tópico $signalTopic não possui exatamente uma partição."
}

foreach ($topic in $sourceTopics) {
    Write-Host "Configurando retenção ilimitada em $topic..."

    Invoke-KafkaCommand `
        -Executable "/opt/kafka/bin/kafka-topics.sh" `
        -Arguments @(
            "--bootstrap-server", $bootstrapServer,
            "--create", "--if-not-exists",
            "--topic", $topic,
            "--partitions", "3",
            "--replication-factor", "1",
            "--config", "retention.ms=-1",
            "--config", "retention.bytes=-1"
        ) `
        -FailureMessage "Falha ao garantir o tópico $topic."

    Invoke-KafkaCommand `
        -Executable "/opt/kafka/bin/kafka-configs.sh" `
        -Arguments @(
            "--bootstrap-server", $bootstrapServer,
            "--entity-type", "topics",
            "--entity-name", $topic,
            "--alter",
            "--add-config", "retention.ms=-1,retention.bytes=-1"
        ) `
        -FailureMessage "Falha ao aplicar retenção ilimitada em $topic."

    $description = Get-TopicDescription $topic
    if ($description -notmatch "PartitionCount:\s*3(?:\s|$)") {
        throw "O tópico $topic não possui exatamente três partições."
    }

    $config = Get-TopicConfig $topic
    if ($config -notmatch "retention[.]ms=-1(?:\s|,|$)" -or
        $config -notmatch "retention[.]bytes=-1(?:\s|,|$)") {
        throw "A retenção ilimitada não foi confirmada em $topic."
    }
}

# A mensagem WAL usada somente para destravar a retomada pode criar este
# tópico auxiliar em versões/configurações antigas. Ele não é fonte da Raw e
# deve continuar sujeito à retenção padrão do broker, preservando o escopo
# ilimitado exclusivamente nas oito tabelas CDC.
$messageTopic = "demandflow.message"
$messageTopicExists = $false
$previousErrorAction = $ErrorActionPreference
try {
    $ErrorActionPreference = "Continue"
    & docker exec $kafkaContainer `
        /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $bootstrapServer `
        --describe `
        --topic $messageTopic `
        1>$null `
        2>$null
    $messageTopicExists = $LASTEXITCODE -eq 0
}
finally {
    $ErrorActionPreference = $previousErrorAction
}

if ($messageTopicExists) {
    $messageConfig = Get-TopicConfig $messageTopic
    $overrides = @()
    if ($messageConfig -match "retention[.]ms=") {
        $overrides += "retention.ms"
    }
    if ($messageConfig -match "retention[.]bytes=") {
        $overrides += "retention.bytes"
    }
    if ($overrides.Count -gt 0) {
        Invoke-KafkaCommand `
            -Executable "/opt/kafka/bin/kafka-configs.sh" `
            -Arguments @(
                "--bootstrap-server", $bootstrapServer,
                "--entity-type", "topics",
                "--entity-name", $messageTopic,
                "--alter",
                "--delete-config", [string]::Join(",", $overrides)
            ) `
            -FailureMessage "Falha ao restaurar a retenção padrão de $messageTopic."
    }
}

Write-Host "Tópico de sinalização e oito tópicos CDC validados."
Write-Host "KAFKA_CDC_RETENTION_MS=-1"
Write-Host "KAFKA_CDC_RETENTION_BYTES=-1"
