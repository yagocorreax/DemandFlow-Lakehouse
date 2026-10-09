$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$ivyCachePath = "/opt/demandflow/.ivy2"
$stackStartAttempted = $false
$validationCompleted = $false
$primaryFailure = $null
$cleanupSummary = $null

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

function Invoke-RawValidationAndGetCount {
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\validate-raw.ps1"
        )
        $invocationSucceeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    foreach ($line in $output) {
        Write-Host ([string]$line)
    }

    if (-not $invocationSucceeded -or $exitCode -ne 0) {
        throw "A validação estrutural da Raw falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $countMatches = [regex]::Matches(
        $outputText,
        "(?m)^RAW_EVENT_COUNT=(?<count>[0-9]+)[\r ]*$"
    )

    if ($countMatches.Count -ne 1) {
        throw "A validação Raw não publicou uma contagem inequívoca."
    }

    return [long]$countMatches[0].Groups["count"].Value
}

function Invoke-SecondRawAndAssertNoInput {
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\invoke-raw-spark.ps1"
        )
        $invocationSucceeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    foreach ($line in $output) {
        Write-Host ([string]$line)
    }

    if (-not $invocationSucceeded -or $exitCode -ne 0) {
        throw "A segunda execução Kafka -> Raw falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $metricMatches = [regex]::Matches(
        $outputText,
        "(?m)^RAW_INPUT_ROWS_TOTAL=(?<count>[0-9]+)[\r ]*$"
    )

    if ($metricMatches.Count -ne 1) {
        throw "A segunda execução não publicou a métrica de entrada esperada."
    }

    $inputCount = [long]$metricMatches[0].Groups["count"].Value
    if ($inputCount -ne 0) {
        throw "A repetição consumiu $inputCount eventos; o checkpoint não foi idempotente."
    }

    Write-Host "A repetição consumiu zero eventos novos."
}

function Get-IvyCacheInventory {
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker compose `
                --profile processing `
                run `
                --rm `
                --no-deps `
                spark `
                sh `
                -ec `
                "test -w $ivyCachePath && find $ivyCachePath -type f -print" `
                2>&1
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "O cache Ivy não está acessível para escrita pelo usuário spark."
    }

    $files = @(
        $output |
            ForEach-Object { [string]$_ } |
            Where-Object { $_.StartsWith("$ivyCachePath/") } |
            Sort-Object -Unique
    )

    if ($files.Count -eq 0) {
        throw "O cache Ivy persistente está vazio."
    }

    $inventoryText = [string]::Join("`n", $files)
    foreach ($requiredJar in @(
        "spark-sql-kafka-0-10_2.12-3.5.9.jar",
        "hadoop-aws-3.3.4.jar"
    )) {
        if (-not $inventoryText.Contains($requiredJar)) {
            throw "O artefato obrigatório $requiredJar não está no cache Ivy."
        }
    }

    return @($files)
}

Push-Location $projectRoot

try {
    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "A validação Raw exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true

    Write-Host "Executando o primeiro consumo Kafka -> Raw..."
    & "$PSScriptRoot\run-raw-ingestion.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "O primeiro consumo Kafka -> Raw falhou."
    }

    $firstRawCount = Invoke-RawValidationAndGetCount
    $cacheBefore = @(Get-IvyCacheInventory)
    Write-Host "Cache Ivy persistente contém $($cacheBefore.Count) arquivos."

    Write-Host "Repetindo o consumo com o mesmo checkpoint..."
    Invoke-SecondRawAndAssertNoInput

    $cacheAfter = @(Get-IvyCacheInventory)
    if (Compare-Object $cacheBefore $cacheAfter) {
        throw "O inventário Ivy mudou ao repetir exatamente as mesmas dependências."
    }

    $secondRawCount = Invoke-RawValidationAndGetCount
    if ($secondRawCount -ne $firstRawCount) {
        throw (
            "A contagem Raw mudou sem eventos novos: " +
            "$firstRawCount -> $secondRawCount."
        )
    }

    Write-Host "Contagem Raw permaneceu em $secondRawCount eventos."
    Write-Host "Cache Ivy foi reutilizado sem novo download ou perda de artefatos."

    $validationCompleted = $true
    Write-Host "VALIDAÇÃO KAFKA -> RAW CONCLUÍDA COM SUCESSO."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
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
        Write-Host "Todos os serviços usados pela validação Raw foram parados."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Kafka -> Raw falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }

    throw "A validação Kafka -> Raw falhou: $primaryFailure"
}

if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação Raw exige revisão em: $cleanupSummary"
}

if (-not $validationCompleted) {
    throw "A validação Kafka -> Raw não foi concluída."
}
