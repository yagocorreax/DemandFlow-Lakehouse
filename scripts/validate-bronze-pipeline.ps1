$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$stackStartAttempted = $false
$validationCompleted = $false
$primaryFailure = $null
$cleanupSummary = $null
$tableConfig = Get-Content `
    -LiteralPath (Join-Path $projectRoot "config\tables.json") `
    -Raw |
    ConvertFrom-Json
$expectedTables = @(
    $tableConfig.PSObject.Properties.Name |
        Sort-Object
)

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

function Invoke-BronzeRun {
    param(
        [switch]$RequireZero
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\run-bronze.ps1"
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
        throw "A execução Raw -> Bronze falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $matches = [regex]::Matches(
        $outputText,
        "(?m)^BRONZE_NEW_EVENTS_(?<table>[a-z_]+)=" +
        "(?<count>[0-9]+)[\r ]*$"
    )
    $counts = @{}

    foreach ($match in $matches) {
        $tableName = $match.Groups["table"].Value
        if ($counts.ContainsKey($tableName)) {
            throw "A métrica de novos eventos foi duplicada para $tableName."
        }

        $counts[$tableName] = [long]$match.Groups["count"].Value
    }

    if ($counts.Count -ne $expectedTables.Count) {
        throw (
            "A transformação publicou métricas para " +
            "$($counts.Count)/$($expectedTables.Count) tabelas."
        )
    }

    foreach ($tableName in $expectedTables) {
        if (-not $counts.ContainsKey($tableName)) {
            throw "A transformação não publicou métrica para $tableName."
        }

        if ($RequireZero -and $counts[$tableName] -ne 0) {
            throw (
                "A repetição encontrou $($counts[$tableName]) eventos " +
                "novos em $tableName."
            )
        }
    }

    if ($RequireZero) {
        Write-Host "A repetição encontrou zero eventos novos nas 8 tabelas."
    }
}

function Invoke-BronzeValidationSnapshot {
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\validate-bronze.ps1"
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
        throw "A validação estrutural e semântica da Bronze falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $requiredMarkers = @(
        "BRONZE_TABLE_COUNT=$($expectedTables.Count)",
        "BRONZE_MISSING_EVENTS_FROM_RAW=0",
        "BRONZE_EXTRA_EVENTS_NOT_IN_RAW=0",
        "BRONZE_PAYLOAD_MISMATCH_COUNT=0",
        "BRONZE_CURRENT_MISMATCH_COUNT=0",
        "BRONZE_CDF_ENABLED=true"
    )

    foreach ($marker in $requiredMarkers) {
        if (-not $outputText.Contains($marker)) {
            throw "A validação Bronze não comprovou: $marker"
        }
    }

    $rawMatch = [regex]::Match(
        $outputText,
        "(?m)^BRONZE_RAW_EVENT_COUNT=(?<count>[0-9]+)[\r ]*$"
    )
    $eventsMatch = [regex]::Match(
        $outputText,
        "(?m)^BRONZE_EVENTS_COUNT=(?<count>[0-9]+)[\r ]*$"
    )
    if (-not $rawMatch.Success -or -not $eventsMatch.Success) {
        throw "A validação Bronze não publicou as contagens totais."
    }

    if ($rawMatch.Groups["count"].Value -ne $eventsMatch.Groups["count"].Value) {
        throw "A contagem total de Events não corresponde à Raw."
    }

    $matches = [regex]::Matches(
        $outputText,
        "(?m)^(?<key>BRONZE_(?:EVENTS|CURRENT)_" +
        "(?:COUNT|VERSION)_(?<table>[a-z_]+))=" +
        "(?<value>[0-9]+)[\r ]*$"
    )
    $metrics = @{}

    foreach ($match in $matches) {
        $key = $match.Groups["key"].Value
        if ($metrics.ContainsKey($key)) {
            throw "A validação publicou a métrica $key mais de uma vez."
        }

        $metrics[$key] = $match.Groups["value"].Value
    }

    foreach ($tableName in $expectedTables) {
        foreach ($prefix in @(
            "BRONZE_EVENTS_COUNT",
            "BRONZE_EVENTS_VERSION",
            "BRONZE_CURRENT_COUNT",
            "BRONZE_CURRENT_VERSION"
        )) {
            $key = "${prefix}_${tableName}"
            if (-not $metrics.ContainsKey($key)) {
                throw "A validação não publicou $key."
            }
        }
    }

    if ($metrics.Count -ne (4 * $expectedTables.Count)) {
        throw "O conjunto de métricas por tabela é ambíguo."
    }

    return @(
        $metrics.GetEnumerator() |
            ForEach-Object { "$($_.Key)=$($_.Value)" } |
            Sort-Object
    )
}

Push-Location $projectRoot

try {
    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "A validação Bronze exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true

    Write-Host "Executando a primeira transformação Raw -> Bronze..."
    Invoke-BronzeRun

    Write-Host "Validando equivalência Raw/Events e replay de Current..."
    $firstSnapshot = @(Invoke-BronzeValidationSnapshot)

    Write-Host "Repetindo Raw -> Bronze para provar idempotência..."
    Invoke-BronzeRun -RequireZero

    Write-Host "Confirmando que contagens e versões Delta permaneceram estáveis..."
    $secondSnapshot = @(Invoke-BronzeValidationSnapshot)
    $difference = @(
        Compare-Object `
            -ReferenceObject $firstSnapshot `
            -DifferenceObject $secondSnapshot
    )

    if ($difference.Count -ne 0) {
        throw "Contagens ou versões Delta mudaram durante a repetição sem entrada."
    }

    Write-Host "Contagens e versões Delta permaneceram inalteradas."
    $validationCompleted = $true
    Write-Host "VALIDAÇÃO RAW -> BRONZE CONCLUÍDA COM SUCESSO."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
    $cleanupFailures = New-Object System.Collections.Generic.List[string]

    if ($stackStartAttempted) {
        foreach ($service in @("spark", "minio")) {
            Write-Host "Parando $service..."
            & docker compose --profile processing stop -t 30 $service
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
        Write-Host "Todos os serviços usados pela validação Bronze foram parados."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Raw -> Bronze falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }

    throw "A validação Raw -> Bronze falhou: $primaryFailure"
}

if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação Bronze exige revisão em: $cleanupSummary"
}

if (-not $validationCompleted) {
    throw "A validação Raw -> Bronze não foi concluída."
}
