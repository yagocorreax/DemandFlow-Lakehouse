$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$stackStartAttempted = $false
$validationCompleted = $false
$primaryFailure = $null
$cleanupSummary = $null
$expectedTables = @(
    "daily_sales",
    "forecast_accuracy",
    "inventory_health",
    "product_sales"
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

function Invoke-GoldRun {
    param(
        [switch]$RequireNoWrites
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\run-gold.ps1")
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
        throw "A execução Silver -> Gold falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $writeMatches = [regex]::Matches(
        $outputText,
        "(?m)^GOLD_WRITE_APPLIED_(?<table>[a-z_]+)=" +
        "(?<value>true|false)[\r ]*$"
    )
    $countMatches = [regex]::Matches(
        $outputText,
        "(?m)^GOLD_ROW_COUNT_(?<table>[a-z_]+)=" +
        "(?<value>[0-9]+)[\r ]*$"
    )
    $writes = @{}
    $counts = @{}
    foreach ($match in $writeMatches) {
        $tableName = $match.Groups["table"].Value
        if ($writes.ContainsKey($tableName)) {
            throw "A métrica de escrita Gold foi duplicada para $tableName."
        }
        $writes[$tableName] = $match.Groups["value"].Value
    }
    foreach ($match in $countMatches) {
        $tableName = $match.Groups["table"].Value
        if ($counts.ContainsKey($tableName)) {
            throw "A contagem Gold foi duplicada para $tableName."
        }
        $counts[$tableName] = $match.Groups["value"].Value
    }

    foreach ($tableName in $expectedTables) {
        if (-not $writes.ContainsKey($tableName)) {
            throw "A transformação não publicou escrita para $tableName."
        }
        if (-not $counts.ContainsKey($tableName)) {
            throw "A transformação não publicou contagem para $tableName."
        }
        if ($RequireNoWrites -and $writes[$tableName] -ne "false") {
            throw "A repetição realizou escrita inesperada em $tableName."
        }
    }
    if (
        $writes.Count -ne $expectedTables.Count -or
        $counts.Count -ne $expectedTables.Count
    ) {
        throw "O conjunto de métricas da transformação Gold é ambíguo."
    }
    if ($RequireNoWrites) {
        Write-Host "A repetição não escreveu em nenhuma tabela Gold."
    }
}

function Invoke-GoldValidationSnapshot {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\validate-gold.ps1")
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
        throw "A validação estrutural e semântica da Gold falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $requiredMarkers = @(
        "GOLD_TABLE_COUNT=$($expectedTables.Count)",
        "GOLD_EXACT_MISMATCH_COUNT=0",
        "GOLD_FINANCIAL_RECONCILIATION=true",
        "GOLD_SOURCE_RECONCILIATION=true",
        "GOLD_RULE_PROBES_PASSED=6",
        "GOLD_CDF_ENABLED=true"
    )
    foreach ($marker in $requiredMarkers) {
        if (-not $outputText.Contains($marker)) {
            throw "A validação Gold não comprovou: $marker"
        }
    }

    $snapshot = @{}
    foreach ($tableName in $expectedTables) {
        foreach ($prefix in @("GOLD_ROW_COUNT", "GOLD_VERSION")) {
            $key = "${prefix}_${tableName}"
            $match = [regex]::Match(
                $outputText,
                "(?m)^${key}=(?<value>[0-9]+)[\r ]*$"
            )
            if (-not $match.Success) {
                throw "A validação não publicou $key."
            }
            $snapshot[$key] = $match.Groups["value"].Value
        }
        if (-not $outputText.Contains(
            "GOLD_EXACT_MISMATCH_COUNT_${tableName}=0"
        )) {
            throw "A comparação exata falhou para $tableName."
        }
    }

    $businessMetrics = @(
        "GOLD_TOTAL_ROW_COUNT",
        "GOLD_ELIGIBLE_ORDER_COUNT",
        "GOLD_TOTAL_REVENUE",
        "GOLD_NET_REVENUE",
        "GOLD_TOTAL_UNITS_SOLD",
        "GOLD_OUT_OF_STOCK_COUNT",
        "GOLD_LOW_STOCK_COUNT",
        "GOLD_HEALTHY_STOCK_COUNT",
        "GOLD_FORECAST_MAE"
    )
    foreach ($key in $businessMetrics) {
        $match = [regex]::Match(
            $outputText,
            "(?m)^${key}=(?<value>[0-9]+(?:[.][0-9]+)?)[\r ]*$"
        )
        if (-not $match.Success) {
            throw "A validação não publicou $key."
        }
        $snapshot[$key] = $match.Groups["value"].Value
    }

    return @(
        $snapshot.GetEnumerator() |
            ForEach-Object { "$($_.Key)=$($_.Value)" } |
            Sort-Object
    )
}

Push-Location $projectRoot

try {
    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "A validação Gold exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true
    Write-Host "Executando a primeira transformação Silver -> Gold..."
    Invoke-GoldRun

    Write-Host "Validando métricas, schemas e reconciliações Gold..."
    $firstSnapshot = @(Invoke-GoldValidationSnapshot)

    Write-Host "Repetindo Silver -> Gold para provar idempotência..."
    Invoke-GoldRun -RequireNoWrites

    Write-Host "Confirmando métricas e versões Delta estáveis..."
    $secondSnapshot = @(Invoke-GoldValidationSnapshot)
    $difference = @(
        Compare-Object `
            -ReferenceObject $firstSnapshot `
            -DifferenceObject $secondSnapshot
    )
    if ($difference.Count -ne 0) {
        throw "Métricas ou versões Gold mudaram durante a repetição."
    }

    Write-Host "Métricas e versões Gold permaneceram inalteradas."
    $validationCompleted = $true
    Write-Host "VALIDAÇÃO SILVER -> GOLD CONCLUÍDA COM SUCESSO."
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
    if ($stackStartAttempted -and $cleanupFailures.Count -eq 0) {
        Write-Host "Todos os serviços usados pela validação Gold foram parados."
    }
    elseif ($stackStartAttempted) {
        Write-Warning "A limpeza da validação Gold não pôde ser confirmada."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Silver -> Gold falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }
    throw "A validação Silver -> Gold falhou: $primaryFailure"
}
if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação Gold exige revisão em: $cleanupSummary"
}
if (-not $validationCompleted) {
    throw "A validação Silver -> Gold não foi concluída."
}
