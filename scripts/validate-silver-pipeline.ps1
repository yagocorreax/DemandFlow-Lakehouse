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

function Invoke-SilverRun {
    param(
        [switch]$RequireNoWrites
    )

    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\run-silver.ps1"
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
        throw "A execução Bronze -> Silver falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $matches = [regex]::Matches(
        $outputText,
        "(?m)^(?<layer>SILVER|QUARANTINE)_WRITE_APPLIED_" +
        "(?<table>[a-z_]+)=(?<value>true|false)[\r ]*$"
    )
    $metrics = @{}

    foreach ($match in $matches) {
        $key = "$($match.Groups['layer'].Value)_$($match.Groups['table'].Value)"
        if ($metrics.ContainsKey($key)) {
            throw "A métrica de escrita foi duplicada para $key."
        }
        $metrics[$key] = $match.Groups["value"].Value
    }

    if ($metrics.Count -ne (2 * $expectedTables.Count)) {
        throw "A transformação não publicou todas as métricas de escrita."
    }

    foreach ($tableName in $expectedTables) {
        foreach ($layer in @("SILVER", "QUARANTINE")) {
            $key = "${layer}_${tableName}"
            if (-not $metrics.ContainsKey($key)) {
                throw "A transformação não publicou a métrica $key."
            }
            if ($RequireNoWrites -and $metrics[$key] -ne "false") {
                throw "A repetição realizou escrita inesperada em $key."
            }
        }
    }

    if ($RequireNoWrites) {
        Write-Host "A repetição não escreveu em nenhuma tabela Silver/Quarantine."
    }
}

function Invoke-SilverValidationSnapshot {
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & "$PSScriptRoot\validate-silver.ps1"
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
        throw "A validação estrutural e semântica da Silver falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    $requiredMarkers = @(
        "SILVER_TABLE_COUNT=$($expectedTables.Count)",
        "SILVER_EXACT_MISMATCH_COUNT=0",
        "SILVER_ORPHAN_COUNT=0",
        "SILVER_RULE_PROBES_PASSED=6",
        "SILVER_CDF_ENABLED=true"
    )

    foreach ($marker in $requiredMarkers) {
        if (-not $outputText.Contains($marker)) {
            throw "A validação Silver não comprovou: $marker"
        }
    }

    $totalPatterns = @{
        bronze = "SILVER_BRONZE_COUNT"
        valid = "SILVER_VALID_COUNT"
        quarantine = "SILVER_QUARANTINE_COUNT"
    }
    $totals = @{}
    foreach ($name in $totalPatterns.Keys) {
        $metricName = $totalPatterns[$name]
        $match = [regex]::Match(
            $outputText,
            "(?m)^${metricName}=(?<count>[0-9]+)[\r ]*$"
        )
        if (-not $match.Success) {
            throw "A validação não publicou $metricName."
        }
        $totals[$name] = [long]$match.Groups["count"].Value
    }
    if ($totals["valid"] + $totals["quarantine"] -ne $totals["bronze"]) {
        throw "As contagens Silver e Quarantine não recompõem a Bronze."
    }

    $matches = [regex]::Matches(
        $outputText,
        "(?m)^(?<key>(?:SILVER_(?:BRONZE|VALID|QUARANTINE)_COUNT|" +
        "SILVER_VERSION|QUARANTINE_VERSION)_(?<table>[a-z_]+))=" +
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
            "SILVER_BRONZE_COUNT",
            "SILVER_VALID_COUNT",
            "SILVER_QUARANTINE_COUNT",
            "SILVER_VERSION",
            "QUARANTINE_VERSION"
        )) {
            $key = "${prefix}_${tableName}"
            if (-not $metrics.ContainsKey($key)) {
                throw "A validação não publicou $key."
            }
        }
    }

    if ($metrics.Count -ne (5 * $expectedTables.Count)) {
        throw "O conjunto de métricas Silver por tabela é ambíguo."
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
        throw "A validação Silver exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true

    Write-Host "Executando a primeira transformação Bronze -> Silver..."
    Invoke-SilverRun

    Write-Host "Validando tipagem, qualidade, quarentena e referências..."
    $firstSnapshot = @(Invoke-SilverValidationSnapshot)

    Write-Host "Repetindo Bronze -> Silver para provar idempotência..."
    Invoke-SilverRun -RequireNoWrites

    Write-Host "Confirmando contagens e versões Delta estáveis..."
    $secondSnapshot = @(Invoke-SilverValidationSnapshot)
    $difference = @(
        Compare-Object `
            -ReferenceObject $firstSnapshot `
            -DifferenceObject $secondSnapshot
    )
    if ($difference.Count -ne 0) {
        throw "Contagens ou versões Delta mudaram durante a repetição."
    }

    Write-Host "Contagens e versões Delta permaneceram inalteradas."
    $validationCompleted = $true
    Write-Host "VALIDAÇÃO BRONZE -> SILVER CONCLUÍDA COM SUCESSO."
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
        Write-Host "Todos os serviços usados pela validação Silver foram parados."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Bronze -> Silver falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }

    throw "A validação Bronze -> Silver falhou: $primaryFailure"
}

if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação Silver exige revisão em: $cleanupSummary"
}

if (-not $validationCompleted) {
    throw "A validação Bronze -> Silver não foi concluída."
}
