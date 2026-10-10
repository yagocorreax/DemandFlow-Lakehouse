$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
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

function Invoke-TrinoRegistration {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\register-gold-trino.ps1")
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
        throw "O registro das tabelas Gold no Trino falhou."
    }

    $validatedTables = @(
        $output |
            Where-Object {
                ([string]$_) -match '^TRINO_TABLE_VALIDATED_[a-z_]+=true$'
            }
    )
    if ($validatedTables.Count -ne 4) {
        throw "O registro Trino não validou exatamente quatro tabelas existentes."
    }
}

function Invoke-TrinoValidationSnapshot {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& "$PSScriptRoot\validate-gold-trino.ps1")
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
        throw "A validação Gold no Trino falhou."
    }

    $requiredMarkers = @(
        "TRINO_TABLE_COUNT=4",
        "TRINO_EXACT_TABLE_SET=true",
        "TRINO_SCHEMA_CONTRACT=true",
        "TRINO_KEY_CONTRACT=true",
        "TRINO_FINANCIAL_RECONCILIATION=true",
        "TRINO_METRIC_RULES=true"
    )
    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    foreach ($marker in $requiredMarkers) {
        if (-not $outputText.Contains($marker)) {
            throw "A validação Trino não comprovou: $marker"
        }
    }

    return @(
        $output |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ -match '^TRINO_[A-Z0-9_]+=' } |
            Sort-Object
    )
}

function Invoke-SupersetInitialization {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker compose `
                --profile query `
                --profile bi `
                run `
                --rm `
                --no-deps `
                superset-init
        )
        $invocationSucceeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if (-not $invocationSucceeded -or $exitCode -ne 0) {
        throw "A inicialização e conexão do Superset falharam."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    foreach ($marker in @(
        "SUPERSET_METADATA_READY=true",
        "SUPERSET_ADMIN_READY=true",
        "SUPERSET_TRINO_CONNECTION=true"
    )) {
        if (-not $outputText.Contains($marker)) {
            throw "A inicialização Superset não comprovou: $marker"
        }
        Write-Host $marker
    }
}

function Start-AndValidateSuperset {
    & docker compose `
        --profile query `
        --profile bi `
        up `
        -d `
        --no-deps `
        --wait `
        --wait-timeout 180 `
        superset
    if ($LASTEXITCODE -ne 0) {
        throw "O servidor Superset não ficou saudável."
    }

    $health = Invoke-RestMethod `
        -Uri "http://127.0.0.1:8088/health" `
        -TimeoutSec 5
    if ([string]$health -ne "OK") {
        throw "O endpoint de saúde do Superset retornou resposta inesperada."
    }
    Write-Host "SUPERSET_HTTP_HEALTH=true"
}

Push-Location $projectRoot

try {
    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "A validação de consulta exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true
    Write-Host "Registrando e validando o catálogo Gold no Trino..."
    Invoke-TrinoRegistration

    Write-Host "Executando a primeira auditoria Trino..."
    $firstSnapshot = @(Invoke-TrinoValidationSnapshot)

    Write-Host "Inicializando e validando a conexão Superset -> Trino..."
    Invoke-SupersetInitialization
    Start-AndValidateSuperset

    Write-Host "Repetindo a auditoria Trino após o acesso do Superset..."
    $secondSnapshot = @(Invoke-TrinoValidationSnapshot)
    $difference = @(
        Compare-Object `
            -ReferenceObject $firstSnapshot `
            -DifferenceObject $secondSnapshot
    )
    if ($difference.Count -ne 0) {
        throw "As métricas Trino mudaram durante a validação do Superset."
    }

    Write-Host "QUERY_STACK_METRICS_STABLE=true"
    $validationCompleted = $true
    Write-Host "VALIDAÇÃO HIVE + TRINO + SUPERSET CONCLUÍDA COM SUCESSO."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
    $cleanupFailures = New-Object System.Collections.Generic.List[string]
    if ($stackStartAttempted) {
        foreach ($service in @("superset", "trino", "hive-metastore", "minio")) {
            Write-Host "Parando $service..."
            & docker compose `
                --profile query `
                --profile bi `
                stop -t 30 $service
            if ($LASTEXITCODE -ne 0) {
                $cleanupFailures.Add("serviço $service")
            }
        }

        & docker compose `
            --profile query `
            --profile bi `
            rm -f superset-init
        if ($LASTEXITCODE -ne 0) {
            $cleanupFailures.Add("contêiner efêmero superset-init")
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
        Write-Host "Todos os serviços da validação de consulta foram parados."
    }
    elseif ($stackStartAttempted) {
        Write-Warning "A limpeza da validação de consulta não pôde ser confirmada."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Hive/Trino/Superset falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }
    throw "A validação Hive/Trino/Superset falhou: $primaryFailure"
}
if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação de consulta exige revisão em: $cleanupSummary"
}
if (-not $validationCompleted) {
    throw "A validação Hive/Trino/Superset não foi concluída."
}
