param(
    [ValidatePattern("^[A-Za-z0-9_.-]+$")]
    [string]$RecoveryRunId
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$dagId = "demandflow_pipeline"
$stackStartAttempted = $false
$validationCompleted = $false
$primaryFailure = $null
$cleanupSummary = $null
$airflowControlPlane = @(
    "airflow-api-server",
    "airflow-dag-processor",
    "airflow-scheduler",
    "airflow-triggerer",
    "airflow-worker"
)
$cleanupServices = @(
    "airflow-worker",
    "airflow-triggerer",
    "airflow-scheduler",
    "airflow-dag-processor",
    "airflow-api-server",
    "airflow-redis",
    "airflow-db",
    "debezium",
    "kafka",
    "postgres",
    "minio"
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
    if ($matches.Count -ne 1) {
        throw "Variável $Name ausente ou duplicada no .env."
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

function Invoke-AirflowMetadataQuery {
    param(
        [Parameter(Mandatory)]
        [string]$Sql
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec `
                demandflow-airflow-db `
                psql `
                -X `
                -qAt `
                -F "|" `
                -v ON_ERROR_STOP=1 `
                -U $script:airflowDbUser `
                -d $script:airflowDbName `
                -c $Sql `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Uma consulta somente leitura aos metadados Airflow falhou."
    }
    return @($output | ForEach-Object { ([string]$_).Trim() })
}

function Start-HealthyService {
    param(
        [Parameter(Mandatory)]
        [string]$Service,

        [int]$TimeoutSeconds = 240
    )

    Write-Host "Iniciando $Service e aguardando saúde..."
    & docker compose `
        --profile cdc `
        --profile orchestration `
        up `
        -d `
        --no-deps `
        --wait `
        --wait-timeout $TimeoutSeconds `
        $Service
    if ($LASTEXITCODE -ne 0) {
        throw "$Service não ficou saudável dentro do prazo."
    }
}

function Invoke-AirflowInitialization {
    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker compose `
                --profile orchestration `
                run `
                --rm `
                --no-deps `
                airflow-init `
                2>&1
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "A migração ou inicialização idempotente do Airflow falhou."
    }

    $outputText = [string]::Join(
        "`n",
        @($output | ForEach-Object { [string]$_ })
    )
    foreach ($marker in @(
        "AIRFLOW_METADATA_READY=true",
        "AIRFLOW_ADMIN_READY=true"
    )) {
        if (-not $outputText.Contains($marker)) {
            throw "A inicialização Airflow não comprovou: $marker"
        }
        Write-Host $marker
    }

    $actionMatches = [regex]::Matches(
        $outputText,
        "(?m)^AIRFLOW_ADMIN_ACTION=(?:created|updated)[\r ]*$"
    )
    if ($actionMatches.Count -ne 1) {
        throw "A ação idempotente sobre o administrador ficou ambígua."
    }
    Write-Host $actionMatches[0].Value.Trim()
}

function Start-AirflowControlPlane {
    foreach ($service in $script:airflowControlPlane) {
        Start-HealthyService -Service $service
    }
}

function Stop-AirflowService {
    param(
        [Parameter(Mandatory)]
        [string]$Service
    )

    Write-Host "Parando $Service..."
    & docker compose `
        --profile cdc `
        --profile orchestration `
        stop `
        -t 60 `
        $Service
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível parar $Service."
    }
}

function Stop-AirflowControlPlane {
    foreach ($service in @(
        "airflow-worker",
        "airflow-triggerer",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-api-server"
    )) {
        Stop-AirflowService -Service $service
    }
}

function Invoke-AirflowJson {
    param(
        [Parameter(Mandatory)]
        [string[]]$CommandArguments
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker compose `
                --profile orchestration `
                exec `
                -T `
                airflow-scheduler `
                airflow `
                @CommandArguments `
                --output json `
                2>$null
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        throw "Um comando de consulta da CLI Airflow falhou."
    }

    $text = [string]::Join("`n", @($output | ForEach-Object { [string]$_ }))
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "A CLI Airflow retornou uma resposta vazia."
    }

    try {
        $parsed = $text | ConvertFrom-Json
    }
    catch {
        throw "A CLI Airflow não retornou JSON válido."
    }

    return @($parsed)
}

function Assert-AirflowReady {
    try {
        $health = Invoke-RestMethod `
            -Uri "http://127.0.0.1:8081/api/v2/monitor/health" `
            -TimeoutSec 5
    }
    catch {
        throw "O endpoint de saúde do Airflow não respondeu."
    }

    if ($health.metadatabase.status -ne "healthy") {
        throw "O Airflow não confirmou a saúde do banco de metadados."
    }

    $importErrors = @(
        Invoke-AirflowJson `
            -CommandArguments @("dags", "list-import-errors")
    )
    if ($importErrors.Count -ne 0) {
        throw "O Airflow encontrou erros de importação em DAGs."
    }
    Write-Host "AIRFLOW_IMPORT_ERRORS=0"

    $dags = @(
        Invoke-AirflowJson -CommandArguments @("dags", "list")
    )
    $matchingDags = @($dags | Where-Object { $_.dag_id -eq $script:dagId })
    if ($matchingDags.Count -ne 1) {
        throw "O DAG $script:dagId não foi serializado de forma inequívoca."
    }
    Write-Host "AIRFLOW_DAG_AVAILABLE=true"
}

function Invoke-AirflowCommand {
    param(
        [Parameter(Mandatory)]
        [string[]]$CommandArguments
    )

    & docker compose `
        --profile orchestration `
        exec `
        -T `
        airflow-scheduler `
        airflow `
        @CommandArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Um comando operacional da CLI Airflow falhou."
    }
}

function Get-DagRunState {
    param(
        [Parameter(Mandatory)]
        [string]$RunId
    )

    if ($RunId -cnotmatch "^[A-Za-z0-9_.-]+$") {
        throw "O run_id contém caracteres não permitidos."
    }
    $states = @(
        Invoke-AirflowMetadataQuery -Sql (
            "SELECT state FROM dag_run " +
            "WHERE dag_id = '$script:dagId' " +
            "AND run_id = '$RunId';"
        )
    )
    if ($states.Count -gt 1) {
        throw "O run_id $RunId está duplicado nos metadados."
    }
    if ($states.Count -eq 0) {
        return $null
    }
    return ([string]$states[0]).ToLowerInvariant()
}

function Get-TaskStates {
    param(
        [Parameter(Mandatory)]
        [string]$RunId
    )

    if ($RunId -cnotmatch "^[A-Za-z0-9_.-]+$") {
        throw "O run_id contém caracteres não permitidos."
    }
    $rows = @(
        Invoke-AirflowMetadataQuery -Sql (
            "SELECT task_id, COALESCE(state, ''), try_number " +
            "FROM task_instance " +
            "WHERE dag_id = '$script:dagId' " +
            "AND run_id = '$RunId' ORDER BY task_id;"
        )
    )
    $states = New-Object System.Collections.Generic.List[object]
    foreach ($row in $rows) {
        $fields = @(([string]$row).Split("|"))
        if ($fields.Count -ne 3) {
            throw "Uma linha de estado de task ficou ambígua."
        }
        $states.Add(
            [pscustomobject]@{
                task_id = $fields[0]
                state = $fields[1]
                try_number = $fields[2]
            }
        )
    }
    return $states.ToArray()
}

function Wait-DagRunSuccess {
    param(
        [Parameter(Mandatory)]
        [string]$RunId,

        [int]$Attempts = 180
    )

    $lastState = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        $state = Get-DagRunState -RunId $RunId
        if ($state -ne $lastState -or $attempt % 3 -eq 0) {
            $displayState = if ($null -eq $state) { "pending" } else { $state }
            Write-Host "Airflow run ${RunId}: $displayState"
            $lastState = $state
        }

        if ($state -eq "success") {
            return
        }
        if ($state -eq "failed") {
            $states = @(Get-TaskStates -RunId $RunId)
            foreach ($task in $states) {
                Write-Host (
                    "AIRFLOW_FAILED_RUN_TASK={0};STATE={1};TRY={2}" -f
                    $task.task_id,
                    $task.state,
                    $task.try_number
                )
            }
            throw "O DAG $RunId terminou em falha."
        }

        Start-Sleep -Seconds 20
    }

    throw "O DAG $RunId não terminou dentro da janela de uma hora."
}

function Assert-SuccessfulTaskSet {
    param(
        [Parameter(Mandatory)]
        [string]$RunId
    )

    $expectedTaskIds = @(
        "check_infrastructure",
        "raw_ingestion",
        "validate_raw",
        "bronze",
        "validate_bronze",
        "silver",
        "validate_silver",
        "gold",
        "validate_gold"
    ) | Sort-Object
    $states = @(Get-TaskStates -RunId $RunId)
    $actualTaskIds = @($states | ForEach-Object { [string]$_.task_id }) |
        Sort-Object

    if ($states.Count -ne $expectedTaskIds.Count -or
        @(Compare-Object $expectedTaskIds $actualTaskIds).Count -ne 0) {
        throw "O conjunto de tasks executado pelo Airflow divergiu do DAG."
    }
    foreach ($task in $states) {
        if (([string]$task.state).ToLowerInvariant() -ne "success") {
            throw "A task $($task.task_id) não terminou em sucesso."
        }
    }

    Write-Host "AIRFLOW_TASK_COUNT=$($states.Count)"
    Write-Host "AIRFLOW_ALL_TASKS_SUCCESS=true"
}

function Start-DagRun {
    param(
        [Parameter(Mandatory)]
        [string]$RunId
    )

    Invoke-AirflowCommand `
        -CommandArguments @("dags", "unpause", $script:dagId, "--yes")
    Invoke-AirflowCommand `
        -CommandArguments @(
            "dags",
            "trigger",
            $script:dagId,
            "--run-id",
            $RunId
        )
    Wait-DagRunSuccess -RunId $RunId
    Assert-SuccessfulTaskSet -RunId $RunId
}

function Get-TaskLogText {
    param(
        [Parameter(Mandatory)]
        [string]$RunId,

        [Parameter(Mandatory)]
        [string]$TaskId
    )

    $taskDirectory = Join-Path `
        $script:projectRoot `
        "airflow\logs\dag_id=$script:dagId\run_id=$RunId\task_id=$TaskId"
    if (-not (Test-Path -LiteralPath $taskDirectory -PathType Container)) {
        throw "O diretório de log da task $TaskId não foi encontrado."
    }

    $logFile = Get-ChildItem `
        -LiteralPath $taskDirectory `
        -Filter "attempt=*.log" `
        -File |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
    if ($null -eq $logFile) {
        throw "O log da task $TaskId não foi encontrado."
    }

    return [System.IO.File]::ReadAllText($logFile.FullName)
}

function Assert-RepeatDidNotWrite {
    param(
        [Parameter(Mandatory)]
        [string]$RunId
    )

    $rawLog = Get-TaskLogText -RunId $RunId -TaskId "raw_ingestion"
    if (-not $rawLog.Contains("RAW_INPUT_ROWS_TOTAL=0")) {
        throw "A repetição do Airflow consumiu eventos Raw inesperados."
    }
    Write-Host "AIRFLOW_REPEAT_RAW_INPUT_ZERO=true"

    $goldLog = Get-TaskLogText -RunId $RunId -TaskId "gold"
    foreach ($tableName in @(
        "daily_sales",
        "forecast_accuracy",
        "inventory_health",
        "product_sales"
    )) {
        if (-not $goldLog.Contains(
            "GOLD_WRITE_APPLIED_${tableName}=false"
        )) {
            throw "A repetição escreveu novamente a tabela Gold $tableName."
        }
    }
    Write-Host "AIRFLOW_REPEAT_GOLD_NO_WRITES=true"
}

Push-Location $projectRoot

try {
    if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
        throw "Arquivo .env não encontrado."
    }
    $envText = [System.IO.File]::ReadAllText($envFile)
    $airflowDbUser = Get-DotEnvValue $envText "AIRFLOW_DB_USER"
    $airflowDbName = Get-DotEnvValue $envText "AIRFLOW_DB_NAME"
    $envText = $null

    $runningBefore = @(Get-RunningServices)
    if ($runningBefore.Count -ne 0) {
        throw "A validação Airflow exige todos os serviços inicialmente parados."
    }

    $stackStartAttempted = $true
    & docker compose --profile orchestration rm -f airflow-init 1>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível remover o contêiner antigo do inicializador."
    }

    Start-HealthyService -Service "minio"
    & "$PSScriptRoot\bootstrap-s3.ps1"
    if ($LASTEXITCODE -ne 0) {
        throw "O bootstrap S3 falhou."
    }

    Write-Host "Preparando PostgreSQL, Kafka, tópicos e Debezium em sequência..."
    & "$PSScriptRoot\register-debezium-connector.ps1"
    if ($LASTEXITCODE -ne 0) {
        throw "A preparação CDC para o DAG falhou."
    }

    Start-HealthyService -Service "airflow-db"
    Start-HealthyService -Service "airflow-redis"
    Invoke-AirflowInitialization
    Start-AirflowControlPlane
    Assert-AirflowReady

    $runPrefix = "validation_{0}_{1}" -f (
        [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
    ), ([guid]::NewGuid().ToString("N").Substring(0, 8))
    $secondRunId = "${runPrefix}_repeat"

    if ([string]::IsNullOrWhiteSpace($RecoveryRunId)) {
        $firstRunId = "${runPrefix}_first"
        Write-Host "Executando o primeiro DAG ponta a ponta..."
        Start-DagRun -RunId $firstRunId
    }
    else {
        $firstRunId = $RecoveryRunId
        Write-Host "Retomando o DAG interrompido $firstRunId..."
        Wait-DagRunSuccess -RunId $firstRunId
        Assert-SuccessfulTaskSet -RunId $firstRunId
        Write-Host "AIRFLOW_INTERRUPTED_RUN_RECOVERED=true"
    }
    Write-Host "AIRFLOW_FIRST_RUN_SUCCESS=true"

    Write-Host "Reiniciando o plano de controle e seus estados persistentes..."
    Stop-AirflowControlPlane
    Stop-AirflowService -Service "airflow-redis"
    Stop-AirflowService -Service "airflow-db"
    Start-HealthyService -Service "airflow-db"
    Start-HealthyService -Service "airflow-redis"
    Invoke-AirflowInitialization
    Start-AirflowControlPlane
    Assert-AirflowReady

    if ((Get-DagRunState -RunId $firstRunId) -ne "success") {
        throw "O primeiro run não persistiu após o reinício do Airflow."
    }
    Write-Host "AIRFLOW_METADATA_PERSISTED=true"
    Write-Host "AIRFLOW_RESTART_RECOVERY=true"

    Write-Host "Repetindo o DAG após o reinício..."
    Start-DagRun -RunId $secondRunId
    Assert-RepeatDidNotWrite -RunId $secondRunId
    Write-Host "AIRFLOW_REPEAT_RUN_SUCCESS=true"

    $validationCompleted = $true
    Write-Host "VALIDAÇÃO AIRFLOW PONTA A PONTA CONCLUÍDA COM SUCESSO."
}
catch {
    $primaryFailure = $_.Exception.Message
}
finally {
    $cleanupFailures = New-Object System.Collections.Generic.List[string]
    if ($stackStartAttempted) {
        foreach ($service in $cleanupServices) {
            Write-Host "Parando $service..."
            & docker compose `
                --profile cdc `
                --profile orchestration `
                stop `
                -t 60 `
                $service
            if ($LASTEXITCODE -ne 0) {
                $cleanupFailures.Add("serviço $service")
            }
        }

        & docker compose --profile orchestration rm -f airflow-init 1>$null
        if ($LASTEXITCODE -ne 0) {
            $cleanupFailures.Add("contêiner airflow-init")
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
        Write-Host "Todos os serviços usados pela validação Airflow foram parados."
    }
    elseif ($stackStartAttempted) {
        Write-Warning "A limpeza da validação Airflow não pôde ser confirmada."
    }
}

if ($null -ne $primaryFailure) {
    if ($null -ne $cleanupSummary) {
        throw (
            "A validação Airflow falhou: $primaryFailure " +
            "A limpeza exige revisão em: $cleanupSummary"
        )
    }
    throw "A validação Airflow falhou: $primaryFailure"
}
if ($null -ne $cleanupSummary) {
    throw "A limpeza da validação Airflow exige revisão em: $cleanupSummary"
}
if (-not $validationCompleted) {
    throw "A validação Airflow não foi concluída."
}
