$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$sqlFile = Join-Path $projectRoot "infra\postgres\cdc\setup.sql"
$containerName = "demandflow-postgres"
$postgresStartAttempted = $false
$configurationCompleted = $false
$invalidPassword = $null

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

function ConvertTo-Base64Utf8 {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    return [Convert]::ToBase64String(
        [Text.Encoding]::UTF8.GetBytes($Value)
    )
}

function ConvertTo-SqlLiteral {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    return "'" + $Value.Replace("'", "''") + "'"
}

function Invoke-CdcSetup {
    $encodedSql = ConvertTo-Base64Utf8 $script:setupSql
    $encodedPostgresUser = ConvertTo-Base64Utf8 $script:postgresUser
    $encodedDatabase = ConvertTo-Base64Utf8 $script:database
    $encodedCdcUser = ConvertTo-Base64Utf8 $script:cdcUser
    $encodedCdcPassword = ConvertTo-Base64Utf8 $script:cdcPassword
    $setupScript = (
        'PGUSER=$(printf ''%s'' ''{0}'' | base64 -d); ' +
        'PGDATABASE=$(printf ''%s'' ''{1}'' | base64 -d); ' +
        'DEBEZIUM_POSTGRES_USER=$(printf ''%s'' ''{2}'' | base64 -d); ' +
        'DEBEZIUM_POSTGRES_PASSWORD=$(printf ''%s'' ''{3}'' | base64 -d); ' +
        'POSTGRES_DATABASE=$(printf ''%s'' ''{1}'' | base64 -d); ' +
        'export PGUSER PGDATABASE DEBEZIUM_POSTGRES_USER ' +
        'DEBEZIUM_POSTGRES_PASSWORD POSTGRES_DATABASE; ' +
        'printf ''%s'' ''{4}'' | base64 -d | ' +
        'psql -X -q -v ON_ERROR_STOP=1; exit_code=$?; ' +
        'unset PGUSER PGDATABASE DEBEZIUM_POSTGRES_USER ' +
        'DEBEZIUM_POSTGRES_PASSWORD POSTGRES_DATABASE; ' +
        'exit $exit_code; # end'
    ) -f (
        $encodedPostgresUser,
        $encodedDatabase,
        $encodedCdcUser,
        $encodedCdcPassword,
        $encodedSql
    )
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $setupScript |
            & docker exec -i $script:containerName sh 1>$null 2>$null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
        $setupScript = $null
        $encodedSql = $null
        $encodedPostgresUser = $null
        $encodedDatabase = $null
        $encodedCdcUser = $null
        $encodedCdcPassword = $null
    }

    if ($exitCode -ne 0) {
        throw "O PostgreSQL recusou a configuração CDC."
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
                & docker exec -i $script:containerName `
                    psql `
                    -X `
                    -qAt `
                    -v ON_ERROR_STOP=1 `
                    -U $script:postgresUser `
                    -d $script:database `
                    2>&1
        )
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if ($exitCode -ne 0) {
        $diagnostic = ([string]($output | Select-Object -First 1)).Trim()
        $diagnostic = [regex]::Replace($diagnostic, "[\r\n]+", " ")
        if ($diagnostic.Length -gt 300) {
            $diagnostic = $diagnostic.Substring(0, 300)
        }
        throw "Uma validação interna do PostgreSQL falhou: $diagnostic"
    }

    return ([string]::Join("", $output)).Trim()
}

function Assert-CdcConfiguration {
    $quotedCdcUser = ConvertTo-SqlLiteral $script:cdcUser
    $quotedPostgresUser = ConvertTo-SqlLiteral $script:postgresUser
    $validationSql = @"
SELECT CASE WHEN
    r.rolcanlogin
    AND r.rolreplication
    AND current_setting('wal_level') = 'logical'
    AND has_database_privilege(r.oid, current_database(), 'CONNECT')
    AND has_schema_privilege(r.oid, 'public', 'USAGE')
    AND NOT EXISTS (
        SELECT 1
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND CASE
              WHEN c.relkind IN ('r', 'p', 'v', 'm', 'f')
              THEN NOT has_table_privilege(r.oid, c.oid, 'SELECT')
              ELSE false
          END
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND CASE
              WHEN c.relkind = 'S'
              THEN NOT has_sequence_privilege(r.oid, c.oid, 'SELECT')
              ELSE false
          END
    )
    AND EXISTS (
        SELECT 1
        FROM pg_default_acl AS d
        CROSS JOIN LATERAL aclexplode(d.defaclacl) AS a
        JOIN pg_roles AS owner_role ON owner_role.oid = d.defaclrole
        WHERE d.defaclnamespace = 'public'::regnamespace
          AND d.defaclobjtype = 'r'
          AND owner_role.rolname = $quotedPostgresUser
          AND a.grantee = r.oid
          AND a.privilege_type = 'SELECT'
    )
    AND EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'demandflow_publication'
          AND puballtables
    )
THEN 'ok' ELSE 'invalid' END
FROM pg_roles AS r
WHERE r.rolname = $quotedCdcUser;
"@

    if ((Invoke-PostgresScalar $validationSql) -ne "ok") {
        throw (
            "A validação de papel, privilégios, WAL lógico ou publicação " +
            "CDC falhou."
        )
    }
}

function Test-CdcLogin {
    param(
        [Parameter(Mandatory)]
        [string]$Password
    )

    $encodedPassword = ConvertTo-Base64Utf8 $Password
    $encodedUser = ConvertTo-Base64Utf8 $script:cdcUser
    $encodedDatabase = ConvertTo-Base64Utf8 $script:database
    $loginScript = (
        'PGPASSWORD=$(printf ''%s'' ''{0}'' | base64 -d); ' +
        'PGUSER=$(printf ''%s'' ''{1}'' | base64 -d); ' +
        'PGDATABASE=$(printf ''%s'' ''{2}'' | base64 -d); ' +
        'export PGPASSWORD PGUSER PGDATABASE; ' +
        'exec psql -X -qAt -h postgres -c "SELECT 1"; # end'
    ) -f $encodedPassword, $encodedUser, $encodedDatabase
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = $loginScript |
            & docker exec -i $script:containerName sh 2>$null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
        $loginScript = $null
        $encodedPassword = $null
        $encodedUser = $null
        $encodedDatabase = $null
    }

    return (
        $exitCode -eq 0 -and
        ([string]::Join("", @($output))).Trim() -eq "1"
    )
}

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Arquivo .env não encontrado."
}
if (-not (Test-Path -LiteralPath $sqlFile -PathType Leaf)) {
    throw "Arquivo SQL de configuração CDC não encontrado."
}

$envText = [System.IO.File]::ReadAllText($envFile)
$setupSql = [System.IO.File]::ReadAllText($sqlFile)
$postgresUser = Get-DotEnvValue $envText "POSTGRES_USER"
$database = Get-DotEnvValue $envText "POSTGRES_DATABASE"
$cdcUser = Get-DotEnvValue $envText "DEBEZIUM_POSTGRES_USER"
$cdcPassword = Get-DotEnvValue `
    $envText `
    "DEBEZIUM_POSTGRES_PASSWORD"

Push-Location $projectRoot

try {
    $runningBefore = @(
        & docker compose ps --status running --services 2>$null |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível consultar o estado do Docker Compose."
    }
    if ($runningBefore.Count -ne 0) {
        throw (
            "Há serviços do projeto em execução; a configuração isolada " +
            "foi cancelada."
        )
    }

    Write-Host "Iniciando somente o PostgreSQL..."
    $postgresStartAttempted = $true
    & docker compose up -d --no-deps postgres
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível iniciar o PostgreSQL."
    }

    $healthy = $false
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $health = & docker inspect `
            --format "{{.State.Health.Status}}" `
            $containerName `
            2>$null

        if ($LASTEXITCODE -eq 0 -and $health -eq "healthy") {
            $healthy = $true
            break
        }

        Start-Sleep -Seconds 2
    }
    if (-not $healthy) {
        throw "O PostgreSQL não ficou saudável dentro do prazo."
    }

    $runningDuring = @(
        & docker compose ps --status running --services 2>$null |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0 -or
        $runningDuring.Count -ne 1 -or
        $runningDuring[0].Trim() -ne "postgres") {
        throw "O isolamento falhou: outro serviço do projeto foi iniciado."
    }

    Write-Host "PostgreSQL saudável e isolado; aplicando configuração CDC..."
    Invoke-CdcSetup
    Assert-CdcConfiguration

    if (-not (Test-CdcLogin $cdcPassword)) {
        throw "A credencial CDC do .env não autenticou via SCRAM."
    }

    $invalidPassword = [guid]::NewGuid().ToString("N")
    if (Test-CdcLogin $invalidPassword) {
        throw "O PostgreSQL aceitou uma credencial CDC incorreta."
    }
    $invalidPassword = $null

    Write-Host "Reaplicando a configuração para comprovar repetibilidade..."
    Invoke-CdcSetup
    Assert-CdcConfiguration
    if (-not (Test-CdcLogin $cdcPassword)) {
        throw "A credencial CDC falhou após a reaplicação da configuração."
    }

    $configurationCompleted = $true
    Write-Host (
        "CDC configurado duas vezes e validado sem expor credenciais " +
        "em argumentos."
    )
}
finally {
    $envText = $null
    $setupSql = $null
    $cdcPassword = $null
    $invalidPassword = $null

    if ($postgresStartAttempted) {
        Write-Host "Parando o PostgreSQL..."
        & docker compose stop -t 30 postgres
        $stopExitCode = $LASTEXITCODE

        $runningAfter = @(
            & docker compose ps --status running --services 2>$null |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )

        if ($stopExitCode -ne 0 -or $runningAfter.Count -ne 0) {
            throw "Não foi possível confirmar a parada do PostgreSQL."
        }

        Write-Host "PostgreSQL parado; nenhum serviço do projeto está ativo."
    }

    Pop-Location
}

if (-not $configurationCompleted) {
    throw "A configuração CDC não foi concluída."
}
