$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$containerName = "demandflow-postgres"
$preparedEnvFile = $null
$postgresStartAttempted = $false
$databaseUsesNewPassword = $false
$envUsesNewPassword = $false
$rotationCompleted = $false

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

function Set-DotEnvValueInText {
    param(
        [Parameter(Mandatory)]
        [string]$Text,

        [Parameter(Mandatory)]
        [string]$Name,

        [Parameter(Mandatory)]
        [string]$Value
    )

    $pattern = (
        "(?m)^(?<prefix>[\t ]*" +
        [regex]::Escape($Name) +
        "[\t ]*=)[^\r\n]*$"
    )
    $entryMatches = [regex]::Matches($Text, $pattern)
    if ($entryMatches.Count -ne 1) {
        throw "Não foi possível substituir $Name com segurança no .env."
    }

    $evaluator = [System.Text.RegularExpressions.MatchEvaluator] {
        param($match)
        return $match.Groups["prefix"].Value + $Value
    }

    return [regex]::Replace($Text, $pattern, $evaluator)
}

function New-CryptographicPassword {
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()

    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }

    return -join ($bytes | ForEach-Object { $_.ToString("x2") })
}

function New-PreparedEnvFile {
    param(
        [Parameter(Mandatory)]
        [string]$Text,

        [Parameter(Mandatory)]
        [System.Text.Encoding]$Encoding,

        [Parameter(Mandatory)]
        [System.Security.AccessControl.FileSecurity]$AccessControl
    )

    $temporaryFile = Join-Path (
        Split-Path -Parent $envFile
    ) (".env.rotation-{0}.tmp" -f [guid]::NewGuid().ToString("N"))

    try {
        [System.IO.File]::WriteAllText($temporaryFile, $Text, $Encoding)
        [System.IO.File]::SetAccessControl($temporaryFile, $AccessControl)

        if ([System.IO.File]::ReadAllText($temporaryFile) -cne $Text) {
            throw "A verificação do arquivo temporário do .env falhou."
        }

        return $temporaryFile
    }
    catch {
        if (Test-Path -LiteralPath $temporaryFile) {
            Remove-Item -LiteralPath $temporaryFile -Force
        }
        throw
    }
}

function Replace-EnvAtomically {
    param(
        [Parameter(Mandatory)]
        [string]$PreparedFile
    )

    [System.IO.File]::Replace($PreparedFile, $envFile, $null)
}

function Set-CdcDatabasePassword {
    param(
        [Parameter(Mandatory)]
        [string]$Password
    )

    $quotedRole = '"' + $script:cdcUser.Replace('"', '""') + '"'
    $quotedPassword = "'" + $Password.Replace("'", "''") + "'"
    $sql = "ALTER ROLE $quotedRole WITH LOGIN REPLICATION PASSWORD $quotedPassword;"

    $sql | & docker exec -i $script:containerName `
        psql `
        -X `
        -q `
        -v ON_ERROR_STOP=1 `
        -U $script:postgresUser `
        -d $script:database `
        1>$null `
        2>$null
    $exitCode = $LASTEXITCODE
    $sql = $null
    $quotedPassword = $null

    if ($exitCode -ne 0) {
        throw "O PostgreSQL recusou a alteração da credencial CDC."
    }
}

function Test-CdcLogin {
    param(
        [Parameter(Mandatory)]
        [string]$Password
    )

    $loginScript = @'
IFS= read -r PGPASSWORD
IFS= read -r PGUSER
IFS= read -r PGDATABASE
carriage_return="$(printf '\r')"
PGPASSWORD="${PGPASSWORD%"$carriage_return"}"
PGUSER="${PGUSER%"$carriage_return"}"
PGDATABASE="${PGDATABASE%"$carriage_return"}"
export PGPASSWORD PGUSER PGDATABASE
exec psql -X -qAt -h 127.0.0.1 -c "SELECT 1"
'@

    $loginInput = (
        $Password,
        $script:cdcUser,
        $script:database
    ) -join "`n"
    $previousErrorAction = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"
        $output = ($loginInput + "`n") |
            & docker exec -i $script:containerName `
                sh -c $loginScript `
                2>$null
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    return (
        $exitCode -eq 0 -and
        ([string]::Join("", @($output))).Trim() -eq "1"
    )
}

function Assert-CdcRole {
    $quotedRole = "'" + $script:cdcUser.Replace("'", "''") + "'"
    $sql = (
        "SELECT CASE WHEN rolcanlogin AND rolreplication " +
        "THEN 'ok' ELSE 'invalid' END " +
        "FROM pg_roles WHERE rolname = $quotedRole;"
    )

    $output = $sql |
        & docker exec -i $script:containerName `
            psql `
            -X `
            -qAt `
            -v ON_ERROR_STOP=1 `
            -U $script:postgresUser `
            -d $script:database `
            2>$null
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0 -or
        ([string]::Join("", @($output))).Trim() -ne "ok") {
        throw "O papel CDC não existe ou não possui LOGIN e REPLICATION."
    }
}

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Arquivo .env não encontrado."
}

$envBytes = [System.IO.File]::ReadAllBytes($envFile)
$hasUtf8Bom = (
    $envBytes.Length -ge 3 -and
    $envBytes[0] -eq 0xEF -and
    $envBytes[1] -eq 0xBB -and
    $envBytes[2] -eq 0xBF
)
$utf8Encoding = [System.Text.UTF8Encoding]::new($hasUtf8Bom)
$originalEnvText = [System.IO.File]::ReadAllText($envFile)
$originalEnvAcl = [System.IO.File]::GetAccessControl($envFile)

$postgresUser = Get-DotEnvValue $originalEnvText "POSTGRES_USER"
$database = Get-DotEnvValue $originalEnvText "POSTGRES_DATABASE"
$cdcUser = Get-DotEnvValue $originalEnvText "DEBEZIUM_POSTGRES_USER"
$oldPassword = Get-DotEnvValue `
    $originalEnvText `
    "DEBEZIUM_POSTGRES_PASSWORD"

do {
    $newPassword = New-CryptographicPassword
} while ($newPassword -ceq $oldPassword)

$newEnvText = Set-DotEnvValueInText `
    $originalEnvText `
    "DEBEZIUM_POSTGRES_PASSWORD" `
    $newPassword

Push-Location $projectRoot

try {
    $preparedEnvFile = New-PreparedEnvFile `
        $newEnvText `
        $utf8Encoding `
        $originalEnvAcl

    $runningBefore = @(
        & docker compose ps --status running --services 2>$null |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Não foi possível consultar o estado do Docker Compose."
    }
    if ($runningBefore.Count -ne 0) {
        throw "Há serviços do projeto em execução; a rotação isolada foi cancelada."
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

    Write-Host "PostgreSQL saudável e isolado."
    Assert-CdcRole

    if (-not (Test-CdcLogin $oldPassword)) {
        throw "A credencial CDC atual não autentica; nenhuma rotação foi feita."
    }
    if (Test-CdcLogin $newPassword) {
        throw "A autenticação não está rejeitando uma credencial incorreta."
    }

    Write-Host "Estado atual validado; rotacionando a credencial CDC..."
    Set-CdcDatabasePassword $newPassword
    $databaseUsesNewPassword = $true

    if (-not (Test-CdcLogin $newPassword)) {
        throw "A nova credencial não autenticou após a alteração no banco."
    }
    if (Test-CdcLogin $oldPassword) {
        throw "A credencial anterior continuou autenticando após a rotação."
    }

    Replace-EnvAtomically $preparedEnvFile
    $preparedEnvFile = $null
    $envUsesNewPassword = $true

    $persistedText = [System.IO.File]::ReadAllText($envFile)
    $persistedPassword = Get-DotEnvValue `
        $persistedText `
        "DEBEZIUM_POSTGRES_PASSWORD"
    if ($persistedPassword -cne $newPassword) {
        throw "O .env não preservou a nova credencial."
    }
    if (-not (Test-CdcLogin $persistedPassword)) {
        throw "A credencial persistida no .env não autentica."
    }

    $rotationCompleted = $true
    Write-Host "Credencial CDC rotacionada e validada sem exibir valores."
}
catch {
    $originalFailure = $_.Exception.Message
    $stateWasChanged = $databaseUsesNewPassword -or $envUsesNewPassword
    $rollbackFailures = New-Object System.Collections.Generic.List[string]

    if ($databaseUsesNewPassword) {
        try {
            Set-CdcDatabasePassword $oldPassword
            if (-not (Test-CdcLogin $oldPassword)) {
                throw "A credencial anterior não autenticou após o rollback."
            }
            $databaseUsesNewPassword = $false
        }
        catch {
            $rollbackFailures.Add("banco")
        }
    }

    if ($envUsesNewPassword -and -not $databaseUsesNewPassword) {
        try {
            $rollbackEnvFile = New-PreparedEnvFile `
                $originalEnvText `
                $utf8Encoding `
                $originalEnvAcl
            Replace-EnvAtomically $rollbackEnvFile
            $envUsesNewPassword = $false
        }
        catch {
            $rollbackFailures.Add(".env")
        }
    }
    elseif (-not $envUsesNewPassword -and $databaseUsesNewPassword) {
        try {
            $recoveryEnvFile = New-PreparedEnvFile `
                $newEnvText `
                $utf8Encoding `
                $originalEnvAcl
            Replace-EnvAtomically $recoveryEnvFile
            $envUsesNewPassword = $true
        }
        catch {
            $rollbackFailures.Add("sincronização emergencial do .env")
        }
    }

    if ($rollbackFailures.Count -ne 0) {
        throw (
            "A rotação falhou e o rollback exige revisão manual em: " +
            [string]::Join(", ", $rollbackFailures) +
            ". Motivo inicial: $originalFailure"
        )
    }

    if ($stateWasChanged) {
        throw "Rotação cancelada com estado anterior restaurado: $originalFailure"
    }

    throw "Rotação cancelada antes de qualquer alteração: $originalFailure"
}
finally {
    $oldPassword = $null
    $newPassword = $null
    $persistedPassword = $null

    if ($preparedEnvFile -and (Test-Path -LiteralPath $preparedEnvFile)) {
        Remove-Item -LiteralPath $preparedEnvFile -Force
    }

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

if (-not $rotationCompleted) {
    throw "A rotação não foi concluída."
}
