$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $projectRoot ".env"
$secretName = "AIRFLOW_API_SECRET_KEY"
$temporaryFile = $null
$backupFile = $null

function Get-SecretMatches {
    param(
        [Parameter(Mandatory)]
        [string]$Text
    )

    $pattern = (
        "(?m)^[\t ]*" +
        [regex]::Escape($script:secretName) +
        "[\t ]*=(?<value>[^\r\n]*)$"
    )
    return [regex]::Matches($Text, $pattern)
}

function Get-UnquotedValue {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    $result = $Value.Trim()
    if ($result.Length -ge 2) {
        $first = $result[0]
        $last = $result[$result.Length - 1]
        if (($first -eq '"' -and $last -eq '"') -or
            ($first -eq "'" -and $last -eq "'")) {
            $result = $result.Substring(1, $result.Length - 2)
        }
    }
    return $result
}

function Assert-ValidSecret {
    param(
        [Parameter(Mandatory)]
        [string]$Value
    )

    if ($Value -cnotmatch '^[A-Za-z0-9_-]{64}$') {
        throw "$secretName deve conter exatamente 64 caracteres Base64URL."
    }
}

function New-CryptographicSecret {
    $bytes = New-Object byte[] 48
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    }
    finally {
        $generator.Dispose()
    }

    return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
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
$encoding = [System.Text.UTF8Encoding]::new($hasUtf8Bom)
$originalText = [System.IO.File]::ReadAllText($envFile)
$matches = @(Get-SecretMatches -Text $originalText)

if ($matches.Count -gt 1) {
    throw "$secretName está duplicada no .env."
}
if ($matches.Count -eq 1) {
    $existingSecret = Get-UnquotedValue $matches[0].Groups["value"].Value
    Assert-ValidSecret -Value $existingSecret
    $existingSecret = $null
    Write-Host "AIRFLOW_API_SECRET_ACTION=preserved"
    Write-Host "AIRFLOW_API_SECRET_READY=true"
    exit 0
}

$newSecret = New-CryptographicSecret
Assert-ValidSecret -Value $newSecret
$newline = if ($originalText.Contains("`r`n")) { "`r`n" } else { "`n" }
$separator = if (
    $originalText.Length -eq 0 -or
    $originalText.EndsWith("`n") -or
    $originalText.EndsWith("`r")
) { "" } else { $newline }
$newText = $originalText + $separator + $secretName + "=" + $newSecret + $newline
$directory = Split-Path -Parent $envFile
$temporaryFile = Join-Path $directory (
    ".env.airflow-secret-{0}.tmp" -f [guid]::NewGuid().ToString("N")
)
$backupFile = Join-Path $directory (
    ".env.airflow-secret-backup-{0}.tmp" -f [guid]::NewGuid().ToString("N")
)
$accessControl = [System.IO.File]::GetAccessControl($envFile)

try {
    [System.IO.File]::WriteAllText($temporaryFile, $newText, $encoding)
    [System.IO.File]::SetAccessControl($temporaryFile, $accessControl)

    $preparedText = [System.IO.File]::ReadAllText($temporaryFile)
    $preparedMatches = @(Get-SecretMatches -Text $preparedText)
    if ($preparedMatches.Count -ne 1) {
        throw "A preparação atômica de $secretName falhou."
    }
    $preparedSecret = Get-UnquotedValue $preparedMatches[0].Groups["value"].Value
    Assert-ValidSecret -Value $preparedSecret
    if ($preparedSecret -cne $newSecret) {
        throw "A verificação do segredo preparado falhou."
    }
    $preparedSecret = $null

    [System.IO.File]::Replace($temporaryFile, $envFile, $backupFile)
    $temporaryFile = $null
    [System.IO.File]::SetAccessControl($envFile, $accessControl)
    [System.IO.File]::Delete($backupFile)
    $backupFile = $null

    Write-Host "AIRFLOW_API_SECRET_ACTION=created"
    Write-Host "AIRFLOW_API_SECRET_READY=true"
}
finally {
    $newSecret = $null
    $newText = $null
    $originalText = $null
    if ($temporaryFile -and (Test-Path -LiteralPath $temporaryFile)) {
        Remove-Item -LiteralPath $temporaryFile -Force
    }
    if ($backupFile -and (Test-Path -LiteralPath $backupFile)) {
        Remove-Item -LiteralPath $backupFile -Force
    }
}
