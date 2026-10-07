$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

function New-RandomHex {
    param([int]$ByteCount)
    $bytes = New-Object byte[] $ByteCount
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
        return [BitConverter]::ToString($bytes).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $rng.Dispose()
    }
}

Push-Location $projectRoot
try {
    # Resolve only configuration; this does not contact/start the Docker Engine.
    $configJson = docker compose config --no-env-resolution --format json 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Compose invalido. Confira DEMANDFLOW_SECRETS_DIR no .env."
    }
    $config = ($configJson -join [Environment]::NewLine) | ConvertFrom-Json
    $licensePath = $config.secrets.minio_license.file
    if (-not (Test-Path -LiteralPath $licensePath -PathType Leaf)) {
        throw "Coloque minio.license no diretorio externo configurado antes de preparar as credenciais."
    }
    $secretsDir = Split-Path -Parent $licensePath
    $resolvedDir = (Resolve-Path -LiteralPath $secretsDir).Path
    $resolvedProject = (Resolve-Path -LiteralPath $projectRoot).Path
    if ($resolvedDir -eq $resolvedProject -or
        $resolvedDir.StartsWith($resolvedProject + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "As credenciais devem ficar fora do projeto."
    }
    foreach ($syncRoot in @($env:OneDrive, $env:OneDriveConsumer, $env:OneDriveCommercial)) {
        if ($syncRoot -and ($resolvedDir -eq $syncRoot -or
            $resolvedDir.StartsWith($syncRoot.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase))) {
            throw "Use um diretorio fora do OneDrive."
        }
    }

    $targets = @(
        (Join-Path $resolvedDir "minio-root-user"),
        (Join-Path $resolvedDir "minio-root-password"),
        (Join-Path $resolvedDir "s3.env")
    )
    $present = @($targets | Where-Object { Test-Path -LiteralPath $_ })
    if ($present.Count -eq $targets.Count) {
        foreach ($target in $targets) {
            if (-not (Test-Path -LiteralPath $target -PathType Leaf) -or
                (Get-Item -LiteralPath $target).Length -eq 0) {
                throw "Arquivo de credencial vazio/invalido; nenhuma alteracao foi feita."
            }
        }
        Write-Host "Credenciais ja existem; preservadas sem rotacao."
        return
    }
    if ($present.Count -ne 0) {
        throw "Conjunto de credenciais incompleto. Interrompido para evitar sobrescrita/rotacao acidental."
    }

    $rootUser = "dfad" + (New-RandomHex 8)
    $rootPassword = New-RandomHex 32
    $appUser = "dfpl" + (New-RandomHex 8)
    $appPassword = New-RandomHex 32
    $contents = @(
        $rootUser,
        $rootPassword,
        "AWS_ACCESS_KEY_ID=$appUser`nAWS_SECRET_ACCESS_KEY=$appPassword`n"
    )
    for ($index = 0; $index -lt $targets.Count; $index++) {
        # CreateNew refuses to overwrite even if another process creates the file.
        $stream = [IO.File]::Open($targets[$index], [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        try {
            $bytes = [Text.Encoding]::UTF8.GetBytes($contents[$index])
            $stream.Write($bytes, 0, $bytes.Length)
        }
        finally {
            $stream.Dispose()
        }
    }
    Write-Host "Tres arquivos de credenciais criados no diretorio externo. Valores nao exibidos."
    Write-Host "Licenca preservada; nenhum container foi iniciado."
}
finally {
    Pop-Location
}
