$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$count = 0
foreach ($file in Get-ChildItem -LiteralPath $PSScriptRoot -Filter "*.ps1" -File) {
    $tokens = $null
    $parseErrors = $null
    $null = [Management.Automation.Language.Parser]::ParseFile(
        $file.FullName, [ref]$tokens, [ref]$parseErrors
    )
    if ($parseErrors.Count -gt 0) {
        # Report positions, never contents of script expressions.
        foreach ($parseError in $parseErrors) {
            Write-Host ("Parse error: {0}:{1} ({2})" -f
                $file.Name, $parseError.Extent.StartLineNumber, $parseError.ErrorId)
        }
        throw "Falha na sintaxe PowerShell."
    }
    $count++
}
Write-Host "$count scripts PowerShell analisados; nenhum script de operacao executado."
