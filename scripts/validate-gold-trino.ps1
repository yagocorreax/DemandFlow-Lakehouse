$ErrorActionPreference = "Stop"

function Invoke-TrinoQuery {
    param(
        [Parameter(Mandatory)]
        [string]$Sql
    )

    docker exec demandflow-trino `
        trino `
        --execute $Sql

    if ($LASTEXITCODE -ne 0) {
        throw "Consulta Trino falhou."
    }
}


Write-Host ""
Write-Host "=== TABELAS GOLD ==="

Invoke-TrinoQuery `
    "SHOW TABLES FROM delta.gold;"


Write-Host ""
Write-Host "=== CONTAGEM ==="

Invoke-TrinoQuery @"
SELECT
    'daily_sales' AS table_name,
    COUNT(*) AS records
FROM delta.gold.daily_sales

UNION ALL

SELECT
    'product_sales',
    COUNT(*)
FROM delta.gold.product_sales

UNION ALL

SELECT
    'inventory_health',
    COUNT(*)
FROM delta.gold.inventory_health

UNION ALL

SELECT
    'forecast_accuracy',
    COUNT(*)
FROM delta.gold.forecast_accuracy;
"@


Write-Host ""
Write-Host "=== TOP PRODUTOS ==="

Invoke-TrinoQuery @"
SELECT
    product_name,
    units_sold,
    net_revenue
FROM delta.gold.product_sales
ORDER BY net_revenue DESC
LIMIT 5;
"@


Write-Host ""
Write-Host "=== SAÚDE DO ESTOQUE ==="

Invoke-TrinoQuery @"
SELECT
    stock_status,
    COUNT(*) AS records
FROM delta.gold.inventory_health
GROUP BY stock_status
ORDER BY records DESC;
"@


Write-Host ""
Write-Host "VALIDAÇÃO GOLD + TRINO CONCLUÍDA COM SUCESSO."