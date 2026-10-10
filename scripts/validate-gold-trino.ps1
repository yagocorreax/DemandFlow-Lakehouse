$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$tableContracts = [ordered]@{
    "daily_sales" = @{
        Columns = @(
            "sales_date|date",
            "store_id|integer",
            "store_code|varchar",
            "store_name|varchar",
            "city|varchar",
            "state|varchar",
            "order_count|bigint",
            "total_revenue|decimal(20,2)",
            "average_order_value|decimal(20,2)"
        )
        Key = "sales_date, store_id"
        Required = @(
            "sales_date",
            "store_id",
            "store_code",
            "store_name",
            "city",
            "state",
            "order_count",
            "total_revenue",
            "average_order_value"
        )
        Invalid = @"
order_count <= 0
OR total_revenue < DECIMAL '0.00'
OR average_order_value < DECIMAL '0.00'
OR trim(store_code) = ''
OR trim(store_name) = ''
OR trim(city) = ''
OR trim(state) = ''
"@
    }
    "forecast_accuracy" = @{
        Columns = @(
            "forecast_id|bigint",
            "store_id|integer",
            "product_id|integer",
            "forecast_date|date",
            "model_version|varchar",
            "forecast_quantity|decimal(12,2)",
            "actual_quantity|bigint",
            "forecast_error|decimal(18,2)",
            "absolute_error|decimal(18,2)",
            "absolute_percentage_error|decimal(18,4)"
        )
        Key = "forecast_id"
        Required = @(
            "forecast_id",
            "store_id",
            "product_id",
            "forecast_date",
            "model_version",
            "forecast_quantity",
            "actual_quantity",
            "forecast_error",
            "absolute_error"
        )
        Invalid = @"
forecast_quantity < DECIMAL '0.00'
OR actual_quantity < 0
OR absolute_error < DECIMAL '0.00'
OR absolute_percentage_error < DECIMAL '0.0000'
OR trim(model_version) = ''
OR forecast_error <> CAST(
    CAST(actual_quantity AS DECIMAL(18,2))
    - CAST(forecast_quantity AS DECIMAL(18,2))
    AS DECIMAL(18,2)
)
OR absolute_error <> abs(forecast_error)
OR (
    actual_quantity = 0
    AND absolute_percentage_error IS NOT NULL
)
OR (
    actual_quantity > 0
    AND abs(
        CAST(absolute_percentage_error AS DOUBLE)
        - (
            CAST(absolute_error AS DOUBLE)
            / CAST(actual_quantity AS DOUBLE)
            * 100.0
        )
    ) > 0.0001
)
"@
    }
    "inventory_health" = @{
        Columns = @(
            "store_id|integer",
            "store_name|varchar",
            "product_id|integer",
            "sku|varchar",
            "product_name|varchar",
            "category|varchar",
            "stock_quantity|integer",
            "safety_stock|integer",
            "quantity_above_safety_stock|integer",
            "stock_status|varchar",
            "updated_at|timestamp(3) with time zone"
        )
        Key = "store_id, product_id"
        Required = @(
            "store_id",
            "store_name",
            "product_id",
            "sku",
            "product_name",
            "category",
            "stock_quantity",
            "safety_stock",
            "quantity_above_safety_stock",
            "stock_status",
            "updated_at"
        )
        Invalid = @"
stock_quantity < 0
OR safety_stock < 0
OR trim(store_name) = ''
OR trim(sku) = ''
OR trim(product_name) = ''
OR trim(category) = ''
OR quantity_above_safety_stock <> stock_quantity - safety_stock
OR stock_status <> CASE
    WHEN stock_quantity <= 0 THEN 'OUT_OF_STOCK'
    WHEN stock_quantity <= safety_stock THEN 'LOW_STOCK'
    ELSE 'HEALTHY'
END
"@
    }
    "product_sales" = @{
        Columns = @(
            "product_id|integer",
            "sku|varchar",
            "product_name|varchar",
            "category|varchar",
            "order_count|bigint",
            "units_sold|bigint",
            "gross_revenue|decimal(20,2)",
            "discount_amount|decimal(20,2)",
            "net_revenue|decimal(20,2)"
        )
        Key = "product_id"
        Required = @(
            "product_id",
            "sku",
            "product_name",
            "category",
            "order_count",
            "units_sold",
            "gross_revenue",
            "discount_amount",
            "net_revenue"
        )
        Invalid = @"
order_count <= 0
OR units_sold <= 0
OR gross_revenue < DECIMAL '0.00'
OR discount_amount < DECIMAL '0.00'
OR net_revenue < DECIMAL '0.00'
OR net_revenue > gross_revenue
OR trim(sku) = ''
OR trim(product_name) = ''
OR trim(category) = ''
"@
    }
}

function Invoke-TrinoLines {
    param(
        [Parameter(Mandatory)]
        [string]$Sql
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(
            & docker exec demandflow-trino `
                trino `
                --output-format TSV `
                --execute $Sql 2>&1
        )
        $invocationSucceeded = $?
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorAction
    }

    if (-not $invocationSucceeded -or $exitCode -ne 0) {
        $detail = [string]::Join(
            " ",
            @($output | ForEach-Object { ([string]$_).Trim() })
        )
        throw "Consulta Trino falhou: $detail"
    }

    return @(
        $output |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
}

function Assert-ExactSequence {
    param(
        [Parameter(Mandatory)]
        [string]$Label,

        [Parameter(Mandatory)]
        [string[]]$Expected,

        [Parameter(Mandatory)]
        [string[]]$Actual
    )

    $expectedText = [string]::Join("`n", $Expected)
    $actualText = [string]::Join("`n", $Actual)
    if ($actualText -cne $expectedText) {
        throw "$Label diverge do contrato esperado."
    }
}

$expectedTables = @($tableContracts.Keys)
$actualTables = @(
    Invoke-TrinoLines -Sql @"
SELECT table_name
FROM delta.information_schema.tables
WHERE table_schema = 'gold'
ORDER BY table_name
"@
)
Assert-ExactSequence `
    -Label "Conjunto de tabelas Gold" `
    -Expected $expectedTables `
    -Actual $actualTables

$expectedSchema = @(
    foreach ($tableName in $expectedTables) {
        foreach ($column in $tableContracts[$tableName].Columns) {
            "$tableName|$column"
        }
    }
)
$actualSchema = @(
    Invoke-TrinoLines -Sql @"
SELECT table_name || '|' || column_name || '|' || data_type
FROM delta.information_schema.columns
WHERE table_schema = 'gold'
ORDER BY table_name, ordinal_position
"@
)
Assert-ExactSequence `
    -Label "Schemas Gold" `
    -Expected $expectedSchema `
    -Actual $actualSchema

$auditStatements = @(
    foreach ($tableName in $expectedTables) {
        $contract = $tableContracts[$tableName]
        $nullCondition = [string]::Join(
            " OR ",
            @($contract.Required | ForEach-Object { "$_ IS NULL" })
        )
        @"
SELECT
    '$tableName' AS table_name,
    CAST(COUNT(*) AS VARCHAR) AS row_count,
    CAST((
        SELECT COUNT(*)
        FROM (
            SELECT $($contract.Key)
            FROM delta.gold.$tableName
            GROUP BY $($contract.Key)
            HAVING COUNT(*) > 1
        ) duplicate_keys
    ) AS VARCHAR) AS duplicate_key_count,
    CAST(count_if($nullCondition) AS VARCHAR) AS null_required_count,
    CAST(count_if($($contract.Invalid)) AS VARCHAR) AS invalid_metric_count
FROM delta.gold.$tableName
"@
    }
)
$auditSql = (
    [string]::Join("`nUNION ALL`n", $auditStatements) +
    "`nORDER BY table_name"
)
$auditLines = @(Invoke-TrinoLines -Sql $auditSql)
if ($auditLines.Count -ne $expectedTables.Count) {
    throw "A auditoria Trino não retornou uma linha por tabela Gold."
}

$rowCounts = [ordered]@{}
foreach ($line in $auditLines) {
    $fields = @($line -split "`t", 5)
    if ($fields.Count -ne 5) {
        throw "A auditoria Trino retornou uma linha inválida."
    }
    $tableName, $rowCount, $duplicateCount, $nullCount, $invalidCount = $fields
    if ($tableName -notin $expectedTables) {
        throw "A auditoria Trino retornou tabela inesperada: $tableName."
    }
    if ([int64]$rowCount -le 0) {
        throw "$tableName não contém registros."
    }
    if ($duplicateCount -ne "0") {
        throw "$tableName contém $duplicateCount chave(s) duplicada(s)."
    }
    if ($nullCount -ne "0") {
        throw "$tableName contém $nullCount campo(s) obrigatório(s) nulo(s)."
    }
    if ($invalidCount -ne "0") {
        throw "$tableName contém $invalidCount métrica(s) inválida(s)."
    }
    $rowCounts[$tableName] = $rowCount
}

$businessLines = @(
    Invoke-TrinoLines -Sql @"
SELECT
    CAST((SELECT SUM(order_count) FROM delta.gold.daily_sales) AS VARCHAR),
    CAST((SELECT SUM(total_revenue) FROM delta.gold.daily_sales) AS VARCHAR),
    CAST((SELECT SUM(net_revenue) FROM delta.gold.product_sales) AS VARCHAR),
    CAST((SELECT SUM(units_sold) FROM delta.gold.product_sales) AS VARCHAR),
    CAST((SELECT count_if(stock_status = 'OUT_OF_STOCK') FROM delta.gold.inventory_health) AS VARCHAR),
    CAST((SELECT count_if(stock_status = 'LOW_STOCK') FROM delta.gold.inventory_health) AS VARCHAR),
    CAST((SELECT count_if(stock_status = 'HEALTHY') FROM delta.gold.inventory_health) AS VARCHAR),
    CAST((
        SELECT CAST(
            AVG(CAST(absolute_error AS DOUBLE))
            AS DECIMAL(20,4)
        )
        FROM delta.gold.forecast_accuracy
    ) AS VARCHAR)
"@
)
if ($businessLines.Count -ne 1) {
    throw "A reconciliação de negócio Trino não retornou uma linha."
}
$businessFields = @($businessLines[0] -split "`t", 8)
if ($businessFields.Count -ne 8) {
    throw "A reconciliação de negócio Trino retornou formato inválido."
}
$eligibleOrderCount = $businessFields[0]
$totalRevenue = $businessFields[1]
$netRevenue = $businessFields[2]
$totalUnitsSold = $businessFields[3]
$outOfStockCount = $businessFields[4]
$lowStockCount = $businessFields[5]
$healthyStockCount = $businessFields[6]
$forecastMae = $businessFields[7]
if ($totalRevenue -ne $netRevenue) {
    throw "daily_sales e product_sales apresentam receitas divergentes."
}

$totalRows = 0L
foreach ($rowCount in $rowCounts.Values) {
    $totalRows += [int64]$rowCount
}

foreach ($tableName in $expectedTables) {
    Write-Output "TRINO_ROW_COUNT_${tableName}=$($rowCounts[$tableName])"
}
Write-Output "TRINO_TABLE_COUNT=$($expectedTables.Count)"
Write-Output "TRINO_TOTAL_ROW_COUNT=$totalRows"
Write-Output "TRINO_ELIGIBLE_ORDER_COUNT=$eligibleOrderCount"
Write-Output "TRINO_TOTAL_REVENUE=$totalRevenue"
Write-Output "TRINO_NET_REVENUE=$netRevenue"
Write-Output "TRINO_TOTAL_UNITS_SOLD=$totalUnitsSold"
Write-Output "TRINO_OUT_OF_STOCK_COUNT=$outOfStockCount"
Write-Output "TRINO_LOW_STOCK_COUNT=$lowStockCount"
Write-Output "TRINO_HEALTHY_STOCK_COUNT=$healthyStockCount"
Write-Output "TRINO_FORECAST_MAE=$forecastMae"
Write-Output "TRINO_EXACT_TABLE_SET=true"
Write-Output "TRINO_SCHEMA_CONTRACT=true"
Write-Output "TRINO_KEY_CONTRACT=true"
Write-Output "TRINO_FINANCIAL_RECONCILIATION=true"
Write-Output "TRINO_METRIC_RULES=true"
Write-Host "VALIDAÇÃO GOLD + TRINO CONCLUÍDA COM SUCESSO."
