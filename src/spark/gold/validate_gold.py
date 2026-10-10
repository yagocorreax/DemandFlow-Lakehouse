from datetime import date, datetime
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver_to_gold import (
    GOLD_CONFIG,
    SILVER_REQUIRED_COLUMNS,
    build_daily_sales,
    build_forecast_accuracy,
    build_gold_tables,
    build_inventory_health,
    build_product_sales,
    create_spark,
    eligible_orders,
    read_silver,
    required_env,
    schema_signature,
    validate_output_contract,
)


def exact_mismatch_count(expected: DataFrame, actual: DataFrame) -> int:
    differences = expected.exceptAll(actual).unionByName(actual.exceptAll(expected))
    return differences.limit(1).count()


def delta_version_and_require_cdf(
    delta_table: DeltaTable,
    table_name: str,
) -> int:
    detail = delta_table.detail().select("properties").first()
    properties = detail["properties"] or {}
    if str(properties.get("delta.enableChangeDataFeed", "false")).lower() != "true":
        raise RuntimeError(f"{table_name}: Change Data Feed não está habilitado.")

    history = delta_table.history(1).select("version").first()
    return int(history["version"])


def validate_gold_table(
    spark: SparkSession,
    table_name: str,
    expected: DataFrame,
    gold_root: str,
) -> tuple[DataFrame, dict]:
    path = f"{gold_root}/{table_name}"
    if not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(f"Tabela Gold ausente: {table_name}")

    actual = (
        spark.read.format("delta")
        .load(path)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        expected_count = validate_output_contract(expected, table_name)
        actual_count = validate_output_contract(actual, table_name)
        if expected_count != actual_count:
            raise RuntimeError(
                f"{table_name}: contagem Gold divergente "
                f"(esperado={expected_count}, atual={actual_count})."
            )
        if schema_signature(expected) != schema_signature(actual):
            raise RuntimeError(f"{table_name}: schema Gold divergente.")

        mismatch_count = exact_mismatch_count(expected, actual)
        if mismatch_count:
            raise RuntimeError(f"{table_name}: conteúdo Gold divergente da Silver.")

        version = delta_version_and_require_cdf(
            DeltaTable.forPath(spark, path),
            table_name,
        )
        return actual, {
            "row_count": actual_count,
            "version": version,
        }
    except Exception:
        actual.unpersist()
        raise


def decimal_text(value, scale: int) -> str:
    decimal_value = Decimal(0) if value is None else Decimal(value)
    quantum = Decimal(1).scaleb(-scale)
    return format(decimal_value.quantize(quantum), f".{scale}f")


def validate_business_reconciliation(
    silver_tables: dict[str, DataFrame],
    gold_tables: dict[str, DataFrame],
) -> dict[str, str]:
    source_orders = eligible_orders(silver_tables["orders"]).agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("total_amount").cast("decimal(20,2)").alias("revenue"),
    ).first()
    daily = gold_tables["daily_sales"].agg(
        F.sum("order_count").cast("long").alias("order_count"),
        F.sum("total_revenue").cast("decimal(20,2)").alias("revenue"),
    ).first()
    product = gold_tables["product_sales"].agg(
        F.sum("units_sold").cast("long").alias("units_sold"),
        F.sum("net_revenue").cast("decimal(20,2)").alias("net_revenue"),
    ).first()

    source_order_count = int(source_orders["order_count"] or 0)
    daily_order_count = int(daily["order_count"] or 0)
    source_revenue = Decimal(source_orders["revenue"] or 0)
    daily_revenue = Decimal(daily["revenue"] or 0)
    product_revenue = Decimal(product["net_revenue"] or 0)
    if daily_order_count != source_order_count:
        raise RuntimeError("daily_sales não recompõe os pedidos realizados.")
    if daily_revenue != source_revenue:
        raise RuntimeError("daily_sales não recompõe a receita da Silver.")
    if product_revenue != daily_revenue:
        raise RuntimeError(
            "product_sales e daily_sales apresentam receitas divergentes."
        )

    inventory_source_count = silver_tables["inventory"].count()
    inventory_rows = gold_tables["inventory_health"].groupBy(
        "stock_status"
    ).count().collect()
    inventory_counts = {
        row["stock_status"]: int(row["count"])
        for row in inventory_rows
    }
    allowed_statuses = {"OUT_OF_STOCK", "LOW_STOCK", "HEALTHY"}
    if set(inventory_counts) - allowed_statuses:
        raise RuntimeError("inventory_health contém status desconhecido.")
    if sum(inventory_counts.values()) != inventory_source_count:
        raise RuntimeError("inventory_health não recompõe o inventário Silver.")

    forecast_source_count = silver_tables["demand_forecasts"].count()
    forecast = gold_tables["forecast_accuracy"]
    if forecast.count() != forecast_source_count:
        raise RuntimeError("forecast_accuracy não recompõe as previsões Silver.")

    expected_error = (
        F.col("actual_quantity").cast("decimal(18,2)")
        - F.col("forecast_quantity").cast("decimal(18,2)")
    ).cast("decimal(18,2)")
    expected_absolute = F.abs(expected_error).cast("decimal(18,2)")
    expected_percentage = (
        expected_absolute
        / F.col("actual_quantity").cast("decimal(18,4)")
        * F.lit(100)
    ).cast("decimal(18,4)")
    invalid_forecast = forecast.filter(
        (~F.col("forecast_error").eqNullSafe(expected_error))
        | (~F.col("absolute_error").eqNullSafe(expected_absolute))
        | (
            (F.col("actual_quantity") > 0)
            & (~F.col("absolute_percentage_error").eqNullSafe(expected_percentage))
        )
        | (
            (F.col("actual_quantity") == 0)
            & F.col("absolute_percentage_error").isNotNull()
        )
    ).limit(1).count()
    if invalid_forecast:
        raise RuntimeError("forecast_accuracy contém cálculo de erro inconsistente.")

    forecast_mae = forecast.agg(
        F.avg("absolute_error").cast("decimal(20,4)").alias("mae")
    ).first()["mae"]
    return {
        "eligible_order_count": str(source_order_count),
        "total_revenue": decimal_text(daily_revenue, 2),
        "net_revenue": decimal_text(product_revenue, 2),
        "total_units_sold": str(int(product["units_sold"] or 0)),
        "out_of_stock_count": str(inventory_counts.get("OUT_OF_STOCK", 0)),
        "low_stock_count": str(inventory_counts.get("LOW_STOCK", 0)),
        "healthy_stock_count": str(inventory_counts.get("HEALTHY", 0)),
        "forecast_mae": decimal_text(forecast_mae, 4),
    }


def validate_rule_probes(spark: SparkSession) -> int:
    stores = spark.createDataFrame(
        [(1, "S001", "Loja Teste", "São Paulo", "SP")],
        "store_id int, store_code string, store_name string, city string, state string",
    )
    products = spark.createDataFrame(
        [
            (1, "SKU-1", "Produto 1", "A"),
            (2, "SKU-2", "Produto 2", "A"),
            (3, "SKU-3", "Produto 3", "B"),
        ],
        "product_id int, sku string, product_name string, category string",
    )
    orders = spark.createDataFrame(
        [
            (1, 1, "PAID", datetime(2026, 1, 1, 12), Decimal("20.00")),
            (2, 1, "CANCELLED", datetime(2026, 1, 1, 13), Decimal("100.00")),
            (3, 1, "CREATED", datetime(2026, 1, 1, 14), Decimal("100.00")),
            (4, 1, "SHIPPED", datetime(2026, 1, 1, 15), Decimal("5.00")),
        ],
        (
            "order_id long, store_id int, order_status string, "
            "order_date timestamp, total_amount decimal(14,2)"
        ),
    )
    order_items = spark.createDataFrame(
        [
            (1, 1, 2, Decimal("10.00"), Decimal("0.00")),
            (2, 1, 10, Decimal("10.00"), Decimal("0.00")),
            (3, 1, 10, Decimal("10.00"), Decimal("0.00")),
            (4, 2, 1, Decimal("5.00"), Decimal("0.00")),
        ],
        (
            "order_id long, product_id int, quantity int, "
            "unit_price decimal(12,2), discount_amount decimal(12,2)"
        ),
    )
    inventory = spark.createDataFrame(
        [
            (1, 1, 0, 2, datetime(2026, 1, 1, 10)),
            (1, 2, 2, 2, datetime(2026, 1, 1, 10)),
            (1, 3, 3, 2, datetime(2026, 1, 1, 10)),
        ],
        (
            "store_id int, product_id int, stock_quantity int, "
            "safety_stock int, updated_at timestamp"
        ),
    )
    forecasts = spark.createDataFrame(
        [
            (1, 1, 1, date(2026, 1, 1), "v1", Decimal("4.00")),
            (2, 1, 2, date(2026, 1, 2), "v1", Decimal("3.00")),
        ],
        (
            "forecast_id long, store_id int, product_id int, "
            "forecast_date date, model_version string, "
            "forecast_quantity decimal(12,2)"
        ),
    )

    daily = build_daily_sales(orders, stores).first()
    if int(daily["order_count"]) != 2:
        raise RuntimeError("Probe Gold: status não realizado entrou em daily_sales.")
    if daily["total_revenue"] != Decimal("25.00"):
        raise RuntimeError("Probe Gold: receita diária incorreta.")

    product_rows = {
        int(row["product_id"]): row
        for row in build_product_sales(orders, order_items, products).collect()
    }
    if set(product_rows) != {1, 2}:
        raise RuntimeError("Probe Gold: produto de pedido não realizado foi agregado.")
    if (
        int(product_rows[1]["units_sold"]) != 2
        or product_rows[1]["net_revenue"] != Decimal("20.00")
        or product_rows[2]["net_revenue"] != Decimal("5.00")
    ):
        raise RuntimeError("Probe Gold: desempenho de produto incorreto.")

    inventory_rows = {
        int(row["product_id"]): row["stock_status"]
        for row in build_inventory_health(inventory, stores, products).collect()
    }
    if inventory_rows != {
        1: "OUT_OF_STOCK",
        2: "LOW_STOCK",
        3: "HEALTHY",
    }:
        raise RuntimeError("Probe Gold: fronteiras de estoque incorretas.")

    forecast_rows = {
        int(row["forecast_id"]): row
        for row in build_forecast_accuracy(
            forecasts,
            orders,
            order_items,
        ).collect()
    }
    first = forecast_rows[1]
    if (
        int(first["actual_quantity"]) != 2
        or first["forecast_error"] != Decimal("-2.00")
        or first["absolute_error"] != Decimal("2.00")
        or first["absolute_percentage_error"] != Decimal("100.0000")
    ):
        raise RuntimeError("Probe Gold: erro percentual não usa o realizado.")
    second = forecast_rows[2]
    if (
        int(second["actual_quantity"]) != 0
        or second["forecast_error"] != Decimal("-3.00")
        or second["absolute_error"] != Decimal("3.00")
        or second["absolute_percentage_error"] is not None
    ):
        raise RuntimeError("Probe Gold: previsão sem realizado foi calculada incorretamente.")
    return 6


def main() -> None:
    for environment_name in (
        "S3_INTERNAL_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ):
        required_env(environment_name)

    spark = create_spark("DemandFlowGoldValidation")
    spark.sparkContext.setLogLevel("WARN")
    silver_tables: dict[str, DataFrame] = {}
    gold_tables: dict[str, DataFrame] = {}

    try:
        silver_root = required_env("SILVER_S3_PATH")
        gold_root = required_env("GOLD_S3_PATH")
        for table_name in SILVER_REQUIRED_COLUMNS:
            silver_tables[table_name] = read_silver(
                spark,
                silver_root,
                table_name,
            )

        expected_tables = build_gold_tables(silver_tables)
        total_rows = 0
        for table_name, expected_candidate in expected_tables.items():
            print("")
            print(f"========== {table_name} ==========")
            expected = expected_candidate.localCheckpoint(eager=True)
            try:
                actual, metrics = validate_gold_table(
                    spark=spark,
                    table_name=table_name,
                    expected=expected,
                    gold_root=gold_root,
                )
                gold_tables[table_name] = actual
                total_rows += metrics["row_count"]
                print(
                    f"GOLD_ROW_COUNT_{table_name}="
                    f"{metrics['row_count']}"
                )
                print(
                    f"GOLD_VERSION_{table_name}="
                    f"{metrics['version']}"
                )
                print(f"GOLD_EXACT_MISMATCH_COUNT_{table_name}=0")
                print("Validação exata OK.")
            finally:
                expected.unpersist()

        business_metrics = validate_business_reconciliation(
            silver_tables,
            gold_tables,
        )
        probes_passed = validate_rule_probes(spark)

        print("")
        print("======================================")
        print("RESUMO DA VALIDAÇÃO GOLD")
        print("======================================")
        print(f"GOLD_TABLE_COUNT={len(GOLD_CONFIG)}")
        print(f"GOLD_TOTAL_ROW_COUNT={total_rows}")
        print(
            "GOLD_ELIGIBLE_ORDER_COUNT="
            f"{business_metrics['eligible_order_count']}"
        )
        print(f"GOLD_TOTAL_REVENUE={business_metrics['total_revenue']}")
        print(f"GOLD_NET_REVENUE={business_metrics['net_revenue']}")
        print(
            "GOLD_TOTAL_UNITS_SOLD="
            f"{business_metrics['total_units_sold']}"
        )
        print(
            "GOLD_OUT_OF_STOCK_COUNT="
            f"{business_metrics['out_of_stock_count']}"
        )
        print(
            "GOLD_LOW_STOCK_COUNT="
            f"{business_metrics['low_stock_count']}"
        )
        print(
            "GOLD_HEALTHY_STOCK_COUNT="
            f"{business_metrics['healthy_stock_count']}"
        )
        print(f"GOLD_FORECAST_MAE={business_metrics['forecast_mae']}")
        print("GOLD_EXACT_MISMATCH_COUNT=0")
        print("GOLD_FINANCIAL_RECONCILIATION=true")
        print("GOLD_SOURCE_RECONCILIATION=true")
        print(f"GOLD_RULE_PROBES_PASSED={probes_passed}")
        print("GOLD_CDF_ENABLED=true")
        print("VALIDAÇÃO GOLD CONCLUÍDA COM SUCESSO.")
    finally:
        for dataframe in gold_tables.values():
            dataframe.unpersist()
        for dataframe in silver_tables.values():
            dataframe.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
