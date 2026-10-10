import os
from functools import reduce

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


REVENUE_ORDER_STATUSES = ("PAID", "SHIPPED", "DELIVERED")

SILVER_REQUIRED_COLUMNS = {
    "stores": {
        "store_id",
        "store_code",
        "store_name",
        "city",
        "state",
    },
    "products": {
        "product_id",
        "sku",
        "product_name",
        "category",
    },
    "orders": {
        "order_id",
        "store_id",
        "order_status",
        "order_date",
        "total_amount",
    },
    "order_items": {
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
    },
    "inventory": {
        "store_id",
        "product_id",
        "stock_quantity",
        "safety_stock",
        "updated_at",
    },
    "demand_forecasts": {
        "forecast_id",
        "store_id",
        "product_id",
        "forecast_date",
        "model_version",
        "forecast_quantity",
    },
}

GOLD_CONFIG = {
    "daily_sales": {
        "primary_key": ["sales_date", "store_id"],
        "types": {
            "sales_date": "date",
            "store_id": "int",
            "store_code": "string",
            "store_name": "string",
            "city": "string",
            "state": "string",
            "order_count": "long",
            "total_revenue": "decimal(20,2)",
            "average_order_value": "decimal(20,2)",
        },
    },
    "product_sales": {
        "primary_key": ["product_id"],
        "types": {
            "product_id": "int",
            "sku": "string",
            "product_name": "string",
            "category": "string",
            "order_count": "long",
            "units_sold": "long",
            "gross_revenue": "decimal(20,2)",
            "discount_amount": "decimal(20,2)",
            "net_revenue": "decimal(20,2)",
        },
    },
    "inventory_health": {
        "primary_key": ["store_id", "product_id"],
        "types": {
            "store_id": "int",
            "store_name": "string",
            "product_id": "int",
            "sku": "string",
            "product_name": "string",
            "category": "string",
            "stock_quantity": "int",
            "safety_stock": "int",
            "quantity_above_safety_stock": "int",
            "stock_status": "string",
            "updated_at": "timestamp",
        },
    },
    "forecast_accuracy": {
        "primary_key": ["forecast_id"],
        "nullable": ["absolute_percentage_error"],
        "types": {
            "forecast_id": "long",
            "store_id": "int",
            "product_id": "int",
            "forecast_date": "date",
            "model_version": "string",
            "forecast_quantity": "decimal(12,2)",
            "actual_quantity": "long",
            "forecast_error": "decimal(18,2)",
            "absolute_error": "decimal(18,2)",
            "absolute_percentage_error": "decimal(18,4)",
        },
    },
}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variável obrigatória {name} não encontrada.")
    return value


def create_spark(
    app_name: str = "DemandFlowSilverToGold",
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            required_env("S3_INTERNAL_ENDPOINT"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            required_env("AWS_ACCESS_KEY_ID"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            required_env("AWS_SECRET_ACCESS_KEY"),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def read_silver(
    spark: SparkSession,
    silver_root: str,
    table_name: str,
) -> DataFrame:
    path = f"{silver_root}/{table_name}"
    if not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(f"Tabela Silver ausente: {table_name}")

    dataframe = spark.read.format("delta").load(path)
    missing = SILVER_REQUIRED_COLUMNS[table_name] - set(dataframe.columns)
    if missing:
        raise RuntimeError(
            f"{table_name}: colunas Silver ausentes: "
            + ", ".join(sorted(missing))
        )

    return dataframe.persist(StorageLevel.MEMORY_AND_DISK)


def eligible_orders(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("order_status").isin(*REVENUE_ORDER_STATUSES))


def project_gold(dataframe: DataFrame, table_name: str) -> DataFrame:
    return dataframe.select(
        *[
            F.col(column_name).cast(data_type).alias(column_name)
            for column_name, data_type in GOLD_CONFIG[table_name]["types"].items()
        ]
    )


def build_daily_sales(orders: DataFrame, stores: DataFrame) -> DataFrame:
    aggregated = (
        eligible_orders(orders)
        .withColumn("sales_date", F.to_date("order_date"))
        .groupBy("sales_date", "store_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("average_order_value"),
        )
    )

    enriched = aggregated.join(
        stores.select(
            "store_id",
            "store_code",
            "store_name",
            "city",
            "state",
        ),
        on="store_id",
        how="left",
    )
    return project_gold(enriched, "daily_sales")


def build_product_sales(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
) -> DataFrame:
    eligible_items = order_items.join(
        eligible_orders(orders).select("order_id"),
        on="order_id",
        how="inner",
    )
    calculated = (
        eligible_items.withColumn(
            "gross_revenue_line",
            (F.col("quantity") * F.col("unit_price")).cast("decimal(20,2)"),
        )
        .withColumn(
            "discount_amount_line",
            F.coalesce(
                F.col("discount_amount"),
                F.lit(0).cast("decimal(20,2)"),
            ).cast("decimal(20,2)"),
        )
        .withColumn(
            "net_revenue_line",
            (
                F.col("gross_revenue_line")
                - F.col("discount_amount_line")
            ).cast("decimal(20,2)"),
        )
    )
    aggregated = calculated.groupBy("product_id").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("gross_revenue_line").alias("gross_revenue"),
        F.sum("discount_amount_line").alias("discount_amount"),
        F.sum("net_revenue_line").alias("net_revenue"),
    )
    enriched = aggregated.join(
        products.select(
            "product_id",
            "sku",
            "product_name",
            "category",
        ),
        on="product_id",
        how="left",
    )
    return project_gold(enriched, "product_sales")


def build_inventory_health(
    inventory: DataFrame,
    stores: DataFrame,
    products: DataFrame,
) -> DataFrame:
    enriched = (
        inventory.join(
            stores.select("store_id", "store_name"),
            on="store_id",
            how="left",
        )
        .join(
            products.select(
                "product_id",
                "sku",
                "product_name",
                "category",
            ),
            on="product_id",
            how="left",
        )
        .withColumn(
            "stock_status",
            F.when(F.col("stock_quantity") <= 0, F.lit("OUT_OF_STOCK"))
            .when(
                F.col("stock_quantity") <= F.col("safety_stock"),
                F.lit("LOW_STOCK"),
            )
            .otherwise(F.lit("HEALTHY")),
        )
        .withColumn(
            "quantity_above_safety_stock",
            F.col("stock_quantity") - F.col("safety_stock"),
        )
    )
    return project_gold(enriched, "inventory_health")


def build_forecast_accuracy(
    forecasts: DataFrame,
    orders: DataFrame,
    order_items: DataFrame,
) -> DataFrame:
    actual_sales = (
        order_items.join(
            eligible_orders(orders).select(
                "order_id",
                "store_id",
                "order_date",
            ),
            on="order_id",
            how="inner",
        )
        .withColumn("sales_date", F.to_date("order_date"))
        .groupBy("store_id", "product_id", "sales_date")
        .agg(F.sum("quantity").cast("long").alias("actual_quantity"))
        .alias("actual")
    )
    forecast = forecasts.withColumn(
        "forecast_date_join",
        F.to_date("forecast_date"),
    ).alias("forecast")
    joined = forecast.join(
        actual_sales,
        (F.col("forecast.store_id") == F.col("actual.store_id"))
        & (F.col("forecast.product_id") == F.col("actual.product_id"))
        & (F.col("forecast.forecast_date_join") == F.col("actual.sales_date")),
        how="left",
    ).select(
        F.col("forecast.forecast_id").alias("forecast_id"),
        F.col("forecast.store_id").alias("store_id"),
        F.col("forecast.product_id").alias("product_id"),
        F.col("forecast.forecast_date_join").alias("forecast_date"),
        F.col("forecast.model_version").alias("model_version"),
        F.col("forecast.forecast_quantity").alias("forecast_quantity"),
        F.coalesce(
            F.col("actual.actual_quantity"),
            F.lit(0).cast("long"),
        ).alias("actual_quantity"),
    )
    result = (
        joined.withColumn(
            "forecast_error",
            (
                F.col("actual_quantity").cast("decimal(18,2)")
                - F.col("forecast_quantity").cast("decimal(18,2)")
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "absolute_error",
            F.abs("forecast_error").cast("decimal(18,2)"),
        )
        .withColumn(
            "absolute_percentage_error",
            F.when(
                F.col("actual_quantity") > 0,
                (
                    F.col("absolute_error")
                    / F.col("actual_quantity").cast("decimal(18,4)")
                    * F.lit(100)
                ).cast("decimal(18,4)"),
            ).otherwise(F.lit(None).cast("decimal(18,4)")),
        )
    )
    return project_gold(result, "forecast_accuracy")


def build_gold_tables(silver_tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        "daily_sales": build_daily_sales(
            orders=silver_tables["orders"],
            stores=silver_tables["stores"],
        ),
        "product_sales": build_product_sales(
            orders=silver_tables["orders"],
            order_items=silver_tables["order_items"],
            products=silver_tables["products"],
        ),
        "inventory_health": build_inventory_health(
            inventory=silver_tables["inventory"],
            stores=silver_tables["stores"],
            products=silver_tables["products"],
        ),
        "forecast_accuracy": build_forecast_accuracy(
            forecasts=silver_tables["demand_forecasts"],
            orders=silver_tables["orders"],
            order_items=silver_tables["order_items"],
        ),
    }


def validate_output_contract(dataframe: DataFrame, table_name: str) -> int:
    config = GOLD_CONFIG[table_name]
    expected_columns = list(config["types"])
    if dataframe.columns != expected_columns:
        raise RuntimeError(f"{table_name}: contrato de colunas Gold divergente.")

    nullable = set(config.get("nullable", []))
    required_columns = [
        column_name
        for column_name in expected_columns
        if column_name not in nullable
    ]
    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(column_name).isNull() for column_name in required_columns],
    )
    primary_key = config["primary_key"]
    summary = dataframe.agg(
        F.count(F.lit(1)).alias("total"),
        F.countDistinct(*primary_key).alias("distinct_keys"),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_required"),
    ).first()
    total = int(summary["total"])
    if int(summary["distinct_keys"]) != total:
        raise RuntimeError(f"{table_name}: chave Gold nula ou duplicada.")
    if int(summary["null_required"] or 0) != 0:
        raise RuntimeError(f"{table_name}: coluna obrigatória nula.")

    invalid_condition = None
    if table_name == "daily_sales":
        invalid_condition = (
            (F.col("order_count") <= 0)
            | (F.col("total_revenue") < 0)
            | (F.col("average_order_value") < 0)
        )
    elif table_name == "product_sales":
        invalid_condition = (
            (F.col("order_count") <= 0)
            | (F.col("units_sold") <= 0)
            | (F.col("gross_revenue") < 0)
            | (F.col("discount_amount") < 0)
            | (F.col("net_revenue") < 0)
            | (F.col("net_revenue") > F.col("gross_revenue"))
        )
    elif table_name == "inventory_health":
        invalid_condition = ~F.col("stock_status").isin(
            "OUT_OF_STOCK",
            "LOW_STOCK",
            "HEALTHY",
        )
    elif table_name == "forecast_accuracy":
        invalid_condition = (
            (F.col("forecast_quantity") < 0)
            | (F.col("actual_quantity") < 0)
            | (F.col("absolute_error") < 0)
            | (F.col("absolute_percentage_error") < 0)
        )

    if (
        invalid_condition is not None
        and dataframe.filter(invalid_condition).limit(1).count() > 0
    ):
        raise RuntimeError(f"{table_name}: métrica Gold inválida.")
    return total


def schema_signature(dataframe: DataFrame) -> list[tuple[str, str]]:
    return [
        (field.name, field.dataType.simpleString())
        for field in dataframe.schema.fields
    ]


def create_delta(dataframe: DataFrame, path: str) -> None:
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .save(path)
    )


def has_data_changes(source: DataFrame, target: DataFrame) -> bool:
    differences = source.exceptAll(target).unionByName(target.exceptAll(source))
    return differences.limit(1).count() > 0


def sync_gold(
    spark: SparkSession,
    source: DataFrame,
    path: str,
    primary_key: list[str],
) -> bool:
    if not DeltaTable.isDeltaTable(spark, path):
        create_delta(source, path)
        return True

    target_dataframe = spark.read.format("delta").load(path)
    if schema_signature(target_dataframe) != schema_signature(source):
        create_delta(source, path)
        return True
    if not has_data_changes(source, target_dataframe):
        return False

    non_key_columns = [
        column_name
        for column_name in source.columns
        if column_name not in primary_key
    ]
    merge_condition = " AND ".join(
        [
            f"target.`{column_name}` <=> source.`{column_name}`"
            for column_name in primary_key
        ]
    )
    changed_condition = " OR ".join(
        [
            f"NOT (target.`{column_name}` <=> source.`{column_name}`)"
            for column_name in non_key_columns
        ]
    )
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("target")
        .merge(source.alias("source"), merge_condition)
        .whenMatchedUpdateAll(condition=changed_condition)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    return True


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    silver_tables: dict[str, DataFrame] = {}

    try:
        silver_root = required_env("SILVER_S3_PATH")
        gold_root = required_env("GOLD_S3_PATH")
        for table_name in SILVER_REQUIRED_COLUMNS:
            silver_tables[table_name] = read_silver(
                spark,
                silver_root,
                table_name,
            )

        gold_tables = build_gold_tables(silver_tables)
        for table_name, candidate in gold_tables.items():
            print("")
            print(f"========== {table_name} ==========")
            materialized = candidate.localCheckpoint(eager=True)
            try:
                row_count = validate_output_contract(materialized, table_name)
                changed = sync_gold(
                    spark=spark,
                    source=materialized,
                    path=f"{gold_root}/{table_name}",
                    primary_key=GOLD_CONFIG[table_name]["primary_key"],
                )
                print(f"GOLD_ROW_COUNT_{table_name}={row_count}")
                print(
                    f"GOLD_WRITE_APPLIED_{table_name}="
                    f"{str(changed).lower()}"
                )
                print("Processamento concluído.")
            finally:
                materialized.unpersist()

        print("")
        print("CAMADA GOLD CONCLUÍDA COM SUCESSO.")
    finally:
        for dataframe in silver_tables.values():
            dataframe.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
