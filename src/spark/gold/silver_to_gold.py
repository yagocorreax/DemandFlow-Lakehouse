import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável obrigatória {name} não encontrada."
        )

    return value


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DemandFlowSilverToGold")
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
            required_env(
                "LOCALSTACK_INTERNAL_ENDPOINT"
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            required_env(
                "AWS_ACCESS_KEY_ID"
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            required_env(
                "AWS_SECRET_ACCESS_KEY"
            ),
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "2",
        )
        .getOrCreate()
    )


def read_silver(
    spark: SparkSession,
    silver_root: str,
    table_name: str,
):
    path = (
        f"{silver_root}/{table_name}"
    )

    return (
        spark.read
        .format("delta")
        .load(path)
    )


def write_gold(
    df,
    path: str,
) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .option(
            "delta.enableChangeDataFeed",
            "true",
        )
        .save(path)
    )


def build_daily_sales(
    orders,
    stores,
):
    orders_prepared = (
        orders
        .withColumn(
            "sales_date",
            F.to_date(
                F.col("order_date")
            ),
        )
    )

    aggregated = (
        orders_prepared
        .groupBy(
            "sales_date",
            "store_id",
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            F.sum(
                "total_amount"
            ).alias(
                "total_revenue"
            ),

            F.avg(
                "total_amount"
            ).alias(
                "average_order_value"
            ),
        )
    )

    return (
        aggregated
        .join(
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
        .select(
            "sales_date",
            "store_id",
            "store_code",
            "store_name",
            "city",
            "state",
            "order_count",
            "total_revenue",
            "average_order_value",
        )
        .orderBy(
            "sales_date",
            "store_id",
        )
    )


def build_product_sales(
    orders,
    order_items,
    products,
):
    items_with_orders = (
        order_items
        .join(
            orders.select(
                "order_id",
                "order_date",
                "order_status",
                "store_id",
            ),
            on="order_id",
            how="inner",
        )
    )

    calculated = (
        items_with_orders
        .withColumn(
            "gross_revenue",
            F.col("quantity")
            * F.col("unit_price"),
        )
        .withColumn(
            "net_revenue",
            (
                F.col("quantity")
                * F.col("unit_price")
            )
            - F.coalesce(
                F.col("discount_amount"),
                F.lit(0),
            ),
        )
    )

    aggregated = (
        calculated
        .groupBy(
            "product_id",
        )
        .agg(
            F.countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            F.sum(
                "quantity"
            ).alias(
                "units_sold"
            ),

            F.sum(
                "gross_revenue"
            ).alias(
                "gross_revenue"
            ),

            F.sum(
                F.coalesce(
                    F.col(
                        "discount_amount"
                    ),
                    F.lit(0),
                )
            ).alias(
                "discount_amount"
            ),

            F.sum(
                "net_revenue"
            ).alias(
                "net_revenue"
            ),
        )
    )

    return (
        aggregated
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
        .select(
            "product_id",
            "sku",
            "product_name",
            "category",
            "order_count",
            "units_sold",
            "gross_revenue",
            "discount_amount",
            "net_revenue",
        )
        .orderBy(
            F.col(
                "net_revenue"
            ).desc()
        )
    )


def build_inventory_health(
    inventory,
    stores,
    products,
):
    enriched = (
        inventory
        .join(
            stores.select(
                "store_id",
                "store_name",
            ),
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
    )

    return (
        enriched
        .withColumn(
            "stock_status",
            F.when(
                F.col(
                    "stock_quantity"
                ) <= 0,
                F.lit(
                    "OUT_OF_STOCK"
                ),
            )
            .when(
                F.col(
                    "stock_quantity"
                )
                <= F.col(
                    "safety_stock"
                ),
                F.lit(
                    "LOW_STOCK"
                ),
            )
            .otherwise(
                F.lit(
                    "HEALTHY"
                )
            ),
        )
        .withColumn(
            "quantity_above_safety_stock",
            F.col(
                "stock_quantity"
            )
            - F.col(
                "safety_stock"
            ),
        )
        .select(
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
            "updated_at",
        )
        .orderBy(
            "store_id",
            "product_id",
        )
    )


def build_forecast_accuracy(
    forecasts,
    orders,
    order_items,
):
    actual_sales = (
        order_items
        .join(
            orders.select(
                "order_id",
                "store_id",
                "order_date",
            ),
            on="order_id",
            how="inner",
        )
        .withColumn(
            "sales_date",
            F.to_date(
                F.col("order_date")
            ),
        )
        .groupBy(
            "store_id",
            "product_id",
            "sales_date",
        )
        .agg(
            F.sum(
                "quantity"
            ).alias(
                "actual_quantity"
            )
        )
    )

    forecast_prepared = (
        forecasts
        .withColumn(
            "forecast_date_join",
            F.to_date(
                F.col(
                    "forecast_date"
                )
            ),
        )
    )

    result = (
        forecast_prepared
        .join(
            actual_sales,
            (
                forecast_prepared.store_id
                == actual_sales.store_id
            )
            & (
                forecast_prepared.product_id
                == actual_sales.product_id
            )
            & (
                forecast_prepared.forecast_date_join
                == actual_sales.sales_date
            ),
            how="left",
        )
        .drop(
            actual_sales.store_id
        )
        .drop(
            actual_sales.product_id
        )
        .withColumn(
            "actual_quantity",
            F.coalesce(
                F.col(
                    "actual_quantity"
                ),
                F.lit(0),
            ),
        )
        .withColumn(
            "forecast_error",
            F.col(
                "actual_quantity"
            )
            - F.col(
                "forecast_quantity"
            ),
        )
        .withColumn(
            "absolute_error",
            F.abs(
                F.col(
                    "forecast_error"
                )
            ),
        )
        .withColumn(
            "absolute_percentage_error",
            F.when(
                F.col(
                    "forecast_quantity"
                ) > 0,
                (
                    F.col(
                        "absolute_error"
                    )
                    / F.col(
                        "forecast_quantity"
                    )
                )
                * 100,
            ),
        )
        .select(
            "forecast_id",
            forecast_prepared.store_id.alias(
                "store_id"
            ),
            forecast_prepared.product_id.alias(
                "product_id"
            ),
            F.col(
                "forecast_date_join"
            ).alias(
                "forecast_date"
            ),
            "model_version",
            "forecast_quantity",
            "actual_quantity",
            "forecast_error",
            "absolute_error",
            "absolute_percentage_error",
        )
    )

    return result


def main() -> None:
    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    silver_root = required_env(
        "SILVER_S3_PATH"
    )

    gold_root = required_env(
        "GOLD_S3_PATH"
    )

    try:
        print(
            "Carregando tabelas Silver..."
        )

        stores = read_silver(
            spark,
            silver_root,
            "stores",
        )

        products = read_silver(
            spark,
            silver_root,
            "products",
        )

        orders = read_silver(
            spark,
            silver_root,
            "orders",
        )

        order_items = read_silver(
            spark,
            silver_root,
            "order_items",
        )

        inventory = read_silver(
            spark,
            silver_root,
            "inventory",
        )

        forecasts = read_silver(
            spark,
            silver_root,
            "demand_forecasts",
        )

        print(
            "Criando daily_sales..."
        )

        daily_sales = build_daily_sales(
            orders=orders,
            stores=stores,
        )

        write_gold(
            daily_sales,
            f"{gold_root}/daily_sales",
        )

        print(
            "Criando product_sales..."
        )

        product_sales = build_product_sales(
            orders=orders,
            order_items=order_items,
            products=products,
        )

        write_gold(
            product_sales,
            f"{gold_root}/product_sales",
        )

        print(
            "Criando inventory_health..."
        )

        inventory_health = (
            build_inventory_health(
                inventory=inventory,
                stores=stores,
                products=products,
            )
        )

        write_gold(
            inventory_health,
            f"{gold_root}/inventory_health",
        )

        print(
            "Criando forecast_accuracy..."
        )

        forecast_accuracy = (
            build_forecast_accuracy(
                forecasts=forecasts,
                orders=orders,
                order_items=order_items,
            )
        )

        write_gold(
            forecast_accuracy,
            f"{gold_root}/forecast_accuracy",
        )

        print("")
        print(
            "CAMADA GOLD CONCLUÍDA "
            "COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()