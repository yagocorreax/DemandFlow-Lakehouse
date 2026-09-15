import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TABLE_CONFIG = {
    "stores": {
        "primary_key": ["store_id"],
        "types": {
            "store_id": "int",
            "store_code": "string",
            "store_name": "string",
            "city": "string",
            "state": "string",
            "is_active": "boolean",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },

    "products": {
        "primary_key": ["product_id"],
        "types": {
            "product_id": "int",
            "sku": "string",
            "product_name": "string",
            "category": "string",
            "unit_price": "decimal(12,2)",
            "is_active": "boolean",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },

    "promotions": {
        "primary_key": ["promotion_id"],
        "types": {
            "promotion_id": "int",
            "promotion_name": "string",
            "discount_percentage": "decimal(5,2)",
            "start_date": "date",
            "end_date": "date",
            "created_at": "timestamp",
        },
    },

    "orders": {
        "primary_key": ["order_id"],
        "types": {
            "order_id": "long",
            "store_id": "int",
            "order_status": "string",
            "order_date": "timestamp",
            "total_amount": "decimal(14,2)",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },

    "order_items": {
        "primary_key": ["order_item_id"],
        "types": {
            "order_item_id": "long",
            "order_id": "long",
            "product_id": "int",
            "quantity": "int",
            "unit_price": "decimal(12,2)",
            "discount_amount": "decimal(12,2)",
            "created_at": "timestamp",
        },
    },

    "inventory": {
        "primary_key": [
            "store_id",
            "product_id",
        ],
        "types": {
            "store_id": "int",
            "product_id": "int",
            "stock_quantity": "int",
            "safety_stock": "int",
            "updated_at": "timestamp",
        },
    },

    "inventory_movements": {
        "primary_key": ["movement_id"],
        "types": {
            "movement_id": "long",
            "store_id": "int",
            "product_id": "int",
            "movement_type": "string",
            "quantity": "int",
            "reason": "string",
            "movement_date": "timestamp",
            "created_at": "timestamp",
        },
    },

    "demand_forecasts": {
        "primary_key": ["forecast_id"],
        "types": {
            "forecast_id": "long",
            "store_id": "int",
            "product_id": "int",
            "forecast_date": "date",
            "forecast_quantity": "decimal(14,2)",
            "model_version": "string",
            "created_at": "timestamp",
        },
    },
}


METADATA_COLUMNS = [
    "_operation",
    "_event_id",
    "_source_lsn",
    "_source_ts_ms",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_ingested_at",
]


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável {name} não encontrada."
        )

    return value


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DemandFlowBronzeToSilver")
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


def transform_types(
    bronze: DataFrame,
    config: dict,
) -> DataFrame:
    expressions = []

    for column_name, data_type in config[
        "types"
    ].items():
        expressions.append(
            F.col(column_name)
            .cast(data_type)
            .alias(column_name)
        )

    for column_name in METADATA_COLUMNS:
        if column_name in bronze.columns:
            expressions.append(
                F.col(column_name)
            )

    return bronze.select(
        *expressions
    )


def add_error(
    errors: list,
    condition,
    message: str,
) -> None:
    errors.append(
        F.when(
            condition,
            F.lit(message),
        )
    )


def apply_quality_rules(
    df: DataFrame,
    table_name: str,
    primary_key: list[str],
) -> DataFrame:
    errors = []

    for key in primary_key:
        add_error(
            errors,
            F.col(key).isNull(),
            f"NULL_PRIMARY_KEY:{key}",
        )

    if table_name == "stores":
        add_error(
            errors,
            F.col("store_code").isNull()
            | (
                F.trim(
                    F.col("store_code")
                ) == ""
            ),
            "INVALID_STORE_CODE",
        )

        add_error(
            errors,
            F.col("store_name").isNull()
            | (
                F.trim(
                    F.col("store_name")
                ) == ""
            ),
            "INVALID_STORE_NAME",
        )

    elif table_name == "products":
        add_error(
            errors,
            F.col("sku").isNull()
            | (
                F.trim(
                    F.col("sku")
                ) == ""
            ),
            "INVALID_SKU",
        )

        add_error(
            errors,
            F.col("unit_price").isNull()
            | (
                F.col("unit_price") < 0
            ),
            "INVALID_UNIT_PRICE",
        )

    elif table_name == "promotions":
        add_error(
            errors,
            F.col(
                "discount_percentage"
            ).isNull()
            | (
                F.col(
                    "discount_percentage"
                ) < 0
            )
            | (
                F.col(
                    "discount_percentage"
                ) > 100
            ),
            "INVALID_DISCOUNT_PERCENTAGE",
        )

        add_error(
            errors,
            F.col("start_date").isNull()
            | F.col("end_date").isNull(),
            "INVALID_PROMOTION_DATE",
        )

        add_error(
            errors,
            F.col("end_date")
            < F.col("start_date"),
            "END_DATE_BEFORE_START_DATE",
        )

    elif table_name == "orders":
        add_error(
            errors,
            F.col("store_id").isNull(),
            "NULL_STORE_ID",
        )

        add_error(
            errors,
            F.col("total_amount").isNull()
            | (
                F.col("total_amount") < 0
            ),
            "INVALID_TOTAL_AMOUNT",
        )

    elif table_name == "order_items":
        add_error(
            errors,
            F.col("order_id").isNull(),
            "NULL_ORDER_ID",
        )

        add_error(
            errors,
            F.col("product_id").isNull(),
            "NULL_PRODUCT_ID",
        )

        add_error(
            errors,
            F.col("quantity").isNull()
            | (
                F.col("quantity") <= 0
            ),
            "INVALID_QUANTITY",
        )

        add_error(
            errors,
            F.col("unit_price").isNull()
            | (
                F.col("unit_price") < 0
            ),
            "INVALID_UNIT_PRICE",
        )

    elif table_name == "inventory":
        add_error(
            errors,
            F.col("stock_quantity").isNull()
            | (
                F.col("stock_quantity") < 0
            ),
            "INVALID_STOCK_QUANTITY",
        )

        add_error(
            errors,
            F.col("safety_stock").isNull()
            | (
                F.col("safety_stock") < 0
            ),
            "INVALID_SAFETY_STOCK",
        )

    elif table_name == "inventory_movements":
        add_error(
            errors,
            F.col("store_id").isNull(),
            "NULL_STORE_ID",
        )

        add_error(
            errors,
            F.col("product_id").isNull(),
            "NULL_PRODUCT_ID",
        )

        add_error(
            errors,
            F.col("quantity").isNull()
            | (
                F.col("quantity") <= 0
            ),
            "INVALID_MOVEMENT_QUANTITY",
        )

    elif table_name == "demand_forecasts":
        add_error(
            errors,
            F.col("store_id").isNull(),
            "NULL_STORE_ID",
        )

        add_error(
            errors,
            F.col("product_id").isNull(),
            "NULL_PRODUCT_ID",
        )

        add_error(
            errors,
            F.col(
                "forecast_quantity"
            ).isNull()
            | (
                F.col(
                    "forecast_quantity"
                ) < 0
            ),
            "INVALID_FORECAST_QUANTITY",
        )

        add_error(
            errors,
            F.col("forecast_date").isNull(),
            "INVALID_FORECAST_DATE",
        )

    return (
        df
        .withColumn(
            "_dq_errors_raw",
            F.array(
                *errors
            ),
        )
        .withColumn(
            "_dq_errors",
            F.filter(
                F.col("_dq_errors_raw"),
                lambda item: item.isNotNull(),
            ),
        )
        .drop(
            "_dq_errors_raw"
        )
        .withColumn(
            "_dq_validated_at",
            F.current_timestamp(),
        )
    )


def write_delta(
    df: DataFrame,
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


def process_table(
    spark: SparkSession,
    table_name: str,
    config: dict,
    bronze_root: str,
    silver_root: str,
    quarantine_root: str,
) -> None:
    print("")
    print(
        f"========== {table_name} =========="
    )

    bronze_path = (
        f"{bronze_root}/current/{table_name}"
    )

    silver_path = (
        f"{silver_root}/{table_name}"
    )

    quarantine_path = (
        f"{quarantine_root}/{table_name}"
    )

    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path,
    ):
        raise RuntimeError(
            f"Bronze Current não encontrada: "
            f"{table_name}"
        )

    bronze = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    bronze_count = bronze.count()

    print(
        f"Bronze Current: {bronze_count}"
    )

    transformed = transform_types(
        bronze=bronze,
        config=config,
    )

    validated = apply_quality_rules(
        df=transformed,
        table_name=table_name,
        primary_key=config[
            "primary_key"
        ],
    )

    valid = (
        validated
        .filter(
            F.size(
                F.col("_dq_errors")
            ) == 0
        )
        .drop(
            "_dq_errors"
        )
    )

    invalid = (
        validated
        .filter(
            F.size(
                F.col("_dq_errors")
            ) > 0
        )
    )

    valid_count = valid.count()
    invalid_count = invalid.count()

    if (
        valid_count + invalid_count
        != bronze_count
    ):
        raise RuntimeError(
            f"{table_name}: inconsistência "
            "na separação de Data Quality."
        )

    print(
        f"Silver válidos: {valid_count}"
    )

    print(
        f"Quarantine: {invalid_count}"
    )

    write_delta(
        df=valid,
        path=silver_path,
    )

    write_delta(
        df=invalid,
        path=quarantine_path,
    )

    print(
        "Processamento concluído."
    )


def main() -> None:
    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    bronze_root = required_env(
        "BRONZE_S3_PATH"
    )

    silver_root = required_env(
        "SILVER_S3_PATH"
    )

    quarantine_root = required_env(
        "QUARANTINE_S3_PATH"
    )

    try:
        for table_name, config in (
            TABLE_CONFIG.items()
        ):
            process_table(
                spark=spark,
                table_name=table_name,
                config=config,
                bronze_root=bronze_root,
                silver_root=silver_root,
                quarantine_root=quarantine_root,
            )

        print("")
        print(
            "CAMADA SILVER CONCLUÍDA "
            "COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()