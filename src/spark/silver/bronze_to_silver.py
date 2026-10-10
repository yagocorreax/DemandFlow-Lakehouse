import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


TABLE_CONFIG = {
    "stores": {
        "primary_key": ["store_id"],
        "unique_keys": [["store_code"]],
        "non_empty": ["store_code", "store_name", "city", "state"],
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
        "unique_keys": [["sku"]],
        "non_empty": ["sku", "product_name", "category"],
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
        "non_empty": ["promotion_name"],
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
        "non_empty": ["order_status"],
        "allowed_values": {
            "order_status": [
                "CREATED",
                "PAID",
                "CANCELLED",
                "SHIPPED",
                "DELIVERED",
            ],
        },
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
        "unique_keys": [["order_id", "product_id"]],
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
        "primary_key": ["store_id", "product_id"],
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
        "nullable": ["reason"],
        "non_empty": ["movement_type"],
        "allowed_values": {
            "movement_type": ["IN", "OUT", "ADJUSTMENT", "RETURN"],
        },
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
        "unique_keys": [
            [
                "store_id",
                "product_id",
                "forecast_date",
                "model_version",
            ]
        ],
        "non_empty": ["model_version"],
        "types": {
            "forecast_id": "long",
            "store_id": "int",
            "product_id": "int",
            "forecast_date": "date",
            "forecast_quantity": "decimal(12,2)",
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
        raise RuntimeError(f"Variável {name} não encontrada.")

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
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def require_bronze_contract(
    bronze: DataFrame,
    table_name: str,
    config: dict,
) -> None:
    required = set(config["types"]) | set(METADATA_COLUMNS)
    missing = required - set(bronze.columns)

    if missing:
        raise RuntimeError(
            f"{table_name}: Bronze Current não contém: "
            + ", ".join(sorted(missing))
        )


def build_error_array(errors: list):
    return F.filter(
        F.array(*errors),
        lambda item: item.isNotNull(),
    )


def cast_value(column_name: str, data_type: str):
    source = F.col(column_name)

    if data_type == "date":
        text = F.trim(source.cast("string"))
        return F.when(
            text.rlike(r"^[+-]?[0-9]+$"),
            F.date_add(
                F.to_date(F.lit("1970-01-01")),
                text.cast("int"),
            ),
        ).otherwise(F.to_date(text))

    return source.cast(data_type)


def transform_types(
    bronze: DataFrame,
    config: dict,
) -> DataFrame:
    business_columns = list(config["types"])
    casted_columns = []
    cast_errors = []

    for column_name, data_type in config["types"].items():
        converted = cast_value(column_name, data_type)
        casted_columns.append(converted.alias(column_name))
        cast_errors.append(
            F.when(
                F.col(column_name).isNotNull() & converted.isNull(),
                F.lit(f"INVALID_CAST:{column_name}:{data_type}"),
            )
        )

    return bronze.select(
        *casted_columns,
        *[F.col(column_name) for column_name in METADATA_COLUMNS],
        F.to_json(
            F.struct(*[F.col(column_name) for column_name in business_columns])
        ).alias("_dq_original_record"),
        build_error_array(cast_errors).alias("_dq_cast_errors"),
    )


def add_error(errors: list, condition, message: str) -> None:
    errors.append(F.when(condition, F.lit(message)))


def apply_quality_rules(
    df: DataFrame,
    table_name: str,
    config: dict,
) -> DataFrame:
    errors = []
    nullable = set(config.get("nullable", []))

    for column_name in config["types"]:
        if column_name not in nullable:
            add_error(
                errors,
                F.col(column_name).isNull(),
                f"NULL_REQUIRED_COLUMN:{column_name}",
            )

    for column_name in config.get("non_empty", []):
        add_error(
            errors,
            F.col(column_name).isNotNull()
            & (F.trim(F.col(column_name)) == ""),
            f"EMPTY_REQUIRED_STRING:{column_name}",
        )

    for column_name, allowed in config.get("allowed_values", {}).items():
        add_error(
            errors,
            F.col(column_name).isNotNull()
            & (~F.col(column_name).isin(*allowed)),
            f"INVALID_VALUE:{column_name}",
        )

    keys_to_check = [config["primary_key"]] + config.get("unique_keys", [])
    for key_columns in keys_to_check:
        duplicate_window = Window.partitionBy(*key_columns)
        add_error(
            errors,
            F.count(F.lit(1)).over(duplicate_window) > 1,
            "DUPLICATE_KEY:" + ",".join(key_columns),
        )

    if table_name == "stores":
        add_error(
            errors,
            F.col("state").isNotNull()
            & (~F.col("state").rlike(r"^[A-Z]{2}$")),
            "INVALID_STATE",
        )
    elif table_name == "products":
        add_error(
            errors,
            F.col("unit_price").isNotNull() & (F.col("unit_price") < 0),
            "NEGATIVE_UNIT_PRICE",
        )
    elif table_name == "promotions":
        add_error(
            errors,
            F.col("discount_percentage").isNotNull()
            & (
                (F.col("discount_percentage") < 0)
                | (F.col("discount_percentage") > 100)
            ),
            "INVALID_DISCOUNT_PERCENTAGE",
        )
        add_error(
            errors,
            F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
            & (F.col("end_date") < F.col("start_date")),
            "END_DATE_BEFORE_START_DATE",
        )
    elif table_name == "orders":
        add_error(
            errors,
            F.col("total_amount").isNotNull() & (F.col("total_amount") < 0),
            "NEGATIVE_TOTAL_AMOUNT",
        )
    elif table_name == "order_items":
        add_error(
            errors,
            F.col("quantity").isNotNull() & (F.col("quantity") <= 0),
            "NON_POSITIVE_QUANTITY",
        )
        add_error(
            errors,
            F.col("unit_price").isNotNull() & (F.col("unit_price") < 0),
            "NEGATIVE_UNIT_PRICE",
        )
        add_error(
            errors,
            F.col("discount_amount").isNotNull()
            & (F.col("discount_amount") < 0),
            "NEGATIVE_DISCOUNT_AMOUNT",
        )
    elif table_name == "inventory":
        add_error(
            errors,
            F.col("stock_quantity").isNotNull()
            & (F.col("stock_quantity") < 0),
            "NEGATIVE_STOCK_QUANTITY",
        )
        add_error(
            errors,
            F.col("safety_stock").isNotNull() & (F.col("safety_stock") < 0),
            "NEGATIVE_SAFETY_STOCK",
        )
    elif table_name == "inventory_movements":
        add_error(
            errors,
            F.col("quantity").isNotNull() & (F.col("quantity") == 0),
            "ZERO_MOVEMENT_QUANTITY",
        )
        add_error(
            errors,
            (
                (F.col("movement_type") == "OUT")
                & (F.col("quantity") >= 0)
            )
            | (
                F.col("movement_type").isin("IN", "RETURN")
                & (F.col("quantity") <= 0)
            ),
            "MOVEMENT_SIGN_MISMATCH",
        )
    elif table_name == "demand_forecasts":
        add_error(
            errors,
            F.col("forecast_quantity").isNotNull()
            & (F.col("forecast_quantity") < 0),
            "NEGATIVE_FORECAST_QUANTITY",
        )

    return (
        df.withColumn(
            "_dq_errors",
            F.array_distinct(
                F.concat(
                    F.col("_dq_cast_errors"),
                    build_error_array(errors),
                )
            ),
        )
        .drop("_dq_cast_errors")
        .withColumn("_dq_validated_at", F.current_timestamp())
    )


def create_delta(df: DataFrame, path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .save(path)
    )


def has_data_changes(source: DataFrame, target: DataFrame) -> bool:
    comparison_columns = [
        column_name
        for column_name in source.columns
        if column_name != "_dq_validated_at"
    ]
    source_projection = source.select(*comparison_columns)
    target_projection = target.select(*comparison_columns)
    differences = source_projection.exceptAll(target_projection).unionByName(
        target_projection.exceptAll(source_projection)
    )
    return differences.limit(1).count() > 0


def sync_delta(
    spark: SparkSession,
    source: DataFrame,
    path: str,
) -> bool:
    if not DeltaTable.isDeltaTable(spark, path):
        create_delta(source, path)
        return True

    target_df = spark.read.format("delta").load(path)
    source_schema = [
        (field.name, field.dataType.simpleString())
        for field in source.schema.fields
    ]
    target_schema = [
        (field.name, field.dataType.simpleString())
        for field in target_df.schema.fields
    ]
    if target_schema != source_schema:
        create_delta(source, path)
        return True

    if not has_data_changes(source, target_df):
        return False

    comparison_columns = [
        column_name
        for column_name in source.columns
        if column_name not in ("_dq_validated_at", "_event_id")
    ]
    changed_condition = " OR ".join(
        [
            f"NOT (target.`{column_name}` <=> source.`{column_name}`)"
            for column_name in comparison_columns
        ]
    )
    target = DeltaTable.forPath(spark, path)

    (
        target.alias("target")
        .merge(
            source.alias("source"),
            "target.`_event_id` <=> source.`_event_id`",
        )
        .whenMatchedUpdateAll(condition=changed_condition)
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    return True


def process_table(
    spark: SparkSession,
    table_name: str,
    config: dict,
    bronze_root: str,
    silver_root: str,
    quarantine_root: str,
) -> None:
    print("")
    print(f"========== {table_name} ==========")

    bronze_path = f"{bronze_root}/current/{table_name}"
    silver_path = f"{silver_root}/{table_name}"
    quarantine_path = f"{quarantine_root}/{table_name}"

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        raise RuntimeError(f"Bronze Current não encontrada: {table_name}")

    bronze = spark.read.format("delta").load(bronze_path)
    require_bronze_contract(bronze, table_name, config)

    # The quality expressions are intentionally comprehensive and create a
    # large logical plan.  Truncate that lineage once, after materializing the
    # result, so the two-sided idempotency comparison does not ask Catalyst to
    # expand the full rule tree multiple times.
    validated = apply_quality_rules(
        df=transform_types(bronze=bronze, config=config),
        table_name=table_name,
        config=config,
    ).localCheckpoint(eager=True)

    try:
        summary = validated.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.size("_dq_errors") == 0, 1).otherwise(0)).alias(
                "valid"
            ),
            F.sum(F.when(F.size("_dq_errors") > 0, 1).otherwise(0)).alias(
                "invalid"
            ),
            F.countDistinct("_event_id").alias("distinct_event_ids"),
        ).first()

        bronze_count = int(summary["total"])
        valid_count = int(summary["valid"] or 0)
        invalid_count = int(summary["invalid"] or 0)
        distinct_event_ids = int(summary["distinct_event_ids"])

        if valid_count + invalid_count != bronze_count:
            raise RuntimeError(
                f"{table_name}: inconsistência na separação de Data Quality."
            )
        if distinct_event_ids != bronze_count:
            raise RuntimeError(
                f"{table_name}: _event_id nulo ou duplicado na Bronze."
            )

        valid = (
            validated.filter(F.size("_dq_errors") == 0)
            .drop("_dq_errors", "_dq_original_record")
        )
        invalid = validated.filter(F.size("_dq_errors") > 0)

        silver_changed = sync_delta(spark, valid, silver_path)
        quarantine_changed = sync_delta(spark, invalid, quarantine_path)

        print(f"SILVER_BRONZE_COUNT_{table_name}={bronze_count}")
        print(f"SILVER_VALID_COUNT_{table_name}={valid_count}")
        print(f"SILVER_QUARANTINE_COUNT_{table_name}={invalid_count}")
        print(
            f"SILVER_WRITE_APPLIED_{table_name}="
            f"{str(silver_changed).lower()}"
        )
        print(
            f"QUARANTINE_WRITE_APPLIED_{table_name}="
            f"{str(quarantine_changed).lower()}"
        )
        print("Processamento concluído.")
    finally:
        validated.unpersist()


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_root = required_env("BRONZE_S3_PATH")
        silver_root = required_env("SILVER_S3_PATH")
        quarantine_root = required_env("QUARANTINE_S3_PATH")

        for table_name, config in TABLE_CONFIG.items():
            process_table(
                spark=spark,
                table_name=table_name,
                config=config,
                bronze_root=bronze_root,
                silver_root=silver_root,
                quarantine_root=quarantine_root,
            )

        print("")
        print("CAMADA SILVER CONCLUÍDA COM SUCESSO.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
