import os
from datetime import date
from functools import reduce

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bronze_to_silver import (
    TABLE_CONFIG,
    apply_quality_rules,
    transform_types,
)


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Variável {name} não encontrada.")

    return value


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DemandFlowSilverValidation")
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


def schema_signature(dataframe: DataFrame) -> list[tuple[str, str]]:
    return [
        (field.name, field.dataType.simpleString())
        for field in dataframe.schema.fields
    ]


def delta_version_and_require_cdf(
    delta_table: DeltaTable,
    table_name: str,
    layer_name: str,
) -> int:
    detail = delta_table.detail().select("properties").first()
    properties = detail["properties"] or {}

    if str(properties.get("delta.enableChangeDataFeed", "false")).lower() != "true":
        raise RuntimeError(
            f"{table_name}: Change Data Feed não está ativo em {layer_name}."
        )

    history = delta_table.history(1).select("version").first()
    return int(history["version"])


def exact_mismatch_count(expected: DataFrame, actual: DataFrame) -> int:
    comparison_columns = [
        column_name
        for column_name in expected.columns
        if column_name != "_dq_validated_at"
    ]
    expected_projection = expected.select(*comparison_columns)
    actual_projection = actual.select(*comparison_columns)
    differences = expected_projection.exceptAll(actual_projection).unionByName(
        actual_projection.exceptAll(expected_projection)
    )
    return differences.count()


def assert_unique_keys(
    silver: DataFrame,
    table_name: str,
    config: dict,
) -> None:
    expressions = [F.count(F.lit(1)).alias("total")]
    key_sets = [config["primary_key"]] + config.get("unique_keys", [])

    for index, key_columns in enumerate(key_sets):
        expressions.append(
            F.countDistinct(*key_columns).alias(f"distinct_{index}")
        )

    row = silver.agg(*expressions).first()
    total = int(row["total"])

    for index, key_columns in enumerate(key_sets):
        distinct = int(row[f"distinct_{index}"])
        if distinct != total:
            raise RuntimeError(
                f"{table_name}: chave não única na Silver: "
                + ",".join(key_columns)
            )


def validate_table(
    spark: SparkSession,
    table_name: str,
    config: dict,
    bronze_root: str,
    silver_root: str,
    quarantine_root: str,
) -> tuple[DataFrame, DataFrame, DataFrame, dict]:
    bronze_path = f"{bronze_root}/current/{table_name}"
    silver_path = f"{silver_root}/{table_name}"
    quarantine_path = f"{quarantine_root}/{table_name}"

    for path, layer in (
        (bronze_path, "Bronze"),
        (silver_path, "Silver"),
        (quarantine_path, "Quarantine"),
    ):
        if not DeltaTable.isDeltaTable(spark, path):
            raise RuntimeError(f"{layer} ausente para {table_name}.")

    bronze = (
        spark.read.format("delta")
        .load(bronze_path)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    silver = (
        spark.read.format("delta")
        .load(silver_path)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    quarantine = (
        spark.read.format("delta")
        .load(quarantine_path)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    # Materialize and truncate the comprehensive rule plan before it is used
    # by both sides of each exact comparison.  Without this boundary Catalyst
    # repeatedly expands the complete quality-expression tree, which is
    # disproportionately expensive for the small local dataset.
    validated = apply_quality_rules(
        df=transform_types(bronze=bronze, config=config),
        table_name=table_name,
        config=config,
    ).localCheckpoint(eager=True)
    expected_silver = validated.filter(F.size("_dq_errors") == 0).drop(
        "_dq_errors",
        "_dq_original_record",
    )
    expected_quarantine = validated.filter(F.size("_dq_errors") > 0)

    try:
        if schema_signature(silver) != schema_signature(expected_silver):
            raise RuntimeError(f"{table_name}: schema Silver divergente.")
        if schema_signature(quarantine) != schema_signature(expected_quarantine):
            raise RuntimeError(f"{table_name}: schema Quarantine divergente.")

        silver_summary = silver.agg(
            F.count(F.lit(1)).alias("count"),
            F.countDistinct("_event_id").alias("distinct_event_ids"),
            F.sum(
                F.when(F.col("_dq_validated_at").isNull(), 1).otherwise(0)
            ).alias("null_validation_time"),
        ).first()
        quarantine_summary = quarantine.agg(
            F.count(F.lit(1)).alias("count"),
            F.countDistinct("_event_id").alias("distinct_event_ids"),
            F.sum(
                F.when(
                    F.col("_dq_validated_at").isNull()
                    | F.col("_dq_original_record").isNull()
                    | (F.size("_dq_errors") == 0),
                    1,
                ).otherwise(0)
            ).alias("invalid_dq_metadata"),
        ).first()

        silver_count = int(silver_summary["count"])
        quarantine_count = int(quarantine_summary["count"])
        bronze_count = bronze.count()

        if int(silver_summary["distinct_event_ids"]) != silver_count:
            raise RuntimeError(f"{table_name}: _event_id inválido na Silver.")
        if int(quarantine_summary["distinct_event_ids"]) != quarantine_count:
            raise RuntimeError(f"{table_name}: _event_id inválido na Quarantine.")
        if int(silver_summary["null_validation_time"] or 0) != 0:
            raise RuntimeError(f"{table_name}: validação sem timestamp na Silver.")
        if int(quarantine_summary["invalid_dq_metadata"] or 0) != 0:
            raise RuntimeError(
                f"{table_name}: metadados de diagnóstico inválidos na Quarantine."
            )

        silver_mismatch = exact_mismatch_count(expected_silver, silver)
        quarantine_mismatch = exact_mismatch_count(
            expected_quarantine,
            quarantine,
        )
        if silver_mismatch or quarantine_mismatch:
            raise RuntimeError(
                f"{table_name}: saída Silver divergente da classificação "
                f"(silver={silver_mismatch}, quarantine={quarantine_mismatch})."
            )

        overlap = silver.select("_event_id").join(
            quarantine.select("_event_id"),
            on="_event_id",
            how="inner",
        ).count()
        if overlap:
            raise RuntimeError(
                f"{table_name}: {overlap} eventos aparecem nas duas saídas."
            )
        if silver_count + quarantine_count != bronze_count:
            raise RuntimeError(
                f"{table_name}: Silver + Quarantine diverge da Bronze."
            )

        assert_unique_keys(silver, table_name, config)

        silver_version = delta_version_and_require_cdf(
            DeltaTable.forPath(spark, silver_path),
            table_name,
            "Silver",
        )
        quarantine_version = delta_version_and_require_cdf(
            DeltaTable.forPath(spark, quarantine_path),
            table_name,
            "Quarantine",
        )

        metrics = {
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "quarantine_count": quarantine_count,
            "silver_version": silver_version,
            "quarantine_version": quarantine_version,
        }
        return bronze, silver, quarantine, metrics
    finally:
        validated.unpersist()


def validate_foreign_keys(silver_tables: dict[str, DataFrame]) -> None:
    relationships = [
        ("orders", "stores", ["store_id"]),
        ("order_items", "orders", ["order_id"]),
        ("order_items", "products", ["product_id"]),
        ("inventory", "stores", ["store_id"]),
        ("inventory", "products", ["product_id"]),
        ("inventory_movements", "stores", ["store_id"]),
        ("inventory_movements", "products", ["product_id"]),
        ("demand_forecasts", "stores", ["store_id"]),
        ("demand_forecasts", "products", ["product_id"]),
    ]
    orphan_frames = []

    for child_name, parent_name, keys in relationships:
        child = silver_tables[child_name]
        parent_keys = silver_tables[parent_name].select(*keys).dropDuplicates()
        orphan_frames.append(
            child.join(parent_keys, on=keys, how="left_anti").select(
                F.lit(f"{child_name}->{parent_name}").alias("relationship")
            )
        )

    orphan_count = reduce(
        lambda left, right: left.unionByName(right),
        orphan_frames,
    ).count()
    if orphan_count:
        raise RuntimeError(
            f"Foram encontradas {orphan_count} referências órfãs na Silver."
        )


def errors_for_probe(
    dataframe: DataFrame,
    table_name: str,
) -> list[list[str]]:
    config = TABLE_CONFIG[table_name]
    return [
        list(row["_dq_errors"])
        for row in (
            apply_quality_rules(
                df=transform_types(dataframe, config),
                table_name=table_name,
                config=config,
            )
            .select("_dq_errors")
            .collect()
        )
    ]


def validate_rule_probes(bronze_tables: dict[str, DataFrame]) -> int:
    invalid_price = bronze_tables["products"].limit(1).withColumn(
        "unit_price",
        F.lit("not-a-decimal"),
    )
    price_errors = errors_for_probe(invalid_price, "products")[0]
    if "INVALID_CAST:unit_price:decimal(12,2)" not in price_errors:
        raise RuntimeError("Probe de cast decimal inválido não foi rejeitado.")

    invalid_status = bronze_tables["orders"].limit(1).withColumn(
        "order_status",
        F.lit("UNKNOWN"),
    )
    status_errors = errors_for_probe(invalid_status, "orders")[0]
    if "INVALID_VALUE:order_status" not in status_errors:
        raise RuntimeError("Probe de status inválido não foi rejeitado.")

    movement_base = bronze_tables["inventory_movements"].limit(1)
    valid_out = (
        movement_base.withColumn("movement_type", F.lit("OUT"))
        .withColumn("quantity", F.lit(-1))
    )
    invalid_out = (
        movement_base.withColumn("movement_type", F.lit("OUT"))
        .withColumn("quantity", F.lit(1))
    )
    valid_out_errors = errors_for_probe(valid_out, "inventory_movements")[0]
    invalid_out_errors = errors_for_probe(invalid_out, "inventory_movements")[0]
    if valid_out_errors:
        raise RuntimeError("Probe OUT negativo válido foi rejeitado.")
    if "MOVEMENT_SIGN_MISMATCH" not in invalid_out_errors:
        raise RuntimeError("Probe OUT positivo inválido foi aceito.")

    null_timestamp = bronze_tables["stores"].limit(1).withColumn(
        "created_at",
        F.lit(None).cast("string"),
    )
    timestamp_errors = errors_for_probe(null_timestamp, "stores")[0]
    if "NULL_REQUIRED_COLUMN:created_at" not in timestamp_errors:
        raise RuntimeError("Probe de timestamp obrigatório nulo foi aceito.")

    epoch_dates = (
        bronze_tables["promotions"].limit(1)
        .withColumn("start_date", F.lit("0"))
        .withColumn("end_date", F.lit("1"))
    )
    config = TABLE_CONFIG["promotions"]
    parsed_dates = transform_types(epoch_dates, config).select(
        "start_date",
        "end_date",
    ).first()
    if parsed_dates["start_date"] != date(1970, 1, 1):
        raise RuntimeError("Data Debezium em epoch-day não foi convertida.")
    if parsed_dates["end_date"] != date(1970, 1, 2):
        raise RuntimeError("Data Debezium em epoch-day perdeu precisão.")

    return 6


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_tables = {}
    silver_tables = {}
    quarantine_tables = {}

    try:
        bronze_root = required_env("BRONZE_S3_PATH")
        silver_root = required_env("SILVER_S3_PATH")
        quarantine_root = required_env("QUARANTINE_S3_PATH")

        total_bronze = 0
        total_silver = 0
        total_quarantine = 0

        for table_name, config in TABLE_CONFIG.items():
            print("")
            print(f"========== {table_name} ==========")

            bronze, silver, quarantine, metrics = validate_table(
                spark=spark,
                table_name=table_name,
                config=config,
                bronze_root=bronze_root,
                silver_root=silver_root,
                quarantine_root=quarantine_root,
            )
            bronze_tables[table_name] = bronze
            silver_tables[table_name] = silver
            quarantine_tables[table_name] = quarantine

            total_bronze += metrics["bronze_count"]
            total_silver += metrics["silver_count"]
            total_quarantine += metrics["quarantine_count"]

            print(
                f"SILVER_BRONZE_COUNT_{table_name}="
                f"{metrics['bronze_count']}"
            )
            print(
                f"SILVER_VALID_COUNT_{table_name}="
                f"{metrics['silver_count']}"
            )
            print(
                f"SILVER_QUARANTINE_COUNT_{table_name}="
                f"{metrics['quarantine_count']}"
            )
            print(
                f"SILVER_VERSION_{table_name}="
                f"{metrics['silver_version']}"
            )
            print(
                f"QUARANTINE_VERSION_{table_name}="
                f"{metrics['quarantine_version']}"
            )
            print("Validação exata OK.")

        validate_foreign_keys(silver_tables)
        probes_passed = validate_rule_probes(bronze_tables)

        print("")
        print("======================================")
        print("RESUMO DA VALIDAÇÃO SILVER")
        print("======================================")
        print(f"SILVER_TABLE_COUNT={len(TABLE_CONFIG)}")
        print(f"SILVER_BRONZE_COUNT={total_bronze}")
        print(f"SILVER_VALID_COUNT={total_silver}")
        print(f"SILVER_QUARANTINE_COUNT={total_quarantine}")
        print("SILVER_EXACT_MISMATCH_COUNT=0")
        print("SILVER_ORPHAN_COUNT=0")
        print(f"SILVER_RULE_PROBES_PASSED={probes_passed}")
        print("SILVER_CDF_ENABLED=true")
        print("VALIDAÇÃO SILVER CONCLUÍDA COM SUCESSO.")
    finally:
        for dataframe in bronze_tables.values():
            dataframe.unpersist()
        for dataframe in silver_tables.values():
            dataframe.unpersist()
        for dataframe in quarantine_tables.values():
            dataframe.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
