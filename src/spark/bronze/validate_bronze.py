import json
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


CONFIG_PATH = Path("/opt/demandflow/config/tables.json")

EVENT_REQUIRED_COLUMNS = {
    "event_id",
    "source_table",
    "load_type",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "operation",
    "source_lsn",
    "source_ts_ms",
    "before",
    "after",
}

CURRENT_METADATA_COLUMNS = [
    "_operation",
    "_event_id",
    "_source_lsn",
    "_source_ts_ms",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_ingested_at",
]

RAW_COMPARISON_COLUMNS = [
    "source_table",
    "load_type",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "operation",
]

MINIMUM_INITIAL_SNAPSHOT_COUNTS = {
    "stores": 3,
    "products": 6,
    "promotions": 2,
    "inventory": 18,
    "demand_forecasts": 252,
}


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Variável {name} não encontrada.")

    return value


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DemandFlowBronzeValidation")
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
        .getOrCreate()
    )


def require_columns(
    dataframe: DataFrame,
    required: set[str],
    description: str,
) -> None:
    missing = required - set(dataframe.columns)

    if missing:
        raise RuntimeError(
            f"{description}: colunas ausentes: "
            + ", ".join(sorted(missing))
        )


def null_condition(columns: list[str]):
    condition = None

    for column_name in columns:
        current = F.col(column_name).isNull()
        condition = current if condition is None else condition | current

    return condition


def nested_null_condition(parent: str, columns: list[str]):
    condition = F.col(parent).isNull()

    for column_name in columns:
        condition = condition | F.col(f"{parent}.{column_name}").isNull()

    return condition


def require_business_structs(
    events: DataFrame,
    table_name: str,
    business_columns: list[str],
) -> None:
    for image_name in ("before", "after"):
        data_type = events.schema[image_name].dataType

        if not isinstance(data_type, T.StructType):
            raise RuntimeError(
                f"{table_name}: {image_name} não é uma estrutura válida."
            )

        missing = set(business_columns) - set(data_type.fieldNames())
        if missing:
            raise RuntimeError(
                f"{table_name}: {image_name} não contém: "
                + ", ".join(sorted(missing))
            )


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


def validate_raw_contract(
    raw: DataFrame,
    configured_tables: list[str],
) -> int:
    require_columns(
        raw,
        {
            "event_id",
            "kafka_value",
            "source_table",
            "load_type",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "operation",
        },
        "Raw",
    )

    raw_count = raw.count()
    if raw_count == 0:
        raise RuntimeError("A camada Raw está vazia.")

    invalid_metadata = raw.filter(
        F.col("event_id").isNull()
        | (F.length(F.trim(F.col("event_id"))) == 0)
        | F.col("source_table").isNull()
        | F.col("operation").isNull()
        | (~F.col("operation").isin("r", "c", "u", "d"))
        | F.col("load_type").isNull()
        | (~F.col("load_type").isin("full_load", "cdc"))
        | F.col("kafka_value").isNull()
    ).count()
    if invalid_metadata:
        raise RuntimeError(
            f"Raw possui {invalid_metadata} eventos com contrato inválido."
        )

    duplicate_ids = (
        raw.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_ids:
        raise RuntimeError(
            f"Raw possui {duplicate_ids} event_ids duplicados."
        )

    raw_tables = {
        row["source_table"]
        for row in raw.select("source_table").distinct().collect()
    }
    configured = set(configured_tables)
    if raw_tables != configured:
        missing = sorted(configured - raw_tables)
        extra = sorted(raw_tables - configured)
        raise RuntimeError(
            "O conjunto de tabelas da Raw diverge da configuração. "
            f"Ausentes={missing}; extras={extra}."
        )

    snapshot_counts = {
        row["source_table"]: row["count"]
        for row in (
            raw.filter(F.col("operation") == "r")
            .groupBy("source_table")
            .count()
            .collect()
        )
    }
    insufficient = [
        f"{table_name}={snapshot_counts.get(table_name, 0)}/{minimum}"
        for table_name, minimum in MINIMUM_INITIAL_SNAPSHOT_COUNTS.items()
        if snapshot_counts.get(table_name, 0) < minimum
    ]
    if insufficient:
        raise RuntimeError(
            "Snapshot inicial Raw incompleto: " + ", ".join(insufficient)
        )

    return raw_count


def compare_event_metadata(
    raw_table: DataFrame,
    events: DataFrame,
) -> int:
    raw_projection = raw_table.select(
        "event_id",
        *[
            F.col(column_name).alias(f"raw_{column_name}")
            for column_name in RAW_COMPARISON_COLUMNS
        ],
    )
    event_projection = events.select(
        "event_id",
        *[
            F.col(column_name).alias(f"event_{column_name}")
            for column_name in RAW_COMPARISON_COLUMNS
        ],
    )
    joined = raw_projection.join(
        event_projection,
        on="event_id",
        how="inner",
    )

    mismatch = None
    for column_name in RAW_COMPARISON_COLUMNS:
        different = ~F.col(f"raw_{column_name}").eqNullSafe(
            F.col(f"event_{column_name}")
        )
        mismatch = different if mismatch is None else mismatch | different

    return joined.filter(mismatch).count()


def compare_event_payload(
    raw_table: DataFrame,
    events: DataFrame,
) -> int:
    business_schema = events.schema["before"].dataType
    source_schema = T.StructType(
        [
            T.StructField("lsn", T.LongType(), True),
            T.StructField("ts_ms", T.LongType(), True),
        ]
    )
    envelope_schema = T.StructType(
        [
            T.StructField("before", business_schema, True),
            T.StructField("after", business_schema, True),
            T.StructField("source", source_schema, True),
            T.StructField("op", T.StringType(), True),
            T.StructField("ts_ms", T.LongType(), True),
        ]
    )
    wrapped_schema = T.StructType(
        [T.StructField("payload", envelope_schema, True)]
    )
    parsed = (
        raw_table.select(
            "event_id",
            F.from_json("kafka_value", envelope_schema).alias("direct"),
            F.from_json("kafka_value", wrapped_schema).alias("wrapped"),
        )
        .select(
            "event_id",
            F.coalesce("direct.before", "wrapped.payload.before").alias(
                "expected_before"
            ),
            F.coalesce("direct.after", "wrapped.payload.after").alias(
                "expected_after"
            ),
            F.coalesce("direct.source.lsn", "wrapped.payload.source.lsn").alias(
                "expected_source_lsn"
            ),
            F.coalesce(
                "direct.source.ts_ms",
                "wrapped.payload.source.ts_ms",
            ).alias("expected_source_ts_ms"),
        )
    )
    actual = events.select(
        "event_id",
        F.col("before").alias("actual_before"),
        F.col("after").alias("actual_after"),
        F.col("source_lsn").alias("actual_source_lsn"),
        F.col("source_ts_ms").alias("actual_source_ts_ms"),
    )
    joined = parsed.join(actual, on="event_id", how="inner")

    mismatch = (
        ~F.col("expected_before").eqNullSafe(F.col("actual_before"))
        | ~F.col("expected_after").eqNullSafe(F.col("actual_after"))
        | ~F.col("expected_source_lsn").eqNullSafe(
            F.col("actual_source_lsn")
        )
        | ~F.col("expected_source_ts_ms").eqNullSafe(
            F.col("actual_source_ts_ms")
        )
    )
    return joined.filter(mismatch).count()


def validate_events(
    spark: SparkSession,
    raw_table: DataFrame,
    table_name: str,
    events_path: str,
    primary_key: list[str],
    business_columns: list[str],
) -> tuple[DataFrame, int, int]:
    if not DeltaTable.isDeltaTable(spark, events_path):
        raise RuntimeError(f"Bronze Events não existe para {table_name}.")

    delta_table = DeltaTable.forPath(spark, events_path)
    events = spark.read.format("delta").load(events_path).cache()

    require_columns(events, EVENT_REQUIRED_COLUMNS, f"Events/{table_name}")
    require_business_structs(events, table_name, business_columns)

    events_count = events.count()
    raw_count = raw_table.count()
    duplicate_events = (
        events.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_events:
        raise RuntimeError(
            f"{table_name}: {duplicate_events} event_ids duplicados."
        )

    invalid_metadata = events.filter(
        F.col("event_id").isNull()
        | (F.length(F.trim(F.col("event_id"))) == 0)
        | F.col("source_table").isNull()
        | (F.col("source_table") != table_name)
        | F.col("operation").isNull()
        | (~F.col("operation").isin("r", "c", "u", "d"))
        | F.col("load_type").isNull()
        | (~F.col("load_type").isin("full_load", "cdc"))
        | (
            F.col("operation").isin("r", "c", "u")
            & nested_null_condition("after", primary_key)
        )
        | (
            (F.col("operation") == "d")
            & nested_null_condition("before", primary_key)
        )
    ).count()
    if invalid_metadata:
        raise RuntimeError(
            f"{table_name}: {invalid_metadata} eventos inválidos ou sem PK."
        )

    raw_ids = raw_table.select("event_id")
    event_ids = events.select("event_id")
    missing_from_events = raw_ids.join(
        event_ids,
        on="event_id",
        how="left_anti",
    ).count()
    extra_in_events = event_ids.join(
        raw_ids,
        on="event_id",
        how="left_anti",
    ).count()
    metadata_mismatches = compare_event_metadata(raw_table, events)
    payload_mismatches = compare_event_payload(raw_table, events)

    if (
        events_count != raw_count
        or missing_from_events
        or extra_in_events
        or metadata_mismatches
        or payload_mismatches
    ):
        raise RuntimeError(
            f"{table_name}: Events diverge da Raw "
            f"(raw={raw_count}, events={events_count}, "
            f"ausentes={missing_from_events}, extras={extra_in_events}, "
            f"metadados={metadata_mismatches}, payload={payload_mismatches})."
        )

    version = delta_version_and_require_cdf(
        delta_table,
        table_name,
        "Events",
    )
    return events, events_count, version


def expected_current(
    events: DataFrame,
    primary_key: list[str],
    business_columns: list[str],
) -> DataFrame:
    selected_business = [
        F.when(
            F.col("operation") == "d",
            F.col(f"before.{column_name}"),
        )
        .otherwise(F.col(f"after.{column_name}"))
        .alias(column_name)
        for column_name in business_columns
    ]
    records = events.select(
        *selected_business,
        F.col("operation").alias("_operation"),
        F.col("event_id").alias("_event_id"),
        F.col("source_lsn").alias("_source_lsn"),
        F.col("source_ts_ms").alias("_source_ts_ms"),
        F.col("kafka_partition").alias("_kafka_partition"),
        F.col("kafka_offset").alias("_kafka_offset"),
        F.col("kafka_timestamp").alias("_kafka_timestamp"),
        F.col("ingested_at").alias("_ingested_at"),
    )
    window = Window.partitionBy(*primary_key).orderBy(
        F.col("_source_lsn").desc_nulls_last(),
        F.col("_kafka_timestamp").desc_nulls_last(),
        F.col("_kafka_partition").desc(),
        F.col("_kafka_offset").desc(),
    )

    return (
        records.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .filter(F.col("_operation") != "d")
        .drop("_row_number")
    )


def validate_current(
    spark: SparkSession,
    events: DataFrame,
    table_name: str,
    current_path: str,
    primary_key: list[str],
    business_columns: list[str],
) -> tuple[int, int]:
    if not DeltaTable.isDeltaTable(spark, current_path):
        raise RuntimeError(f"Bronze Current não existe para {table_name}.")

    delta_table = DeltaTable.forPath(spark, current_path)
    current = spark.read.format("delta").load(current_path).cache()
    comparison_columns = business_columns + CURRENT_METADATA_COLUMNS

    try:
        require_columns(
            current,
            set(comparison_columns),
            f"Current/{table_name}",
        )

        current_count = current.count()
        duplicate_pk = (
            current.groupBy(*primary_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        null_pk = current.filter(null_condition(primary_key)).count()
        invalid_operations = current.filter(
            F.col("_operation").isNull()
            | (~F.col("_operation").isin("r", "c", "u"))
        ).count()

        if duplicate_pk or null_pk or invalid_operations:
            raise RuntimeError(
                f"{table_name}: Current inválida "
                f"(PKs duplicadas={duplicate_pk}, PKs nulas={null_pk}, "
                f"operações inválidas={invalid_operations})."
            )

        expected = expected_current(
            events=events,
            primary_key=primary_key,
            business_columns=business_columns,
        ).select(*comparison_columns)
        actual = current.select(*comparison_columns)
        missing_from_current = expected.exceptAll(actual).count()
        extra_in_current = actual.exceptAll(expected).count()
        mismatch_count = missing_from_current + extra_in_current

        if mismatch_count:
            raise RuntimeError(
                f"{table_name}: Current diverge do replay completo de Events "
                f"(ausentes={missing_from_current}, extras={extra_in_current})."
            )

        expected_count = expected.count()
        if current_count != expected_count:
            raise RuntimeError(
                f"{table_name}: Current possui {current_count} linhas; "
                f"o replay exige {expected_count}."
            )

        version = delta_version_and_require_cdf(
            delta_table,
            table_name,
            "Current",
        )
        return current_count, version
    finally:
        current.unpersist()


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = None
    try:
        with CONFIG_PATH.open("r", encoding="utf-8-sig") as file:
            tables = json.load(file)

        configured_tables = list(tables)
        raw = (
            spark.read.format("parquet")
            .load(required_env("RAW_S3_PATH"))
            .cache()
        )
        raw_count = validate_raw_contract(raw, configured_tables)
        bronze_root = required_env("BRONZE_S3_PATH")

        total_events = 0
        total_current = 0

        for table_name, config in tables.items():
            print("")
            print(f"========== {table_name} ==========")

            raw_table = raw.filter(F.col("source_table") == table_name)
            events_path = f"{bronze_root}/events/{table_name}"
            current_path = f"{bronze_root}/current/{table_name}"
            business_columns = list(config["columns"])

            events, events_count, events_version = validate_events(
                spark=spark,
                raw_table=raw_table,
                table_name=table_name,
                events_path=events_path,
                primary_key=config["primary_key"],
                business_columns=business_columns,
            )

            try:
                current_count, current_version = validate_current(
                    spark=spark,
                    events=events,
                    table_name=table_name,
                    current_path=current_path,
                    primary_key=config["primary_key"],
                    business_columns=business_columns,
                )
            finally:
                events.unpersist()

            total_events += events_count
            total_current += current_count

            print(f"BRONZE_EVENTS_COUNT_{table_name}={events_count}")
            print(f"BRONZE_EVENTS_VERSION_{table_name}={events_version}")
            print(f"BRONZE_CURRENT_COUNT_{table_name}={current_count}")
            print(f"BRONZE_CURRENT_VERSION_{table_name}={current_version}")
            print("Validação exata OK.")

        if total_events != raw_count:
            raise RuntimeError(
                f"Events totalizou {total_events}; Raw possui {raw_count}."
            )

        print("")
        print("======================================")
        print("RESUMO DA VALIDAÇÃO")
        print("======================================")
        print(f"BRONZE_TABLE_COUNT={len(configured_tables)}")
        print(f"BRONZE_RAW_EVENT_COUNT={raw_count}")
        print(f"BRONZE_EVENTS_COUNT={total_events}")
        print(f"BRONZE_CURRENT_COUNT={total_current}")
        print("BRONZE_MISSING_EVENTS_FROM_RAW=0")
        print("BRONZE_EXTRA_EVENTS_NOT_IN_RAW=0")
        print("BRONZE_PAYLOAD_MISMATCH_COUNT=0")
        print("BRONZE_CURRENT_MISMATCH_COUNT=0")
        print("BRONZE_CDF_ENABLED=true")
        print("VALIDAÇÃO BRONZE CONCLUÍDA COM SUCESSO.")
    finally:
        if raw is not None:
            raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
