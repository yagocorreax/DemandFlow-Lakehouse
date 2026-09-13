import json
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


CONFIG_PATH = Path(
    "/opt/demandflow/config/tables.json"
)


TYPE_MAPPING = {
    "integer": T.IntegerType(),
    "long": T.LongType(),
    "string": T.StringType(),
    "boolean": T.BooleanType(),
}


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável obrigatória {name} não encontrada."
        )

    return value


def load_config() -> dict:
    with CONFIG_PATH.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        return json.load(file)


def create_spark_session() -> SparkSession:
    endpoint = get_required_env(
        "LOCALSTACK_INTERNAL_ENDPOINT"
    )

    return (
        SparkSession.builder
        .appName("DemandFlowRawToBronze")
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
            endpoint,
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
            get_required_env("AWS_ACCESS_KEY_ID"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            get_required_env("AWS_SECRET_ACCESS_KEY"),
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


def create_business_schema(
    columns: dict,
) -> T.StructType:
    fields = []

    for name, data_type in columns.items():
        if data_type not in TYPE_MAPPING:
            raise ValueError(
                f"Tipo não suportado: {data_type}"
            )

        fields.append(
            T.StructField(
                name,
                TYPE_MAPPING[data_type],
                True,
            )
        )

    return T.StructType(fields)


def create_envelope_schema(
    business_schema: T.StructType,
) -> T.StructType:
    source_schema = T.StructType(
        [
            T.StructField(
                "lsn",
                T.LongType(),
                True,
            ),
            T.StructField(
                "ts_ms",
                T.LongType(),
                True,
            ),
        ]
    )

    return T.StructType(
        [
            T.StructField(
                "before",
                business_schema,
                True,
            ),
            T.StructField(
                "after",
                business_schema,
                True,
            ),
            T.StructField(
                "source",
                source_schema,
                True,
            ),
            T.StructField(
                "op",
                T.StringType(),
                True,
            ),
            T.StructField(
                "ts_ms",
                T.LongType(),
                True,
            ),
        ]
    )


def parse_events(
    raw: DataFrame,
    table_name: str,
    columns: dict,
) -> DataFrame:
    business_schema = create_business_schema(
        columns
    )

    envelope_schema = create_envelope_schema(
        business_schema
    )

    wrapped_schema = T.StructType(
        [
            T.StructField(
                "payload",
                envelope_schema,
                True,
            )
        ]
    )

    return (
        raw
        .filter(
            F.col("source_table") == table_name
        )

        # Formato sem envelope schema/payload
        .withColumn(
            "direct_event",
            F.from_json(
                F.col("kafka_value"),
                envelope_schema,
            ),
        )

        # Formato Kafka Connect com payload
        .withColumn(
            "wrapped_event",
            F.from_json(
                F.col("kafka_value"),
                wrapped_schema,
            ),
        )

        .withColumn(
            "operation_parsed",
            F.coalesce(
                F.col("direct_event.op"),
                F.col("wrapped_event.payload.op"),
            ),
        )

        .withColumn(
            "before_parsed",
            F.coalesce(
                F.col("direct_event.before"),
                F.col("wrapped_event.payload.before"),
            ),
        )

        .withColumn(
            "after_parsed",
            F.coalesce(
                F.col("direct_event.after"),
                F.col("wrapped_event.payload.after"),
            ),
        )

        .withColumn(
            "source_lsn_parsed",
            F.coalesce(
                F.col("direct_event.source.lsn"),
                F.col("wrapped_event.payload.source.lsn"),
            ),
        )

        .withColumn(
            "source_ts_ms_parsed",
            F.coalesce(
                F.col("direct_event.source.ts_ms"),
                F.col("wrapped_event.payload.source.ts_ms"),
            ),
        )

        .select(
            "event_id",
            "source_table",
            "load_type",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",

            F.col(
                "operation_parsed"
            ).alias(
                "operation"
            ),

            F.col(
                "source_lsn_parsed"
            ).alias(
                "source_lsn"
            ),

            F.col(
                "source_ts_ms_parsed"
            ).alias(
                "source_ts_ms"
            ),

            F.col(
                "before_parsed"
            ).alias(
                "before"
            ),

            F.col(
                "after_parsed"
            ).alias(
                "after"
            ),
        )

        .filter(
            F.col("operation").isin(
                "r",
                "c",
                "u",
                "d",
            )
        )

        .dropDuplicates(
            ["event_id"]
        )
    )


def get_new_events(
    spark: SparkSession,
    events: DataFrame,
    events_path: str,
) -> DataFrame:
    if not DeltaTable.isDeltaTable(
        spark,
        events_path,
    ):
        return events.cache()

    existing_ids = (
        spark.read
        .format("delta")
        .load(events_path)
        .select("event_id")
    )

    return (
        events.join(
            existing_ids,
            on="event_id",
            how="left_anti",
        )
        .cache()
    )


def write_bronze_events(
    spark: SparkSession,
    new_events: DataFrame,
    events_path: str,
) -> None:
    if DeltaTable.isDeltaTable(
        spark,
        events_path,
    ):
        target = DeltaTable.forPath(
            spark,
            events_path,
        )

        (
            target.alias("target")
            .merge(
                new_events.alias("source"),
                "target.event_id = source.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        return

    (
        new_events.write
        .format("delta")
        .mode("overwrite")
        .option(
            "delta.enableChangeDataFeed",
            "true",
        )
        .save(events_path)
    )


def build_current_records(
    new_events: DataFrame,
    primary_key: list[str],
    columns: dict,
) -> DataFrame:
    selected_columns = []

    for column_name in columns:
        selected_columns.append(
            F.coalesce(
                F.col(
                    f"after.{column_name}"
                ),
                F.col(
                    f"before.{column_name}"
                ),
            ).alias(
                column_name
            )
        )

    records = new_events.select(
        *selected_columns,

        F.col("operation").alias(
            "_operation"
        ),

        F.col("event_id").alias(
            "_event_id"
        ),

        F.col("source_lsn").alias(
            "_source_lsn"
        ),

        F.col("source_ts_ms").alias(
            "_source_ts_ms"
        ),

        F.col("kafka_partition").alias(
            "_kafka_partition"
        ),

        F.col("kafka_offset").alias(
            "_kafka_offset"
        ),

        F.col("kafka_timestamp").alias(
            "_kafka_timestamp"
        ),

        F.col("ingested_at").alias(
            "_ingested_at"
        ),
    )

    missing_pk = None

    for key in primary_key:
        condition = F.col(key).isNull()

        if missing_pk is None:
            missing_pk = condition
        else:
            missing_pk = (
                missing_pk | condition
            )

    invalid_count = (
        records
        .filter(missing_pk)
        .count()
    )

    if invalid_count > 0:
        raise RuntimeError(
            f"{invalid_count} eventos possuem "
            "chave primária nula."
        )

    window = (
        Window
        .partitionBy(
            *primary_key
        )
        .orderBy(
            F.col(
                "_source_lsn"
            ).desc_nulls_last(),

            F.col(
                "_kafka_timestamp"
            ).desc_nulls_last(),

            F.col(
                "_kafka_partition"
            ).desc(),

            F.col(
                "_kafka_offset"
            ).desc(),
        )
    )

    return (
        records
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )


def merge_current(
    spark: SparkSession,
    records: DataFrame,
    current_path: str,
    primary_key: list[str],
) -> None:
    active_records = (
        records
        .filter(
            F.col("_operation") != "d"
        )
    )

    if not DeltaTable.isDeltaTable(
        spark,
        current_path,
    ):
        if active_records.limit(1).count() == 0:
            return

        (
            active_records.write
            .format("delta")
            .mode("overwrite")
            .option(
                "delta.enableChangeDataFeed",
                "true",
            )
            .save(current_path)
        )

        return

    target = DeltaTable.forPath(
        spark,
        current_path,
    )

    merge_condition = " AND ".join(
        [
            f"target.{key} = source.{key}"
            for key in primary_key
        ]
    )

    (
        target.alias("target")
        .merge(
            records.alias("source"),
            merge_condition,
        )
        .whenMatchedDelete(
            condition=(
                "source._operation = 'd'"
            )
        )
        .whenMatchedUpdateAll(
            condition=(
                "source._operation <> 'd'"
            )
        )
        .whenNotMatchedInsertAll(
            condition=(
                "source._operation <> 'd'"
            )
        )
        .execute()
    )


def process_tabledef(
    spark: SparkSession,
    raw: DataFrame,
    bronze_root: str,
    table_name: str,
    configuration: dict,
) -> None:
    print("")
    print(
        f"========== {table_name} =========="
    )

    events_path = (
        f"{bronze_root}/events/{table_name}"
    )

    current_path = (
        f"{bronze_root}/current/{table_name}"
    )

    events = parse_events(
        raw=raw,
        table_name=table_name,
        columns=configuration["columns"],
    )

    new_events = get_new_events(
        spark=spark,
        events=events,
        events_path=events_path,
    )

    new_count = new_events.count()

    print(
        f"Novos eventos encontrados: {new_count}"
    )

    try:
        #
        # 1. Acrescenta novos eventos à Bronze Events
        #
        if new_count > 0:
            write_bronze_events(
                spark=spark,
                new_events=new_events,
                events_path=events_path,
            )

        events_exists = DeltaTable.isDeltaTable(
            spark,
            events_path,
        )

        current_exists = DeltaTable.isDeltaTable(
            spark,
            current_path,
        )

        #
        # 2. Se Bronze Events ainda nem existe,
        # não há nada para processar.
        #
        if not events_exists:
            print(
                f"Nenhum evento disponível para {table_name}."
            )
            return

        #
        # 3. Se Bronze Current ainda não existe,
        # reconstrói usando TODO o histórico de Events.
        #
        if not current_exists:
            print(
                "Bronze Current não encontrada. "
                "Reconstruindo a partir da Bronze Events..."
            )

            all_events = (
                spark.read
                .format("delta")
                .load(events_path)
            )

            if all_events.limit(1).count() == 0:
                print(
                    "Bronze Events está vazia."
                )
                return

            current_records = build_current_records(
                new_events=all_events,
                primary_key=configuration[
                    "primary_key"
                ],
                columns=configuration[
                    "columns"
                ],
            )

            merge_current(
                spark=spark,
                records=current_records,
                current_path=current_path,
                primary_key=configuration[
                    "primary_key"
                ],
            )

            print(
                f"Bronze Current reconstruída para {table_name}."
            )

            return

        #
        # 4. Current existe e não há eventos novos.
        #
        if new_count == 0:
            print(
                "Nenhuma alteração nova para processar."
            )
            return

        #
        # 5. Current existe e chegaram novos eventos.
        #
        current_records = build_current_records(
            new_events=new_events,
            primary_key=configuration[
                "primary_key"
            ],
            columns=configuration[
                "columns"
            ],
        )

        merge_current(
            spark=spark,
            records=current_records,
            current_path=current_path,
            primary_key=configuration[
                "primary_key"
            ],
        )

        print(
            f"Bronze atualizada para {table_name}."
        )

    finally:
        new_events.unpersist()


def main() -> None:
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_path = get_required_env(
            "RAW_S3_PATH"
        )

        bronze_root = get_required_env(
            "BRONZE_S3_PATH"
        )

        config = load_config()

        print(
            "Carregando camada Raw..."
        )

        raw = (
            spark.read
            .format("parquet")
            .load(raw_path)
        )

        for table_name, configuration in config.items():
            process_tabledef(
                spark=spark,
                raw=raw,
                bronze_root=bronze_root,
                table_name=table_name,
                configuration=configuration,
            )

        print("")
        print(
            "CAMADA BRONZE CONCLUÍDA COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
