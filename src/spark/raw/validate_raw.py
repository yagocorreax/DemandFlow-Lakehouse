import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


REQUIRED_COLUMNS = {
    "kafka_key",
    "kafka_value",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "source_table",
    "operation",
    "load_type",
    "event_id",
    "ingested_at",
    "ingestion_date",
    "ingestion_hour",
}

CONFIG_PATH = Path(
    "/opt/demandflow/config/tables.json"
)

# These tables are populated by 02_seed.sql before Debezium starts. Requiring
# their minimum snapshot cardinality prevents a first Raw consumer from
# silently accepting a Kafka log whose old snapshot segments have expired.
MINIMUM_INITIAL_SNAPSHOT_COUNTS = {
    "stores": 3,
    "products": 6,
    "promotions": 2,
    "inventory": 18,
    "demand_forecasts": 252,
}


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável {name} não encontrada."
        )

    return value


def create_spark_session() -> SparkSession:
    endpoint = get_required_env(
        "S3_INTERNAL_ENDPOINT"
    )

    return (
        SparkSession.builder
        .appName("DemandFlowRawValidation")
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
        .getOrCreate()
    )


def validate_initial_snapshot(raw: DataFrame) -> dict[str, int]:
    with CONFIG_PATH.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        configured_tables = set(json.load(file))

    unknown_tables = (
        set(MINIMUM_INITIAL_SNAPSHOT_COUNTS)
        - configured_tables
    )
    if unknown_tables:
        raise RuntimeError(
            "O contrato do snapshot referencia tabelas não configuradas: "
            + ", ".join(sorted(unknown_tables))
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
    insufficient = []

    for table_name, minimum_count in (
        MINIMUM_INITIAL_SNAPSHOT_COUNTS.items()
    ):
        actual_count = snapshot_counts.get(table_name, 0)
        if actual_count < minimum_count:
            insufficient.append(
                f"{table_name}={actual_count}/{minimum_count}"
            )

    if insufficient:
        raise RuntimeError(
            "Snapshot inicial Raw ausente ou incompleto: "
            + ", ".join(insufficient)
            + ". Recupere o snapshot antes de executar a Bronze."
        )

    return {
        table_name: snapshot_counts.get(table_name, 0)
        for table_name in sorted(configured_tables)
    }


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_path = get_required_env("RAW_S3_PATH")

        raw = (
            spark.read
            .format("parquet")
            .load(raw_path)
        )

        missing_columns = (
            REQUIRED_COLUMNS - set(raw.columns)
        )

        if missing_columns:
            raise RuntimeError(
                "Colunas ausentes: "
                + ", ".join(
                    sorted(missing_columns)
                )
            )

        total = raw.count()

        if total == 0:
            raise RuntimeError(
                "A camada Raw está vazia."
            )

        duplicate_count = (
            raw.groupBy("event_id")
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        if duplicate_count > 0:
            raise RuntimeError(
                f"Foram encontrados "
                f"{duplicate_count} event_ids duplicados."
            )

        expected_event_id = F.concat_ws(
            "-",
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
        )
        invalid_identity_count = (
            raw.filter(
                F.col("event_id").isNull()
                | (F.length(F.trim(F.col("event_id"))) == 0)
                | (F.col("event_id") != expected_event_id)
                | F.col("kafka_topic").isNull()
                | (F.length(F.trim(F.col("kafka_topic"))) == 0)
                | F.col("kafka_partition").isNull()
                | F.col("kafka_offset").isNull()
                | F.col("source_table").isNull()
                | (F.length(F.trim(F.col("source_table"))) == 0)
                | F.col("operation").isNull()
                | (~F.col("operation").isin("r", "c", "u", "d"))
                | F.col("load_type").isNull()
                | (~F.col("load_type").isin("full_load", "cdc"))
            )
            .count()
        )

        if invalid_identity_count > 0:
            raise RuntimeError(
                f"Foram encontrados {invalid_identity_count} eventos "
                "com identidade ou metadados CDC inválidos."
            )

        snapshot_counts = validate_initial_snapshot(raw)

        checkpoint_path = get_required_env("RAW_CHECKPOINT_PATH")
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
        filesystem = hadoop_path.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )

        if not filesystem.exists(hadoop_path):
            raise RuntimeError(
                "O checkpoint da camada Raw não existe."
            )

        print("")
        print(f"Total de eventos Raw: {total}")
        print(f"RAW_EVENT_COUNT={total}")
        print("RAW_DUPLICATE_EVENT_IDS=0")
        print("RAW_INVALID_IDENTITY_COUNT=0")
        print("RAW_INITIAL_SNAPSHOT_PRESENT=true")
        print("RAW_CHECKPOINT_PRESENT=true")
        print(
            "RAW_SNAPSHOT_EVENT_COUNT="
            f"{sum(snapshot_counts.values())}"
        )
        for table_name, count in snapshot_counts.items():
            print(f"RAW_SNAPSHOT_COUNT_{table_name}={count}")

        print("")
        print("Eventos por tabela e tipo:")

        (
            raw.groupBy(
                "source_table",
                "load_type",
                "operation",
            )
            .count()
            .orderBy(
                "source_table",
                "load_type",
                "operation",
            )
            .show(
                100,
                truncate=False,
            )
        )

        print("")
        print("Amostra:")

        (
            raw.select(
                "event_id",
                "source_table",
                "operation",
                "load_type",
                "kafka_timestamp",
                "ingested_at",
            )
            .orderBy(
                F.col("ingested_at").desc()
            )
            .show(
                20,
                truncate=False,
            )
        )

        print("")
        print("Nenhum event_id duplicado.")
        print("VALIDAÇÃO RAW CONCLUÍDA COM SUCESSO.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
