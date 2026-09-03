import os

from pyspark.sql import SparkSession
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


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável {name} não encontrada."
        )

    return value


def create_spark_session() -> SparkSession:
    endpoint = get_required_env(
        "LOCALSTACK_INTERNAL_ENDPOINT"
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

        print("")
        print(f"Total de eventos Raw: {total}")

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
