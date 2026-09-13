import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variável obrigatória {name} não encontrada."
        )

    return value


def create_spark_session() -> SparkSession:
    endpoint = get_required_env(
        "LOCALSTACK_INTERNAL_ENDPOINT"
    )

    access_key = get_required_env(
        "AWS_ACCESS_KEY_ID"
    )

    secret_key = get_required_env(
        "AWS_SECRET_ACCESS_KEY"
    )

    return (
        SparkSession.builder
        .appName("DemandFlowRawKafkaIngestion")
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
            access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            secret_key,
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


def main() -> None:
    kafka_bootstrap_servers = get_required_env(
        "KAFKA_INTERNAL_BOOTSTRAP_SERVERS"
    )

    topic_pattern = get_required_env(
        "KAFKA_TOPIC_PATTERN"
    )

    raw_path = get_required_env(
        "RAW_S3_PATH"
    )

    checkpoint_path = get_required_env(
        "RAW_CHECKPOINT_PATH"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Iniciando ingestão Kafka -> Raw")
        print(f"Kafka: {kafka_bootstrap_servers}")
        print(f"Tópicos: {topic_pattern}")
        print(f"Raw: {raw_path}")
        print(f"Checkpoint: {checkpoint_path}")

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                kafka_bootstrap_servers,
            )
            .option(
                "subscribePattern",
                topic_pattern,
            )
            .option(
                "startingOffsets",
                "earliest",
            )
            .option(
                "failOnDataLoss",
                "true",
            )
            .option(
                "maxOffsetsPerTrigger",
                "5000",
            )
            .load()
        )

        raw_events = (
            kafka_stream
            .select(
                F.col("key")
                .cast("string")
                .alias("kafka_key"),

                F.col("value")
                .cast("string")
                .alias("kafka_value"),

                F.col("topic")
                .alias("kafka_topic"),

                F.col("partition")
                .alias("kafka_partition"),

                F.col("offset")
                .alias("kafka_offset"),

                F.col("timestamp")
                .alias("kafka_timestamp"),
            )
            .withColumn(
                "source_table",
                F.regexp_extract(
                    F.col("kafka_topic"),
                    r"^demandflow[.]public[.](.+)$",
                    1,
                ),
            )
            .withColumn(
                "operation",
                F.coalesce(
        F.get_json_object(
            F.col("kafka_value"),
            "$.op",
        ),
        F.get_json_object(
            F.col("kafka_value"),
            "$.payload.op",
                ),
            )
            )
            .withColumn(
                "load_type",
                F.when(
                    F.col("operation") == "r",
                    F.lit("full_load"),
                ).otherwise(
                    F.lit("cdc")
                ),
            )
            .withColumn(
                "event_id",
                F.concat_ws(
                    "-",
                    F.col("kafka_topic"),
                    F.col("kafka_partition"),
                    F.col("kafka_offset"),
                ),
            )
            .withColumn(
                "ingested_at",
                F.current_timestamp(),
            )
            .withColumn(
                "ingestion_date",
                F.to_date(
                    F.col("ingested_at")
                ),
            )
            .withColumn(
                "ingestion_hour",
                F.date_format(
                    F.col("ingested_at"),
                    "HH",
                ),
            )
        )

        query = (
            raw_events.writeStream
            .format("parquet")
            .outputMode("append")
            .partitionBy(
                "source_table",
                "load_type",
                "ingestion_date",
                "ingestion_hour",
            )
            .option(
                "checkpointLocation",
                checkpoint_path,
            )
            .trigger(
                availableNow=True
            )
            .start(raw_path)
        )

        query.awaitTermination()

        progress = query.lastProgress

        if progress:
            print(
                "Último micro-batch processou "
                f"{progress.get('numInputRows', 0)} eventos."
            )
        else:
            print(
                "Nenhum novo evento precisava ser processado."
            )

        print("")
        print("INGESTÃO RAW CONCLUÍDA COM SUCESSO.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
