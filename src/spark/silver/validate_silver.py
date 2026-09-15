import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bronze_to_silver import TABLE_CONFIG


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
            print("")
            print(
                f"========== {table_name} =========="
            )

            bronze_path = (
                f"{bronze_root}/current/"
                f"{table_name}"
            )

            silver_path = (
                f"{silver_root}/{table_name}"
            )

            quarantine_path = (
                f"{quarantine_root}/{table_name}"
            )

            for path, layer in [
                (bronze_path, "Bronze"),
                (silver_path, "Silver"),
                (
                    quarantine_path,
                    "Quarantine",
                ),
            ]:
                if not DeltaTable.isDeltaTable(
                    spark,
                    path,
                ):
                    raise RuntimeError(
                        f"{layer} ausente para "
                        f"{table_name}."
                    )

            bronze = (
                spark.read
                .format("delta")
                .load(bronze_path)
            )

            silver = (
                spark.read
                .format("delta")
                .load(silver_path)
            )

            quarantine = (
                spark.read
                .format("delta")
                .load(quarantine_path)
            )

            bronze_count = bronze.count()
            silver_count = silver.count()
            quarantine_count = (
                quarantine.count()
            )

            if (
                silver_count
                + quarantine_count
                != bronze_count
            ):
                raise RuntimeError(
                    f"{table_name}: "
                    "Silver + Quarantine "
                    "diferente da Bronze."
                )

            pk = config["primary_key"]

            duplicate_pk = (
                silver
                .groupBy(*pk)
                .count()
                .filter(
                    F.col("count") > 1
                )
                .count()
            )

            if duplicate_pk > 0:
                raise RuntimeError(
                    f"{table_name}: PK duplicada "
                    "na Silver."
                )

            print(
                f"Bronze: {bronze_count}"
            )

            print(
                f"Silver: {silver_count}"
            )

            print(
                f"Quarantine: "
                f"{quarantine_count}"
            )

            print(
                "Validação OK."
            )

        print("")
        print(
            "8 TABELAS VALIDADAS."
        )

        print(
            "VALIDAÇÃO SILVER "
            "CONCLUÍDA COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()