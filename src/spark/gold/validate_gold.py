import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


GOLD_TABLES = {
    "daily_sales": [
        "sales_date",
        "store_id",
    ],

    "product_sales": [
        "product_id",
    ],

    "inventory_health": [
        "store_id",
        "product_id",
    ],

    "forecast_accuracy": [
        "forecast_id",
    ],
}


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
        .appName("DemandFlowGoldValidation")
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

    gold_root = required_env(
        "GOLD_S3_PATH"
    )

    try:
        for table_name, primary_key in (
            GOLD_TABLES.items()
        ):
            print("")
            print(
                f"========== {table_name} =========="
            )

            path = (
                f"{gold_root}/{table_name}"
            )

            if not DeltaTable.isDeltaTable(
                spark,
                path,
            ):
                raise RuntimeError(
                    f"Tabela Gold ausente: "
                    f"{table_name}"
                )

            df = (
                spark.read
                .format("delta")
                .load(path)
            )

            count = df.count()

            duplicate_count = (
                df.groupBy(
                    *primary_key
                )
                .count()
                .filter(
                    F.col("count") > 1
                )
                .count()
            )

            if duplicate_count > 0:
                raise RuntimeError(
                    f"{table_name}: "
                    f"{duplicate_count} "
                    "chaves duplicadas."
                )

            print(
                f"Registros: {count}"
            )

            print(
                "Validação OK."
            )

        print("")
        print(
            "4 TABELAS GOLD VALIDADAS."
        )

        print(
            "VALIDAÇÃO GOLD "
            "CONCLUÍDA COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()