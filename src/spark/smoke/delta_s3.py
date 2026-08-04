import os
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


TABLE_PATH = (
    "s3a://demandflow-bronze/"
    "smoke/products_delta"
)


def create_spark_session() -> SparkSession:
    endpoint = os.getenv(
        "LOCALSTACK_INTERNAL_ENDPOINT",
        "http://localstack:4566", 
    )

    access_key = os.getenv(
        "AWS_ACCESS_KEY_ID",
        "test",
    )

    secret_key = os.getenv(
        "AWS_SECRET_ACCESS_KEY",
        "test",
    )

    return (
        SparkSession.builder
        .appName("DemandFlowDeltaS3SmokeTest")
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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField(
                "product_id",
                IntegerType(),
                nullable=False,
            ),
            StructField(
                "sku",
                StringType(),
                nullable=False,
            ),
            StructField(
                "product_name",
                StringType(),
                nullable=False,
            ),
            StructField(
                "unit_price",
                DecimalType(12, 2),
                nullable=False,
            ),
            StructField(
                "is_active",
                BooleanType(),
                nullable=False,
            ),
        ]
    )

    initial_data = [
        (
            1,
            "SKU-001",
            "Perfume Aurora 100ml",
            Decimal("299.90"),
            True,
        ),
        (
            2,
            "SKU-002",
            "Perfume Horizon 100ml",
            Decimal("349.90"),
            True,
        ),
        (
            3,
            "SKU-003",
            "Body Splash Flora 200ml",
            Decimal("89.90"),
            True,
        ),
    ]

    updates = [
        (
            2,
            "SKU-002",
            "Perfume Horizon 100ml",
            Decimal("379.90"),
            True,
        ),
        (
            3,
            "SKU-003",
            "Body Splash Flora 200ml",
            Decimal("89.90"),
            False,
        ),
        (
            4,
            "SKU-004",
            "Creme Corporal Essence",
            Decimal("69.90"),
            True,
        ),
    ]

    try:
        print("\n[1/6] Gravando tabela Delta inicial...")

        initial_df = spark.createDataFrame(
            initial_data,
            schema=schema,
        )

        (
            initial_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option(
                "delta.enableChangeDataFeed",
                "true",
            )
            .save(TABLE_PATH)
        )

        history_before_merge = spark.sql(
            f"DESCRIBE HISTORY delta.`{TABLE_PATH}`"
        )

        version_before_merge = (
            history_before_merge
            .select("version")
            .first()["version"]
        )

        print(
            "[2/6] Versão anterior ao MERGE: "
            f"{version_before_merge}"
        )

        updates_df = spark.createDataFrame(
            updates,
            schema=schema,
        )

        updates_df.createOrReplaceTempView(
            "product_updates"
        )

        print("[3/6] Executando MERGE/UPSERT...")

        spark.sql(
            f"""
            MERGE INTO delta.`{TABLE_PATH}` AS target
            USING product_updates AS source
                ON target.product_id = source.product_id

            WHEN MATCHED THEN
                UPDATE SET *

            WHEN NOT MATCHED THEN
                INSERT *
            """
        )

        print("[4/6] Estado atual da tabela:")

        final_df = (
            spark.read
            .format("delta")
            .load(TABLE_PATH)
            .orderBy("product_id")
        )

        final_df.show(truncate=False)

        rows = {
            row["product_id"]: row.asDict()
            for row in final_df.collect()
        }

        assert len(rows) == 4
        assert rows[2]["unit_price"] == Decimal("379.90")
        assert rows[3]["is_active"] is False
        assert rows[4]["sku"] == "SKU-004"

        history_after_merge = spark.sql(
            f"DESCRIBE HISTORY delta.`{TABLE_PATH}`"
        )

        version_after_merge = (
            history_after_merge
            .select("version")
            .first()["version"]
        )

        print("[5/6] Histórico da tabela:")

        (
            history_after_merge
            .select(
                "version",
                "timestamp",
                "operation",
            )
            .orderBy(
                "version",
                ascending=False,
            )
            .show(truncate=False)
        )

        print("[6/6] Alterações registradas no CDF:")

        changes_df = (
            spark.read
            .format("delta")
            .option(
                "readChangeFeed",
                "true",
            )
            .option(
                "startingVersion",
                version_before_merge + 1,
            )
            .option(
                "endingVersion",
                version_after_merge,
            )
            .load(TABLE_PATH)
        )

        (
            changes_df
            .select(
                "product_id",
                "sku",
                "unit_price",
                "is_active",
                "_change_type",
                "_commit_version",
            )
            .orderBy(
                "product_id",
                "_change_type",
            )
            .show(truncate=False)
        )

        print(
            "\nEstado anterior ao MERGE "
            "utilizando Time Travel:"
        )

        (
            spark.read
            .format("delta")
            .option(
                "versionAsOf",
                version_before_merge,
            )
            .load(TABLE_PATH)
            .orderBy("product_id")
            .show(truncate=False)
        )

        print("\nSMOKE TEST CONCLUÍDO COM SUCESSO.")
        print(f"Tabela: {TABLE_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
