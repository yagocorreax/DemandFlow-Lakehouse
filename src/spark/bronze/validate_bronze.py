import json
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CONFIG_PATH = Path(
    "/opt/demandflow/config/tables.json"
)


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


def build_null_pk_condition(
    primary_key: list[str],
):
    condition = None

    for key in primary_key:
        current_condition = F.col(key).isNull()

        if condition is None:
            condition = current_condition
        else:
            condition = (
                condition | current_condition
            )

    return condition


def validate_events(
    spark: SparkSession,
    table_name: str,
    events_path: str,
) -> int:
    if not DeltaTable.isDeltaTable(
        spark,
        events_path,
    ):
        print(
            f"Bronze Events não existe para "
            f"{table_name}. Pulando validação."
        )
        return 0

    events = (
        spark.read
        .format("delta")
        .load(events_path)
    )

    events_count = events.count()

    duplicate_events = (
        events
        .groupBy("event_id")
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_event_ids = (
        events
        .filter(
            F.col("event_id").isNull()
        )
        .count()
    )

    print(
        f"Events: {events_count}"
    )

    if duplicate_events > 0:
        raise RuntimeError(
            f"{table_name}: "
            f"{duplicate_events} event_ids duplicados."
        )

    if null_event_ids > 0:
        raise RuntimeError(
            f"{table_name}: "
            f"{null_event_ids} event_ids nulos."
        )

    return events_count


def validate_current(
    spark: SparkSession,
    table_name: str,
    current_path: str,
    primary_key: list[str],
) -> int:
    if not DeltaTable.isDeltaTable(
        spark,
        current_path,
    ):
        print(
            f"Bronze Current não existe para "
            f"{table_name}. Pulando validação."
        )
        return 0

    current = (
        spark.read
        .format("delta")
        .load(current_path)
    )

    current_count = current.count()

    duplicate_pk = (
        current
        .groupBy(
            *primary_key
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .count()
    )

    null_condition = (
        build_null_pk_condition(
            primary_key
        )
    )

    null_pk = (
        current
        .filter(
            null_condition
        )
        .count()
    )

    print(
        f"Current: {current_count}"
    )

    if duplicate_pk > 0:
        raise RuntimeError(
            f"{table_name}: "
            f"{duplicate_pk} chaves primárias duplicadas."
        )

    if null_pk > 0:
        raise RuntimeError(
            f"{table_name}: "
            f"{null_pk} registros com PK nula."
        )

    return current_count


def main() -> None:
    spark = create_spark()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    bronze_root = required_env(
        "BRONZE_S3_PATH"
    )

    with CONFIG_PATH.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        tables = json.load(file)

    validated_tables = 0
    skipped_tables = 0

    try:
        for table_name, config in tables.items():
            print("")
            print(
                f"========== {table_name} =========="
            )

            events_path = (
                f"{bronze_root}/events/"
                f"{table_name}"
            )

            current_path = (
                f"{bronze_root}/current/"
                f"{table_name}"
            )

            events_exists = (
                DeltaTable.isDeltaTable(
                    spark,
                    events_path,
                )
            )

            current_exists = (
                DeltaTable.isDeltaTable(
                    spark,
                    current_path,
                )
            )

            if (
                not events_exists
                and not current_exists
            ):
                print(
                    "Nenhuma estrutura Bronze "
                    "encontrada para esta tabela."
                )

                skipped_tables += 1
                continue

            events_count = validate_events(
                spark=spark,
                table_name=table_name,
                events_path=events_path,
            )

            current_count = validate_current(
                spark=spark,
                table_name=table_name,
                current_path=current_path,
                primary_key=config[
                    "primary_key"
                ],
            )

            if (
                events_count > 0
                or current_count > 0
            ):
                validated_tables += 1

            print(
                "Validação OK."
            )

        print("")
        print(
            "======================================"
        )
        print(
            "RESUMO DA VALIDAÇÃO"
        )
        print(
            "======================================"
        )
        print(
            f"Tabelas validadas: "
            f"{validated_tables}"
        )
        print(
            f"Tabelas ignoradas: "
            f"{skipped_tables}"
        )
        print("")
        print(
            "Nenhum event_id duplicado."
        )
        print(
            "Nenhuma PK duplicada."
        )
        print(
            "Nenhuma PK nula."
        )
        print("")
        print(
            "VALIDAÇÃO BRONZE "
            "CONCLUÍDA COM SUCESSO."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()