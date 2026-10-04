import os
from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator



DOCKER_NETWORK = os.environ["DEMANDFLOW_DOCKER_NETWORK"]

SPARK_IMAGE = "demandflow-spark:3.5.9"

DOCKER_URL = "unix://var/run/docker.sock"


SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.3.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    ]
)


SPARK_ENV = {
    "LOCALSTACK_INTERNAL_ENDPOINT": os.environ[
        "LOCALSTACK_INTERNAL_ENDPOINT"
    ],

    "AWS_ACCESS_KEY_ID": os.environ[
        "AWS_ACCESS_KEY_ID"
    ],

    "AWS_SECRET_ACCESS_KEY": os.environ[
        "AWS_SECRET_ACCESS_KEY"
    ],

    "AWS_REGION": os.environ[
        "AWS_REGION"
    ],

    "AWS_DEFAULT_REGION": os.environ[
        "AWS_DEFAULT_REGION"
    ],

    "KAFKA_INTERNAL_BOOTSTRAP_SERVERS": os.environ[
        "KAFKA_INTERNAL_BOOTSTRAP_SERVERS"
    ],

    "KAFKA_TOPIC_PATTERN": os.environ[
        "KAFKA_TOPIC_PATTERN"
    ],

    "RAW_S3_PATH": os.environ[
        "RAW_S3_PATH"
    ],

    "RAW_CHECKPOINT_PATH": os.environ[
        "RAW_CHECKPOINT_PATH"
    ],

    "BRONZE_S3_PATH": os.environ[
        "BRONZE_S3_PATH"
    ],

    "SILVER_S3_PATH": os.environ[
        "SILVER_S3_PATH"
    ],

    "QUARANTINE_S3_PATH": os.environ[
        "QUARANTINE_S3_PATH"
    ],

    "GOLD_S3_PATH": os.environ[
        "GOLD_S3_PATH"
    ],
}


DEFAULT_ARGS = {
    "retries": 0,
}


def spark_task(
    task_id: str,
    script_path: str,
) -> DockerOperator:

    return DockerOperator(
        task_id=task_id,

        image=SPARK_IMAGE,

        command=[
            "/opt/spark/bin/spark-submit",

            "--master",
            "local[2]",

            "--driver-memory",
            "1g",

            "--packages",
            SPARK_PACKAGES,

            "--conf",
            "spark.jars.ivy=/tmp/.ivy2",

            script_path,
        ],

        environment=SPARK_ENV,

        docker_url=DOCKER_URL,

        network_mode=DOCKER_NETWORK,

        auto_remove="success",

        mount_tmp_dir=False,

        force_pull=False,

        do_xcom_push=False,

        execution_timeout=timedelta(minutes=10)
    )

with DAG(
    dag_id="demandflow_pipeline",

    description=(
        "Pipeline end-to-end do DemandFlow Lakehouse: "
        "Raw -> Bronze -> Silver -> Gold"
    ),

    schedule=None,

    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),

    catchup=False,

    max_active_runs=1,

    default_args=DEFAULT_ARGS,

    tags=[
        "demandflow",
        "lakehouse",
        "data-engineering",
        "medallion",
    ],

) as dag:

    check_infrastructure = DockerOperator(
        task_id="check_infrastructure",

        image="alpine:3.22",

        command=[
            "sh",
            "-c",
            """
            set -e

            echo "===================================="
            echo " DemandFlow Infrastructure Check"
            echo "===================================="

            echo "Checking LocalStack..."
            nc -z -w 5 localstack 4566

            echo "Checking PostgreSQL..."
            nc -z -w 5 postgres 5432

            echo "Checking Kafka..."
            nc -z -w 5 kafka 29092

            echo "Checking Debezium..."
            nc -z -w 5 debezium 8083

            echo ""
            echo "Infrastructure OK."
            """,
        ],

        docker_url=DOCKER_URL,

        network_mode=DOCKER_NETWORK,

        auto_remove="success",

        mount_tmp_dir=False,

        do_xcom_push=False,
    )


    raw_ingestion = spark_task(
        task_id="raw_ingestion",
        script_path=(
            "/opt/demandflow/"
            "src/spark/raw/kafka_to_raw.py"
        ),
    )


    validate_raw = spark_task(
        task_id="validate_raw",
        script_path=(
            "/opt/demandflow/"
            "src/spark/raw/validate_raw.py"
        ),
    )

    bronze = spark_task(
        task_id="bronze",
        script_path=(
            "/opt/demandflow/"
            "src/spark/bronze/raw_to_bronze.py"
        ),
    )


    validate_bronze = spark_task(
        task_id="validate_bronze",
        script_path=(
            "/opt/demandflow/"
            "src/spark/bronze/validate_bronze.py"
        ),
    )


    silver = spark_task(
        task_id="silver",
        script_path=(
            "/opt/demandflow/"
            "src/spark/silver/bronze_to_silver.py"
        ),
    )


    validate_silver = spark_task(
        task_id="validate_silver",
        script_path=(
            "/opt/demandflow/"
            "src/spark/silver/validate_silver.py"
        ),
    )


    gold = spark_task(
        task_id="gold",
        script_path=(
            "/opt/demandflow/"
            "src/spark/gold/silver_to_gold.py"
        ),
    )


    validate_gold = spark_task(
        task_id="validate_gold",
        script_path=(
            "/opt/demandflow/"
            "src/spark/gold/validate_gold.py"
        ),
    )


    (
        check_infrastructure
        >> raw_ingestion
        >> validate_raw
        >> bronze
        >> validate_bronze
        >> silver
        >> validate_silver
        >> gold
        >> validate_gold
    )