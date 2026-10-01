import os

import pendulum

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator


DOCKER_NETWORK = os.environ["DEMANDFLOW_DOCKER_NETWORK"]


with DAG(
    dag_id="demandflow_spark_smoke_test",
    description="Valida Airflow -> Docker -> Spark",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "demandflow",
        "spark",
        "smoke-test",
    ],
) as dag:

    spark_smoke_test = DockerOperator(
        task_id="spark_smoke_test",

        image=(
            "demandflow-spark:3.5.9"
        ),

        command=[
            "/opt/spark/bin/spark-submit",
            "--version",
        ],

        docker_url="unix://var/run/docker.sock",

        network_mode=DOCKER_NETWORK,

        auto_remove="success",

        mount_tmp_dir=False,
    )