import os

import pendulum

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator


DOCKER_NETWORK = os.environ.get(
    "DEMANDFLOW_DOCKER_NETWORK"
)


with DAG(
    dag_id="demandflow_docker_smoke_test",
    description="Valida Airflow -> Docker Engine",
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
        "infrastructure",
        "smoke-test",
    ],
) as dag:

    docker_smoke_test = DockerOperator(
        task_id="docker_smoke_test",

        image="alpine:3.22",

        command=[
            "sh",
            "-c",
            """
            echo "=================================="
            echo " DemandFlow Docker Smoke Test"
            echo "=================================="
            echo "Container executado pelo Airflow."
            echo "Hostname:"
            hostname
            echo "Network:"
            cat /etc/hosts
            echo "Teste concluido."
            """,
        ],

        docker_url="unix://var/run/docker.sock",

        network_mode=DOCKER_NETWORK,

        auto_remove="success",

        mount_tmp_dir=False,
    )