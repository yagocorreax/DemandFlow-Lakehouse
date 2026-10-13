FROM apache/airflow:3.3.2

RUN python -m pip install --no-cache-dir \
    apache-airflow==3.3.2 \
    apache-airflow-providers-docker==4.5.9 \
    apache-airflow-providers-fab==3.9.0

RUN install -d -m 0750 /opt/airflow/demandflow

COPY --chown=airflow:root --chmod=0440 \
    infra/airflow/initialize.sh \
    /opt/airflow/demandflow/initialize.sh

COPY --chown=airflow:root --chmod=0440 \
    infra/airflow/api_healthcheck.py \
    /opt/airflow/demandflow/api_healthcheck.py

COPY --chown=airflow:root --chmod=0440 \
    infra/airflow/job_healthcheck.py \
    /opt/airflow/demandflow/job_healthcheck.py

COPY --chown=airflow:root --chmod=0440 \
    infra/airflow/worker_healthcheck.py \
    /opt/airflow/demandflow/worker_healthcheck.py
