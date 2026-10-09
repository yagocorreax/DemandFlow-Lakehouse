FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5

USER root

WORKDIR /opt/demandflow

COPY src/spark /opt/demandflow/src/spark
COPY config/tables.json /opt/demandflow/config/tables.json

RUN mkdir -p /opt/demandflow/.ivy2 \
    && chown -R spark:spark /opt/demandflow

USER spark
