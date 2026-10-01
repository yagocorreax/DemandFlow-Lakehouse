FROM apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu

USER root

WORKDIR /opt/demandflow

COPY src /opt/demandflow/src
COPY config /opt/demandflow/config

RUN mkdir -p /tmp/.ivy2 \
    && chown -R spark:spark /opt/demandflow /tmp/.ivy2

USER spark