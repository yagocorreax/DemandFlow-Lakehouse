FROM apache/superset:latest

USER root

RUN uv pip install \
    --python /app/.venv/bin/python \
    trino

USER supersetar