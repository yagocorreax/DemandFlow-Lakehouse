#!/usr/bin/env bash
set -Eeuo pipefail

required_variables=(
    AIRFLOW_ADMIN_USERNAME
    AIRFLOW_ADMIN_PASSWORD
    AIRFLOW_ADMIN_EMAIL
)

for variable_name in "${required_variables[@]}"; do
    if [[ -z "${!variable_name:-}" ]]; then
        printf 'Variavel obrigatoria ausente: %s\n' "$variable_name" >&2
        exit 1
    fi
done

airflow db migrate
airflow db check --retry 10 --retry-delay 3

if airflow users list --output json | python -c '
import json
import os
import sys

users = json.load(sys.stdin)
username = os.environ["AIRFLOW_ADMIN_USERNAME"]
sys.exit(0 if any(user.get("username") == username for user in users) else 1)
'; then
    airflow users reset-password \
        --username "$AIRFLOW_ADMIN_USERNAME" \
        --password "$AIRFLOW_ADMIN_PASSWORD"
    printf 'AIRFLOW_ADMIN_ACTION=updated\n'
else
    airflow users create \
        --username "$AIRFLOW_ADMIN_USERNAME" \
        --password "$AIRFLOW_ADMIN_PASSWORD" \
        --firstname DemandFlow \
        --lastname Admin \
        --role Admin \
        --email "$AIRFLOW_ADMIN_EMAIL"
    printf 'AIRFLOW_ADMIN_ACTION=created\n'
fi

printf 'AIRFLOW_METADATA_READY=true\n'
printf 'AIRFLOW_ADMIN_READY=true\n'
