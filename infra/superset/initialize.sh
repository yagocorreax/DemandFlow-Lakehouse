#!/usr/bin/env bash
set -euo pipefail

: "${SUPERSET_ADMIN_USERNAME:?SUPERSET_ADMIN_USERNAME is required}"
: "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD is required}"
: "${SUPERSET_ADMIN_FIRSTNAME:?SUPERSET_ADMIN_FIRSTNAME is required}"
: "${SUPERSET_ADMIN_LASTNAME:?SUPERSET_ADMIN_LASTNAME is required}"
: "${SUPERSET_ADMIN_EMAIL:?SUPERSET_ADMIN_EMAIL is required}"
: "${SUPERSET_TRINO_URI:?SUPERSET_TRINO_URI is required}"

superset db upgrade

if ! superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME}" \
    --password "${SUPERSET_ADMIN_PASSWORD}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
    --lastname "${SUPERSET_ADMIN_LASTNAME}" \
    --email "${SUPERSET_ADMIN_EMAIL}"; then
    superset fab list-users \
        | grep -F -- "${SUPERSET_ADMIN_USERNAME}" \
        > /dev/null
fi

superset init
superset set-database-uri \
    --database_name "DemandFlow Trino" \
    --uri "${SUPERSET_TRINO_URI}"
superset test-db \
    --connect-args '{}' \
    "${SUPERSET_TRINO_URI}"

echo "SUPERSET_METADATA_READY=true"
echo "SUPERSET_ADMIN_READY=true"
echo "SUPERSET_TRINO_CONNECTION=true"
