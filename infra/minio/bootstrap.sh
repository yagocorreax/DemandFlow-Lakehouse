#!/bin/sh
set -eu
umask 077

# Executed inside the existing minio container. Never enable shell tracing.
for secret in minio_root_user minio_root_password s3_credentials; do
    test -s "/run/secrets/$secret" || {
        echo "Missing storage credential file." >&2
        exit 1
    }
done

root_user=$(cat /run/secrets/minio_root_user)
root_password=$(cat /run/secrets/minio_root_password)
app_user=$(sed -n 's/^AWS_ACCESS_KEY_ID=//p' /run/secrets/s3_credentials)
app_password=$(sed -n 's/^AWS_SECRET_ACCESS_KEY=//p' /run/secrets/s3_credentials)

# Provisioning generates alphanumeric values; reject malformed/duplicate entries.
for value in "$root_user" "$root_password" "$app_user" "$app_password"; do
    case "$value" in
        ''|*[!A-Za-z0-9]*)
            echo "Invalid storage credentials; use initialize-storage-secrets.ps1." >&2
            exit 1
            ;;
    esac
done
test "$app_user" != "$root_user" || {
    echo "The pipeline must use a separate identity." >&2
    exit 1
}

export MC_HOST_admin="http://$root_user:$root_password@127.0.0.1:9000"
export MC_HOST_pipeline="http://$app_user:$app_password@127.0.0.1:9000"
# No credentials are written into mc's persistent configuration.
export MC_CONFIG_DIR=/tmp/demandflow-mc

mc ready admin >/dev/null 2>&1 || {
    echo "AIStor is not ready or admin authentication failed." >&2
    exit 1
}

for bucket in demandflow-raw demandflow-bronze demandflow-silver demandflow-gold demandflow-quarantine demandflow-checkpoints; do
    mc mb --ignore-existing "admin/$bucket" >/dev/null 2>&1 || {
        echo "Could not prepare bucket: $bucket" >&2
        exit 1
    }
    echo "[READY] $bucket"
done

mc admin policy create admin demandflow-lakehouse /opt/demandflow/lakehouse-policy.json >/dev/null 2>&1 || {
    echo "Could not create the lakehouse policy." >&2
    exit 1
}
mc admin user add admin "$app_user" "$app_password" >/dev/null 2>&1 || {
    echo "Could not provision the pipeline identity." >&2
    exit 1
}
mc admin policy attach admin demandflow-lakehouse --user "$app_user" >/dev/null 2>&1 || {
    echo "Could not attach the lakehouse policy." >&2
    exit 1
}

for bucket in demandflow-raw demandflow-bronze demandflow-silver demandflow-gold demandflow-quarantine demandflow-checkpoints; do
    mc stat "pipeline/$bucket" >/dev/null 2>&1 || {
        echo "Pipeline cannot access bucket: $bucket" >&2
        exit 1
    }
done
echo "S3 bootstrap complete; six buckets accessible to the pipeline."
