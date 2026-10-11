"""Check an Airflow job heartbeat without importing the full Airflow CLI."""

import os
import socket
import sys

import psycopg2


ALLOWED_JOB_TYPES = {"SchedulerJob", "DagProcessorJob", "TriggererJob"}


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ALLOWED_JOB_TYPES:
        return 2

    connection_url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    connection_url = connection_url.replace(
        "postgresql+psycopg2://",
        "postgresql://",
        1,
    )

    try:
        with psycopg2.connect(connection_url, connect_timeout=3) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM job
                        WHERE job_type = %s
                          AND hostname = %s
                          AND state = 'running'
                          AND latest_heartbeat >=
                              CURRENT_TIMESTAMP - INTERVAL '90 seconds'
                    )
                    """,
                    (sys.argv[1], socket.gethostname()),
                )
                healthy = bool(cursor.fetchone()[0])
    except (KeyError, OSError, psycopg2.Error):
        healthy = False

    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
