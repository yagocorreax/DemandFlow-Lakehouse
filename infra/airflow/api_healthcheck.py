"""Fail unless the local Airflow API and metadata database are ready."""

import json
import sys
import urllib.request


URL = "http://127.0.0.1:8080/api/v2/monitor/health"


try:
    with urllib.request.urlopen(URL, timeout=3) as response:
        payload = json.load(response)
        healthy = (
            response.status == 200
            and payload.get("metadatabase", {}).get("status") == "healthy"
        )
except (OSError, ValueError, json.JSONDecodeError):
    healthy = False

sys.exit(0 if healthy else 1)
