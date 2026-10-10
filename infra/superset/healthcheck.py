from urllib.request import urlopen


with urlopen("http://127.0.0.1:8088/health", timeout=2) as response:
    if response.status != 200 or response.read().strip() != b"OK":
        raise RuntimeError("Superset health endpoint is not ready.")
