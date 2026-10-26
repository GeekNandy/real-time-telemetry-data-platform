#!/usr/bin/env python3
"""Wait for both local roles; a successful container start is not service readiness."""
import time
import urllib.error
import urllib.request

for port in (8080, 8082):
    deadline = time.monotonic() + 120
    while True:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/actuator/health/readiness", timeout=2) as response:
                if response.status == 200:
                    break
        except (urllib.error.URLError, TimeoutError):
            pass
        if time.monotonic() >= deadline:
            raise SystemExit(f"Role at port {port} did not become ready; inspect docker compose logs")
        time.sleep(1)
print("Collector and worker are ready")
