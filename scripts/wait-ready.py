#!/usr/bin/env python3
"""Require healthy local roles within one bounded startup deadline."""
import time
import urllib.error
import urllib.request


def wait_for_roles(ports=(8080, 8082), timeout=120, interval=1):
    deadline = time.monotonic() + timeout
    for port in ports:
        while time.monotonic() < deadline:
            try:
                remaining = max(0.001, min(2, deadline - time.monotonic()))
                with urllib.request.urlopen(
                        f'http://127.0.0.1:{port}/actuator/health/readiness',
                        timeout=remaining) as response:
                    if response.status == 200:
                        break
            except urllib.error.HTTPError as failure:
                failure.close()
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(interval, remaining))
        else:
            raise SystemExit(f'Role at port {port} did not become ready; inspect role logs')
    print('Collector and worker are ready')


if __name__ == '__main__':
    wait_for_roles()
