#!/usr/bin/env python3
"""Register bundled schemas through a separate migration identity, before read-only services start."""
import argparse
import json
import os
from pathlib import Path
import urllib.error
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--registry-url", required=True)
parser.add_argument("--allow-local-http", action="store_true")
args = parser.parse_args()
url = urllib.parse.urlparse(args.registry_url)
if url.username or url.password or not url.hostname or (
        url.scheme != "https" and not (args.allow_local_http and url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1"))):
    parser.error("Use HTTPS without embedded credentials; local HTTP requires an explicit flag")
authorization = os.environ.get("SCHEMA_REGISTRY_AUTHORIZATION", "")
base = args.registry_url.rstrip("/")
subject = "telemetry.events-value"


def request(method, path, body=None):
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    if authorization:
        headers["Authorization"] = authorization
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(urllib.request.Request(base + path, method=method, headers=headers, data=data), timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as failure:
        if failure.code == 404:
            return 404, {}
        raise SystemExit(f"Schema migration stopped: registry HTTP {failure.code}") from None
    except urllib.error.URLError:
        raise SystemExit("Schema migration stopped: registry unavailable") from None


status, config = request("GET", "/config/" + subject + "?defaultToGlobal=false")
if status == 404:
    request("PUT", "/config/" + subject, {"compatibility": "FULL_TRANSITIVE"})
elif config.get("compatibilityLevel") != "FULL_TRANSITIVE":
    raise SystemExit("Existing subject has a different compatibility policy; review before changing it")
root = Path(__file__).resolve().parents[1] / "event-schemas/src/main/resources/avro"
for name in ("telemetry-v1.avsc", "telemetry-v2.avsc"):
    body = {"schemaType": "AVRO", "schema": (root / name).read_text()}
    status, compatibility = request("POST", "/compatibility/subjects/" + subject + "/versions/latest", body)
    if status != 404 and compatibility.get("is_compatible") is not True:
        raise SystemExit(f"Compatibility rejected {name}; no version was registered")
    _, registered = request("POST", "/subjects/" + subject + "/versions?normalize=true", body)
    print(json.dumps({"schema": name, "id": registered["id"]}))
