#!/usr/bin/env python3
"""Issue a local demo token using .env; never use this issuer mode for deployment."""
import argparse
import base64
import hashlib
import hmac
import json
from pathlib import Path
import re
import time

parser = argparse.ArgumentParser()
parser.add_argument("--tenant", default="demo")
scopes = parser.add_mutually_exclusive_group()
scopes.add_argument("--observe", action="store_true", help="Issue a metrics-only local token")
scopes.add_argument("--console", action="store_true", help="Issue local read/write/observe scopes for the console")
parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[1]/".env")
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.tenant):
    raise SystemExit("Invalid tenant")
config = dict(line.split("=", 1) for line in args.env_file.read_text().splitlines() if "=" in line)
def encode(value):
    return base64.urlsafe_b64encode(json.dumps(value,separators=(",",":")).encode()).decode().rstrip("=")
now=int(time.time())
scope = "telemetry.observe" if args.observe else "telemetry.write telemetry.read" + (" telemetry.observe" if args.console else "")
message=encode({"alg":"HS256","typ":"JWT"})+"."+encode({"iss":"http://localhost:8080","aud":["telemetry"],"sub":"local-demo","tenant_id":args.tenant,"scope":scope,"iat":now,"exp":now+3600})
signature=base64.urlsafe_b64encode(hmac.new(config["DEMO_JWT_SECRET"].encode(),message.encode(),hashlib.sha256).digest()).decode().rstrip("=")
print(message+"."+signature)
