#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .env ]; then
  umask 077
  python3 - <<'PY'
from pathlib import Path
import secrets
Path(".env").write_text("DATABASE_PASSWORD="+secrets.token_hex(24)+"\nDEMO_JWT_SECRET="+secrets.token_hex(32)+"\n")
PY
fi
mkdir -p data/auth data/profiles
python3 scripts/demo-token.py --observe > data/auth/observe.token
chmod 644 data/auth/observe.token # Prometheus runs under a separate container UID.
docker compose --env-file .env "$@" up --build -d
python3 scripts/wait-ready.py
