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
docker compose --env-file .env up --build -d
