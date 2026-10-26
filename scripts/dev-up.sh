#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
if [ ! -f .env ]; then
  umask 077
  python3 - <<'PY'
from pathlib import Path
import secrets
Path(".env").write_text("DATABASE_PASSWORD="+secrets.token_hex(24)+"\nDEMO_JWT_SECRET="+secrets.token_hex(32)+"\n")
PY
fi
mkdir -p data/auth data/profiles
chmod 700 data/auth
python3 - <<'PY'
from pathlib import Path
import secrets
password = Path('data/auth/grafana-admin-password')
if not password.exists():
    password.write_text(secrets.token_urlsafe(24))
password.chmod(0o644) # Single-file bind mount for Grafana's separate UID; parent stays 0700.
PY
python3 scripts/demo-token.py --observe > data/auth/observe.token
chmod 644 data/auth/observe.token # Prometheus runs under a separate container UID.
python3 scripts/demo-token.py --console > data/auth/console.token
chmod 600 data/auth/console.token
docker compose --env-file .env --profile console --profile observability "$@" up --build -d
python3 scripts/wait-ready.py
printf '%s\n' 'Operator console: http://127.0.0.1:8088' 'Use data/auth/console.token in Connect session' 'Grafana: http://127.0.0.1:3000 · user operator · password in data/auth/grafana-admin-password'
