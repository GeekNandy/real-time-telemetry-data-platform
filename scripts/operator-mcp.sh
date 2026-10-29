#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
jar="$root/tools/operator-mcp/target/operator-mcp.jar"
if [[ ! -f "$jar" ]]; then
  echo 'Build first: ./mvnw --batch-mode --no-transfer-progress -f tools/operator-mcp/pom.xml verify' >&2
  exit 1
fi
java_bin=java
if [[ -n "${JAVA_HOME:-}" ]]; then
  java_bin="$JAVA_HOME/bin/java"
fi
exec "$java_bin" -Xms16m -Xmx128m -jar "$jar" "$@"
