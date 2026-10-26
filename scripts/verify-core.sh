#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p core/target/contract-classes
mapfile -t sources < <(find core/src/main/java -name '*.java' -print)
sources+=(core/src/test/java/dev/sharad/telemetry/core/CoreContractCases.java)
if command -v javac >/dev/null 2>&1; then
  javac --release 17 -Xlint:all -Werror -d core/target/contract-classes "${sources[@]}"
else
  java -m jdk.compiler/com.sun.tools.javac.Main --release 17 -Xlint:all -Werror -d core/target/contract-classes "${sources[@]}"
fi
java -ea -cp core/target/contract-classes dev.sharad.telemetry.core.CoreContractCases
