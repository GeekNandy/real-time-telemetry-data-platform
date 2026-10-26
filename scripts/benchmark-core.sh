#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p core/target/benchmark-classes
mapfile -t sources < <(find core/src/main/java -name '*.java' -print)
sources+=(core/src/test/java/dev/sharad/telemetry/core/CoreBenchmark.java)
if command -v javac >/dev/null 2>&1; then
  javac --release 17 -Xlint:all -Werror -d core/target/benchmark-classes "${sources[@]}"
else
  java -m jdk.compiler/com.sun.tools.javac.Main --release 17 -Xlint:all -Werror -d core/target/benchmark-classes "${sources[@]}"
fi
revision=$(git rev-parse HEAD)
dirty=false
if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then dirty=true; fi
java_options=(-Xms256m -Xmx256m "-Dbenchmark.revision=$revision" "-Dbenchmark.dirty=$dirty")
if [[ "${PROFILE:-0}" == "1" ]]; then
  java -m jdk.jfr/jdk.jfr.internal.tool.Main configure --input profile.jfc \
    --output core/target/profile-safe.jfc \
    'jdk.InitialEnvironmentVariable#enabled=false' \
    'jdk.InitialSystemProperty#enabled=false' \
    'jdk.SystemProcess#enabled=false' >/dev/null
  profile_output=$(mktemp core/target/profile-result-XXXXXXXX.json)
  trap 'rm -f "$profile_output"' EXIT
  java_options+=("-Dbenchmark.output=$profile_output" "-XX:StartFlightRecording=filename=core/target/core-overhead.jfr,settings=core/target/profile-safe.jfc,dumponexit=true")
  # JVM/JFR startup messages belong on stderr, while stdout remains a JSON report.
  java "${java_options[@]}" -cp core/target/benchmark-classes dev.sharad.telemetry.core.CoreBenchmark "${@}" >&2
  cat "$profile_output"
else
  java "${java_options[@]}" -cp core/target/benchmark-classes dev.sharad.telemetry.core.CoreBenchmark "${@}"
fi
