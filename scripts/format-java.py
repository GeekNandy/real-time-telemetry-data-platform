#!/usr/bin/env python3
"""Check or apply the pinned Java formatter, with its artifact checksum verified."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request

VERSION = "1.28.0"
CHECKSUM = "32342e7c1b4600f80df3471da46aee8012d3e1445d5ea1be1fb71289b07cc735"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--write", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
cache = Path(tempfile.gettempdir()) / "telemetry-format" / VERSION
cache.mkdir(parents=True, exist_ok=True)
jar = cache / "google-java-format.jar"
if not jar.exists():
    url = f"https://repo.maven.apache.org/maven2/com/google/googlejavaformat/google-java-format/{VERSION}/google-java-format-{VERSION}-all-deps.jar"
    with urllib.request.urlopen(url, timeout=45) as response:
        jar.write_bytes(response.read())
if hashlib.sha256(jar.read_bytes()).hexdigest() != CHECKSUM:
    raise SystemExit("Formatter checksum mismatch")
files = sorted(str(p) for module in ("core", "platform-config", "durable-buffer", "platform-observability", "event-schemas", "service")
               for p in (root / module / "src").rglob("*.java"))
java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else "java"
options = ["--replace"] if args.write else ["--dry-run", "--set-exit-if-changed"]
raise SystemExit(subprocess.call([java, "-jar", str(jar), *options, *files]))
