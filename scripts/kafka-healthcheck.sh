#!/usr/bin/env bash
# Check the local PLAINTEXT broker protocol without starting a JVM. Application
# readiness separately checks topic configuration, leaders and in-sync replicas.
set -euo pipefail
if (( $# > 2 )); then
  echo "Usage: $0 [host [port]]" >&2
  exit 2
fi
host="${1:-127.0.0.1}"
port="${2:-9092}"
if [[ ! "$port" =~ ^[0-9]{1,5}$ ]] || (( 10#$port < 1 || 10#$port > 65535 )); then
  echo "Invalid Kafka port" >&2
  exit 2
fi

# One deadline covers connect, write and every read, including a peer that sends
# bytes slowly. KILL also bounds blocked shell/coreutils children in this group.
exec timeout --signal=KILL 3s bash -s -- "$host" "$port" <<'PROBE'
set -euo pipefail
export LC_ALL=C
exec 3<>"/dev/tcp/$1/$2"

# ApiVersions v0 (key 18), correlation ID 1, empty client ID. This non-flexible
# version is supported by Kafka 4.0 and avoids version negotiation or metadata
# discovery. Do not hold binary/NUL bytes in Bash variables.
printf '\x00\x00\x00\x0a\x00\x12\x00\x00\x00\x00\x00\x01\x00\x00' >&3
read_bytes() {
  local count="$1" output
  output=$(head -c "$count" <&3 | od -An -v -tu1) || return 1
  read -r -a bytes <<< "${output//$'\n'/ }"
  (( ${#bytes[@]} == count ))
}
read_bytes 4
length=$(( bytes[0] * 16777216 + bytes[1] * 65536 + bytes[2] * 256 + bytes[3] ))
# v0: correlation(4), error(2), array count(4), API entries(6 each). Refuse a
# hostile length before reading the body; the current response is under 1 KiB.
(( length >= 10 && length <= 4096 ))
read_bytes "$length"
(( bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 1 ))
(( bytes[4] == 0 && bytes[5] == 0 ))
count=$(( bytes[6] * 16777216 + bytes[7] * 65536 + bytes[8] * 256 + bytes[9] ))
(( length == 10 + 6 * count ))
declare -A versions=()
for (( offset=10; offset<length; offset+=6 )); do
  api=$(( bytes[offset] * 256 + bytes[offset+1] ))
  minimum=$(( bytes[offset+2] * 256 + bytes[offset+3] ))
  maximum=$(( bytes[offset+4] * 256 + bytes[offset+5] ))
  # All three fields are nonnegative signed int16 values. Duplicate keys or an
  # inverted version range indicate a malformed response, even for unused APIs.
  (( api < 32768 && minimum <= maximum && maximum < 32768 ))
  [[ ! -v "versions[$api]" ]]
  versions[$api]=1
done
# A controller listener can answer ApiVersions too. Require the broker APIs
# used for data publication and metadata; a TCP accept alone cannot pass.
[[ -v 'versions[0]' && -v 'versions[3]' ]]
PROBE
