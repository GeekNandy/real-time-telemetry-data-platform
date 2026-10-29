# Local read-only MCP operator

The optional Java 21 adapter lets an MCP client inspect aggregate platform state
and public project evidence. It runs as a local stdio process, separately from
collector and worker. It has no HTTP listener, ingest dependency, Render service,
or cloud resource. The API remains the authority for access control.

| Tool | Result | Access |
| --- | --- | --- |
| `get_platform_health` | Collector and worker status only | Public health endpoints; no token sent |
| `get_ingestion_metrics` | Allowlisted aggregate counters for `collector` or `worker` | Existing JWT with `telemetry.observe` scope |
| `get_event_schema` | Bundled Avro version 1 or 2 | Public schema files |
| `get_benchmark_results` | Bundled synthetic evidence and fixed SLO profiles | Public project evidence; not a live measurement |

The adapter provides no event lookup, tenant enumeration, payload access, SQL,
shell commands, configuration changes, deploys, or replay operations. Read-only
tool annotations describe behavior; the fixed implementation and backend
authorization enforce the boundary. The client should treat returned content as
data, never as instructions.

## Build and connect

```bash
./mvnw --batch-mode --no-transfer-progress -f tools/operator-mcp/pom.xml verify
python3 scripts/test-operator-mcp.py
```

For the local development stack, create a private copy of its generated observe
credential. The original remains readable by the Prometheus container's UID.

```bash
install -m 600 data/auth/observe.token data/auth/mcp-observe.token
```

Use a stdio-capable MCP client. Replace `/absolute/path/to/repository` below with
the checkout path. Client configuration formats vary; this is the common
`mcpServers` format, not a claim that every client supports local processes.

```json
{
  "mcpServers": {
    "telemetry-operator": {
      "command": "/absolute/path/to/repository/scripts/operator-mcp.sh",
      "env": {
        "TELEMETRY_COLLECTOR_URL": "http://127.0.0.1:8080",
        "TELEMETRY_WORKER_URL": "http://127.0.0.1:8082",
        "TELEMETRY_OBSERVE_TOKEN_FILE": "/absolute/path/to/repository/data/auth/mcp-observe.token"
      }
    }
  }
}
```

The local [development stack](console.md) generates the observe token. On POSIX
systems the MCP copy must allow only owner read/write access (`chmod 600`). Supply
credentials through that file, never tool arguments, a committed client config,
or a command-line argument. The adapter rereads the bounded file for each metrics
call, so the operator can replace an expired JWT without restarting the process.
It does not mint or refresh tokens. Without a configured token, health, schemas
and benchmark evidence still work; metrics return an explicit authorization
configuration error. Existing API issuer, signature, expiry and scope checks
remain in force. This adapter does not bypass or extend token authority.

Origins are set only at process startup. Only origin URLs are accepted: no user
information, query, fragment, or application path. HTTP is restricted to explicit
loopback hosts; other origins require HTTPS with the JVM's normal certificate
and hostname validation. Redirects are refused. Tool arguments cannot select a
URL or local file. For the recorded Render explorer use its normal login: it is
not a live collector/worker endpoint for this adapter.

## Bounds and failure behavior

Two dedicated read threads have at most eight waiting tasks in total. Calls have
a seven-second deadline including queue time; HTTP connect time is capped at one
second and each complete response, including its body, at three seconds. Responses
are limited to 32 KiB and inbound stdio messages to 65,536 characters. Saturation,
timeout, authorization failure, invalid response and oversize return bounded
errors without copying upstream bodies. Cancelling a call interrupts its read;
closing the process cancels HTTP work. The launcher caps Java heap at 128 MiB.

The MCP SDK's stdio transport assumes a cooperating local client; its internal
protocol queues are not an Internet admission boundary. Keep this process local
and let its client manage its lifecycle. A future remote MCP service would need
separate transport authentication, authorization, admission controls and a new
security review before deployment.

Stdout contains only JSON-RPC. Stderr records fixed tool names, outcome and elapsed
time, without JWTs, request arguments, response bodies or exception text. SDK
logging uses the no-op SLF4J provider to avoid raw-message diagnostics. The
launcher requires a prebuilt jar so a build cannot corrupt the protocol stream.

The standalone Maven project pins the official [MCP Java SDK 2.1.0](https://github.com/modelcontextprotocol/java-sdk/releases/tag/v2.1.0).
It is excluded from the backend's Maven modules and container image. Resources
include the repository's original Avro schemas and SLO profiles, rather than
separate editable copies. Benchmark evidence is explicitly tied to its measured
source and remains `release_ready: false` until the full fixed protocol passes.
Rebuild the adapter to pick up changed bundled evidence.

## Verification

CI builds the standalone project and exercises an actual packaged SDK server over
stdio: initialization, tool discovery, all four tools, schema validation,
authorization failures and token-file replacement. Real local HTTP peers verify
GET-only access, no credential on health requests, metric-field filtering,
redirect refusal, oversized and drip responses, deadline recovery, and an
oversized unterminated protocol line. Java tests exercise configured-origin
validation and bounded-scheduler overload/recovery. These are local adapter
contracts, not a remote MCP deployment or an independent security audit.
