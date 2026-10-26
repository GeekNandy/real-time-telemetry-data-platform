# Event-schema and registry contracts

The Confluent-compatible wire envelope contains a magic byte, schema ID and Avro
record. Bundled v2 adds an optional field with a default and is compatible with
v1 in both reader directions. This is separate from business schemaVersion 1.

Writers preload bundled schema IDs. Registration is explicit and disabled by
default. Unknown IDs are looked up through a bounded cache. An unavailable
registry remains a retryable infrastructure failure; invalid event envelopes are
classified separately.

Each registry request has a three-second deadline for the complete HTTP response,
including its body. A subscriber accepts at most 64 KiB and cancels oversized
publishers. Timeout or interruption cancels both subscription and exchange.
Response bodies and authorization values are not included in error messages.

Tests exercise actual loopback HTTP transport, including a response that sends
headers and one body byte then stalls, both chunked and fixed-length oversized
bodies, the exact size boundary and retryable HTTP failures. Schema tests cover
compatible evolution, wire framing and event validation.

```bash
./mvnw --batch-mode --no-transfer-progress verify
```
