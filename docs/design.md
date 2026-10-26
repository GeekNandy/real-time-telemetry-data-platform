# Collector design and fault model

The collector derives tenant identity from a verified JWT, validates the event,
and acquires bounded global and per-tenant admission. Filesystem work runs on
bounded executor threads instead of the reactive HTTP event loop.

HTTP 202 follows a synchronized append to the collector's retained volume.
One dispatcher publishes entries in volume order to Kafka, waits for producer
acknowledgement and then persists a delivered checkpoint before reclamation.
An acknowledgement/checkpoint crash gap may publish a duplicate. Volume-local
ordering is not a global event order. Downstream processing must be replay-safe.

| Failure | Behavior |
| --- | --- |
| HTTP response is lost after append | Retry the same tenant/event identity and payload |
| Client cancels | Already-started durable work keeps its admission lease until settlement |
| Collector process is killed | Retained-volume entries are scanned and replayed |
| Kafka is unavailable | Entries remain buffered; a full budget rejects new events |
| Reclamation fails after checkpoint | Retry cleanup without stranding the head |
| Registry response stalls | The complete-response deadline bounds the request |
| Committed data is corrupt | Fail closed and preserve evidence |
| Collector volume is permanently lost before Kafka acknowledgement | Accepted events can be lost |

No transaction spans the filesystem and Kafka. The local-volume acceptance
contract does not establish storage replication or availability-zone survival.

Shared observability uses fixed-cardinality stage metrics and W3C trace context.
Credentials, baggage, payloads and tenant/event IDs are excluded from metric labels.
Production policy requires external-issuer JWT authentication and encrypted
Kafka/registry connections. Demo authentication is rejected in production mode.

Avro evolution and HTTP business schema versions are separate contracts. See
[schema management](schemas.md) and [persistent recovery](buffer.md).
