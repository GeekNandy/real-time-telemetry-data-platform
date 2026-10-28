# Security and privacy boundaries

The public Render site is an authenticated recorded-run viewer. Its three accounts
and private, expiring sessions protect that viewer. It does not expose Kafka,
PostgreSQL or a live ingestion deployment. A password does not establish denial
of service resistance; no attack-proof claim is made.

## Live API identities

Production uses OAuth 2.0 resource-server JWT verification with HTTPS OIDC
discovery and RS256 signing keys. Issuer, audience, expiry and a bounded tenant
claim are mandatory. `telemetry.write`, `telemetry.read` and `telemetry.observe`
are separate authorities. Request bodies cannot select a tenant; database queries
include the verified tenant. Default response headers prohibit caching event/API
responses. Local HS256 demo mode requires an explicit 32-byte minimum key and is
not a production identity provider.

Discovery/JWKS requests use a dedicated four-connection pool with eight queued
acquisitions, a 500 ms acquisition limit, a one-second connect limit, a three-second
whole-response deadline and a 64 KiB decoded-body limit. Bytes trickling in do not
extend the absolute deadline. Redirects are disabled. By default all discovery and
JWKS URLs must have the issuer's HTTPS origin. A provider with a separate signing
key origin requires an explicit comma-separated `JWT_TRUSTED_JWKS_ORIGINS` list
of HTTPS origins, with no paths, credentials, queries or fragments. Network egress
policy must allow those same trusted endpoints. Certificate-chain and hostname
checks remain enabled; private CAs belong in managed trust material.

`OidcHttpsContractTest` runs a real HTTPS discovery/key server, real RSA signatures
and a real Netty resource-server HTTP endpoint. It verifies rotation, unknown keys,
issuer/audience/tenant/expiry/not-before rejection, certificate and SAN rejection,
foreign discovery/JWKS and redirect rejection, oversized and continuously streamed
responses, scopes and no-store headers. Fixtures use ephemeral private keys and
synthetic events. Credentials and complete tokens are not emitted in evidence.

## Data inventory and retention

| Copy | Protection and lifetime | Operator action |
| --- | --- | --- |
| Collector spool | Private process-owned directory; bounded bytes/records; reclaimed after confirmed Kafka publication | Encrypt backing storage; preserve/reconcile retained volumes during outages |
| Kafka source and DLT | Production TLS, RF3/minISR2; seven-day default retention | Apply broker identities/ACLs, quotas, private networking and deletion policy; DLT contains original data |
| PostgreSQL events | Tenant-qualified access, idempotent identities; bounded 30-day storage-time retention sweep | Require verify-full TLS, restricted credentials, encrypted storage and tested backups |
| Traces and metrics | Bounded labels and W3C trace context; no Authorization, baggage, event IDs or payload attributes | Control collector/exporter access and retention; review any instrumentation changes |
| Backups and snapshots | Separate from application retention | Inventory access/expiry, encryption and restore reconciliation before live data use |

The event allowlist and string limits reduce collection; an allowed value can
still contain personal data. Demonstrations use synthetic content. Individual
erasure must cover every copy, replay source and restore path. Deleting one row
alone can allow a later replay to restore it. This project does not claim to
implement an organization-wide erasure service or establish GDPR compliance.
A real deployment needs a data owner, purpose/lawful basis, region/retention
agreement, request handling and legal review appropriate to its use.

## Network and operational controls

Production charts require pinned image digests, TLS Kafka, HTTPS registry/issuer,
verify-full database TLS and explicit network peers. Application pods run as a
non-root user with a read-only root filesystem, dropped capabilities, runtime
seccomp and no service-account token. An enforcing CNI is required; a rendered
NetworkPolicy alone proves nothing. The Kubernetes CI exercise checks allowed
and denied peers, rollout/rollback and reconciliation. AWS infrastructure tests
use mocked providers and do not establish a deployed or operated EKS service.

Remaining deployment responsibilities include ingress rate limits/WAF as needed,
issuer/client and broker ACL lifecycle, key/secret rotation, vulnerability response,
backup/restore drills, alert routing and an on-call owner. These boundaries are
visible so an operator can make informed deployment decisions.
