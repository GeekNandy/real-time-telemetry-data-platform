# Recorded-run explorer

The hosted explorer presents captured requests from a successful verification run
against real Kafka, Schema Registry and PostgreSQL dependencies. The repository's
Website link opens the deployed explorer.

The environment selector switches between Compose and Kubernetes records.
Individual events expose their HTTP receipt and observed query-visibility bound.
The Evidence view links the workflow, source revision and executed merge revision.
Recorded client timings include network, polling and sweep delays.

The hosted explorer requires a review account. All accounts provide read-only
access to the same captured run. Live event submission and operational metrics
are available in the [local operator console](console.md).

## Build

Requires Node.js 20.19 or later. The static build has no dependency-install step:

    node console/build-showcase.mjs

On Render, use a Free Node Web Service with the build command above and
the start command:

    node console/showcase-server.mjs

Deploy the checked main revision after the required CI gates pass. Disable
automatic deploys and PR previews. The service binds 0.0.0.0 on Render's PORT.
It needs no database, persistent disk or backend pipeline.

Configure SHOWCASE_USERS_JSON in Render Environment. Its value is an array of
username, salt and hash records produced by credentialRecord(username, password)
in console/showcase-access.js. Generate long random passwords and share them
privately. Store no deployed credentials or hashes in GitHub or build artifacts.
The server refuses to start without valid access configuration.

## Access boundary

The server checks an expiring session before serving the index, scripts, styles
or recorded-run.json. Password verification uses salted scrypt; login work and
attempts are bounded. Sessions use random tokens in host-only Secure, HttpOnly,
SameSite=Strict cookies. Sign-out revokes the session. Sessions expire after
eight hours and are lost on restart or idle spin-down.

Responses prohibit caching and framing. Login and logout require a matching
request origin; there is no public registration or event-write endpoint.
Authentication restricts the hosted viewer. The source repository and its
verification evidence remain public; it is not a claim of comprehensive attack
prevention.

A static entry URL can point to the protected service by setting
SHOWCASE_REDIRECT_URL to that service's HTTPS origin during its build. This mode
publishes only an entry page, with no explorer assets or recording.

The build verifies SHA-256 digests for the original reports under
evidence/hosted-run-2026-10-27 before assembling the public recording.
The explorer bounds the recording read to 64 KiB and uses same-origin assets.
The separate sign-in form sends credentials only to the access server. The
viewer sends no event-write requests and stores no credentials in browser storage.

Twenty-event smokes establish correctness for the recorded revision and
environment. They do not establish pipeline capacity or hosted backend availability.
