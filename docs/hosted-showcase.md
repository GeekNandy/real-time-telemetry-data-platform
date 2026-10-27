# Recorded-run explorer

The hosted explorer presents captured requests from a successful verification run
against real Kafka, Schema Registry and PostgreSQL dependencies. The repository's
Website link opens the deployed explorer.

The environment selector switches between Compose and Kubernetes records.
Individual events expose their HTTP receipt and observed query-visibility bound.
The Evidence view links the workflow, source revision and executed merge revision.
Recorded client timings include network, polling and sweep delays.

The explorer serves static assets. Live event submission and operational metrics
are available in the [local operator console](console.md).

## Build

Requires Node.js 20.19 or later. The static build has no dependency-install step:

    node console/build-showcase.mjs

Publish the generated console/dist directory. On Render, use a Static Site,
deploy the main branch, set the build command above and disable automatic deploys.
Deploy the checked main revision explicitly after the required CI gates pass.

The build verifies SHA-256 digests for the original reports under
evidence/hosted-run-2026-10-27 before assembling the public recording.
The browser bounds the recording read to 64 KiB, uses same-origin assets and
does not accept credentials or send event-write requests.

Twenty-event smokes establish correctness for the recorded revision and
environment. They do not establish pipeline capacity or hosted backend availability.
