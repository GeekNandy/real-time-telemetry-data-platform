CREATE TABLE telemetry_events (
    tenant_id VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    occurred_at TIMESTAMPTZ NOT NULL,
    attributes JSONB NOT NULL,
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint) = 32),
    stored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, event_id)
);
CREATE INDEX telemetry_events_tenant_time ON telemetry_events (tenant_id, occurred_at DESC);
