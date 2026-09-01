CREATE TABLE ingestion_command_receipt (
    command_id UUID PRIMARY KEY,
    fingerprint CHAR(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    tenant_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    commit_sha CHAR(40) NOT NULL CHECK (commit_sha ~ '^[0-9a-f]{40}$'),
    path TEXT NOT NULL,
    source_sha256 CHAR(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE INDEX ingestion_command_receipt_scope_idx
    ON ingestion_command_receipt (tenant_id, repository, completed_at DESC);
