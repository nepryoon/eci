ALTER TABLE ingestion_command_receipt
    ADD COLUMN operation TEXT NOT NULL DEFAULT 'UPSERT'
        CHECK (operation IN ('UPSERT', 'DELETE'));

ALTER TABLE ingestion_command_receipt
    ALTER COLUMN source_sha256 DROP NOT NULL;

ALTER TABLE ingestion_command_receipt
    ADD CONSTRAINT ingestion_command_receipt_source_by_operation_check
    CHECK (
        (operation = 'UPSERT' AND source_sha256 IS NOT NULL)
        OR (operation = 'DELETE' AND source_sha256 IS NULL)
    );

-- A canonical, monotonic event coordinate survives publication to consumer
-- retry topics. Consumers use it to prevent an older retried UPSERT from
-- recreating a projection after a later DELETE has completed.
ALTER TABLE outbox
    ADD COLUMN event_sequence BIGINT GENERATED ALWAYS AS IDENTITY;

CREATE UNIQUE INDEX outbox_event_sequence_idx
    ON outbox (event_sequence);

CREATE TABLE consumer_projection_watermark (
    consumer_name  TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    event_sequence BIGINT NOT NULL CHECK (event_sequence > 0),
    operation      TEXT NOT NULL CHECK (operation IN ('UPSERT', 'DELETE')),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (consumer_name, aggregate_type, aggregate_id)
);
