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
