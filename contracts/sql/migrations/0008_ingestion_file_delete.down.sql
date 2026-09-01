DROP TABLE consumer_projection_watermark;

ALTER TABLE outbox
    DROP COLUMN event_sequence;

ALTER TABLE ingestion_command_receipt
    DROP CONSTRAINT ingestion_command_receipt_source_by_operation_check;

-- The UPSERT-only schema has no honest representation for a completed DELETE:
-- fabricating a source digest would corrupt receipt conflict semantics. Runtime
-- rollback therefore discards only DELETE receipts; canonical deletions remain
-- deleted and a later authorized UPSERT creates a new representable receipt.
DELETE FROM ingestion_command_receipt
WHERE operation = 'DELETE';

ALTER TABLE ingestion_command_receipt
    ALTER COLUMN source_sha256 SET NOT NULL;

ALTER TABLE ingestion_command_receipt
    DROP COLUMN operation;
