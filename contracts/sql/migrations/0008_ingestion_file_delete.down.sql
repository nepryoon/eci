ALTER TABLE ingestion_command_receipt
    DROP CONSTRAINT ingestion_command_receipt_source_by_operation_check;

ALTER TABLE ingestion_command_receipt
    ALTER COLUMN source_sha256 SET NOT NULL;

ALTER TABLE ingestion_command_receipt
    DROP COLUMN operation;
