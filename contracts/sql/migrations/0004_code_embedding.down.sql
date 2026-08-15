ALTER TABLE outbox DROP CONSTRAINT outbox_aggregate_type_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_aggregate_type_check
    CHECK (aggregate_type IN ('CodeNode','CodeRelation','CodeChunk'));
DROP TABLE code_embedding;
