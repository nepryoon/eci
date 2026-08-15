CREATE TABLE code_chunk (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT NOT NULL DEFAULT 'code',
    entity_id TEXT NOT NULL REFERENCES code_node(id),
    chunk_index INT NOT NULL,
    text TEXT NOT NULL,
    char_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_id, chunk_index)
);
CREATE INDEX idx_code_chunk_entity_id ON code_chunk(entity_id);

-- outbox.aggregate_type era vincolato a ('CodeNode','CodeRelation') da
-- 0001_init.up.sql: esteso per accettare anche 'CodeChunk' (SPEC-029 §2),
-- altrimenti l'INSERT outbox per un chunk violerebbe il CHECK esistente.
ALTER TABLE outbox DROP CONSTRAINT outbox_aggregate_type_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_aggregate_type_check
    CHECK (aggregate_type IN ('CodeNode','CodeRelation','CodeChunk'));
