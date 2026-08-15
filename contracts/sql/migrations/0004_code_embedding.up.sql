CREATE TABLE code_embedding (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain TEXT NOT NULL DEFAULT 'code',
    chunk_id UUID NOT NULL REFERENCES code_chunk(id),
    vector REAL[] NOT NULL,
    model_id TEXT NOT NULL,
    embedding_dim INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chunk_id, model_id)
);

-- outbox.aggregate_type era vincolato a ('CodeNode','CodeRelation','CodeChunk')
-- da 0003_code_chunk.up.sql: esteso per accettare anche 'CodeEmbedding'
-- (SPEC-030 §2), altrimenti l'INSERT outbox per un embedding violerebbe il
-- CHECK esistente.
ALTER TABLE outbox DROP CONSTRAINT outbox_aggregate_type_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_aggregate_type_check
    CHECK (aggregate_type IN ('CodeNode','CodeRelation','CodeChunk','CodeEmbedding'));
