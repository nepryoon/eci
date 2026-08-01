CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- per gen_random_uuid()

CREATE TABLE code_node (
  id              TEXT PRIMARY KEY,
  domain          TEXT NOT NULL CHECK (domain IN ('code','doc','legal','compliance','contract')),
  node_type       TEXT NOT NULL,
  name            TEXT NOT NULL,
  ast_hash        CHAR(64) NOT NULL,
  content_hash    CHAR(64),
  embedding_ref   JSONB,
  provenance      JSONB NOT NULL,
  version         INTEGER NOT NULL DEFAULT 1,
  is_current      BOOLEAN NOT NULL DEFAULT TRUE,
  valid_from      TIMESTAMPTZ NOT NULL DEFAULT now(),
  valid_to        TIMESTAMPTZ,
  supersedes      TEXT REFERENCES code_node(id),
  ext             JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_code_node_ast_hash ON code_node(ast_hash);
CREATE INDEX idx_code_node_domain   ON code_node(domain);

CREATE TABLE code_relation (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  domain      TEXT NOT NULL CHECK (domain IN ('code','doc','legal','compliance','contract')),
  rel_type    TEXT NOT NULL CHECK (rel_type IN (
                'CALLS','IMPORTS','EXTENDS','IMPLEMENTS','CONTAINS','DEPENDS_ON',
                'REFERENCES','OVERRIDES','DERIVED_FROM','GOVERNED_BY','CITES')),
  from_id     TEXT NOT NULL REFERENCES code_node(id),
  to_id       TEXT NOT NULL REFERENCES code_node(id),
  weight      NUMERIC,
  provenance  JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_code_relation_from ON code_relation(from_id);
CREATE INDEX idx_code_relation_to   ON code_relation(to_id);

CREATE TABLE outbox (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type  TEXT NOT NULL CHECK (aggregate_type IN ('CodeNode','CodeRelation')),
  aggregate_id    TEXT NOT NULL,
  event_type      TEXT NOT NULL CHECK (event_type IN ('UPSERT','DELETE')),
  payload         JSONB NOT NULL,
  trace_id        TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_created_at ON outbox(created_at);

CREATE TABLE processed_events (
  event_id       UUID PRIMARY KEY,
  consumer_name  TEXT NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
