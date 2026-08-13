CREATE TABLE lineage (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    old_node_id CHAR(64) NOT NULL,
    new_node_id CHAR(64) NOT NULL,
    ast_hash CHAR(64) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason TEXT NOT NULL DEFAULT 'rename'
);
CREATE INDEX idx_lineage_old_node_id ON lineage(old_node_id);
CREATE INDEX idx_lineage_new_node_id ON lineage(new_node_id);
