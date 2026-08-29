// ============================================================
// CONSTRAINTS (uniqueness + existence) — Neo4j 5.x
// ============================================================
CREATE CONSTRAINT code_node_id IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT file_path_unique IF NOT EXISTS
FOR (f:File) REQUIRE (f.repo, f.path) IS UNIQUE;

CREATE CONSTRAINT method_symbol_unique IF NOT EXISTS
FOR (m:Method) REQUIRE m.symbol_id IS UNIQUE;

// ============================================================
// RANGE INDEXES
// ============================================================
CREATE RANGE INDEX code_node_ast_hash IF NOT EXISTS
FOR (n:CodeNode) ON (n.ast_hash);

CREATE RANGE INDEX code_node_domain IF NOT EXISTS
FOR (n:CodeNode) ON (n.domain);

CREATE RANGE INDEX code_node_security_scope IF NOT EXISTS
FOR (n:CodeNode) ON (n.tenant_id, n.repo, n.acl_group);

CREATE RANGE INDEX method_name IF NOT EXISTS
FOR (m:Method) ON (m.name);

CREATE RANGE INDEX rel_commit IF NOT EXISTS
FOR ()-[r:CALLS]-() ON (r.commit_sha);

// ============================================================
// FULL-TEXT INDEXES (Apache Lucene)
// ============================================================
CREATE FULLTEXT INDEX code_fulltext IF NOT EXISTS
FOR (n:Method|Function|Class|File) ON EACH [n.name, n.signature, n.source_text];

CREATE FULLTEXT INDEX doc_fulltext IF NOT EXISTS
FOR (n:Document|Section) ON EACH [n.title, n.body];

// ============================================================
// NATIVE VECTOR INDEX (Neo4j 5.x)
// ============================================================
CREATE VECTOR INDEX code_embeddings IF NOT EXISTS
FOR (n:CodeNode) ON (n.embedding)
OPTIONS { indexConfig: {
  `vector.dimensions`: 1536,
  `vector.similarity_function`: 'cosine'
}};
