// ============================================================
// CONSTRAINTS (uniqueness + existence) — Neo4j 5.x
// ============================================================
CREATE CONSTRAINT code_node_id IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.id IS UNIQUE;

CREATE CONSTRAINT code_node_id_exists IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.id IS NOT NULL;

CREATE CONSTRAINT code_node_domain_exists IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.domain IS NOT NULL;

CREATE CONSTRAINT file_path_unique IF NOT EXISTS
FOR (f:File) REQUIRE (f.repo, f.path) IS UNIQUE;

CREATE CONSTRAINT method_symbol_unique IF NOT EXISTS
FOR (m:Method) REQUIRE m.symbol_id IS UNIQUE;

// Native Vector type + dimension constraint (Cypher 25 / Neo4j 5.x)
CREATE CONSTRAINT emb_vector_type IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.embedding IS :: VECTOR<FLOAT32>(1536);

// ============================================================
// RANGE INDEXES
// ============================================================
CREATE RANGE INDEX code_node_ast_hash IF NOT EXISTS
FOR (n:CodeNode) ON (n.ast_hash);

CREATE RANGE INDEX code_node_domain IF NOT EXISTS
FOR (n:CodeNode) ON (n.domain);

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

// ============================================================
// NODE UPSERT (idempotent MERGE) — sink consumer
// ============================================================
MERGE (m:CodeNode:Method { id: $id })
  ON CREATE SET m.name = $name, m.ast_hash = $ast_hash,
                m.domain = 'code', m.node_type = 'Method',
                m.symbol_id = $symbol_id, m.signature = $signature,
                m.version = $version, m.is_current = true
  ON MATCH  SET m.ast_hash = $ast_hash, m.signature = $signature,
                m.version = $version, m.is_current = true;

// ============================================================
// TYPED RELATIONSHIPS (idempotent) — codebase topology
// ============================================================
MATCH (caller:Method { id: $caller_id })
MATCH (callee:Method { id: $callee_id })
MERGE (caller)-[r:CALLS]->(callee)
  ON CREATE SET r.commit_sha = $commit_sha, r.weight = 1
  ON MATCH  SET r.weight = coalesce(r.weight, 0) + 1;

MATCH (f:File { id: $file_id })
MATCH (c:Class { id: $class_id })
MERGE (f)-[:CONTAINS]->(c);

MATCH (a:Module { id: $from_mod })
MATCH (b:Module { id: $to_mod })
MERGE (a)-[:IMPORTS]->(b);

MATCH (sub:Class { id: $sub_id })
MATCH (sup:Class { id: $sup_id })
MERGE (sub)-[:EXTENDS]->(sup);

MATCH (impl:Class { id: $impl_id })
MATCH (ifc:Interface { id: $ifc_id })
MERGE (impl)-[:IMPLEMENTS]->(ifc);

MATCH (x:Module { id: $x_id })
MATCH (y:Module { id: $y_id })
MERGE (x)-[:DEPENDS_ON]->(y);

// ============================================================
// EXTENSIBILITY: Doc / Legal / Compliance domains
// ============================================================
MERGE (d:CodeNode:Document { id: $doc_id })
  ON CREATE SET d.domain = 'doc', d.node_type = 'Document', d.title = $title;

MERGE (cl:CodeNode:Clause { id: $clause_id })
  ON CREATE SET cl.domain = 'legal', cl.node_type = 'Clause',
                cl.jurisdiction = $jur;

// Cross-domain: un Method GOVERNED_BY una Clause di compliance
MATCH (m:Method { id: $m_id })
MATCH (cl:Clause { id: $clause_id })
MERGE (m)-[:GOVERNED_BY]->(cl);

// Provenance/versioning: nodo DERIVED_FROM la versione precedente
MATCH (curr:CodeNode { id: $curr_id })
MATCH (prev:CodeNode { id: $prev_id })
MERGE (curr)-[:DERIVED_FROM]->(prev);

// ============================================================
// VECTOR QUERY (Cypher 25 SEARCH clause) — hybrid retrieval seed
// ============================================================
// MATCH (n:CodeNode) SEARCH n IN (
//   VECTOR INDEX code_embeddings FOR $queryVector LIMIT 20
// ) SCORE score
// RETURN n.id, n.name, score ORDER BY score DESC;
// modifica di prova per testare guard in CI
