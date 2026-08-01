# ADR-0004 — Rimozione dei property existence constraint da schema.cypher

Stato: accettata · Data: 2026-08-01

## Contesto
`schema.cypher` (D3) includeva due property existence constraint,
`code_node_id_exists` (`n.id IS NOT NULL`) e `code_node_domain_exists`
(`n.domain IS NOT NULL`), su `:CodeNode`. Eseguendo `TestMigrationAgainstRealNeo4j`
contro un'istanza `neo4j:5-community` (immagine usata dal test di integrazione
di SPEC-004), la creazione di entrambi i constraint fallisce con errore
Neo4j: "Property existence constraint requires Neo4j Enterprise Edition" — i
property existence constraint sono una feature Enterprise-only, non
disponibile in Community Edition.

## Decisione
I due existence constraint sono rimossi da `contracts/cypher/schema.cypher`.
Restano invariati `code_node_id` (UNIQUE), `file_path_unique`,
`method_symbol_unique`, `emb_vector_type`, e tutti gli indici range/fulltext/
vector.

L'enforcement di `id`/`domain` non-null su `:CodeNode` resta garantito a
monte: PostgreSQL (source of truth) e la validazione Pydantic/Go di SPEC-003
impediscono che un `CodeNode` privo di `id` o `domain` venga prodotto, prima
che qualunque dato raggiunga Neo4j. Nell'architettura ADD, Neo4j è una vista
materializzata ricostruibile dai sink, non il source of truth — non deve
duplicare invarianti già garantiti a monte.

## Conseguenze
Neo4j Community Edition resta sufficiente per lo sviluppo fino alla Fase 6
(RBAC fine-grained e multi-database), come originariamente previsto da
SPEC-004 §6. Nessun impatto sul runner di migrazione (`tools/migrate-neo4j`)
oltre alla lista di constraint attesi, aggiornata in SPEC-004 §3 e nei test
di integrazione.
