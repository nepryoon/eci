// Costruzione delle query Cypher di MERGE per CodeNode/CodeRelation
// (SPEC-015 §2). Neo4j non supporta label/tipi di relazione parametrizzati
// — vanno interpolati come testo nella query — quindi ENTRAMBI i valori
// che finiscono interpolati (node_type -> label, rel_type -> tipo
// relazione) sono validati contro un enum whitelist esplicito PRIMA di
// costruire la stringa Cypher. Un valore non presente nella whitelist non
// raggiunge mai `fmt.Sprintf`/concatenazione: le funzioni ritornano un
// errore e il chiamante non esegue nessuna query.
package consumer

import "fmt"

// allowedNodeTypes — whitelist deliberatamente più stretta dell'enum D2
// completo (che include anche Module/Package/Parameter/Field/CallSite,
// vedi libs/go/eci/models.CodeExtension): SPEC-015 §2 la restringe
// esplicitamente ai soli node_type che T1.1 (SPEC-013) può effettivamente
// produrre.
var allowedNodeTypes = map[string]bool{
	"File":      true,
	"Class":     true,
	"Interface": true,
	"Method":    true,
	"Function":  true,
}

// allowedRelTypes — stesso enum del CHECK constraint code_relation.rel_type
// di SPEC-005 (contracts/sql/migrations/0001_init.up.sql), non solo i due
// valori (CONTAINS, CALLS) che T1.1 produce oggi: un valore fuori da QUESTO
// enum non può mai comparire in un CodeRelation valido per costruzione, a
// prescindere da quali rel_type la pipeline a monte emette oggi.
var allowedRelTypes = map[string]bool{
	"CALLS":        true,
	"IMPORTS":      true,
	"EXTENDS":      true,
	"IMPLEMENTS":   true,
	"CONTAINS":     true,
	"DEPENDS_ON":   true,
	"REFERENCES":   true,
	"OVERRIDES":    true,
	"DERIVED_FROM": true,
	"GOVERNED_BY":  true,
	"CITES":        true,
}

// Tutte le mutation sink e il write-back GDS seguono lo stesso ordine totale:
// CodeNode (per id) -> GDSPartition (per scope). Il primo statement viene
// consumato nella stessa explicit transaction della mutation, quindi il lock
// resta acquisito mentre lo scope precedente viene letto. Questo impedisce di
// usare uno snapshot pre-lock durante update di ownership concorrenti.
const lockCodeNodeQuery = `// sink-graph-lock-code-node
MERGE (n:CodeNode {id: $id})
SET n._eci_write_lock = coalesce(n._eci_write_lock, 0) + 1
REMOVE n._eci_write_lock
RETURN n.id AS id`

const lockCodeRelationEndpointsQuery = `// sink-graph-lock-relation-endpoints
UNWIND [$from_id, $to_id] AS endpoint_id
WITH DISTINCT endpoint_id
ORDER BY endpoint_id
MERGE (endpoint:CodeNode {id: endpoint_id})
SET endpoint._eci_write_lock = coalesce(endpoint._eci_write_lock, 0) + 1
REMOVE endpoint._eci_write_lock
RETURN count(endpoint) AS locked`

// Delete locks never MERGE: a replay after projection removal must not
// recreate placeholder nodes merely to serialize a no-op tombstone.
const lockDeleteCodeNodeQuery = `// sink-graph-lock-delete-code-node
MATCH (n:CodeNode {id: $id})
SET n._eci_write_lock = coalesce(n._eci_write_lock, 0) + 1
REMOVE n._eci_write_lock
RETURN n.id AS id`

const checkDeleteCodeNodeScopeQuery = `MATCH (n:CodeNode {id: $id})
RETURN n.tenant_id = $tenant_id AND n.repo = $repo
  AND n.acl_group = $acl_group AND n.path = $path AS scope_matches`

const lockDeleteRelationEndpointsQuery = `// sink-graph-lock-delete-relation-endpoints
UNWIND [$from_id, $to_id] AS endpoint_id
WITH DISTINCT endpoint_id
ORDER BY endpoint_id
MATCH (endpoint:CodeNode {id: endpoint_id})
SET endpoint._eci_write_lock = coalesce(endpoint._eci_write_lock, 0) + 1
REMOVE endpoint._eci_write_lock
RETURN count(endpoint) AS locked`

// A node tombstone checks the complete canonical scope before deleting. Every
// adjacent endpoint partition is invalidated because DETACH DELETE changes its
// reachability too. No MATCH means a replay is an external no-op.
const deleteCodeNodeQuery = `MATCH (n:CodeNode {id: $id})
WHERE n.tenant_id = $tenant_id AND n.repo = $repo
  AND n.acl_group = $acl_group AND n.path = $path
OPTIONAL MATCH (n)--(neighbor:CodeNode)
WITH n, collect(DISTINCT {
  tenant_id: neighbor.tenant_id, repo: neighbor.repo, acl_group: neighbor.acl_group
}) + [{tenant_id: n.tenant_id, repo: n.repo, acl_group: n.acl_group}] AS scopes
UNWIND scopes AS scope
WITH DISTINCT n, scope.tenant_id AS scope_tenant_id,
  scope.repo AS scope_repo, scope.acl_group AS scope_acl_group
ORDER BY scope_tenant_id, scope_repo, scope_acl_group
FOREACH (_ IN CASE WHEN scope_tenant_id IS NOT NULL AND scope_repo IS NOT NULL
  AND scope_acl_group IS NOT NULL THEN [1] ELSE [] END |
  MERGE (p:GDSPartition {tenant_id: scope_tenant_id, repo: scope_repo, acl_group: scope_acl_group})
  ON CREATE SET p.generation = 1, p.write_lock = 0
  ON MATCH SET p.generation = coalesce(p.generation, 0) + 1
)
WITH DISTINCT n
DETACH DELETE n`

func deleteCodeRelationQuery(relType string) (string, error) {
	if !allowedRelTypes[relType] {
		return "", fmt.Errorf("rel_type non valido")
	}
	return "MATCH (from:CodeNode {id: $from_id})\n" +
		"MATCH (to:CodeNode {id: $to_id})\n" +
		"MATCH (from)-[r:" + relType + "]->(to)\n" +
		"WITH from, to, collect(r) AS relationships, [\n" +
		"  {tenant_id: from.tenant_id, repo: from.repo, acl_group: from.acl_group},\n" +
		"  {tenant_id: to.tenant_id, repo: to.repo, acl_group: to.acl_group}\n" +
		"] AS scopes\n" +
		"UNWIND scopes AS scope\n" +
		"WITH DISTINCT relationships, scope.tenant_id AS scope_tenant_id,\n" +
		"  scope.repo AS scope_repo, scope.acl_group AS scope_acl_group\n" +
		"ORDER BY scope_tenant_id, scope_repo, scope_acl_group\n" +
		"MERGE (p:GDSPartition {tenant_id: scope_tenant_id, repo: scope_repo, acl_group: scope_acl_group})\n" +
		"ON CREATE SET p.generation = 1, p.write_lock = 0\n" +
		"ON MATCH SET p.generation = coalesce(p.generation, 0) + 1\n" +
		"WITH DISTINCT relationships\n" +
		"FOREACH (rel IN relationships | DELETE rel)", nil
}

// mergeCodeNodeQuery costruisce la query di MERGE per un CodeNode (SPEC-015
// §2). I tre frammenti Cypher illustrativi della SPEC (query base + "SOLO
// per Method" + "SOLO per File") sono unificati qui in una singola query
// per messaggio: per node_type="File" la label specifica (via `{Label}`,
// qui `nodeType` già validato) produce già `SET n:File`, che è esattamente
// il contenuto del terzo blocco della SPEC — ripeterlo come query separata
// sarebbe stato un MERGE ridondante sullo stesso nodo, non un effetto
// diverso (vedi SPEC-015 §10). Per node_type="Method" si aggiunge la
// property `symbol_id` richiesta dal constraint `method_symbol_unique`.
func mergeCodeNodeQuery(nodeType string) (string, error) {
	if !allowedNodeTypes[nodeType] {
		return "", fmt.Errorf(
			"node_type non valido: %q (atteso uno tra File, Class, Interface, Method, Function)",
			nodeType,
		)
	}

	// lockCodeNodeQuery ha già creato/bloccato n nella stessa transazione. Lo
	// scope viene letto solo ora, dopo l'acquisizione del lock, quindi due move
	// concorrenti non possono invalidare una partizione precedente obsoleta.
	unchanged := "n:" + nodeType +
		" AND coalesce(n.domain = $domain, false)" +
		" AND coalesce(n.name = $name, false)" +
		" AND coalesce(n.ast_hash = $ast_hash, false)" +
		" AND coalesce(n.tenant_id = $tenant_id, false)" +
		" AND coalesce(n.repo = $repo, false)" +
		" AND coalesce(n.acl_group = $acl_group, false)" +
		" AND coalesce(n.path = $path, false)"
	if nodeType == "Method" {
		unchanged += " AND coalesce(n.symbol_id = $id, false)"
	}

	query := "MATCH (n:CodeNode {id: $id})\n" +
		"WITH n, NOT (" + unchanged + ") AS changed,\n" +
		"     n.tenant_id AS old_tenant_id, n.repo AS old_repo, n.acl_group AS old_acl_group\n" +
		"WITH n, changed, [\n" +
		"  {tenant_id: old_tenant_id, repo: old_repo, acl_group: old_acl_group},\n" +
		"  {tenant_id: $tenant_id, repo: $repo, acl_group: $acl_group}\n" +
		"] AS scopes\n" +
		"UNWIND scopes AS scope\n" +
		"WITH DISTINCT n, changed, scope.tenant_id AS scope_tenant_id, scope.repo AS scope_repo, scope.acl_group AS scope_acl_group\n" +
		"ORDER BY scope_tenant_id, scope_repo, scope_acl_group\n" +
		"FOREACH (_ IN CASE WHEN scope_tenant_id IS NOT NULL AND scope_repo IS NOT NULL AND scope_acl_group IS NOT NULL THEN [1] ELSE [] END |\n" +
		"  MERGE (p:GDSPartition {tenant_id: scope_tenant_id, repo: scope_repo, acl_group: scope_acl_group})\n" +
		"  ON CREATE SET p.generation = 1, p.write_lock = 0\n" +
		"  ON MATCH SET p.generation = CASE WHEN changed THEN coalesce(p.generation, 0) + 1 ELSE p.generation END\n" +
		")\n" +
		"WITH DISTINCT n\n" +
		"SET n:" + nodeType + ", n.domain = $domain, n.name = $name, n.ast_hash = $ast_hash,\n" +
		"    n.tenant_id = $tenant_id, n.repo = $repo, n.acl_group = $acl_group, n.path = $path"

	if nodeType == "Method" {
		// constraint method_symbol_unique (schema.cypher, D3): symbol_id =
		// id (semplificazione dichiarata, SPEC-015 §2/§5 — non un vero
		// identificatore di simbolo stabile alla LSP/SCIP).
		query += "\nSET n.symbol_id = $id"
	}

	return query, nil
}

// mergeCodeRelationQuery costruisce la query di MERGE per una CodeRelation
// (SPEC-015 §2): endpoint creati/riusati con la sola etichetta generica
// :CodeNode (mai una label specifica — non è garantito sapere il tipo
// reale a questo punto del consumo, vedi commento in §2). Il payload contiene
// il peso già aggregato dal parser: SET assoluto rende il MERGE idempotente
// anche se un crash avviene dopo Neo4j ma prima del marker PostgreSQL
// (ADR-0022).
// Ogni mutazione effettiva della topologia incrementa inoltre la generation di
// entrambe le partizioni endpoint: un singolo edge può cambiare lo score di
// qualunque nodo della proiezione, non soltanto dei suoi estremi (ADR-0015).
// Un MERGE identico dopo un marker PostgreSQL fallito lascia invece invariata
// la generation (ADR-0022).
func mergeCodeRelationQuery(relType string) (string, error) {
	if !allowedRelTypes[relType] {
		return "", fmt.Errorf(
			"rel_type non valido: %q (atteso uno dei valori del CHECK constraint code_relation.rel_type, SPEC-005)",
			relType,
		)
	}

	// Gli endpoint sono già stati creati e bloccati, in ordine di id, da
	// lockCodeRelationEndpointsQuery nella stessa transazione. Le label vengono
	// quindi lette dalla versione committed più recente e non da uno snapshot
	// precedente al lock.
	query := "MATCH (from:CodeNode {id: $from_id})\n" +
		"MATCH (to:CodeNode {id: $to_id})\n" +
		"OPTIONAL MATCH (from)-[existing:" + relType + "]->(to)\n" +
		"WITH from, to, NOT (existing IS NOT NULL AND coalesce(existing.weight = coalesce($weight, 1), false)) AS changed,\n" +
		"     from.tenant_id AS from_tenant_id, from.repo AS from_repo, from.acl_group AS from_acl_group,\n" +
		"     to.tenant_id AS to_tenant_id, to.repo AS to_repo, to.acl_group AS to_acl_group\n" +
		"WITH from, to, changed, [\n" +
		"  {tenant_id: from_tenant_id, repo: from_repo, acl_group: from_acl_group},\n" +
		"  {tenant_id: to_tenant_id, repo: to_repo, acl_group: to_acl_group}\n" +
		"] AS scopes\n" +
		"UNWIND scopes AS scope\n" +
		"WITH DISTINCT from, to, changed, scope.tenant_id AS scope_tenant_id, scope.repo AS scope_repo, scope.acl_group AS scope_acl_group\n" +
		"ORDER BY scope_tenant_id, scope_repo, scope_acl_group\n" +
		"FOREACH (_ IN CASE WHEN scope_tenant_id IS NOT NULL AND scope_repo IS NOT NULL AND scope_acl_group IS NOT NULL THEN [1] ELSE [] END |\n" +
		"  MERGE (p:GDSPartition {tenant_id: scope_tenant_id, repo: scope_repo, acl_group: scope_acl_group})\n" +
		"  ON CREATE SET p.generation = 1, p.write_lock = 0\n" +
		"  ON MATCH SET p.generation = CASE WHEN changed THEN coalesce(p.generation, 0) + 1 ELSE p.generation END\n" +
		")\n" +
		"WITH DISTINCT from, to\n" +
		"MERGE (from)-[r:" + relType + "]->(to)\n" +
		"SET r.weight = coalesce($weight, 1)"

	return query, nil
}
