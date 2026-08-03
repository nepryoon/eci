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

	query := "MERGE (n:CodeNode {id: $id})\n" +
		"SET n:" + nodeType + ", n.domain = $domain, n.name = $name, n.ast_hash = $ast_hash,\n" +
		"    n.repo = $repo, n.path = $path"

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
// reale a questo punto del consumo, vedi commento in §2), weight
// aggregato con lo stesso pattern del Cypher di riferimento D3 (SPEC-004).
func mergeCodeRelationQuery(relType string) (string, error) {
	if !allowedRelTypes[relType] {
		return "", fmt.Errorf(
			"rel_type non valido: %q (atteso uno dei valori del CHECK constraint code_relation.rel_type, SPEC-005)",
			relType,
		)
	}

	query := "MERGE (from:CodeNode {id: $from_id})\n" +
		"MERGE (to:CodeNode {id: $to_id})\n" +
		"MERGE (from)-[r:" + relType + "]->(to)\n" +
		"ON CREATE SET r.weight = coalesce($weight, 1)\n" +
		"ON MATCH SET r.weight = coalesce(r.weight, 0) + coalesce($weight, 1)"

	return query, nil
}
