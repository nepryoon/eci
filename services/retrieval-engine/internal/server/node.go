package server

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
)

// retrievedNodeFromDBNode proietta un dbtype.Node Neo4j su RetrievedNode
// (SPEC-016 §2): SOLO i campi popolati realmente dalla pipeline a monte
// (T1.1-T1.3) — node_id/domain/node_type/name/ast_hash/provenance.repo/
// provenance.path. Tutti gli altri campi (signature, summary, source_text,
// scores, provenance.start_line/end_line/commit_sha) restano allo
// zero-value proto: non vanno fabbricati indipendentemente da
// include_source_text/include_summary nella request, dato che questa SPEC
// non ha ancora nessuna fonte per quei valori.
func retrievedNodeFromDBNode(n dbtype.Node) *retrievalv1.RetrievedNode {
	return &retrievalv1.RetrievedNode{
		NodeId:   stringProp(n.Props, "id"),
		Domain:   domainFromProperty(n.Props["domain"]),
		NodeType: specificLabel(n.Labels),
		Name:     stringProp(n.Props, "name"),
		AstHash:  stringProp(n.Props, "ast_hash"),
		Provenance: &retrievalv1.Provenance{
			Repo: stringProp(n.Props, "repo"),
			Path: stringProp(n.Props, "path"),
		},
	}
}

// stringProp legge una property Neo4j come stringa. Assente o di tipo
// diverso -> "" (zero-value proto), mai un panic: un nodo placeholder
// (creato da una CodeRelation arrivata prima del suo CodeNode, SPEC-015
// §3 scenario 3) ha SOLO la property `id`, tutte le altre lette qui sono
// legittimamente assenti.
func stringProp(props map[string]any, key string) string {
	s, _ := props[key].(string)
	return s
}

// specificLabel ritorna la prima label diversa da "CodeNode" (il
// node_type specifico: Method, Class, Interface, File, Function). "" se
// il nodo ha SOLO l'etichetta generica :CodeNode (placeholder mai
// arricchito) — nessuna label specifica da riportare, non un errore.
func specificLabel(labels []string) string {
	for _, l := range labels {
		if l != "CodeNode" {
			return l
		}
	}
	return ""
}
