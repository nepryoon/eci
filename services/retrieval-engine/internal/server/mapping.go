// Mappatura Domain/EdgeType <-> stringa (SPEC-016 §2): stessi valori del
// CHECK constraint code_node.domain/code_relation.rel_type (SPEC-005),
// stesso enum EdgeType già validato in sink-graph (SPEC-015). Un valore
// enum non mappato non produce mai una stringa arbitraria interpolata in
// Cypher — solo le stringhe di QUESTE mappe raggiungono una query.
package server

import retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"

var domainToString = map[retrievalv1.Domain]string{
	retrievalv1.Domain_DOMAIN_CODE:       "code",
	retrievalv1.Domain_DOMAIN_DOC:        "doc",
	retrievalv1.Domain_DOMAIN_LEGAL:      "legal",
	retrievalv1.Domain_DOMAIN_COMPLIANCE: "compliance",
	retrievalv1.Domain_DOMAIN_CONTRACT:   "contract",
}

var stringToDomain = map[string]retrievalv1.Domain{
	"code":       retrievalv1.Domain_DOMAIN_CODE,
	"doc":        retrievalv1.Domain_DOMAIN_DOC,
	"legal":      retrievalv1.Domain_DOMAIN_LEGAL,
	"compliance": retrievalv1.Domain_DOMAIN_COMPLIANCE,
	"contract":   retrievalv1.Domain_DOMAIN_CONTRACT,
}

// domainFromProperty mappa la property Neo4j n.domain (string, eventualmente
// assente su un nodo placeholder mai arricchito) al Domain proto. Un
// valore assente o non riconosciuto mappa a DOMAIN_UNSPECIFIED — mai un
// panic su dati Neo4j inattesi.
func domainFromProperty(v any) retrievalv1.Domain {
	s, _ := v.(string)
	return stringToDomain[s]
}

var edgeTypeToString = map[retrievalv1.EdgeType]string{
	retrievalv1.EdgeType_EDGE_TYPE_CALLS:        "CALLS",
	retrievalv1.EdgeType_EDGE_TYPE_IMPORTS:      "IMPORTS",
	retrievalv1.EdgeType_EDGE_TYPE_EXTENDS:      "EXTENDS",
	retrievalv1.EdgeType_EDGE_TYPE_IMPLEMENTS:   "IMPLEMENTS",
	retrievalv1.EdgeType_EDGE_TYPE_CONTAINS:     "CONTAINS",
	retrievalv1.EdgeType_EDGE_TYPE_DEPENDS_ON:   "DEPENDS_ON",
	retrievalv1.EdgeType_EDGE_TYPE_REFERENCES:   "REFERENCES",
	retrievalv1.EdgeType_EDGE_TYPE_OVERRIDES:    "OVERRIDES",
	retrievalv1.EdgeType_EDGE_TYPE_DERIVED_FROM: "DERIVED_FROM",
	retrievalv1.EdgeType_EDGE_TYPE_GOVERNED_BY:  "GOVERNED_BY",
	retrievalv1.EdgeType_EDGE_TYPE_CITES:        "CITES",
}

var stringToEdgeType = map[string]retrievalv1.EdgeType{
	"CALLS":        retrievalv1.EdgeType_EDGE_TYPE_CALLS,
	"IMPORTS":      retrievalv1.EdgeType_EDGE_TYPE_IMPORTS,
	"EXTENDS":      retrievalv1.EdgeType_EDGE_TYPE_EXTENDS,
	"IMPLEMENTS":   retrievalv1.EdgeType_EDGE_TYPE_IMPLEMENTS,
	"CONTAINS":     retrievalv1.EdgeType_EDGE_TYPE_CONTAINS,
	"DEPENDS_ON":   retrievalv1.EdgeType_EDGE_TYPE_DEPENDS_ON,
	"REFERENCES":   retrievalv1.EdgeType_EDGE_TYPE_REFERENCES,
	"OVERRIDES":    retrievalv1.EdgeType_EDGE_TYPE_OVERRIDES,
	"DERIVED_FROM": retrievalv1.EdgeType_EDGE_TYPE_DERIVED_FROM,
	"GOVERNED_BY":  retrievalv1.EdgeType_EDGE_TYPE_GOVERNED_BY,
	"CITES":        retrievalv1.EdgeType_EDGE_TYPE_CITES,
}

// validEdgeTypeStrings converte una lista di EdgeType proto in stringhe
// Cypher SOLO per i valori presenti in edgeTypeToString (whitelist):
// EDGE_TYPE_UNSPECIFIED e qualunque valore fuori range vengono
// silenziosamente esclusi dal risultato, MAI interpolati — l'unico modo
// per un valore di finire in una stringa di query è passare da questa
// mappa fissa.
func validEdgeTypeStrings(types []retrievalv1.EdgeType) []string {
	out := make([]string, 0, len(types))
	for _, t := range types {
		if s, ok := edgeTypeToString[t]; ok {
			out = append(out, s)
		}
	}
	return out
}
