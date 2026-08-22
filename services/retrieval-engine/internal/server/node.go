package server

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerank"
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

// retrievedNodeFromHybridSearch proietta un hybridsearch.RetrievedNode
// (SPEC-041, T4.1; esteso da SPEC-045) su RetrievedNode proto. Name/
// SourceText popolati da SPEC-045 (n.name da Neo4j, source_text da
// OpenSearch se include_source_text=true). NodeType/AstHash restano
// zero-value: nessuna fonte per questi campi in nessuna delle due gambe
// (stesso principio già stabilito in SPEC-016 per retrievedNodeFromDBNode).
func retrievedNodeFromHybridSearch(n hybridsearch.RetrievedNode) *retrievalv1.RetrievedNode {
	out := &retrievalv1.RetrievedNode{
		NodeId:     n.NodeID,
		Domain:     domainFromProperty(n.Domain),
		Name:       n.Name,
		SourceText: n.SourceText,
		Scores: &retrievalv1.NodeScores{
			RrfScore:   n.RRFScore,
			FinalScore: n.CombinedScore,
		},
	}
	if n.VectorScore != nil {
		out.Scores.VectorScore = *n.VectorScore
	}
	if n.HopDistance != nil && *n.HopDistance >= 0 {
		out.Scores.HopDistance = uint32(*n.HopDistance)
	}
	if n.Provenance != nil {
		out.Provenance = &retrievalv1.Provenance{
			Repo:      n.Provenance.Repo,
			Path:      n.Provenance.Path,
			StartLine: uint32(n.Provenance.StartLine),
			EndLine:   uint32(n.Provenance.EndLine),
			CommitSha: n.Provenance.Commit,
		}
	}
	return out
}

// retrievedNodeFromRankedNode proietta un rerank.RankedNode (SPEC-044,
// T4.4) su RetrievedNode proto: stessa base di retrievedNodeFromHybridSearch
// (node_id/domain/provenance/rrf_score/hop_distance, invariati — il
// reranking non tocca questi campi), con RerankScore/FinalScore che
// SOSTITUISCONO CombinedScore come final_score — il reranking, quando
// attivato, è il segnale di ordinamento definitivo (SPEC-044 §1), non un
// boost aggiuntivo su CombinedScore.
func retrievedNodeFromRankedNode(rn rerank.RankedNode) *retrievalv1.RetrievedNode {
	out := retrievedNodeFromHybridSearch(rn.Node)
	out.Scores.RerankScore = rn.RerankScore
	out.Scores.FinalScore = rn.FinalScore
	return out
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
