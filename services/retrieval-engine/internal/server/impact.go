// SPEC-042, T4.2 — ImpactAnalysis: prima RPC streaming del progetto.
// Riusa lo scaffold proto D7 già esistente (ImpactAnalysisRequest/
// ImpactedNode/ImpactProgress/ImpactAnalysisEvent), esteso additivamente
// con max_nodes/domain/repos (ADR-0008) — non le forme nuove proposte
// letteralmente da SPEC-042 §2, che collidevano per nome con lo scaffold
// esistente (vedi ADR-0008 per il ragionamento completo).
package server

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/retrieval-engine/internal/impactanalysis"
)

const defaultImpactMaxDepth = 4 // stesso default dichiarato dal commento proto preesistente (D7)

// ImpactAnalysis — SPEC-042 §2/§3: reverse reachability bounded in
// streaming, BFS livello-per-livello (internal/impactanalysis.StreamImpact,
// T4.2), cap max_nodes sul totale esplorato. max_depth non impostato (0)
// -> default 4 (commento preesistente sul campo proto, D7); max_nodes non
// impostato o <= 0 -> errore esplicito PRIMA di avviare la traversata
// (SPEC-042 §3 scenario 6, nessun default silenzioso — a differenza di
// max_depth, questo cap non ha un valore "innocuo" da assumere).
func (s *Server) ImpactAnalysis(req *retrievalv1.ImpactAnalysisRequest, stream retrievalv1.RetrievalEngine_ImpactAnalysisServer) error {
	maxNodes := int(req.GetMaxNodes())
	if maxNodes <= 0 {
		return status.Errorf(codes.InvalidArgument, "max_nodes deve essere >= 1, ricevuto %d", maxNodes)
	}
	maxDepth := int(req.GetMaxDepth())
	if maxDepth == 0 {
		maxDepth = defaultImpactMaxDepth
	}

	var domain *string
	if d, ok := domainToString[req.GetDomain()]; ok && req.GetDomain() != retrievalv1.Domain_DOMAIN_UNSPECIFIED {
		domain = &d
	}
	var repo *string
	if repos := req.GetRepos(); len(repos) > 0 {
		repo = &repos[0]
	}

	ctx := stream.Context()
	emit := func(e impactanalysis.ImpactEvent) error {
		return stream.Send(impactAnalysisEventFromInternal(e))
	}

	if err := impactanalysis.StreamImpact(ctx, s.Driver, req.GetEntryNodeId(), maxDepth, maxNodes, domain, repo, emit); err != nil {
		return status.Errorf(codes.Internal, "impact analysis: %v", err)
	}
	return nil
}

// impactAnalysisEventFromInternal proietta un impactanalysis.ImpactEvent
// (T4.2) sul messaggio proto ImpactAnalysisEvent riusato (ADR-0008).
// Esattamente uno tra Node/Progress è non-nil nell'input (stesso invariante
// del type impactanalysis.ImpactEvent) — mai un evento vuoto costruito.
func impactAnalysisEventFromInternal(e impactanalysis.ImpactEvent) *retrievalv1.ImpactAnalysisEvent {
	switch {
	case e.Node != nil:
		return &retrievalv1.ImpactAnalysisEvent{
			Event: &retrievalv1.ImpactAnalysisEvent_Node{
				Node: impactedNodeFromInternal(e.Node),
			},
		}
	case e.Progress != nil:
		p := e.Progress
		return &retrievalv1.ImpactAnalysisEvent{
			Event: &retrievalv1.ImpactAnalysisEvent_Progress{
				Progress: &retrievalv1.ImpactProgress{
					NodesEmitted: uint32(p.NodesExplored),
					FrontierSize: uint32(p.FrontierSize),
					CurrentDepth: uint32(p.CurrentDepth),
					// SPEC-042 ha un solo concetto di troncamento (cap
					// max_nodes, globale) — mappato su TruncatedByFanoutCap
					// (concettualmente il cap più vicino nello scaffold
					// esistente, anche se qui globale non per-hop, ADR-0008).
					// TruncatedByDepth resta sempre false: raggiungere
					// max_depth naturalmente non è un troncamento in questo
					// modello (§3, nessuno scenario lo tratta come tale).
					TruncatedByFanoutCap: p.Truncated,
					TruncatedByDepth:     false,
				},
			},
		}
	default:
		// Invariante violato da un chiamante malformato di emit — non
		// dovrebbe mai accadere con impactanalysis.StreamImpact reale, ma
		// un panic silenzioso sarebbe peggio di un evento diagnosticabile.
		panic(fmt.Sprintf("impactAnalysisEventFromInternal: evento vuoto %+v", e))
	}
}

// impactedNodeFromInternal proietta un impactanalysis.ImpactNode su
// ImpactedNode proto (ADR-0008): RetrievedNode popolato SOLO con i campi
// che la traversata BFS conosce davvero (node_id/domain/provenance/
// scores.hop_distance — stesso principio già stabilito in T1.4/T4.1).
// ImpactKind (l'enum di severità) resta UNSPECIFIED: richiede GDS (T4.3,
// fuori scope). PathEdgeTypes porta il concetto "impact_kind" di SPEC-042
// (tipo d'arco dell'ultimo hop, percorso più breve) come lista di un solo
// elemento.
func impactedNodeFromInternal(n *impactanalysis.ImpactNode) *retrievalv1.ImpactedNode {
	node := &retrievalv1.RetrievedNode{
		NodeId: n.NodeID,
		Domain: domainFromProperty(n.Domain),
		Scores: &retrievalv1.NodeScores{
			HopDistance: uint32(n.HopDistance),
		},
	}
	if n.Provenance != nil {
		node.Provenance = &retrievalv1.Provenance{
			Repo:      n.Provenance.Repo,
			Path:      n.Provenance.Path,
			StartLine: uint32(n.Provenance.StartLine),
			EndLine:   uint32(n.Provenance.EndLine),
			CommitSha: n.Provenance.Commit,
		}
	}

	var pathEdgeTypes []retrievalv1.EdgeType
	if et, ok := stringToEdgeType[n.EdgeType]; ok {
		pathEdgeTypes = []retrievalv1.EdgeType{et}
	}

	return &retrievalv1.ImpactedNode{
		Node:          node,
		ImpactKind:    retrievalv1.ImpactKind_IMPACT_KIND_UNSPECIFIED,
		PathEdgeTypes: pathEdgeTypes,
		Depth:         uint32(n.HopDistance),
	}
}
