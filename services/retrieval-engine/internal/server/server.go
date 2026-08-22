// Package server implementa RetrievalEngineServer (SPEC-016, T1.4): la
// sola gamba grafo (GetNode, ExpandNeighbors, HybridSearch, Health) contro
// Neo4j. ImpactAnalysis resta Unimplemented (ereditato da
// UnimplementedRetrievalEngineServer, embedded — esplicitamente fuori
// scope SPEC-016 §2).
package server

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerank"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
)

const (
	defaultExpandDepth = 1
	defaultExpandLimit = 100
	defaultGraphLimit  = 200
	defaultTopK        = 25

	// Pesi di default del reranking (SPEC-044 §2, T4.4): w_hop/w_impact
	// dichiarati esplicitamente dalla SPEC; beta riusa lo stesso simbolo/
	// valore già stabilito da ApplyTopologicalProximity (T4.1, SPEC-041) —
	// stesso ruolo architetturale (rerank_score + beta*proximity_boost, già
	// nel commento preesistente di NodeScores.final_score nel proto D7),
	// nessun valore diverso dichiarato altrove per questo simbolo.
	defaultRerankBeta    = 0.15
	defaultRerankWHop    = 0.5
	defaultRerankWImpact = 0.5
)

// edgeTypeOrder fissa un ordine deterministico (quello dei tag proto) per
// costruire ExpandNeighborsResponse.TraversedEdgeTypes — l'iterazione di
// una map Go non è deterministica, la risposta di un'API sì.
var edgeTypeOrder = []string{
	"CALLS", "IMPORTS", "EXTENDS", "IMPLEMENTS", "CONTAINS",
	"DEPENDS_ON", "REFERENCES", "OVERRIDES", "DERIVED_FROM",
	"GOVERNED_BY", "CITES",
}

// Server implementa retrievalv1.RetrievalEngineServer. UnimplementedRetrievalEngineServer
// embedded by value (non puntatore, per compatibilità forward-only del
// codice generato) copre ImpactAnalysis con codes.Unimplemented.
//
// Qdrant/Embedder (SPEC-041, T4.1) sono opzionali: restano nil per un
// Server usato SOLO per GetNode/ExpandNeighbors/Health o per HybridSearch
// senza entry_node_id (comportamento T1.4 invariato, nessuna regressione).
// Servono solo quando HybridSearch riceve un entry_node_id (nuovo percorso
// grafo+vettoriale completo).
type Server struct {
	retrievalv1.UnimplementedRetrievalEngineServer
	Driver   neo4j.DriverWithContext
	Qdrant   *qdrant.Client
	Embedder hybridsearch.Embedder
	// Reranker (SPEC-044, T4.4) è opzionale come Qdrant/Embedder: resta
	// nil per un Server usato senza reranking. Serve solo quando
	// HybridSearch riceve enable_rerank=true.
	Reranker *rerankclient.Client
}

// GetNode — SPEC-016 §2/§3 scenari 1/2: MATCH (n:CodeNode {id: $node_id})
// RETURN n. Nodo assente -> NotFound esplicito, mai una risposta OK vuota.
func (s *Server) GetNode(ctx context.Context, req *retrievalv1.GetNodeRequest) (*retrievalv1.GetNodeResponse, error) {
	session := s.Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "MATCH (n:CodeNode {id: $node_id}) RETURN n", map[string]any{
		"node_id": req.GetNodeId(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query Neo4j: %v", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lettura risultati Neo4j: %v", err)
	}
	if len(records) == 0 {
		return nil, status.Errorf(codes.NotFound, "nodo %q non trovato", req.GetNodeId())
	}

	node, _, err := neo4j.GetRecordValue[dbtype.Node](records[0], "n")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "campo n: %v", err)
	}
	return &retrievalv1.GetNodeResponse{Node: retrievedNodeFromDBNode(node)}, nil
}

// ExpandNeighbors — SPEC-016 §2/§3 scenario 3: traversata Cypher a salto
// variabile bounded da depth (*1..depth, mai illimitato), edge_types
// filtro opzionale (vuoto o solo UNSPECIFIED = nessun filtro),
// direction=UNSPECIFIED = entrambe le direzioni.
func (s *Server) ExpandNeighbors(ctx context.Context, req *retrievalv1.ExpandNeighborsRequest) (*retrievalv1.ExpandNeighborsResponse, error) {
	depth := req.GetDepth()
	if depth == 0 {
		depth = defaultExpandDepth
	}
	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultExpandLimit
	}

	relTypes := validEdgeTypeStrings(req.GetEdgeTypes())
	typeFilter := ""
	if len(relTypes) > 0 {
		typeFilter = ":" + strings.Join(relTypes, "|")
	}

	var left, right string
	switch req.GetDirection() {
	case retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_FORWARD:
		left, right = "-", "->"
	case retrievalv1.TraversalDirection_TRAVERSAL_DIRECTION_REVERSE:
		left, right = "<-", "-"
	default: // UNSPECIFIED: entrambe le direzioni (SPEC-016 §2, scelta dichiarata)
		left, right = "-", "-"
	}

	// depth è un uint32 dal messaggio proto (non testo libero): Neo4j non
	// supporta un parametro per il bound di un pattern a lunghezza
	// variabile (limite del linguaggio Cypher, non di questo codice), va
	// interpolato — ma essendo un intero validato dal tipo stesso (non
	// una stringa), non è mai un rischio di injection, stesso principio
	// di sicurezza usato per label/tipi di relazione.
	query := fmt.Sprintf(
		`MATCH (n:CodeNode {id: $node_id})
MATCH p = (n)%s[r%s*1..%d]%s(m:CodeNode)
WHERE m.id <> $node_id
RETURN DISTINCT m, [x IN relationships(p) | type(x)] AS rel_types
LIMIT $limit`,
		left, typeFilter, depth, right,
	)

	session := s.Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, query, map[string]any{
		"node_id": req.GetNodeId(),
		"limit":   int64(limit),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query Neo4j: %v", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lettura risultati Neo4j: %v", err)
	}

	seen := make(map[string]bool, len(records))
	edgeTypesSeen := make(map[string]bool)
	neighbors := make([]*retrievalv1.RetrievedNode, 0, len(records))
	for _, rec := range records {
		m, _, err := neo4j.GetRecordValue[dbtype.Node](rec, "m")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "campo m: %v", err)
		}
		relTypesRaw, _, err := neo4j.GetRecordValue[[]any](rec, "rel_types")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "campo rel_types: %v", err)
		}
		for _, rt := range relTypesRaw {
			if s, ok := rt.(string); ok {
				edgeTypesSeen[s] = true
			}
		}

		id := stringProp(m.Props, "id")
		if seen[id] {
			continue
		}
		seen[id] = true
		neighbors = append(neighbors, retrievedNodeFromDBNode(m))
	}

	traversed := make([]retrievalv1.EdgeType, 0, len(edgeTypesSeen))
	for _, s := range edgeTypeOrder {
		if edgeTypesSeen[s] {
			traversed = append(traversed, stringToEdgeType[s])
		}
	}

	return &retrievalv1.ExpandNeighborsResponse{
		Neighbors:          neighbors,
		TraversedEdgeTypes: traversed,
	}, nil
}

// HybridSearch — dispatcher additivo (SPEC-041 §2/§9: "estensione additiva
// ... nessuna regressione sui client esistenti"). Un client che imposta
// entry_node_id usa il nuovo percorso grafo+vettoriale completo (T4.1,
// hybridGraphVectorSearch sotto); un client che non lo imposta (stringa
// vuota, comportamento di QUALUNQUE client scritto prima di questa SPEC)
// riceve ESATTAMENTE il comportamento T1.4 invariato
// (hybridSearchFullTextOnly, sola gamba grafo full-text).
func (s *Server) HybridSearch(ctx context.Context, req *retrievalv1.HybridSearchRequest) (*retrievalv1.HybridSearchResponse, error) {
	if req.GetEntryNodeId() != "" {
		return s.hybridSearchGraphVector(ctx, req)
	}
	return s.hybridSearchFullTextOnly(ctx, req)
}

// hybridSearchFullTextOnly — SPEC-016 §2/§3 scenari 4/5: sola gamba grafo,
// full-text nativo Neo4j (code_fulltext, SPEC-004) — copre di fatto solo
// `name` dato che signature/source_text non sono ancora popolati (limite
// dichiarato, non un bug, verificato esplicitamente dallo scenario 5).
// vector_candidates sempre 0, vector_leg_degraded sempre true: la gamba
// vettoriale non esiste in questo percorso, non un errore. INVARIATO
// rispetto a SPEC-016 — nessuna riga toccata nella logica, solo rinominata
// da HybridSearch (SPEC-041 §9: "nessuna regressione sui test di T1.4").
func (s *Server) hybridSearchFullTextOnly(ctx context.Context, req *retrievalv1.HybridSearchRequest) (*retrievalv1.HybridSearchResponse, error) {
	graphLimit := req.GetGraphLimit()
	if graphLimit == 0 {
		graphLimit = defaultGraphLimit
	}
	topK := req.GetTopK()
	if topK == 0 {
		topK = defaultTopK
	}

	params := map[string]any{
		"query_text":  req.GetQueryText(),
		"graph_limit": int64(graphLimit),
	}
	var conditions []string
	if req.GetDomain() != retrievalv1.Domain_DOMAIN_UNSPECIFIED {
		if d, ok := domainToString[req.GetDomain()]; ok {
			conditions = append(conditions, "node.domain = $domain")
			params["domain"] = d
		}
	}
	if repos := req.GetRepos(); len(repos) > 0 {
		conditions = append(conditions, "node.repo IN $repos")
		params["repos"] = repos
	}

	query := "CALL db.index.fulltext.queryNodes('code_fulltext', $query_text) YIELD node, score\n"
	if len(conditions) > 0 {
		query += "WHERE " + strings.Join(conditions, " AND ") + "\n"
	}
	query += "RETURN node, score ORDER BY score DESC LIMIT $graph_limit"

	session := s.Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, query, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query Neo4j: %v", err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "lettura risultati Neo4j: %v", err)
	}

	nodes := make([]*retrievalv1.RetrievedNode, 0, min(int(topK), len(records)))
	for i, rec := range records {
		if uint32(i) >= topK {
			break
		}
		node, _, err := neo4j.GetRecordValue[dbtype.Node](rec, "node")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "campo node: %v", err)
		}
		nodes = append(nodes, retrievedNodeFromDBNode(node))
	}

	return &retrievalv1.HybridSearchResponse{
		Nodes:             nodes,
		GraphCandidates:   uint32(len(records)),
		VectorCandidates:  0,
		VectorLegDegraded: true,
	}, nil
}

// hybridSearchGraphVector — SPEC-041 §2/§3, T4.1: percorso completo
// grafo+vettoriale (hybridsearch.HybridGraphVectorSearch, port 1:1 di D5).
// query_text/entry_node_id obbligatori (§4 edge case, validati dentro
// hybridsearch); max_depth <=0 -> errore esplicito (idem, validato dentro
// GraphTraversal). Diagnostica GraphCandidates/VectorCandidates/
// VectorLegDegraded derivata dal risultato finale (fuso, dopo troncamento
// top_k) — non un conteggio grezzo pre-fusione: la firma di
// HybridGraphVectorSearch (SPEC-041 §2, esplicita nell'interfaccia) ritorna
// solo ([]RetrievedNode, error), nessun conteggio separato, scelta
// dichiarata a fondo SPEC.
func (s *Server) hybridSearchGraphVector(ctx context.Context, req *retrievalv1.HybridSearchRequest) (*retrievalv1.HybridSearchResponse, error) {
	if s.Qdrant == nil || s.Embedder == nil {
		return nil, status.Error(codes.FailedPrecondition, "hybrid search grafo+vettoriale non disponibile: Qdrant/Embedder non configurati su questo server")
	}
	if req.GetQueryText() == "" {
		return nil, status.Error(codes.InvalidArgument, "query_text obbligatorio quando entry_node_id è impostato")
	}

	deps := hybridsearch.Deps{
		Driver:   s.Driver,
		Qdrant:   s.Qdrant,
		Embedder: s.Embedder,
		Logf:     func(format string, args ...any) { log.Printf(format, args...) },
	}

	var opts []hybridsearch.Option
	if d, ok := domainToString[req.GetDomain()]; ok {
		opts = append(opts, hybridsearch.WithDomain(d))
	}
	if repos := req.GetRepos(); len(repos) > 0 {
		opts = append(opts, hybridsearch.WithRepo(repos[0]))
	}
	if req.GetVectorLimit() > 0 {
		opts = append(opts, hybridsearch.WithVectorLimit(int(req.GetVectorLimit())))
	}
	if req.GetGraphLimit() > 0 {
		opts = append(opts, hybridsearch.WithGraphLimit(int(req.GetGraphLimit())))
	}
	if req.GetTopK() > 0 {
		opts = append(opts, hybridsearch.WithTopK(int(req.GetTopK())))
	}

	ranked, err := hybridsearch.HybridGraphVectorSearch(ctx, deps, req.GetQueryText(), req.GetEntryNodeId(), int(req.GetMaxDepth()), opts...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hybrid search grafo+vettoriale: %v", err)
	}

	var graphCandidates, vectorCandidates uint32
	for _, n := range ranked {
		if n.GraphRank != nil {
			graphCandidates++
		}
		if n.VectorRank != nil {
			vectorCandidates++
		}
	}

	var nodes []*retrievalv1.RetrievedNode
	if req.GetEnableRerank() {
		// SPEC-044 §3 scenario 5: enable_rerank=true richiesto
		// esplicitamente dal client — un fallimento del reranking (servizio
		// irraggiungibile incluso) fa fallire l'INTERA RPC, non degrada
		// come la gamba vettoriale di T4.1 (che qui è già stata applicata
		// sopra, invariata).
		if s.Reranker == nil {
			return nil, status.Error(codes.FailedPrecondition, "reranking richiesto (enable_rerank=true) ma il Reranker non è configurato su questo server")
		}
		topK := req.GetTopK()
		if topK == 0 {
			topK = defaultTopK
		}
		rankedByReranker, err := rerank.Rerank(ctx, s.Reranker, s.Driver, req.GetQueryText(), ranked, int(topK), defaultRerankBeta, defaultRerankWHop, defaultRerankWImpact)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "reranking: %v", err)
		}
		nodes = make([]*retrievalv1.RetrievedNode, 0, len(rankedByReranker))
		for _, rn := range rankedByReranker {
			nodes = append(nodes, retrievedNodeFromRankedNode(rn))
		}
	} else {
		nodes = make([]*retrievalv1.RetrievedNode, 0, len(ranked))
		for _, n := range ranked {
			nodes = append(nodes, retrievedNodeFromHybridSearch(n))
		}
	}

	return &retrievalv1.HybridSearchResponse{
		Nodes:             nodes,
		GraphCandidates:   graphCandidates,
		VectorCandidates:  vectorCandidates,
		VectorLegDegraded: vectorCandidates == 0,
	}, nil
}

// Health — SPEC-016 §2/§3 scenario 7: RETURN 1 su Neo4j. Nessun errore
// gRPC generico a Neo4j irraggiungibile — una risposta strutturata che
// descrive lo stato (graph/vector/fulltext leg + status complessivo).
func (s *Server) Health(ctx context.Context, _ *retrievalv1.HealthCheckRequest) (*retrievalv1.HealthCheckResponse, error) {
	healthy := s.pingGraph(ctx)

	st := retrievalv1.HealthCheckResponse_SERVING_STATUS_NOT_SERVING
	if healthy {
		st = retrievalv1.HealthCheckResponse_SERVING_STATUS_SERVING
	}

	return &retrievalv1.HealthCheckResponse{
		Status: st,
		// fulltext_leg_healthy rispecchia graph_leg_healthy (§2: stessa
		// connessione Neo4j, l'indice vive nello stesso DB — nessun
		// controllo separato in questa SPEC).
		GraphLegHealthy:    healthy,
		FulltextLegHealthy: healthy,
		// vector_leg_healthy sempre false: nessuna gamba vettoriale esiste
		// in questa SPEC (§5 non-goals), non uno stato transitorio.
		VectorLegHealthy: false,
	}, nil
}

func (s *Server) pingGraph(ctx context.Context) bool {
	session := s.Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	result, err := session.Run(ctx, "RETURN 1", nil)
	if err != nil {
		return false
	}
	if _, err := result.Consume(ctx); err != nil {
		return false
	}
	return true
}
