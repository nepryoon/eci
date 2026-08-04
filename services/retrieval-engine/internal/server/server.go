// Package server implementa RetrievalEngineServer (SPEC-016, T1.4): la
// sola gamba grafo (GetNode, ExpandNeighbors, HybridSearch, Health) contro
// Neo4j. ImpactAnalysis resta Unimplemented (ereditato da
// UnimplementedRetrievalEngineServer, embedded — esplicitamente fuori
// scope SPEC-016 §2).
package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
)

const (
	defaultExpandDepth = 1
	defaultExpandLimit = 100
	defaultGraphLimit  = 200
	defaultTopK        = 25
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
type Server struct {
	retrievalv1.UnimplementedRetrievalEngineServer
	Driver neo4j.DriverWithContext
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

// HybridSearch — SPEC-016 §2/§3 scenari 4/5: sola gamba grafo, full-text
// nativo Neo4j (code_fulltext, SPEC-004) — copre di fatto solo `name` dato
// che signature/source_text non sono ancora popolati (limite dichiarato,
// non un bug, verificato esplicitamente dallo scenario 5).
// vector_candidates sempre 0, vector_leg_degraded sempre true: la gamba
// vettoriale non esiste in questa SPEC, non un errore.
func (s *Server) HybridSearch(ctx context.Context, req *retrievalv1.HybridSearchRequest) (*retrievalv1.HybridSearchResponse, error) {
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
