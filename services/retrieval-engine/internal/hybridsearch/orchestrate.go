package hybridsearch

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
)

// Deps sono le dipendenze di HybridGraphVectorSearch, iniettate
// esplicitamente (nessuno stato globale, stesso principio già stabilito per
// sink-graph/sink-vector, SPEC-015/033). Logf è opzionale: se nil, la
// degradazione della gamba vettoriale (SPEC-041 §3 scenario 5) non viene
// loggata ma il comportamento resta identico. OpenSearch (SPEC-045) è
// opzionale a sua volta: richiesto SOLO quando WithIncludeSourceText(true)
// è passata — nil va bene se il chiamante non lo userà mai.
type Deps struct {
	Driver     neo4j.DriverWithContext
	Qdrant     *qdrant.Client
	Embedder   Embedder
	OpenSearch *opensearchapi.Client
	Logf       func(format string, args ...any)
}

// options — parametri opzionali di HybridGraphVectorSearch, stessi default
// keyword di D5: collection="code_embeddings" (SPEC-041 §2: nome reale
// SPEC-033, D5 dichiara "code_nodes", stantio — deviazione dichiarata),
// domain="code", vector_limit=50, graph_limit=200, top_k=25, beta=0.15.
type options struct {
	collection        string
	domain            *string
	repo              *string
	vectorLimit       int
	graphLimit        int
	topK              int
	beta              float64
	includeSourceText bool
}

func defaultOptions() options {
	domain := "code"
	return options{
		collection:  "code_embeddings",
		domain:      &domain,
		vectorLimit: 50,
		graphLimit:  200,
		topK:        25,
		beta:        0.15,
	}
}

// Option personalizza HybridGraphVectorSearch — stessi keyword-argument di
// D5 (collection/domain/repo/vector_limit/graph_limit/top_k/beta).
type Option func(*options)

func WithCollection(c string) Option { return func(o *options) { o.collection = c } }

// WithDomain — "" equivale a nessun filtro di dominio (stesso principio di
// D5, domain: Optional[str] = None disabilita il filtro).
func WithDomain(d string) Option {
	return func(o *options) {
		if d == "" {
			o.domain = nil
			return
		}
		o.domain = &d
	}
}
func WithRepo(r string) Option {
	return func(o *options) {
		if r == "" {
			o.repo = nil
			return
		}
		o.repo = &r
	}
}
func WithVectorLimit(n int) Option { return func(o *options) { o.vectorLimit = n } }
func WithGraphLimit(n int) Option  { return func(o *options) { o.graphLimit = n } }
func WithTopK(n int) Option        { return func(o *options) { o.topK = n } }
func WithBeta(b float64) Option    { return func(o *options) { o.beta = b } }

// WithIncludeSourceText (SPEC-045 §2) attiva la lettura di SourceText da
// OpenSearch per i risultati finali (dopo troncamento a top_k). false di
// default: nessuna chiamata OpenSearch, SourceText resta vuoto (SPEC-045
// §3 scenario 3).
func WithIncludeSourceText(v bool) Option { return func(o *options) { o.includeSourceText = v } }

// HybridGraphVectorSearch — port 1:1 di D5 `hybrid_graph_vector_search`:
// (1) embedding query; (2) vector search Qdrant (seed-finding), degrada
// senza abortire se fallisce (SPEC-041 §3 scenario 5); (3) traversata
// tipizzata inversa su Neo4j da entryNodeID, bounded a maxDepth,
// OBBLIGATORIA — fallisce l'intera funzione se fallisce (scenario 6); (4)
// errore esplicito se ENTRAMBE le liste sono vuote (scenario 7); (5)
// fusione RRF (k=60) + scoring con prossimità topologica + troncamento a
// top_k. Stesso ordine di fasi di D5, stessa distinzione degradabile/
// obbligatorio.
func HybridGraphVectorSearch(ctx context.Context, deps Deps, query, entryNodeID string, maxDepth int, opts ...Option) ([]RetrievedNode, error) {
	if _, err := accessscope.FromContext(ctx); err != nil {
		return nil, fmt.Errorf("hybridsearch: security scope non valido: %w", err)
	}
	if query == "" || entryNodeID == "" {
		return nil, fmt.Errorf("hybridsearch: query ed entry_node_id sono obbligatori")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	// (1) embedding — NON degradabile, a differenza della vector search
	// sottostante: stesso comportamento di D5 (_embed_query non è avvolto
	// in un try/except nella funzione principale).
	qvec, err := deps.Embedder.Embed(ctx, query)
	if err != nil {
		return nil, newHybridSearchError("Embedding fallito", err)
	}

	// (2) vector search — degrada senza abortire (SPEC-041 §3 scenario 5).
	vectorNodes, err := VectorSearch(ctx, deps.Qdrant, o.collection, qvec, o.domain, o.repo, o.vectorLimit)
	if err != nil {
		logf(deps, "hybridsearch: vector search degradata: %v", err)
		vectorNodes = nil
	}

	// (3) graph traversal — obbligatoria (SPEC-041 §3 scenario 6).
	graphNodes, err := GraphTraversal(ctx, deps.Driver, entryNodeID, maxDepth, o.domain, o.repo, o.graphLimit)
	if err != nil {
		return nil, newHybridSearchError("Graph traversal fallito", err)
	}

	if len(graphNodes) == 0 && len(vectorNodes) == 0 {
		return nil, newHybridSearchError("Nessun risultato da grafo e vettoriale", nil)
	}

	// (4) fusione RRF (k=60).
	fused := RRFFuse(graphNodes, vectorNodes, RRFK)

	// (5) scoring combinato con prossimità topologica + troncamento a top_k.
	ranked := ApplyTopologicalProximity(fused, o.beta)

	// Defense in depth required by ADD §2.5: ACL may have changed between
	// individual leg retrieval and fusion. No candidate reaches hydration,
	// reranking, or packing without this authoritative graph re-check.
	ranked, err = recheckAuthorized(ctx, deps.Driver, ranked)
	if err != nil {
		return nil, newHybridSearchError("ACL re-check fallito", err)
	}
	if o.topK > 0 && len(ranked) > o.topK {
		ranked = ranked[:o.topK]
	}

	// (6) idratazione contenuto (SPEC-045): SOLO sui risultati FINALI (dopo
	// il troncamento a top_k) — meno nodi da idratare, nessun impatto sulla
	// fusione/ranking (Name/SourceText sono puramente informativi). name è
	// una proprietà base attesa sempre disponibile per un nodo :CodeNode
	// reale: un suo fallimento è un errore esplicito, MAI degradato (a
	// differenza della gamba vettoriale) — SPEC-045 §4.
	if err := hydrateNames(ctx, deps.Driver, ranked); err != nil {
		return nil, newHybridSearchError("Hydration name fallita", err)
	}
	if o.includeSourceText {
		// Esplicitamente richiesto dal client (include_source_text=true):
		// un fallimento OpenSearch fa fallire l'intera chiamata, stesso
		// principio già stabilito per il reranker in T4.4 — SPEC-045 §4.
		if err := hydrateSourceText(ctx, deps.OpenSearch, ranked); err != nil {
			return nil, newHybridSearchError("Hydration source_text fallita", err)
		}
	}

	return ranked, nil
}

func logf(deps Deps, format string, args ...any) {
	if deps.Logf != nil {
		deps.Logf(format, args...)
	}
}
