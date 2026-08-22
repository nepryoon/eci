// Package hybridsearch è il port Go 1:1 dell'algoritmo Python D5
// `hybrid_graph_vector_search` (docs/ADD_Enterprise_Code_Intelligence_consolidato.md,
// copiato verbatim in testdata/hybrid_search_reference/) — SPEC-041, T4.1.
// Estrazione congiunta Neo4j (graph traversal tipizzato) + Qdrant (vector
// search), fusione RRF (k=60), scoring con prossimità topologica.
package hybridsearch

import "context"

// RRFK — invariante dal Modulo 1 dell'ADD, ribadito da D5: "Do not
// 'improve' it" (CLAUDE.md). Stesso valore di D5 RRF_K.
const RRFK = 60

// Provenance canonica (repo/path:righe@commit) — stessa forma di D5.
type Provenance struct {
	Repo, Path, Commit string
	StartLine, EndLine int
}

// RetrievedNode — stessa forma di D5 RetrievedNode, estesa da SPEC-045 con
// Name/SourceText (D5 non li porta — deviazione dichiarata, non
// un'estensione di D5 stesso, vedi SPEC-045 §10). Puntatori = Optional[T]
// di D5: nil significa assente, non zero-value (SPEC-041 §2).
type RetrievedNode struct {
	NodeID                   string
	Domain                   string
	Source                   string // "graph" | "vector" | "fused"
	Name                     string // SPEC-045: da n.name (Neo4j), sempre popolato se il nodo esiste
	SourceText               string // SPEC-045: da OpenSearch, solo se include_source_text=true
	VectorScore, HopDistance *float64
	GraphRank, VectorRank    *int
	RRFScore, CombinedScore  float64
	Provenance               *Provenance
	Payload                  map[string]any
}

// HybridSearchError — stesso ruolo di D5 HybridSearchError: errori di
// dominio (Qdrant/Neo4j irraggiungibile, nessun risultato) distinti dagli
// errori di programmazione.
type HybridSearchError struct {
	msg string
	err error
}

func (e *HybridSearchError) Error() string {
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

func (e *HybridSearchError) Unwrap() error { return e.err }

func newHybridSearchError(msg string, err error) *HybridSearchError {
	return &HybridSearchError{msg: msg, err: err}
}

// Embedder espone l'embedding di una query testuale — stesso contratto di
// D5 (`embedder.encode(query) -> list[float]`), qui come interfaccia Go per
// disaccoppiare hybridsearch dal client HTTP concreto
// (internal/embedclient).
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}
