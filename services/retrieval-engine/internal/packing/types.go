// Package packing impacchetta i candidati riordinati da rerank (T4.4) in
// un contesto a budget di token limitato — SPEC-046, T4.5 (ultimo task di
// Fase 4). Package sorella di rerank/hybridsearch/impactanalysis: nessuna
// nuova infrastruttura esterna, opera interamente su dati già idratati da
// T4.1/T4.4/SPEC-045 — nessuna chiamata di rete, nessun errore possibile
// (Pack non ritorna error).
package packing

// TokenBudget — SPEC-046 §2, default dichiarati: nessun valore analogo
// esiste altrove nel progetto per un budget di context packing, scelta
// ragionevole non desunta da nient'altro.
type TokenBudget struct {
	Total                          int     // default 8000
	DefinitionsFraction            float64 // default 0.4
	RelationsFraction              float64 // default 0.2
	HierarchicalSummariesFraction  float64 // default 0.2 (riservata, mai consumata — §5)
	FullSourceFraction             float64 // default 0.2
	FullSourceTopK                 int     // default 4
	FullSourceImpactScoreThreshold float64 // default 0.5
}

// DefaultTokenBudget — valori di default dichiarati da SPEC-046 §2.
func DefaultTokenBudget() TokenBudget {
	return TokenBudget{
		Total:                          8000,
		DefinitionsFraction:            0.4,
		RelationsFraction:              0.2,
		HierarchicalSummariesFraction:  0.2,
		FullSourceFraction:             0.2,
		FullSourceTopK:                 4,
		FullSourceImpactScoreThreshold: 0.5,
	}
}

// Fragment è un pezzo di contesto impacchettato per UN candidato in UNA
// sezione — un candidato può contribuire più Fragment (uno per sezione a
// cui partecipa: Definizioni sempre, Relazioni sempre, Sorgente integrale
// condizionale). Citation è SEMPRE popolata (placeholder esplicito quando
// Provenance è assente, mai fabbricata — SPEC-046 §2/§3 scenario 4).
type Fragment struct {
	NodeID     string
	Text       string
	Citation   string
	TokenCount int
}

// PackedContext — risultato di Pack (SPEC-046 §2). Order è la sequenza "a
// U" (SPEC-046 §2/§3 scenario 5) dei candidati DEDUPLICATI (uno per
// NodeID): il punteggio più alto in prima posizione, il secondo più alto
// in ultima, poi alternando verso il centro — indipendente da QUALI
// sezioni un candidato è riuscito a raggiungere (Order descrive
// l'arrangiamento di presentazione, non l'esito del budget). Ogni sezione
// elenca i propri Fragment nello STESSO ordine relativo di Order.
// HierarchicalSummaries resta SEMPRE vuoto (§5 Non-goal: nessun
// meccanismo di riassunto esiste nella pipeline oggi) — quota riservata,
// mai consumata.
type PackedContext struct {
	Order                 []string
	Definitions           []Fragment
	Relations             []Fragment
	HierarchicalSummaries []Fragment
	FullSource            []Fragment
	TotalTokens           int
}
