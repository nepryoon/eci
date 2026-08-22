package packing

import (
	"fmt"
	"sort"
	"strings"

	"github.com/eci-project/eci/services/retrieval-engine/internal/hybridsearch"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerank"
)

// missingProvenancePlaceholder — SPEC-046 §2: "il frammento è comunque
// incluso ma SENZA citazione fabbricata — un placeholder esplicito ...
// mai un valore inventato". Stringa letterale dichiarata dalla SPEC.
const missingProvenancePlaceholder = "provenance non disponibile"

// Pack — SPEC-046 §2: dedup per NodeID, quattro sezioni a budget dedicato
// (Definizioni/Relazioni/Summary gerarchici(riservata,vuota)/Sorgente
// integrale), citazione di provenance obbligatoria per ogni frammento,
// ordinamento "a U" per l'intero candidate set deduplicato.
func Pack(ranked []rerank.RankedNode, budget TokenBudget) PackedContext {
	sorted := dedupSortedByScoreDesc(ranked)
	order := uOrder(sorted)

	definitions := packDefinitions(sorted, order, budget)
	relations := packRelations(sorted, order, budget)
	fullSource := packFullSource(sorted, order, budget)

	pc := PackedContext{
		Order:                 order,
		Definitions:           definitions,
		Relations:             relations,
		HierarchicalSummaries: nil, // §5 Non-goal: quota riservata, mai consumata
		FullSource:            fullSource,
	}
	pc.TotalTokens = sumFragmentTokens(definitions) + sumFragmentTokens(relations) + sumFragmentTokens(fullSource)
	return pc
}

// dedupSortedByScoreDesc ordina per FinalScore decrescente, poi deduplica
// per NodeID tenendo la PRIMA occorrenza incontrata — dopo l'ordinamento,
// coincide sempre con l'occorrenza a punteggio più alto (SPEC-046 §3
// scenario 6: "solo uno dei due appare", rete di sicurezza economica su
// una dedup che T4.1/RRF dovrebbe già garantire).
func dedupSortedByScoreDesc(ranked []rerank.RankedNode) []rerank.RankedNode {
	sorted := make([]rerank.RankedNode, len(ranked))
	copy(sorted, ranked)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].FinalScore > sorted[j].FinalScore })

	seen := make(map[string]bool, len(sorted))
	out := make([]rerank.RankedNode, 0, len(sorted))
	for _, rn := range sorted {
		if seen[rn.Node.NodeID] {
			continue
		}
		seen[rn.Node.NodeID] = true
		out = append(out, rn)
	}
	return out
}

// uOrder — SPEC-046 §2, algoritmo dichiarato: "il migliore va in prima
// posizione, il secondo in ultima, il terzo in seconda posizione, il
// quarto in penultima, ..." — riempimento alternato fronte/retro a
// partire dal fronte, nell'ordine di score decrescente già stabilito da
// `sorted`.
func uOrder(sorted []rerank.RankedNode) []string {
	n := len(sorted)
	out := make([]string, n)
	left, right := 0, n-1
	for i, rn := range sorted {
		if i%2 == 0 {
			out[left] = rn.Node.NodeID
			left++
		} else {
			out[right] = rn.Node.NodeID
			right--
		}
	}
	return out
}

func tokenCount(s string) int { return len(s) / 4 }

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

func citation(p *hybridsearch.Provenance) string {
	if p == nil {
		return missingProvenancePlaceholder
	}
	return fmt.Sprintf("%s/%s:%d-%d@%s", p.Repo, p.Path, p.StartLine, p.EndLine, p.Commit)
}

// packDefinitions — SPEC-046 §2: Name + prima riga di SourceText come
// proxy di firma, SEMPRE per tutti i candidati deduplicati, entro la
// quota Definizioni — MA con l'eccezione dell'edge case §4 riga 2: il
// candidato col FinalScore più alto è SEMPRE incluso, anche se da solo
// eccede la quota (mai una risposta completamente vuota).
func packDefinitions(sorted []rerank.RankedNode, order []string, budget TokenBudget) []Fragment {
	quota := int(float64(budget.Total) * budget.DefinitionsFraction)
	var out []Fragment
	used := 0
	for i, rn := range sorted {
		text := definitionText(rn.Node)
		frag := Fragment{
			NodeID:     rn.Node.NodeID,
			Text:       text,
			Citation:   citation(rn.Node.Provenance),
			TokenCount: tokenCount(text) + tokenCount(citation(rn.Node.Provenance)),
		}
		if i == 0 {
			out = append(out, frag) // sempre il migliore, anche se eccede la quota
			used += frag.TokenCount
			continue
		}
		if used+frag.TokenCount > quota {
			break
		}
		out = append(out, frag)
		used += frag.TokenCount
	}
	return reorderByU(out, order)
}

func definitionText(n hybridsearch.RetrievedNode) string {
	fl := firstLine(n.SourceText)
	if fl == "" {
		return n.Name
	}
	return n.Name + ": " + fl
}

// packRelations — SPEC-046 §2: rappresentazione compatta di
// EdgeType/HopDistance per candidato ("<name> (<edge_type>, hop <n>)").
// EdgeType non è disponibile su hybridsearch.RetrievedNode (nessuna SPEC
// precedente lo popola — GraphTraversal usa un'unica query a lunghezza
// variabile, non un BFS livello-per-livello come impactanalysis/T4.2, che
// è l'unico posto nella pipeline dove un "tipo d'arco dell'ultimo hop" è
// mai stato tracciato) — omesso dal formato, MAI fabbricato. Deviazione
// dichiarata, SPEC-046 §10.
func packRelations(sorted []rerank.RankedNode, order []string, budget TokenBudget) []Fragment {
	quota := int(float64(budget.Total) * budget.RelationsFraction)
	var out []Fragment
	used := 0
	for _, rn := range sorted {
		text := relationText(rn.Node)
		frag := Fragment{
			NodeID:     rn.Node.NodeID,
			Text:       text,
			Citation:   citation(rn.Node.Provenance),
			TokenCount: tokenCount(text) + tokenCount(citation(rn.Node.Provenance)),
		}
		if used+frag.TokenCount > quota {
			break
		}
		out = append(out, frag)
		used += frag.TokenCount
	}
	return reorderByU(out, order)
}

func relationText(n hybridsearch.RetrievedNode) string {
	if n.HopDistance == nil {
		return n.Name
	}
	return fmt.Sprintf("%s (hop %d)", n.Name, int(*n.HopDistance))
}

// packFullSource — SPEC-046 §2: criteri (a) HopDistance==0 (target
// diretto) O ImpactScoreNorm sopra soglia; (c) budget residuo nella quota
// dedicata, capped a FullSourceTopK candidati eleggibili (in ordine di
// priorità FinalScore, §3 scenario 3: il budget scarso va al punteggio
// più alto). NESSUNA eccezione "sempre almeno uno" qui — a differenza di
// Definizioni, l'edge case §4 riga 2 è scoped esplicitamente a quella
// sezione; §3 scenario 3 chiede esplicitamente l'ESCLUSIONE quando il
// budget è esaurito.
func packFullSource(sorted []rerank.RankedNode, order []string, budget TokenBudget) []Fragment {
	quota := int(float64(budget.Total) * budget.FullSourceFraction)

	var eligible []rerank.RankedNode
	for _, rn := range sorted {
		isTarget := rn.Node.HopDistance != nil && *rn.Node.HopDistance == 0
		aboveThreshold := rn.ImpactScoreNorm > budget.FullSourceImpactScoreThreshold
		if isTarget || aboveThreshold {
			eligible = append(eligible, rn)
		}
		if len(eligible) >= budget.FullSourceTopK {
			break
		}
	}

	var out []Fragment
	used := 0
	for _, rn := range eligible {
		text := rn.Node.SourceText
		frag := Fragment{
			NodeID:     rn.Node.NodeID,
			Text:       text,
			Citation:   citation(rn.Node.Provenance),
			TokenCount: tokenCount(text) + tokenCount(citation(rn.Node.Provenance)),
		}
		if used+frag.TokenCount > quota {
			// criterio (c): budget esaurito, questo candidato NON riceve
			// sorgente integrale (SPEC-046 §3 scenario 3) — stesso
			// arresto al primo che non entra già usato da
			// packDefinitions/packRelations, priorità FinalScore
			// rispettata rigidamente (mai "salta e prova il prossimo più
			// piccolo").
			break
		}
		out = append(out, frag)
		used += frag.TokenCount
	}
	return reorderByU(out, order)
}

// reorderByU riordina i frammenti inclusi secondo la posizione dei loro
// NodeID in `order` (la sequenza "a U" complessiva) — ogni sezione
// presenta i propri frammenti nello stesso arrangiamento relativo
// dell'intero candidate set, non nell'ordine di priorità budget con cui
// sono stati selezionati.
func reorderByU(frags []Fragment, order []string) []Fragment {
	if len(frags) == 0 {
		return nil
	}
	byID := make(map[string]Fragment, len(frags))
	for _, f := range frags {
		byID[f.NodeID] = f
	}
	out := make([]Fragment, 0, len(frags))
	for _, id := range order {
		if f, ok := byID[id]; ok {
			out = append(out, f)
		}
	}
	return out
}

func sumFragmentTokens(frags []Fragment) int {
	total := 0
	for _, f := range frags {
		total += f.TokenCount
	}
	return total
}
