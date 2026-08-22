// Package gdsimpact implementa il job batch GDS (SPEC-043, T4.3): dato un
// nodo di ingresso, proietta il sottografo reverse-reachable in-memory via
// Neo4j Graph Data Science, esegue PPR seedata + betweenness campionata +
// Leiden, combina i risultati nella formula di priorità dell'ADD, e scrive
// impact_score/community_id/impact_kind sui nodi Neo4j interessati.
package gdsimpact

import (
	"flag"
	"fmt"
)

// Default dichiarati da SPEC-043 §2: nessun valore analogo esiste altrove
// nel progetto per max_depth/pesi — scelte ragionevoli, configurabili via
// flag, non desunte da nient'altro.
const (
	DefaultMaxDepth = 4
	DefaultWPPR     = 0.5
	DefaultWProx    = 0.3
	DefaultWBC      = 0.2
)

// Config — parametri di un'esecuzione di gds-impact (SPEC-043 §2).
type Config struct {
	EntryNodeID string
	MaxDepth    int
	// SamplingSize <= 0 significa "non specificato": gds.betweenness.stream
	// riceve il conteggio dei nodi della proiezione (risultati esatti,
	// SPEC-043 §2 punto 4 / §3 scenario 5) — risolto a runtime, non qui,
	// perché il conteggio dipende dalla proiezione effettiva.
	SamplingSize int
	WPPR         float64
	WProx        float64
	WBC          float64
}

// ParseConfig legge `args` (tipicamente os.Args[1:]) come flag CLI
// (SPEC-043 §2: `--entry-node-id`, `--max-depth`, `--sampling-size`, più i
// pesi `--w-ppr`/`--w-prox`/`--w-bc`, dichiarati configurabili via flag).
// `--entry-node-id` è l'unico obbligatorio — un job senza nodo di ingresso
// non ha senso (la PPR è intrinsecamente personalizzata su un seed, §5).
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("gds-impact", flag.ContinueOnError)
	entryNodeID := fs.String("entry-node-id", "", "id del nodo di ingresso (obbligatorio)")
	maxDepth := fs.Int("max-depth", DefaultMaxDepth, "profondità massima della traversata reverse-reachable")
	samplingSize := fs.Int("sampling-size", 0, "samplingSize per gds.betweenness.stream (default: conteggio nodi della proiezione, risultati esatti)")
	wPPR := fs.Float64("w-ppr", DefaultWPPR, "peso PPR nella formula impact_score")
	wProx := fs.Float64("w-prox", DefaultWProx, "peso prossimità (1/hop_distance) nella formula impact_score")
	wBC := fs.Float64("w-bc", DefaultWBC, "peso betweenness nella formula impact_score")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *entryNodeID == "" {
		return Config{}, fmt.Errorf("--entry-node-id è obbligatorio")
	}
	if *maxDepth <= 0 {
		return Config{}, fmt.Errorf("--max-depth deve essere >= 1, ricevuto %d", *maxDepth)
	}

	return Config{
		EntryNodeID:  *entryNodeID,
		MaxDepth:     *maxDepth,
		SamplingSize: *samplingSize,
		WPPR:         *wPPR,
		WProx:        *wProx,
		WBC:          *wBC,
	}, nil
}
