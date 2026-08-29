package gdsimpact

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Run esegue l'intera pipeline (SPEC-043 §2): discovery del sottografo,
// proiezione in-memory (REVERSE + UNDIRECTED), stima memoria (log-only),
// PPR seedata + betweenness campionata + Leiden, combinazione dei
// punteggi, write-back, pulizia SEMPRE eseguita delle proiezioni (anche in
// caso di errore). logf non è mai nil-checked dai chiamanti interni:
// passare un no-op se non serve logging.
func Run(ctx context.Context, driver neo4j.DriverWithContext, cfg Config, logf func(format string, args ...any), hooks Hooks) (Result, error) {
	if cfg.EntryNodeID == "" {
		return Result{}, fmt.Errorf("gdsimpact: entry_node_id obbligatorio")
	}
	if err := cfg.Scope.validate(); err != nil {
		return Result{}, fmt.Errorf("gdsimpact: projection scope non valido: %w", err)
	}
	if cfg.MaxDepth <= 0 {
		return Result{}, fmt.Errorf("gdsimpact: max_depth deve essere >= 1, ricevuto %d", cfg.MaxDepth)
	}

	deps, err := discoverSubgraph(ctx, driver, cfg.EntryNodeID, cfg.Scope, cfg.MaxDepth)
	if err != nil {
		return Result{}, err
	}
	if len(deps) == 0 {
		// entry_node_id inesistente O senza dipendenti: stesso trattamento
		// (SPEC-043 §3 scenario 4) — nessuna proiezione creata (nulla da
		// pulire), nessun impact_score scritto, nessun errore.
		logf("gdsimpact: nessun dipendente scoperto entro max_depth=%d — nessuna proiezione GDS creata", cfg.MaxDepth)
		return Result{}, nil
	}
	logf("gdsimpact: %d dipendenti scoperti entro max_depth=%d", len(deps), cfg.MaxDepth)

	proj, err := createProjections(ctx, driver, cfg.EntryNodeID, cfg.Scope, deps)
	// SPEC-043 §4: se la proiezione (anche solo quella REVERSE) è già
	// stata creata, un tentativo di gds.graph.drop viene comunque eseguito
	// PRIMA di propagare l'errore originale — defer copre esattamente
	// questo, sia sull'errore di createProjections stesso (proj porta i
	// nomi parzialmente creati) sia su qualunque fallimento successivo.
	defer dropProjections(ctx, driver, proj, logf)
	if err != nil {
		return Result{}, err
	}
	logf("gdsimpact: proiezioni autorizzate create (nodeCount=%d)", proj.NodeCount)

	if hooks.AfterProject != nil {
		if err := hooks.AfterProject(); err != nil {
			return Result{}, err
		}
	}

	samplingSize := cfg.SamplingSize
	if samplingSize <= 0 {
		// SPEC-043 §2 punto 4 / §3 scenario 5: default = conteggio nodi
		// della proiezione (risultati esatti).
		samplingSize = int(proj.NodeCount)
	}
	logAlgorithmEstimates(ctx, driver, proj, samplingSize, logf)

	pprRaw, err := runPageRank(ctx, driver, proj.ReverseName, cfg.EntryNodeID, cfg.Scope)
	if err != nil {
		return Result{}, err
	}
	bcRaw, err := runBetweenness(ctx, driver, proj.ReverseName, samplingSize)
	if err != nil {
		return Result{}, err
	}
	communities, err := runLeiden(ctx, driver, proj.UndirectedName)
	if err != nil {
		return Result{}, err
	}

	scores := combineScores(deps, pprRaw, bcRaw, communities, cfg)

	if err := writeBack(ctx, driver, cfg.Scope, scores); err != nil {
		return Result{}, err
	}
	logf("gdsimpact: impact_score scritto su %d nodi", len(scores))

	return Result{Scores: scores, BetweennessSamplingSize: samplingSize}, nil
}
