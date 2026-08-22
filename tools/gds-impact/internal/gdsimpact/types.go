package gdsimpact

// NodeScore è il dettaglio completo del calcolo per UN nodo del sottografo
// (tutte le componenti intermedie, non solo il risultato finale) — esposto
// da Result così un chiamante (i test di SPEC-043 §3 scenario 1 in primis)
// può ricalcolare `impact_score` con la formula dichiarata (§2 punto 6) a
// partire dalle stesse componenti persistite, senza dover reimplementare
// PageRank/betweenness per verificarne il valore scritto.
type NodeScore struct {
	NodeID          string
	HopDistance     int
	EdgeType        string // tipo d'arco dell'ultimo hop del percorso più breve
	ImpactKind      string // derivato da EdgeType, §2
	PPRRaw          float64
	PPRNorm         float64
	BetweennessRaw  float64
	BetweennessNorm float64
	CommunityID     int64
	ImpactScore     float64
}

// Result riassume un'esecuzione completa del job (SPEC-043 §8, log
// testuale + dati per la verifica nei test).
type Result struct {
	Scores []NodeScore
	// BetweennessSamplingSize è il valore EFFETTIVAMENTE passato a
	// gds.betweenness.stream — SPEC-043 §3 scenario 5: "verificabile
	// ispezionando il parametro passato, non solo il risultato finale".
	BetweennessSamplingSize int
}

// Hooks — seam di test (SPEC-043 §3 scenario 3): AfterProject, se non nil,
// viene invocata subito dopo la creazione delle proiezioni GDS, PRIMA di
// eseguire qualunque algoritmo. Un errore ritornato interrompe la pipeline
// esattamente come un fallimento reale in quel punto — la pulizia delle
// proiezioni (deferred, sempre eseguita) è quindi esercitata dallo stesso
// percorso di codice usato per un fallimento vero, non un percorso di test
// separato. nil in produzione.
type Hooks struct {
	AfterProject func() error
}
