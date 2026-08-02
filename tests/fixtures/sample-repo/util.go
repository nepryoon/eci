package main

// computeTotal è una funzione standalone (nodo Function nel CPG, nessun
// receiver). Definita qui in util.go, chiamata da OrderService.Process in
// order_service.go — relazione CALLS CROSS-FILE, che T1.1 NON deve
// catturare a questo stadio (walking skeleton limitato a CALLS intra-file,
// vedi SPEC-009 §2/§5). Le due chiamate cross-file del fixture esistono
// deliberatamente nel sorgente per verificare che restino senza arco
// CALLS, non per errore di omissione.
func computeTotal(prices []float64) float64 {
	var total float64
	for _, p := range prices {
		total += p
	}
	return total
}
