package main

// main istanzia OrderService e chiama Process — relazione CALLS
// CROSS-FILE rispetto a dove OrderService/Process sono definiti
// (order_service.go). Deliberato: verifica che T1.1 NON produca un arco
// CALLS per questa chiamata (fuori scope walking-skeleton, SPEC-009
// §2/§5). La chiamata resta nel sorgente, com'è normale per codice reale
// con dipendenze cross-file — solo l'arco CALLS non deve comparire
// nell'output di T1.1 a questo stadio.
func main() {
	svc := &OrderService{MinItems: 1}
	if _, err := svc.Process([]float64{19.99, 5.50}); err != nil {
		panic(err)
	}
}
