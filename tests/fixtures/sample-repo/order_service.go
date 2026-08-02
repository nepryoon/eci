package main

import "errors"

// OrderService è un nodo Class nel CPG (struct Go, mappato a Class per
// SPEC-009 §2 — Go non ha "classi" in senso stretto).
type OrderService struct {
	MinItems int
}

// Process è un nodo Method (receiver *OrderService). Chiama Validate nello
// STESSO FILE (order_service.go) — relazione CALLS INTRA-FILE, l'UNICO
// caso che T1.1 deve dimostrare di catturare correttamente (SPEC-009 §2,
// non negoziabile per l'utilità del fixture). Chiama anche computeTotal,
// definita in util.go — relazione CALLS CROSS-FILE, che NON deve avere un
// arco CALLS a questo stadio (fuori scope walking-skeleton, SPEC-009 §5).
func (s *OrderService) Process(prices []float64) (float64, error) {
	if err := s.Validate(prices); err != nil {
		return 0, err
	}
	return computeTotal(prices), nil
}

// Validate è un nodo Method (receiver *OrderService), nessuna chiamata in
// uscita.
func (s *OrderService) Validate(prices []float64) error {
	if len(prices) < s.MinItems {
		return errors.New("too few items")
	}
	return nil
}
