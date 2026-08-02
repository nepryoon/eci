package observability

import (
	"context"
	"testing"
)

// SPEC-011 §3 scenario 1 è verificato manualmente (esempio in
// examplemain/main.go, output incollato nel report) — non automatizzabile
// in modo pulito senza parsing dell'output (§7, stessa scelta di
// SPEC-010).
//
// Qui: verifica automatica che InitTracing non erri alla prima chiamata,
// ritorni uno shutdown funzionante, e che la seconda chiamata nello stesso
// processo sia un no-op non panicante (SPEC-011 §4 edge case, stesso
// comportamento di SPEC-010 §4).
func TestInitTracingSecondCallIsNoOpNotPanic(t *testing.T) {
	shutdown1, err := InitTracing("eci-observability-tests")
	if err != nil {
		t.Fatalf("prima InitTracing: %v", err)
	}
	defer func() {
		if err := shutdown1(context.Background()); err != nil {
			t.Errorf("shutdown1: %v", err)
		}
	}()

	shutdown2, err := InitTracing("eci-observability-tests-second")
	if err != nil {
		t.Fatalf("seconda InitTracing (deve essere no-op, non errore): %v", err)
	}
	if err := shutdown2(context.Background()); err != nil {
		t.Errorf("shutdown2 (provider inerte, mai collegato al provider globale): %v", err)
	}
}
