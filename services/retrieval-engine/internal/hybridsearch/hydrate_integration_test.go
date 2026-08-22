//go:build integration

// Test di integrazione white-box (package hybridsearch, non
// hybridsearch_test): isola SPECIFICAMENTE l'edge case SPEC-045 §4 riga 2
// ("il lookup batch name su Neo4j fallisce -> errore esplicito"), separato
// dal fallimento più generico di GraphTraversal (che condivide lo stesso
// driver — un driver interamente rotto farebbe fallire ENTRAMBI, non
// isolando quale fase ha effettivamente fallito). Qui si chiama
// hydrateNames direttamente con un driver reale ma verso un indirizzo non
// raggiungibile, indipendentemente da qualunque altra fase della pipeline.
package hybridsearch

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestHydrateNames_Neo4jUnreachableFailsExplicitly(t *testing.T) {
	badDriver, err := neo4j.NewDriverWithContext("bolt://127.0.0.1:1", neo4j.NoAuth())
	if err != nil {
		t.Fatalf("driver verso indirizzo non raggiungibile: %v", err)
	}
	defer badDriver.Close(context.Background())

	nodes := []RetrievedNode{{NodeID: "n1"}}
	err = hydrateNames(context.Background(), badDriver, nodes)
	if err == nil {
		t.Fatal("atteso errore esplicito con Neo4j irraggiungibile, ottenuto nil")
	}
}
