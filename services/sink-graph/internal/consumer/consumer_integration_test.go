//go:build integration

// SPEC-015 §7 — test di integrazione: Kafka + Neo4j + Postgres via
// testcontainers (pattern analogo a SPEC-008 per l'orchestrazione
// multi-container, Neo4j al posto di Kafka Connect come destinazione
// finale da verificare). Il consumer (ProcessMessage/FetchAndProcess) è
// istanziato DIRETTAMENTE nel test, non via il binario `sink-graph`:
// messaggi Kafka prodotti sinteticamente con gli header giusti (event_id,
// trace_id), niente Kafka Connect/Debezium qui — quello è verificato
// separatamente in SPEC-008 (routing/header) e nello scenario 4 di questa
// SPEC (end-to-end reale, manuale, non in questo file).
package consumer_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/eci-project/eci/libs/go/eci/metrics"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-graph/internal/consumer"
)

const (
	neo4jAdminPassword = "eci-test-password-1234"
	postgresUser       = "eci"
	postgresPassword   = "eci-test-password-1234"
	postgresDB         = "eci"
	repoPlaceholder    = "local"
	tenantPlaceholder  = "tenant-test"
	aclPlaceholder     = "developers"
)

// stack raggruppa Neo4j/Postgres, condivisi da tutti gli scenari di
// questo file (un solo avvio, stesso spirito di SPEC-008). Kafka NON è
// condiviso: ogni scenario di primo livello avvia il PROPRIO broker
// dedicato (startKafka, sotto) — un consumer group nuovo (StartOffset di
// default = FirstOffset, SPEC-015 §10) rilegge dall'inizio del topic
// condiviso, quindi un broker condiviso tra scenari farebbe rileggere a
// uno scenario i messaggi già prodotti da un altro scenario precedente,
// non solo il proprio. Verificato empiricamente (prima versione di
// questo file, broker condiviso: scenario2 riceveva il messaggio di
// scenario1 come "prima consegna").
type stack struct {
	driver neo4j.DriverWithContext
	db     *sql.DB
}

func TestSinkGraphConsumer(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	t.Run("Scenario1_NewCodeNodeMethodMerged", func(t *testing.T) {
		scenario1NewCodeNodeMethodMerged(t, ctx, st)
	})
	t.Run("Scenario2_RedeliveryDoesNotDuplicate", func(t *testing.T) {
		scenario2RedeliveryDoesNotDuplicate(t, ctx, st)
	})
	t.Run("Scenario3_RelationBeforeNodesThenEnriched", func(t *testing.T) {
		scenario3RelationBeforeNodesThenEnriched(t, ctx, st)
	})
	t.Run("Scenario5_InvalidEnumDiscardedNoNeo4jWrite", func(t *testing.T) {
		scenario5InvalidEnumDiscardedNoNeo4jWrite(t, ctx, st)
	})
}

// ============================================================
// SPEC-035 §3 scenario 5 (T3.3 parte 1/2) — sink RAPPRESENTATIVO scelto
// per il test di integrazione del wrapping resilience.WithRetryAndDLQ
// (motivazione riportata nel report di consegna e in SPEC-035 §10):
// sink-graph è il PRIMO sink costruito in questo progetto (SPEC-015,
// T1.3) e il modello esplicito che gli altri tre dichiarano di replicare
// ("stesso scheletro di sink-graph", commenti in embedding-worker/
// sink-vector/sink-search) — la scelta più naturale per rappresentare il
// pattern comune ai quattro. La libreria condivisa è già testata
// esaustivamente in libs/go/eci/resilience (SPEC-035 §7, tutti gli
// scenari 1-4 + edge case); qui si verifica SOLO che il wiring in
// main.go/FetchAndProcess sia GENUINAMENTE attivo nel consume-loop REALE
// di questo servizio — non presunto dal solo fatto che compili (§3
// scenario 5).
//
// Deps.DB punta DELIBERATAMENTE a un indirizzo Postgres irraggiungibile:
// nessun container Postgres/Neo4j in questo test — non servono. Il dedup
// via processed_events è il PRIMO passo di ProcessMessage per costruzione
// (vedi consumer.go), quindi fallisce già lì per QUALUNQUE messaggio,
// prima di toccare Neo4j — Deps.Neo4j resta nil, mai dereferenziato.
// Questo rende ProcessMessage un fallimento deterministico e sempre
// riproducibile, che esercita DAVVERO il percorso retry->DLQ del wrapper
// reale (resilience.WithRetryAndDLQ) attraverso il consume-loop reale di
// sink-graph (consumer.FetchAndProcess, non una sua reimplementazione).
func TestSinkGraphResilienceWrappingGenuinelyWired(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)

	topic := consumer.TopicCodeNode
	ensureTopics(t, ctx, brokers, topic+".DLQ")

	unreachableDB, err := sql.Open("postgres", "postgres://eci:x@127.0.0.1:1/eci?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open (DSN irraggiungibile): %v", err)
	}
	defer unreachableDB.Close()

	deps := consumer.Deps{
		DB:    unreachableDB,
		Neo4j: nil,
		Logf:  t.Logf,
	}

	retryProducer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer retryProducer.Close()

	cfg := resilience.Config{MaxRetries: 1, BackoffBase: 100 * time.Millisecond}
	process := resilience.WithRetryAndDLQ(cfg, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})

	eventID := uniqueEventID(t)
	nodeID := uniqueID(t, "resilience-wiring")
	payload := codeNodePayload(nodeID, "Whatever", "Method", "go", "x.go")
	produce(t, ctx, brokers, topic, nodeID, payload, eventID, "")

	groupID := "sink-graph-resilience-wiring-" + eventID
	reader := newReaderWithGroup(brokers, groupID)
	defer reader.Close()

	// Primo tentativo (retryCount=0 < MaxRetries=1): il dedup fallisce
	// (Postgres irraggiungibile) -> il wrapper deve ripubblicare sullo
	// STESSO topic con retry-count=1 e ritornare nil (offset originale
	// committato dal consume-loop reale).
	fetchCtx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	outcome1, err := consumer.FetchAndProcess(fetchCtx1, reader, process)
	cancel1()
	if err != nil {
		t.Fatalf("FetchAndProcess (primo tentativo): %v — atteso nil (il wrapper deve gestire l'errore internamente, non propagarlo)", err)
	}
	if outcome1 != resilience.OutcomeRetried {
		t.Fatalf("outcome primo tentativo = %v, want OutcomeRetried — il wrapping NON risulta genuinamente attivo nel consume-loop di sink-graph", outcome1)
	}

	// Secondo tentativo: retryCount=1 == MaxRetries=1 -> DLQ.
	fetchCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	outcome2, err := consumer.FetchAndProcess(fetchCtx2, reader, process)
	cancel2()
	if err != nil {
		t.Fatalf("FetchAndProcess (secondo tentativo): %v", err)
	}
	if outcome2 != resilience.OutcomeDeadLettered {
		t.Fatalf("outcome secondo tentativo = %v, want OutcomeDeadLettered", outcome2)
	}

	// Verifica diretta e conclusiva: il messaggio DEVE comparire sul topic
	// DLQ reale, prodotto dal codice reale di sink-graph attraverso il suo
	// consume-loop reale — prova che il wiring è genuinamente attivo, non
	// presunto dal solo fatto che compili (SPEC-035 §3 scenario 5).
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        "sink-graph-resilience-wiring-dlq-" + eventID,
		GroupTopics:    []string{topic + ".DLQ"},
		CommitInterval: 0,
	})
	defer dlqReader.Close()
	dlqFetchCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	dlqMsg, err := dlqReader.FetchMessage(dlqFetchCtx)
	if err != nil {
		t.Fatalf("FetchMessage su %s.DLQ: %v", topic, err)
	}
	if got := resilience.RetryCount(dlqMsg.Headers); got != cfg.MaxRetries {
		t.Fatalf("retry-count del messaggio in DLQ = %d, want %d (valore massimo)", got, cfg.MaxRetries)
	}
	if string(dlqMsg.Value) != string(payload) {
		t.Fatalf("payload del messaggio in DLQ non combacia con quello originale prodotto da sink-graph")
	}
}

// ============================================================
// SPEC-036 §3 scenario 5 (T3.3 parte 2/2) — sink RAPPRESENTATIVO scelto
// per il test di integrazione di libs/go/eci/metrics: sink-graph, stesso
// principio (e stesso motivo) già usato per SPEC-035 §3 scenario 5 —
// esplicitamente riproposto anche dal testo di SPEC-036 §3 scenario 5
// stessa ("stesso principio di SPEC-035, sink-graph"). Riusa lo stesso
// harness leggero della funzione precedente (Postgres irraggiungibile,
// nessun container Postgres/Neo4j necessario — il dedup fallisce già al
// primo passo di ProcessMessage, producendo un esito deterministico
// (OutcomeRetried) sufficiente a dimostrare la composizione reale
// main.go: metrics.WithPrometheus(resilience.WithRetryAndDLQ(...))),
// aggiungendovi un server HTTP REALE (non httptest.Recorder, già usato
// nel test unitario della libreria stessa) — verifica diretta via
// richiesta di rete reale che /metrics esponga testo Prometheus valido
// coi nomi di metrica dichiarati (§3 scenario 5: "non solo ispezione del
// registry in-process").
func TestSinkGraphMetricsExposedViaRealHTTP(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)

	unreachableDB, err := sql.Open("postgres", "postgres://eci:x@127.0.0.1:1/eci?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open (DSN irraggiungibile): %v", err)
	}
	defer unreachableDB.Close()

	deps := consumer.Deps{DB: unreachableDB, Neo4j: nil, Logf: t.Logf}

	retryProducer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer retryProducer.Close()

	// sinkName UNICO per questo test: metrics.WithPrometheus registra le
	// serie sul registry GLOBALE di default di prometheus/client_golang
	// (stesso registry servito da Handler() in produzione, SPEC-036 §2) —
	// un'etichetta "sink" univoca evita che questo test legga contatori
	// eventualmente incrementati da altri test/processi che condividono
	// lo stesso registry in-process.
	sinkName := "sink-graph-metrics-http-test-" + uniqueEventID(t)

	cfg := resilience.Config{MaxRetries: 5, BackoffBase: 200 * time.Millisecond}
	process := resilience.WithRetryAndDLQ(cfg, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})
	// Stessa identica composizione di main.go (SPEC-036 §2): metriche come
	// strato PIÙ ESTERNO.
	process = metrics.WithPrometheus(sinkName, process)

	// Server HTTP REALE su una porta libera, stessa identica forma di
	// main.go (mux + metrics.Handler() su "/metrics").
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind porta libera: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	eventID := uniqueEventID(t)
	nodeID := uniqueID(t, "metrics-http")
	payload := codeNodePayload(nodeID, "MetricsHTTPTest", "Method", "go", "x.go")
	produce(t, ctx, brokers, consumer.TopicCodeNode, nodeID, payload, eventID, "")

	reader := newReaderWithGroup(brokers, "sink-graph-metrics-http-"+eventID)
	defer reader.Close()

	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	outcome, err := consumer.FetchAndProcess(fetchCtx, reader, process)
	cancel()
	if err != nil {
		t.Fatalf("FetchAndProcess: %v", err)
	}
	if outcome != resilience.OutcomeRetried {
		t.Fatalf("outcome = %v, want OutcomeRetried (precondizione: Postgres irraggiungibile)", outcome)
	}

	// Richiesta HTTP REALE (net/http.Get su una connessione di rete vera,
	// non un ResponseRecorder in-process) verso l'endpoint /metrics.
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", listener.Addr().String()))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status /metrics = %d, want 200", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("lettura corpo /metrics: %v", err)
	}
	body := string(bodyBytes)

	for _, name := range []string{"eci_messages_processed_total", "eci_last_processed_timestamp_seconds"} {
		if !strings.Contains(body, name) {
			t.Fatalf("/metrics non contiene la metrica dichiarata %q:\n%s", name, body)
		}
	}
	wantSeries := fmt.Sprintf(`eci_messages_processed_total{outcome="retried",sink=%q} 1`, sinkName)
	if !strings.Contains(body, wantSeries) {
		t.Fatalf("/metrics non contiene la serie attesa %q:\n%s", wantSeries, body)
	}
	if !strings.Contains(body, fmt.Sprintf(`eci_last_processed_timestamp_seconds{sink=%q}`, sinkName)) {
		t.Fatalf("/metrics non contiene eci_last_processed_timestamp_seconds per sink=%q:\n%s", sinkName, body)
	}
}

// ============================================================
// Scenario 1
// ============================================================

func scenario1NewCodeNodeMethodMerged(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	nodeID := uniqueID(t, "scenario1-method")
	eventID := uniqueEventID(t)

	payload := codeNodePayload(nodeID, "Process", "Method", "go", "order_service.go")
	produce(t, ctx, brokers, consumer.TopicCodeNode, nodeID, payload, eventID, "")

	outcome := fetchAndProcessOnce(t, ctx, brokers, deps)
	if outcome != consumer.OutcomeMerged {
		t.Fatalf("outcome = %v, want OutcomeMerged", outcome)
	}

	labels, props := readNode(t, ctx, st.driver, nodeID)
	wantLabels := map[string]bool{"CodeNode": true, "Method": true}
	if len(labels) != len(wantLabels) {
		t.Fatalf("labels = %v, want esattamente %v", labels, wantLabels)
	}
	for _, l := range labels {
		if !wantLabels[l] {
			t.Fatalf("label inattesa %q in %v", l, labels)
		}
	}
	if props["name"] != "Process" {
		t.Errorf("name = %v, want %q", props["name"], "Process")
	}
	if props["ast_hash"] != "go" {
		t.Errorf("ast_hash = %v, want %q", props["ast_hash"], "go")
	}
	if props["symbol_id"] != nodeID {
		t.Errorf("symbol_id = %v, want %q (= id, semplificazione dichiarata SPEC-015 §2)", props["symbol_id"], nodeID)
	}
	if props["tenant_id"] != tenantPlaceholder || props["repo"] != repoPlaceholder || props["acl_group"] != aclPlaceholder {
		t.Errorf("security labels = (%v,%v,%v), want (%q,%q,%q)",
			props["tenant_id"], props["repo"], props["acl_group"],
			tenantPlaceholder, repoPlaceholder, aclPlaceholder)
	}

	assertProcessedEvent(t, ctx, st.db, eventID)
}

// ============================================================
// Scenario 2 — redelivery at-least-once PRIMA del commit dell'offset.
// ============================================================

func scenario2RedeliveryDoesNotDuplicate(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	nodeID := uniqueID(t, "scenario2-method")
	eventID := uniqueEventID(t)

	payload := codeNodePayload(nodeID, "Validate", "Method", "go", "order_service.go")
	produce(t, ctx, brokers, consumer.TopicCodeNode, nodeID, payload, eventID, "")

	groupID := "sink-graph-test-scenario2-" + eventID

	// Prima "consegna": fetch + process, MA nessun commit — simula un
	// riavvio del consumer prima del commit dell'offset (SPEC-015 §3
	// scenario 2).
	reader1 := newReaderWithGroup(brokers, groupID)
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	msg1, err := reader1.FetchMessage(fetchCtx)
	cancel()
	if err != nil {
		t.Fatalf("FetchMessage (prima consegna): %v", err)
	}
	outcome1, err := consumer.ProcessMessage(ctx, deps, msg1.Topic, msg1.Value, msg1.Headers)
	if err != nil {
		t.Fatalf("ProcessMessage (prima consegna): %v", err)
	}
	if outcome1 != consumer.OutcomeMerged {
		t.Fatalf("outcome1 = %v, want OutcomeMerged", outcome1)
	}
	if err := reader1.Close(); err != nil {
		t.Fatalf("chiusura reader1 (simulazione riavvio): %v", err)
	}

	countAfterFirst := countNodesWithID(t, ctx, st.driver, nodeID)
	if countAfterFirst != 1 {
		t.Fatalf("nodi con id=%s dopo la prima consegna = %d, want 1", nodeID, countAfterFirst)
	}

	// "Riavvio": nuovo reader, STESSO group id — l'offset non committato
	// fa sì che lo stesso messaggio venga riconsegnato.
	reader2 := newReaderWithGroup(brokers, groupID)
	defer reader2.Close()
	fetchCtx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	msg2, err := reader2.FetchMessage(fetchCtx2)
	cancel2()
	if err != nil {
		t.Fatalf("FetchMessage (redelivery): %v", err)
	}
	if string(msg2.Value) != string(msg1.Value) {
		t.Fatalf("redelivery: valore del messaggio diverso dal primo fetch (test invalido): got %s want %s", msg2.Value, msg1.Value)
	}
	outcome2, err := consumer.ProcessMessage(ctx, deps, msg2.Topic, msg2.Value, msg2.Headers)
	if err != nil {
		t.Fatalf("ProcessMessage (redelivery): %v", err)
	}
	if outcome2 != consumer.OutcomeDuplicate {
		t.Fatalf("outcome2 = %v, want OutcomeDuplicate (dedup via processed_events)", outcome2)
	}
	if err := reader2.CommitMessages(ctx, msg2); err != nil {
		t.Fatalf("commit offset dopo redelivery: %v", err)
	}

	countAfterRedelivery := countNodesWithID(t, ctx, st.driver, nodeID)
	if countAfterRedelivery != 1 {
		t.Fatalf("nodi con id=%s dopo la redelivery = %d, want ancora 1 (nessun duplicato)", nodeID, countAfterRedelivery)
	}
}

// ============================================================
// Scenario 3 — CodeRelation arriva PRIMA dei CodeNode corrispondenti.
// ============================================================

func scenario3RelationBeforeNodesThenEnriched(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	// UN SOLO reader/group per l'intero scenario, riusato per tutti e tre
	// i fetch sotto (non un gruppo nuovo per ciascuno, SPEC-015 §10): un
	// gruppo nuovo riparte da FirstOffset, quindi rileggerebbe i messaggi
	// precedenti di QUESTO STESSO scenario già consumati, non solo quello
	// nuovo — stesso meccanismo del problema di isolamento cross-scenario
	// risolto sopra in startKafka, qui dentro un singolo scenario con più
	// messaggi in sequenza sullo stesso topic.
	reader := newReaderWithGroup(brokers, "sink-graph-test-scenario3-"+uniqueID(t, "group"))
	defer reader.Close()

	fromID := uniqueID(t, "scenario3-process")
	toID := uniqueID(t, "scenario3-validate")
	relEventID := uniqueEventID(t)

	relPayload := codeRelationPayload(fromID, toID, "CALLS", intPtr(1))
	produce(t, ctx, brokers, consumer.TopicCodeRelation, fromID+"->"+toID, relPayload, relEventID, "")

	outcome := fetchAndProcessWithReader(t, ctx, reader, deps)
	if outcome != consumer.OutcomeMerged {
		t.Fatalf("outcome (CodeRelation) = %v, want OutcomeMerged", outcome)
	}

	// Placeholder: solo :CodeNode, nessun'altra label, nessuna proprietà
	// oltre id.
	for _, id := range []string{fromID, toID} {
		labels, props := readNode(t, ctx, st.driver, id)
		if len(labels) != 1 || labels[0] != "CodeNode" {
			t.Fatalf("nodo placeholder id=%s: labels = %v, want [CodeNode]", id, labels)
		}
		if len(props) != 1 {
			t.Fatalf("nodo placeholder id=%s: props = %v, want solo {id: ...}", id, props)
		}
	}
	weight := readRelWeight(t, ctx, st.driver, fromID, toID, "CALLS")
	if weight != 1 {
		t.Fatalf("weight arco CALLS = %v, want 1", weight)
	}

	// Ora arrivano i CodeNode: devono arricchire gli STESSI nodi (stesso
	// id, MERGE), non crearne di nuovi.
	fromEventID := uniqueEventID(t)
	toEventID := uniqueEventID(t)
	produce(t, ctx, brokers, consumer.TopicCodeNode, fromID, codeNodePayload(fromID, "Process", "Method", "go", "order_service.go"), fromEventID, "")
	produce(t, ctx, brokers, consumer.TopicCodeNode, toID, codeNodePayload(toID, "Validate", "Method", "go", "order_service.go"), toEventID, "")

	if o := fetchAndProcessWithReader(t, ctx, reader, deps); o != consumer.OutcomeMerged {
		t.Fatalf("outcome (CodeNode from) = %v, want OutcomeMerged", o)
	}
	if o := fetchAndProcessWithReader(t, ctx, reader, deps); o != consumer.OutcomeMerged {
		t.Fatalf("outcome (CodeNode to) = %v, want OutcomeMerged", o)
	}

	if n := countNodesWithID(t, ctx, st.driver, fromID); n != 1 {
		t.Fatalf("nodi con id=%s dopo l'arricchimento = %d, want 1 (nessun duplicato)", fromID, n)
	}
	labelsFrom, propsFrom := readNode(t, ctx, st.driver, fromID)
	if !containsAll(labelsFrom, "CodeNode", "Method") {
		t.Fatalf("labels dopo arricchimento = %v, want CodeNode+Method", labelsFrom)
	}
	if propsFrom["name"] != "Process" {
		t.Fatalf("name dopo arricchimento = %v, want Process", propsFrom["name"])
	}

	weightAfter := readRelWeight(t, ctx, st.driver, fromID, toID, "CALLS")
	if weightAfter != 1 {
		t.Fatalf("weight dopo l'arricchimento dei nodi = %v, want ancora 1 (l'arco non va ritoccato)", weightAfter)
	}
}

// ============================================================
// Scenario 5 — enum non valido: scartato, nessuna scrittura Neo4j.
// ============================================================

func scenario5InvalidEnumDiscardedNoNeo4jWrite(t *testing.T, ctx context.Context, st *stack) {
	deps := newDeps(st)
	brokers := startKafka(t, ctx)
	reader := newReaderWithGroup(brokers, "sink-graph-test-"+uniqueID(t, "scenario5-group"))
	defer reader.Close()

	t.Run("node_type_non_valido", func(t *testing.T) {
		badID := uniqueID(t, "scenario5-bad-node-type")
		eventID := uniqueEventID(t)
		payload := codeNodePayload(badID, "Bogus", "BogusType", "go", "x.go")
		produce(t, ctx, brokers, consumer.TopicCodeNode, badID, payload, eventID, "")

		outcome := fetchAndProcessWithReader(t, ctx, reader, deps)
		if outcome != consumer.OutcomeInvalidSkipped {
			t.Fatalf("outcome = %v, want OutcomeInvalidSkipped", outcome)
		}
		if n := countNodesWithID(t, ctx, st.driver, badID); n != 0 {
			t.Fatalf("nodi con id=%s = %d, want 0 (nessuna scrittura Neo4j per un node_type non valido)", badID, n)
		}
	})

	t.Run("rel_type_non_valido", func(t *testing.T) {
		fromID := uniqueID(t, "scenario5-bad-rel-from")
		toID := uniqueID(t, "scenario5-bad-rel-to")
		eventID := uniqueEventID(t)
		payload := codeRelationPayload(fromID, toID, "BOGUS_REL", nil)
		produce(t, ctx, brokers, consumer.TopicCodeRelation, fromID+"->"+toID, payload, eventID, "")

		outcome := fetchAndProcessWithReader(t, ctx, reader, deps)
		if outcome != consumer.OutcomeInvalidSkipped {
			t.Fatalf("outcome = %v, want OutcomeInvalidSkipped", outcome)
		}
		if n := countNodesWithID(t, ctx, st.driver, fromID); n != 0 {
			t.Fatalf("nodi con id=%s (endpoint from) = %d, want 0 — un rel_type non valido non deve creare NEMMENO i placeholder", fromID, n)
		}
		if n := countNodesWithID(t, ctx, st.driver, toID); n != 0 {
			t.Fatalf("nodi con id=%s (endpoint to) = %d, want 0", toID, n)
		}
	})
}

// ============================================================
// Harness: 3 container testcontainers.
// ============================================================

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()

	driver := startNeo4j(t, ctx)
	db := startPostgres(t, ctx)

	return &stack{driver: driver, db: db}
}

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	// confluentinc/confluent-local, non apache/kafka:latest usato altrove
	// nel repo (SPEC-006/007/008): il modulo ufficiale
	// testcontainers-go/modules/kafka risolve l'advertised.listeners con
	// la porta host dinamica tramite uno starter script iniettato post-
	// avvio — un consumer/produttore kafka-go REALE dal processo host
	// (non un Exec dentro al container, a differenza di SPEC-008) richiede
	// esattamente questo meccanismo. Deviazione dichiarata (SPEC-015 §10).
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("avvio container kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka Brokers: %v", err)
	}
	// confluent-local non ha auto.create.topics.enable=true di default
	// (a differenza di apache/kafka:latest usato nello stack reale,
	// verificato empiricamente: WriteMessages con AllowAutoTopicCreation
	// falliva con "Unknown Topic Or Partition") — i topic vanno creati
	// esplicitamente prima di produrre (SPEC-015 §10).
	ensureTopics(t, ctx, brokers, consumer.TopicCodeNode, consumer.TopicCodeRelation)
	return brokers
}

func ensureTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka per creazione topic: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("kafka Controller: %v", err)
	}
	controllerAddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("dial kafka controller (%s): %v", controllerAddr, err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		t.Fatalf("CreateTopics %v: %v", topics, err)
	}
}

func startNeo4j(t *testing.T, ctx context.Context) neo4j.DriverWithContext {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(neo4jAdminPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j:5-community: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container neo4j: %v", err)
		}
	})
	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("BoltUrl: %v", err)
	}
	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", neo4jAdminPassword, ""))
	if err != nil {
		t.Fatalf("driver neo4j: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(ctx) })

	// D3 (contracts/cypher/schema.cypher, SPEC-004) applicato per davvero:
	// i constraint file_path_unique/method_symbol_unique che questo
	// consumer deve rispettare esistono solo se lo schema è applicato,
	// esattamente come lo stack reale di SPEC-006/007.
	applyNeo4jSchema(t, ctx, driver)

	return driver
}

func applyNeo4jSchema(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	src := readRepoFile(t, "contracts", "cypher", "schema.cypher")
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	for _, stmt := range splitCypherStatements(src) {
		if _, err := session.Run(ctx, stmt, nil); err != nil {
			t.Fatalf("applicazione schema.cypher, statement %q: %v", firstLine(stmt), err)
		}
	}
}

func startPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v (vedi SPEC-005)", err)
	}

	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(postgresUser),
		tcpostgres.WithPassword(postgresPassword),
		tcpostgres.WithDatabase(postgresDB),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("avvio container postgres:17: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container postgres: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}

	migrationsDir := readRepoPath(t, "contracts", "sql", "migrations")
	cmd := exec.CommandContext(ctx, "migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("migrate up fallita: %v\noutput:\n%s", err, out)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

// ============================================================
// Produzione/consumo messaggi sintetici.
// ============================================================

func newDeps(st *stack) consumer.Deps {
	return consumer.Deps{
		DB:    st.db,
		Neo4j: st.driver,
		Logf:  func(format string, args ...any) {},
	}
}

func produce(t *testing.T, ctx context.Context, brokers []string, topic, key string, value []byte, eventID, traceID string) {
	t.Helper()
	headers := []kafka.Header{{Key: "event_id", Value: []byte(eventID)}}
	if traceID != "" {
		headers = append(headers, kafka.Header{Key: "trace_id", Value: []byte(traceID)})
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer w.Close()
	err := w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value, Headers: headers})
	if err != nil {
		t.Fatalf("produzione messaggio sintetico su %s: %v", topic, err)
	}
}

// fetchAndProcessOnce usa un consumer group DEDICATO (univoco per
// chiamata) — ogni scenario legge il proprio messaggio in isolamento,
// senza interferenze di group/partition assignment tra scenari diversi
// che condividono lo stesso stack.
func fetchAndProcessOnce(t *testing.T, ctx context.Context, brokers []string, deps consumer.Deps) consumer.Outcome {
	t.Helper()
	reader := newReaderWithGroup(brokers, "sink-graph-test-"+uniqueID(t, "group"))
	defer reader.Close()
	return fetchAndProcessWithReader(t, ctx, reader, deps)
}

// fetchAndProcessWithReader riusa un reader/group già esistente — a
// differenza di fetchAndProcessOnce (un group nuovo ad ogni chiamata),
// serve quando più messaggi vanno consumati IN SEQUENZA sullo stesso
// scenario (es. scenario 3): un group nuovo per ciascun fetch
// rileggerebbe da FirstOffset anche i messaggi già consumati in
// precedenza dallo stesso scenario (SPEC-015 §10).
//
// SPEC-035: FetchAndProcess accetta ora un resilience.ProcessFunc iniettato
// (nessuna modifica alla logica applicativa di ProcessMessage stessa) e
// ritorna il suo Outcome generico (Processed/Retried/DeadLettered), non
// più il consumer.Outcome specifico del servizio (Merged/Duplicate/
// InvalidSkipped) su cui questi test si basano da SPEC-015. L'adapter qui
// sotto chiama ProcessMessage DIRETTAMENTE (senza passare da
// resilience.WithRetryAndDLQ: questi scenari testano la logica applicativa
// del sink, non il retry/DLQ, già coperto esaustivamente dal test di
// libs/go/eci/resilience, SPEC-035 §7) e cattura il suo Outcome reale
// tramite una variabile catturata per closure, restituita al chiamante
// invece del valore generico di FetchAndProcess.
func fetchAndProcessWithReader(t *testing.T, ctx context.Context, reader *kafka.Reader, deps consumer.Deps) consumer.Outcome {
	t.Helper()
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var innerOutcome consumer.Outcome
	process := func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		var err error
		innerOutcome, err = consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return 0, err
	}

	if _, err := consumer.FetchAndProcess(fetchCtx, reader, process); err != nil {
		t.Fatalf("FetchAndProcess: %v", err)
	}
	return innerOutcome
}

func newReaderWithGroup(brokers []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    []string{consumer.TopicCodeNode, consumer.TopicCodeRelation},
		CommitInterval: 0, // commit sincrono esplicito, mai in background (necessario per lo scenario 2)
	})
}

// ============================================================
// Payload sintetici — STESSA FORMA prodotta da persist.rs (SPEC-014 §2/§10).
// ============================================================

func codeNodePayload(id, name, nodeType, language, path string) []byte {
	payload := map[string]any{
		"id":       id,
		"domain":   "code",
		"name":     name,
		"ast_hash": language, // valore placeholder qualunque, non verificato dal consumer
		"provenance": map[string]any{
			"path": path, "tenant_id": tenantPlaceholder,
			"repo": repoPlaceholder, "acl_group": aclPlaceholder,
		},
		"ext": map[string]any{
			"node_type": nodeType,
			"language":  language,
		},
	}
	out, _ := json.Marshal(payload)
	return out
}

func codeRelationPayload(fromID, toID, relType string, weight *int) []byte {
	payload := map[string]any{
		"id":       fromID + "->" + toID,
		"domain":   "code",
		"rel_type": relType,
		"from_id":  fromID,
		"to_id":    toID,
		"weight":   weight,
	}
	out, _ := json.Marshal(payload)
	return out
}

func intPtr(v int) *int { return &v }

// ============================================================
// Verifiche dirette su Neo4j/Postgres.
// ============================================================

func readNode(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string) (labels []string, props map[string]any) {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (n:CodeNode {id: $id}) RETURN labels(n) AS labels, properties(n) AS props", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("readNode query id=%s: %v", id, err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("readNode id=%s: nodo non trovato: %v", id, err)
	}
	rawLabels, _, err := neo4j.GetRecordValue[[]any](record, "labels")
	if err != nil {
		t.Fatalf("readNode id=%s: campo labels: %v", id, err)
	}
	for _, l := range rawLabels {
		labels = append(labels, l.(string))
	}
	rawProps, _, err := neo4j.GetRecordValue[map[string]any](record, "props")
	if err != nil {
		t.Fatalf("readNode id=%s: campo props: %v", id, err)
	}
	return labels, rawProps
}

func countNodesWithID(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id string) int64 {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (n:CodeNode {id: $id}) RETURN count(n) AS c", map[string]any{"id": id})
	if err != nil {
		t.Fatalf("countNodesWithID id=%s: %v", id, err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("countNodesWithID id=%s (single): %v", id, err)
	}
	count, _, err := neo4j.GetRecordValue[int64](record, "c")
	if err != nil {
		t.Fatalf("countNodesWithID id=%s: campo c: %v", id, err)
	}
	return count
}

func readRelWeight(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, fromID, toID, relType string) float64 {
	t.Helper()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	query := fmt.Sprintf(
		"MATCH (:CodeNode {id: $from_id})-[r:%s]->(:CodeNode {id: $to_id}) RETURN r.weight AS w", relType,
	)
	result, err := session.Run(ctx, query, map[string]any{"from_id": fromID, "to_id": toID})
	if err != nil {
		t.Fatalf("readRelWeight %s->%s: %v", fromID, toID, err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("readRelWeight %s->%s (single): %v", fromID, toID, err)
	}
	w, _, err := neo4j.GetRecordValue[int64](record, "w")
	if err != nil {
		t.Fatalf("readRelWeight %s->%s: campo w: %v", fromID, toID, err)
	}
	return float64(w)
}

func assertProcessedEvent(t *testing.T, ctx context.Context, db *sql.DB, eventID string) {
	t.Helper()
	var consumerName string
	err := db.QueryRowContext(ctx, "SELECT consumer_name FROM processed_events WHERE event_id = $1", eventID).Scan(&consumerName)
	if err != nil {
		t.Fatalf("processed_events per event_id=%s: %v", eventID, err)
	}
	if consumerName != consumer.ConsumerName {
		t.Errorf("processed_events.consumer_name = %q, want %q", consumerName, consumer.ConsumerName)
	}
}

// ============================================================
// Utility.
// ============================================================

func containsAll(haystack []string, wanted ...string) bool {
	set := make(map[string]bool, len(haystack))
	for _, h := range haystack {
		set[h] = true
	}
	for _, w := range wanted {
		if !set[w] {
			return false
		}
	}
	return true
}

var idCounter int

func uniqueID(t *testing.T, prefix string) string {
	t.Helper()
	idCounter++
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), idCounter)
}

// uniqueEventID genera un event_id in formato UUID reale: outbox.id (da
// cui l'header event_id è promosso, SPEC-015 §2) è una colonna UUID
// (SPEC-005), quindi processed_events.event_id lo è altrettanto — un
// event_id sintetico non-UUID farebbe fallire l'INSERT di dedup con un
// errore di sintassi, non un caso realistico da testare qui.
func uniqueEventID(t *testing.T) string {
	t.Helper()
	return uuid.NewString()
}

// splitCypherStatements è una versione minimale, sufficiente per
// contracts/cypher/schema.cypher: righe di solo commento ("//") scartate,
// le rimanenti unite e spezzate su ";" (SPEC-004: uno statement DDL per
// blocco, mai un ';' dentro il corpo di un CREATE CONSTRAINT/INDEX qui
// presente). tools/migrate-neo4j/internal/migrate.ParseStatements fa la
// stessa cosa in modo più completo, ma è un pacchetto interno di un altro
// modulo Go (nessun replace configurato qui) — non vale la pena
// aggiungere quella dipendenza cross-modulo solo per questo test.
func splitCypherStatements(src string) []string {
	var kept []string
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		kept = append(kept, line)
	}
	joined := strings.Join(kept, "\n")

	var statements []string
	for _, raw := range strings.Split(joined, ";") {
		stmt := strings.TrimSpace(raw)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := readRepoPath(t, parts...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lettura %s: %v", path, err)
	}
	return string(data)
}

func readRepoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// services/sink-graph/internal/consumer/consumer_integration_test.go -> repo root 4 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
