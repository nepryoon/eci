// Command sink-graph (SPEC-015, T1.3): consumer Kafka che legge da
// outbox.event.CodeNode/CodeRelation e fa MERGE idempotente su Neo4j,
// deduplicando via processed_events. Chiude per la prima volta il
// percorso end-to-end con dati applicativi reali: parsing (T1.1) ->
// Postgres+outbox (T1.2) -> Kafka (SPEC-008) -> Neo4j (qui).
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/metrics"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-graph/internal/consumer"
)

// sinkName identifica questo servizio nelle metriche Prometheus
// (SPEC-036 §2, label "sink") — stessa stringa di consumer.ConsumerName,
// duplicata qui volutamente: consumer.ConsumerName è un dettaglio del
// dedup Kafka (SPEC-015), sinkName un dettaglio dell'osservabilità
// (SPEC-036), coincidono per leggibilità ma non sono la stessa cosa per
// costruzione (potrebbero divergere in futuro senza impatto sull'altro).
const sinkName = "sink-graph"

// defaultMetricsPort — DEVIAZIONE dal valore letterale ":9090" dichiarato
// da SPEC-036 §2: verificato (non presunto) che i quattro sink NON sono
// servizi Docker Compose isolati in propri namespace di rete — girano
// come processi HOST che condividono lo STESSO namespace di rete
// (l'host), quindi un default IDENTICO per tutti e quattro causerebbe una
// collisione di bind reale se eseguiti insieme. Ogni sink riceve una
// porta di default DISTINTA (9101 qui), sempre sotto lo stesso nome di
// env var METRICS_PORT (override possibile invariato, §2). Vedi
// deploy/compose/prometheus.yml per il dettaglio completo della
// motivazione e delle quattro porte assegnate.
const defaultMetricsPort = "9101"

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("sink-graph")
	if err != nil {
		log.Fatalf("sink-graph: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("sink-graph: shutdown tracing: %v", err)
		}
	}()

	postgresDSN := config.EnvOrDefault("POSTGRES_DSN", "postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable")
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatalf("sink-graph: sql.Open postgres: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("sink-graph: ping postgres (%s): %v", postgresDSN, err)
	}

	neo4jURI := config.EnvOrDefault("NEO4J_URI", "bolt://localhost:7687")
	neo4jUser := config.EnvOrDefault("NEO4J_USER", "neo4j")
	neo4jPassword := config.EnvOrDefault("NEO4J_PASSWORD", "eci-dev-only")
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		log.Fatalf("sink-graph: creazione driver Neo4j (uri=%s): %v", neo4jURI, err)
	}
	defer driver.Close(ctx)
	if err := driver.VerifyAuthentication(ctx, nil); err != nil {
		log.Fatalf("sink-graph: autenticazione Neo4j fallita (uri=%s): %v", neo4jURI, err)
	}

	brokers := strings.Split(config.EnvOrDefault("KAFKA_BROKERS", "localhost:9094"), ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: []string{consumer.TopicCodeNode, consumer.TopicCodeRelation},
	})
	defer reader.Close()

	deps := consumer.Deps{
		DB:    db,
		Neo4j: driver,
		Logf:  log.Printf,
	}

	// SPEC-035 §2 (T3.3 parte 1/2): retry con backoff esponenziale + DLQ,
	// avvolge ProcessMessage senza modificarne la logica applicativa.
	// Topic del producer VOLUTAMENTE non impostato — ogni messaggio
	// ripubblicato/instradato in DLQ specifica il proprio Topic (§2:
	// stesso topic originale per i retry, "{topic}.DLQ" per la coda
	// morta). BatchTimeout basso: il default di kafka-go (1s) altrimenti
	// si somma silenziosamente al backoff configurato, scoperto scrivendo
	// il test di integrazione di libs/go/eci/resilience (SPEC-035 §7).
	retryProducer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer retryProducer.Close()
	process := resilience.WithRetryAndDLQ(resilience.Config{}, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})
	// SPEC-036 §2 (T3.3 parte 2/2): metriche come strato PIÙ ESTERNO della
	// composizione — vede l'outcome FINALE, dopo che retry/DLQ ha già agito.
	process = metrics.WithPrometheus(sinkName, process)

	// Server HTTP delle metriche in una goroutine separata (§2): un
	// fallimento all'avvio (es. porta già occupata, §4 edge case) è
	// osservabilità accessoria, non deve impedire l'avvio del consume-loop
	// principale — loggato esplicitamente, mai un os.Exit/panic qui.
	metricsAddr := ":" + config.EnvOrDefault("METRICS_PORT", defaultMetricsPort)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("sink-graph: server HTTP metriche (%s) non avviato: %v (consume-loop non impattato)", metricsAddr, err)
		}
	}()

	log.Printf("sink-graph: avviato, brokers=%v topics=[%s %s] group=%s metrics_addr=%s",
		brokers, consumer.TopicCodeNode, consumer.TopicCodeRelation, consumer.ConsumerName, metricsAddr)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			log.Printf("sink-graph: elaborazione fallita, offset NON committato: %v", err)
		}
	}
}
