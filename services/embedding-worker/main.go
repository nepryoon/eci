// Command embedding-worker (SPEC-030, pre-T3.1 2/3): consumer Kafka che
// legge da outbox.event.CodeChunk (SPEC-029), chiama il client di embedding
// per ciascun chunk e scrive il vettore risultante in code_embedding più
// una riga outbox (aggregate_type='CodeEmbedding') che T3.1 (sink-vector)
// consumerà. Stesso scheletro di services/sink-graph (SPEC-015, T1.3).
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/kafkaconfig"
	"github.com/eci-project/eci/libs/go/eci/kafkaready"
	"github.com/eci-project/eci/libs/go/eci/metrics"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/embedding-worker/internal/consumer"
	"github.com/eci-project/eci/services/embedding-worker/internal/embedclient"
)

// sinkName identifica questo servizio nelle metriche Prometheus (SPEC-036
// §2, label "sink").
const sinkName = "embedding-worker"

// defaultMetricsPort — vedi commento in services/sink-graph/main.go
// (stessa deviazione dichiarata rispetto al valore letterale ":9090" di
// SPEC-036 §2: i quattro sink condividono il namespace di rete dell'host,
// serve una porta di default distinta per ciascuno).
const defaultMetricsPort = "9102"

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("embedding-worker")
	if err != nil {
		log.Fatalf("embedding-worker: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("embedding-worker: shutdown tracing: %v", err)
		}
	}()

	postgresDSN := config.EnvOrDefault("POSTGRES_DSN", "postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable")
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatalf("embedding-worker: sql.Open postgres: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("embedding-worker: ping postgres (%s): %v", postgresDSN, err)
	}

	// Stesso URL configurabile già stabilito dal client Rust di T2.4
	// (SPEC-023 §2): embedder-fake in sviluppo/test, vero TEI quando
	// disponibile — nessuna differenza di codice, solo di base URL.
	embeddingServiceURL := config.EnvOrDefault("EMBEDDING_SERVICE_URL", "http://localhost:8002")
	modelID := config.EnvOrDefault("EMBEDDING_MODEL_ID", "jina-code-embeddings-1.5b")

	brokers := strings.Split(config.EnvOrDefault("KAFKA_BROKERS", "localhost:9094"), ",")
	kafkaTransport, err := kafkaconfig.FromEnvironment()
	if err != nil {
		log.Fatalf("embedding-worker: configurazione Kafka: %v", err)
	}
	retryTopicSuffix := ".retry." + consumer.ConsumerName
	topics := []string{consumer.TopicCodeChunk, resilience.RetryTopic(consumer.TopicCodeChunk, retryTopicSuffix)}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: topics,
		Dialer:      kafkaTransport.Dialer,
	})
	defer reader.Close()
	readiness, err := kafkaready.New(brokers, kafkaTransport.Transport, consumer.ConsumerName, topics)
	if err != nil {
		log.Fatalf("embedding-worker: configurazione readiness Kafka: %v", err)
	}

	deps := consumer.Deps{
		DB:      db,
		Embed:   embedclient.New(embeddingServiceURL),
		ModelID: modelID,
		Logf:    log.Printf,
	}

	// SPEC-035 §2 (T3.3 parte 1/2): retry con backoff esponenziale + DLQ,
	// stesso principio di sink-graph — vedi commento lì per il dettaglio
	// su Topic/BatchTimeout del producer.
	retryProducer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Transport:              kafkaTransport.Transport,
		AllowAutoTopicCreation: false,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer retryProducer.Close()
	process := resilience.WithRetryAndDLQ(resilience.Config{RetryTopicSuffix: retryTopicSuffix}, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})
	process = metrics.WithPrometheus(sinkName, process)

	metricsAddr := ":" + config.EnvOrDefault("METRICS_PORT", defaultMetricsPort)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.Handle("/ready", kafkaready.Handler(readiness.Check))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("embedding-worker: server HTTP metriche (%s) non avviato: %v (consume-loop non impattato)", metricsAddr, err)
		}
	}()

	log.Printf("embedding-worker: avviato, brokers=%v topic=%s group=%s embedding_service_url=%s model_id=%s metrics_addr=%s",
		brokers, consumer.TopicCodeChunk, consumer.ConsumerName, embeddingServiceURL, modelID, metricsAddr)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			log.Printf("embedding-worker: elaborazione fallita, offset NON committato: %v", err)
		}
	}
}
