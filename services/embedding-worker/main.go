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
	"strings"
	"time"

	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/embedding-worker/internal/consumer"
	"github.com/eci-project/eci/services/embedding-worker/internal/embedclient"
)

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
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: []string{consumer.TopicCodeChunk},
	})
	defer reader.Close()

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
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer retryProducer.Close()
	process := resilience.WithRetryAndDLQ(resilience.Config{}, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})

	log.Printf("embedding-worker: avviato, brokers=%v topic=%s group=%s embedding_service_url=%s model_id=%s",
		brokers, consumer.TopicCodeChunk, consumer.ConsumerName, embeddingServiceURL, modelID)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			log.Printf("embedding-worker: elaborazione fallita, offset NON committato: %v", err)
		}
	}
}
