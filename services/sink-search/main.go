// Command sink-search (SPEC-034, T3.2): consumer Kafka che legge da
// outbox.event.CodeChunk (SPEC-029→032) e indicizza ciascun chunk come
// documento OpenSearch per la ricerca full-text — SECONDO consumatore
// dello stesso topic già consumato da embedding-worker (SPEC-030), con un
// consumer group id proprio e distinto. Stesso scheletro di
// services/sink-graph (SPEC-015, T1.3) e services/sink-vector (SPEC-033).
package main

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-search/internal/consumer"
)

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("sink-search")
	if err != nil {
		log.Fatalf("sink-search: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("sink-search: shutdown tracing: %v", err)
		}
	}()

	postgresDSN := config.EnvOrDefault("POSTGRES_DSN", "postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable")
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatalf("sink-search: sql.Open postgres: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("sink-search: ping postgres (%s): %v", postgresDSN, err)
	}

	openSearchURL := config.EnvOrDefault("OPENSEARCH_URL", "http://localhost:9200")
	osClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{openSearchURL}},
	})
	if err != nil {
		log.Fatalf("sink-search: creazione client OpenSearch (url=%s): %v", openSearchURL, err)
	}

	// SPEC-034 §4: OpenSearch irraggiungibile qui deve fermare l'avvio con
	// un errore esplicito, non lasciare il servizio partire in uno stato
	// inconsistente (indice assente, indicizzazioni fallite a ripetizione).
	if err := consumer.EnsureIndex(ctx, osClient); err != nil {
		log.Fatalf("sink-search: EnsureIndex(%s): %v", consumer.IndexName, err)
	}

	brokers := strings.Split(config.EnvOrDefault("KAFKA_BROKERS", "localhost:9094"), ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: []string{consumer.TopicCodeChunk},
	})
	defer reader.Close()

	deps := consumer.Deps{
		DB:         db,
		OpenSearch: osClient,
		Logf:       log.Printf,
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

	log.Printf("sink-search: avviato, brokers=%v topic=%s group=%s opensearch=%s index=%s",
		brokers, consumer.TopicCodeChunk, consumer.ConsumerName, openSearchURL, consumer.IndexName)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			log.Printf("sink-search: elaborazione fallita, offset NON committato: %v", err)
		}
	}
}
