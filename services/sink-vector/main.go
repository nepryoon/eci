// Command sink-vector (SPEC-033, T3.1): consumer Kafka che legge da
// outbox.event.CodeEmbedding (SPEC-030→032) e scrive ciascun embedding come
// punto Qdrant con payload {node_id, domain, provenance}. Stesso scheletro
// di services/sink-graph (SPEC-015, T1.3) ed services/embedding-worker
// (SPEC-030).
package main

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/qdrant/go-client/qdrant"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/services/sink-vector/internal/consumer"
)

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("sink-vector")
	if err != nil {
		log.Fatalf("sink-vector: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("sink-vector: shutdown tracing: %v", err)
		}
	}()

	postgresDSN := config.EnvOrDefault("POSTGRES_DSN", "postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable")
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		log.Fatalf("sink-vector: sql.Open postgres: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("sink-vector: ping postgres (%s): %v", postgresDSN, err)
	}

	qdrantHost := config.EnvOrDefault("QDRANT_HOST", "localhost")
	qdrantPort := config.EnvOrDefault("QDRANT_GRPC_PORT", "6334")
	qdrantPortNum, err := strconv.Atoi(qdrantPort)
	if err != nil {
		log.Fatalf("sink-vector: QDRANT_GRPC_PORT non valido (%q): %v", qdrantPort, err)
	}
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{Host: qdrantHost, Port: qdrantPortNum})
	if err != nil {
		log.Fatalf("sink-vector: creazione client Qdrant (host=%s port=%d): %v", qdrantHost, qdrantPortNum, err)
	}
	defer qdrantClient.Close()

	// SPEC-033 §4: Qdrant irraggiungibile qui deve fermare l'avvio con un
	// errore esplicito, non lasciare il servizio partire in uno stato
	// inconsistente (collection assente, upsert falliti a ripetizione).
	if err := consumer.EnsureCollection(ctx, qdrantClient); err != nil {
		log.Fatalf("sink-vector: EnsureCollection(%s): %v", consumer.CollectionName, err)
	}

	brokers := strings.Split(config.EnvOrDefault("KAFKA_BROKERS", "localhost:9094"), ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: []string{consumer.TopicCodeEmbedding},
	})
	defer reader.Close()

	deps := consumer.Deps{
		DB:     db,
		Qdrant: qdrantClient,
		Logf:   log.Printf,
	}

	log.Printf("sink-vector: avviato, brokers=%v topic=%s group=%s qdrant=%s:%d collection=%s",
		brokers, consumer.TopicCodeEmbedding, consumer.ConsumerName, qdrantHost, qdrantPortNum, consumer.CollectionName)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, deps); err != nil {
			log.Printf("sink-vector: %v", err)
		}
	}
}
