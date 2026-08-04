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
	"strings"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/services/sink-graph/internal/consumer"
)

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
		Repo:  config.EnvOrDefault("SINK_GRAPH_REPO_PLACEHOLDER", "local"),
		Logf:  log.Printf,
	}

	log.Printf("sink-graph: avviato, brokers=%v topics=[%s %s] group=%s",
		brokers, consumer.TopicCodeNode, consumer.TopicCodeRelation, consumer.ConsumerName)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, deps); err != nil {
			log.Printf("sink-graph: %v", err)
		}
	}
}
