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
	"net/http"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/kafkaconfig"
	"github.com/eci-project/eci/libs/go/eci/kafkaready"
	"github.com/eci-project/eci/libs/go/eci/metrics"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/opensearchconfig"
	"github.com/eci-project/eci/libs/go/eci/resilience"
	"github.com/eci-project/eci/services/sink-search/internal/consumer"
)

// sinkName identifica questo servizio nelle metriche Prometheus (SPEC-036
// §2, label "sink").
const sinkName = "sink-search"

// defaultMetricsPort — vedi commento in services/sink-graph/main.go
// (stessa deviazione dichiarata rispetto al valore letterale ":9090" di
// SPEC-036 §2).
const defaultMetricsPort = "9104"

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
	openSearchTransport, err := opensearchconfig.FromEnvironment(openSearchURL)
	if err != nil {
		log.Fatalf("sink-search: configurazione OpenSearch: %v", err)
	}
	osClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{openSearchURL},
			Username:  openSearchTransport.Username,
			Password:  openSearchTransport.Password,
			CACert:    openSearchTransport.CACert,
		},
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
	kafkaTransport, err := kafkaconfig.FromEnvironment()
	if err != nil {
		log.Fatalf("sink-search: configurazione Kafka: %v", err)
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
		log.Fatalf("sink-search: configurazione readiness Kafka: %v", err)
	}

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
			log.Printf("sink-search: server HTTP metriche (%s) non avviato: %v (consume-loop non impattato)", metricsAddr, err)
		}
	}()

	log.Printf("sink-search: avviato, brokers=%v topic=%s group=%s opensearch=%s index=%s metrics_addr=%s",
		brokers, consumer.TopicCodeChunk, consumer.ConsumerName, openSearchURL, consumer.IndexName, metricsAddr)

	for {
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			log.Printf("sink-search: elaborazione fallita, offset NON committato: %v", err)
		}
	}
}
