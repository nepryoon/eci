// Command embedding-worker (SPEC-030, pre-T3.1 2/3): consumer Kafka che
// legge da outbox.event.CodeChunk (SPEC-029), chiama il client di embedding
// per ciascun chunk e scrive il vettore risultante in code_embedding più
// una riga outbox (aggregate_type='CodeEmbedding') che T3.1 (sink-vector)
// consumerà. Stesso scheletro di services/sink-graph (SPEC-015, T1.3).
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

const dependencyCheckTimeout = 2 * time.Second
const dependencyRetryInterval = time.Second

var errReadinessCheckMissing = errors.New("embedding-worker: readiness check missing")

func combinedReadiness(
	kafkaCheck func(context.Context) error,
	embedderCheck func(context.Context) error,
) func(context.Context) error {
	return func(ctx context.Context) error {
		if kafkaCheck == nil || embedderCheck == nil {
			return errReadinessCheckMissing
		}
		if err := kafkaCheck(ctx); err != nil {
			return err
		}
		return embedderCheck(ctx)
	}
}

func waitUntilDependencyReady(
	ctx context.Context,
	retryInterval time.Duration,
	check func(context.Context) error,
) error {
	if check == nil || retryInterval <= 0 {
		return errReadinessCheckMissing
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := check(ctx); err == nil {
			return nil
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type closeableReader interface {
	consumer.MessageReader
	Close() error
}

func consumeLoop(
	ctx context.Context,
	retryInterval time.Duration,
	dependencyCheck func(context.Context) error,
	newReader func() closeableReader,
	process resilience.ProcessFunc,
	logf func(string, ...any),
) error {
	if newReader == nil || process == nil || logf == nil {
		return errReadinessCheckMissing
	}
	reader := newReader()
	readerClosed := false
	dependencyReady := false
	defer func() {
		if !readerClosed {
			_ = reader.Close()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !dependencyReady {
			if err := waitUntilDependencyReady(ctx, retryInterval, dependencyCheck); err != nil {
				return err
			}
			dependencyReady = true
		}
		if _, err := consumer.FetchAndProcess(ctx, reader, process); err != nil {
			logf("embedding-worker: elaborazione fallita, offset NON committato; reader ricreato prima del prossimo fetch: %v", err)
			if closeErr := reader.Close(); closeErr != nil {
				return fmt.Errorf("embedding-worker: chiusura reader dopo errore non committato: %w", closeErr)
			}
			readerClosed = true
			dependencyReady = false
			if err := ctx.Err(); err != nil {
				return err
			}
			reader = newReader()
			readerClosed = false
		}
	}
}

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
	readerConfig := kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumer.ConsumerName,
		GroupTopics: topics,
		Dialer:      kafkaTransport.Dialer,
	}
	readiness, err := kafkaready.New(brokers, kafkaTransport.Transport, consumer.ConsumerName, topics)
	if err != nil {
		log.Fatalf("embedding-worker: configurazione readiness Kafka: %v", err)
	}

	embedder := embedclient.New(embeddingServiceURL)
	startupCtx, cancelStartup := context.WithTimeout(ctx, dependencyCheckTimeout)
	if err := embedder.Health(startupCtx); err != nil {
		cancelStartup()
		log.Fatalf("embedding-worker: embedder non pronto prima del consume-loop: %v", err)
	}
	cancelStartup()

	deps := consumer.Deps{
		DB:      db,
		Embed:   embedder,
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
	process := resilience.WithRetryAndDLQ(resilience.Config{
		RetryTopicSuffix: retryTopicSuffix,
		ShouldRetry: func(err error) bool {
			return !embedclient.IsUnavailable(err)
		},
	}, retryProducer, func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		_, err := consumer.ProcessMessage(ctx, deps, topic, value, headers)
		return resilience.OutcomeProcessed, err
	})
	process = metrics.WithPrometheus(sinkName, process)

	metricsAddr := ":" + config.EnvOrDefault("METRICS_PORT", defaultMetricsPort)
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.Handle("/ready", kafkaready.Handler(combinedReadiness(readiness.Check, embedder.Health)))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("embedding-worker: server HTTP metriche (%s) non avviato: %v (consume-loop non impattato)", metricsAddr, err)
		}
	}()

	log.Printf("embedding-worker: avviato, brokers=%v topic=%s group=%s embedding_service_url=%s model_id=%s metrics_addr=%s",
		brokers, consumer.TopicCodeChunk, consumer.ConsumerName, embeddingServiceURL, modelID, metricsAddr)

	if err := consumeLoop(ctx, dependencyRetryInterval, func(parent context.Context) error {
		checkCtx, cancel := context.WithTimeout(parent, dependencyCheckTimeout)
		defer cancel()
		return embedder.Health(checkCtx)
	}, func() closeableReader {
		return kafka.NewReader(readerConfig)
	}, process, log.Printf); err != nil {
		log.Fatalf("embedding-worker: consume-loop terminato: %v", err)
	}
}
