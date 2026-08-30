// Command retrieval-engine (SPEC-016, T1.4): server gRPC sulla sola gamba
// grafo (GetNode, ExpandNeighbors, HybridSearch, Health) contro il Neo4j
// popolato da sink-graph (T1.3). Stessa coppia di interceptor
// OTel/SecurityContext già costruita e verificata in interoperabilità
// reale in SPEC-011/SPEC-012.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qdrant/go-client/qdrant"

	"github.com/eci-project/eci/libs/go/eci/authz"
	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/opensearchconfig"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/embedclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/rerankclient"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

const defaultMetricsPort = "9105"
const retrievalCollectionName = "code_embeddings"
const retrievalIndexName = "code_chunks"

func newMetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("retrieval-engine")
	if err != nil {
		log.Fatalf("retrieval-engine: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("retrieval-engine: shutdown tracing: %v", err)
		}
	}()
	authzConfig, err := authz.ConfigFromEnvironment("retrieval-engine")
	if err != nil {
		log.Fatalf("retrieval-engine: configurazione OPA: %v", err)
	}
	authorizer, err := authz.New(ctx, authzConfig, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatalf("retrieval-engine: inizializzazione OPA: %v", err)
	}
	metricsAddr := ":" + config.EnvOrDefault("METRICS_PORT", defaultMetricsPort)
	neo4jURI := config.EnvOrDefault("NEO4J_URI", "bolt://localhost:7687")
	neo4jUser := config.EnvOrDefault("NEO4J_USER", "neo4j")
	neo4jPassword := config.EnvOrDefault("NEO4J_PASSWORD", "eci-dev-only")
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		log.Fatalf("retrieval-engine: creazione driver Neo4j (uri=%s): %v", neo4jURI, err)
	}
	defer driver.Close(ctx)

	// Qdrant/Embedder (SPEC-041, T4.1): stessi nomi env var già usati da
	// sink-vector (QDRANT_HOST/QDRANT_GRPC_PORT, SPEC-033) ed
	// embedding-worker (EMBEDDING_SERVICE_URL, SPEC-030) — stessa
	// convenzione di naming, non introdotta ex novo.
	qdrantHost := config.EnvOrDefault("QDRANT_HOST", "localhost")
	qdrantPort := config.EnvOrDefault("QDRANT_GRPC_PORT", "6334")
	qdrantPortNum, err := strconv.Atoi(qdrantPort)
	if err != nil {
		log.Fatalf("retrieval-engine: QDRANT_GRPC_PORT non valido (%q): %v", qdrantPort, err)
	}
	qdrantClient, err := qdrant.NewClient(&qdrant.Config{Host: qdrantHost, Port: qdrantPortNum})
	if err != nil {
		log.Fatalf("retrieval-engine: creazione client Qdrant (host=%s port=%d): %v", qdrantHost, qdrantPortNum, err)
	}
	defer qdrantClient.Close()

	embeddingServiceURL := config.EnvOrDefault("EMBEDDING_SERVICE_URL", "http://localhost:8002")
	embedder := embedclient.New(embeddingServiceURL)

	// Reranker (SPEC-044, T4.4): stessa convenzione di naming URL-based
	// delle altre dipendenze HTTP di questo servizio (EMBEDDING_SERVICE_URL
	// sopra). Nessuna porta di default nota nel progetto per bge-reranker-
	// v2-m3/TEI-rerank — riusa la stessa porta base di EMBEDDING_SERVICE_URL
	// (8002) come default dichiarato: reranker-fake e embedder-fake sono
	// processi TEI-compatibili distinti, tipicamente su porte diverse in
	// dev (vedi reranker-fake, porta assegnata dinamicamente nei test).
	rerankerServiceURL := config.EnvOrDefault("RERANKER_SERVICE_URL", "http://localhost:8003")
	reranker := rerankclient.New(rerankerServiceURL)

	// OpenSearch (SPEC-045): stesso client/versione /v4 già stabilito in
	// sink-search (SPEC-034) — prima connessione OpenSearch di
	// retrieval-engine, stessa convenzione OPENSEARCH_URL già usata da
	// tools/reconcile.
	openSearchURL := config.EnvOrDefault("OPENSEARCH_URL", "http://localhost:9200")
	openSearchTransport, err := opensearchconfig.FromEnvironment(openSearchURL)
	if err != nil {
		log.Fatalf("retrieval-engine: configurazione OpenSearch: %v", err)
	}
	openSearchClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{openSearchURL},
			Username:  openSearchTransport.Username,
			Password:  openSearchTransport.Password,
			CACert:    openSearchTransport.CACert,
		},
	})
	if err != nil {
		log.Fatalf("retrieval-engine: creazione client OpenSearch (url=%s): %v", openSearchURL, err)
	}

	addr := config.EnvOrDefault("RETRIEVAL_ENGINE_ADDR", ":50053")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("retrieval-engine: net.Listen(%s): %v", addr, err)
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", newMetricsHandler(prometheus.DefaultGatherer))
		mux.Handle("/ready", newDependencyReadinessHandler(
			driver.VerifyConnectivity,
			func(ctx context.Context) error {
				exists, err := qdrantClient.CollectionExists(ctx, retrievalCollectionName)
				if err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("Qdrant collection %q is missing", retrievalCollectionName)
				}
				return nil
			},
			func(ctx context.Context) error {
				response, err := openSearchClient.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
					Indices: []string{retrievalIndexName},
				})
				if response != nil {
					defer response.Body.Close()
					_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				}
				if err != nil {
					return err
				}
				if response == nil || response.StatusCode != http.StatusOK {
					return fmt.Errorf("OpenSearch index %q is unavailable", retrievalIndexName)
				}
				return nil
			},
			embedder.Health,
			reranker.Health,
		))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("retrieval-engine: server HTTP metriche/readiness (%s) non avviato: %v", metricsAddr, err)
		}
	}()

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			secctx.UnaryServerInterceptor(),
			authz.UnaryServerInterceptor(authorizer),
		),
		grpc.ChainStreamInterceptor(
			secctx.StreamServerInterceptor(),
			authz.StreamServerInterceptor(authorizer),
		),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &server.Server{
		Driver:     driver,
		Qdrant:     qdrantClient,
		Embedder:   embedder,
		Reranker:   reranker,
		OpenSearch: openSearchClient,
	})

	log.Printf("retrieval-engine: in ascolto su %s (neo4j=%s, qdrant=%s:%d, embedder=%s, reranker=%s, opensearch=%s, metrics=%s)", addr, neo4jURI, qdrantHost, qdrantPortNum, embeddingServiceURL, rerankerServiceURL, openSearchURL, metricsAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("retrieval-engine: srv.Serve: %v", err)
	}
}
