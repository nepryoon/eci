// Command semantic-cache (SPEC-022, T2.3): server gRPC del Semantic Cache
// Service, chiave->valore su Redis keyed su ast_hash + logic_fingerprint +
// acl_scope (ADD Modulo 3 §2.6.3). Stessa coppia bootstrap
// tracing/net.Listen di retrieval-engine (SPEC-016). Gli interceptor T6.1/T6.2
// autenticano/autorizzano il chiamante; SPEC-058 deriva nuovamente lo scope
// nel handler e richiede che la chiave coincida prima di accedere a Redis.
package main

import (
	"context"
	"log"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/eci-project/eci/libs/go/eci/authz"
	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	semanticcachev1 "github.com/eci-project/eci/libs/go/eci/semanticcache/v1"
	"github.com/eci-project/eci/services/semantic-cache/internal/server"
)

const defaultMetricsPort = "9106"

func newMetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

func main() {
	ctx := context.Background()

	shutdown, err := observability.InitTracing("semantic-cache")
	if err != nil {
		log.Fatalf("semantic-cache: init tracing: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("semantic-cache: shutdown tracing: %v", err)
		}
	}()
	authzConfig, err := authz.ConfigFromEnvironment("semantic-cache")
	if err != nil {
		log.Fatalf("semantic-cache: configurazione OPA: %v", err)
	}
	authorizer, err := authz.New(ctx, authzConfig, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatalf("semantic-cache: inizializzazione OPA: %v", err)
	}
	metricsAddr := ":" + config.EnvOrDefault("METRICS_PORT", defaultMetricsPort)
	go func() {
		if err := http.ListenAndServe(metricsAddr, newMetricsHandler(prometheus.DefaultGatherer)); err != nil {
			log.Printf("semantic-cache: server HTTP metriche (%s) non avviato: %v", metricsAddr, err)
		}
	}()

	redisAddr := config.EnvOrDefault("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	addr := config.EnvOrDefault("SEMANTIC_CACHE_ADDR", ":50054")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("semantic-cache: net.Listen(%s): %v", addr, err)
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			secctx.UnaryServerInterceptor(),
			authz.UnaryServerInterceptor(authorizer),
		),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	semanticcachev1.RegisterSemanticCacheServer(srv, &server.Server{Redis: rdb})

	log.Printf("semantic-cache: in ascolto su %s (redis=%s, metrics=%s)", addr, redisAddr, metricsAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("semantic-cache: srv.Serve: %v", err)
	}
}
