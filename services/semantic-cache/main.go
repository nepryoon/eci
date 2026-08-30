// Command semantic-cache (SPEC-022, T2.3): server gRPC del Semantic Cache
// Service, chiave->valore su Redis keyed su ast_hash + logic_fingerprint +
// acl_scope (ADD Modulo 3 §2.6.3). Stessa coppia bootstrap
// tracing/net.Listen di retrieval-engine (SPEC-016). Gli interceptor T6.1/T6.2
// autenticano/autorizzano il chiamante; SPEC-058 deriva nuovamente lo scope
// nel handler e richiede che la chiave coincida prima di accedere a Redis.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

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
const dependencyCheckTimeout = 2 * time.Second

func newMetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

func newDependencyReadinessHandler(check func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), dependencyCheckTimeout)
		defer cancel()
		response.Header().Set("Cache-Control", "no-store")
		if check == nil || check(ctx) != nil {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})
}

func redisOptionsFromEnvironment() (*redis.Options, error) {
	requireAuth, err := strconv.ParseBool(config.EnvOrDefault("REDIS_REQUIRE_AUTH", "false"))
	if err != nil {
		return nil, fmt.Errorf("REDIS_REQUIRE_AUTH must be a boolean: %w", err)
	}
	password := os.Getenv("REDIS_PASSWORD")
	if requireAuth && password == "" {
		return nil, fmt.Errorf("REDIS_PASSWORD is required when Redis authentication is enabled")
	}
	return &redis.Options{
		Addr:     config.EnvOrDefault("REDIS_ADDR", "localhost:6379"),
		Password: password,
	}, nil
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
	redisOptions, err := redisOptionsFromEnvironment()
	if err != nil {
		log.Fatalf("semantic-cache: configurazione Redis: %v", err)
	}
	redisAddr := redisOptions.Addr
	rdb := redis.NewClient(redisOptions)
	defer rdb.Close()
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", newMetricsHandler(prometheus.DefaultGatherer))
		mux.Handle("/ready", newDependencyReadinessHandler(func(ctx context.Context) error {
			return rdb.Ping(ctx).Err()
		}))
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("semantic-cache: server HTTP metriche/readiness (%s) non avviato: %v", metricsAddr, err)
		}
	}()

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
