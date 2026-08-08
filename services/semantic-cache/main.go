// Command semantic-cache (SPEC-022, T2.3): server gRPC del Semantic Cache
// Service, chiave->valore su Redis keyed su ast_hash + logic_fingerprint +
// acl_scope (ADD Modulo 3 §2.6.3). Stessa coppia bootstrap
// tracing/net.Listen di retrieval-engine (SPEC-016); nessun interceptor
// SecurityContext qui — il contratto di questa SPEC non lo prevede
// (acl_scope è un campo esplicito della chiave, non derivato dai metadata
// gRPC, vedi SPEC-022 §2 "plumbing, non enforcement").
package main

import (
	"context"
	"log"
	"net"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	semanticcachev1 "github.com/eci-project/eci/libs/go/eci/semanticcache/v1"
	"github.com/eci-project/eci/services/semantic-cache/internal/server"
)

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

	redisAddr := config.EnvOrDefault("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	addr := config.EnvOrDefault("SEMANTIC_CACHE_ADDR", ":50054")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("semantic-cache: net.Listen(%s): %v", addr, err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	semanticcachev1.RegisterSemanticCacheServer(srv, &server.Server{Redis: rdb})

	log.Printf("semantic-cache: in ascolto su %s (redis=%s)", addr, redisAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("semantic-cache: srv.Serve: %v", err)
	}
}
