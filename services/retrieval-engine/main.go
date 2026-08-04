// Command retrieval-engine (SPEC-016, T1.4): server gRPC sulla sola gamba
// grafo (GetNode, ExpandNeighbors, HybridSearch, Health) contro il Neo4j
// popolato da sink-graph (T1.3). Stessa coppia di interceptor
// OTel/SecurityContext già costruita e verificata in interoperabilità
// reale in SPEC-011/SPEC-012.
package main

import (
	"context"
	"log"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/eci-project/eci/libs/go/eci/config"
	"github.com/eci-project/eci/libs/go/eci/observability"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
	"github.com/eci-project/eci/services/retrieval-engine/internal/server"
)

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

	neo4jURI := config.EnvOrDefault("NEO4J_URI", "bolt://localhost:7687")
	neo4jUser := config.EnvOrDefault("NEO4J_USER", "neo4j")
	neo4jPassword := config.EnvOrDefault("NEO4J_PASSWORD", "eci-dev-only")
	driver, err := neo4j.NewDriverWithContext(neo4jURI, neo4j.BasicAuth(neo4jUser, neo4jPassword, ""))
	if err != nil {
		log.Fatalf("retrieval-engine: creazione driver Neo4j (uri=%s): %v", neo4jURI, err)
	}
	defer driver.Close(ctx)

	addr := config.EnvOrDefault("RETRIEVAL_ENGINE_ADDR", ":50053")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("retrieval-engine: net.Listen(%s): %v", addr, err)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(secctx.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &server.Server{Driver: driver})

	log.Printf("retrieval-engine: in ascolto su %s (neo4j=%s)", addr, neo4jURI)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("retrieval-engine: srv.Serve: %v", err)
	}
}
