// Server gRPC di prova per la verifica di interoperabilità cross-linguaggio
// SPEC-012 §3 scenari 3/4: importa libs/go/eci/secctx (interceptor SERVER
// già esistente, SPEC-011) e go.opentelemetry.io/contrib/.../otelgrpc, logga
// su stdout il SecurityContext e il trace context ricevuti da un client
// Python reale. Non fa parte di nessun servizio applicativo — scaffold di
// verifica manuale una tantum (SPEC-012 §5/§7), non testato in CI.
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/eci-project/eci/libs/go/eci/observability"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/libs/go/eci/secctx"
)

type interopServer struct {
	retrievalv1.UnimplementedRetrievalEngineServer
}

func (s *interopServer) GetNode(ctx context.Context, req *retrievalv1.GetNodeRequest) (*retrievalv1.GetNodeResponse, error) {
	sc, ok := secctx.FromContext(ctx)
	if !ok {
		fmt.Println("[go-server] SecurityContext: ASSENTE (ok=false)")
	} else {
		fmt.Printf("[go-server] SecurityContext ricevuto: tenant_id=%q user_id=%q allowed_repos=%v acl_groups=%v\n",
			sc.GetTenantId(), sc.GetUserId(), sc.GetAllowedRepos(), sc.GetAclGroups())
	}

	sctx := trace.SpanContextFromContext(ctx)
	if !sctx.IsValid() {
		fmt.Println("[go-server] trace context: ASSENTE/non valido")
	} else {
		fmt.Printf("[go-server] trace context ricevuto: trace_id=%s span_id=%s remote=%v\n",
			sctx.TraceID().String(), sctx.SpanID().String(), sctx.IsRemote())
	}

	return &retrievalv1.GetNodeResponse{
		Node: &retrievalv1.RetrievedNode{NodeId: req.GetNodeId()},
	}, nil
}

func main() {
	// InitTracing (libs/go/eci/observability, corretta retroattivamente per
	// SPEC-012 §10) imposta anche il TextMapPropagator globale W3C — prima
	// di questa correzione lo scaffold impostava la stessa riga qui
	// direttamente; ora arriva dal pacchetto condiviso, a riprova che il
	// fix nel pacchetto risolve davvero il problema (non solo "dovrebbe").
	shutdown, err := observability.InitTracing("interop-go-server")
	if err != nil {
		log.Fatalf("observability.InitTracing: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	lis, err := net.Listen("tcp", "127.0.0.1:50052")
	if err != nil {
		log.Fatalf("net.Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(secctx.UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, &interopServer{})

	fmt.Println("[go-server] in ascolto su 127.0.0.1:50052")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("srv.Serve: %v", err)
	}
}
