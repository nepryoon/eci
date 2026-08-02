package secctx

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
)

// stubServer osserva cosa FromContext(ctx) trova durante GetNode, per
// verificare cosa l'interceptor ha effettivamente attaccato al context
// passato all'handler (SPEC-011 §3 scenari 2/3).
type stubServer struct {
	retrievalv1.UnimplementedRetrievalEngineServer
	gotSC *retrievalv1.SecurityContext
	gotOK bool
}

func (s *stubServer) GetNode(ctx context.Context, _ *retrievalv1.GetNodeRequest) (*retrievalv1.GetNodeResponse, error) {
	s.gotSC, s.gotOK = FromContext(ctx)
	return &retrievalv1.GetNodeResponse{}, nil
}

// startTestServer avvia un server gRPC reale in-process con
// UnaryServerInterceptor() installato ASSIEME a otelgrpc.NewServerHandler()
// (SPEC-011 §2: "usare otelgrpc, passato come grpc.StatsHandler" — qui
// verificato che coesista senza conflitti con l'interceptor SecurityContext,
// non solo documentato), e ritorna un client connesso più una func di
// cleanup.
func startTestServer(t *testing.T, stub *stubServer) retrievalv1.RetrievalEngineClient {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryServerInterceptor()),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	retrievalv1.RegisterRetrievalEngineServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return retrievalv1.NewRetrievalEngineClient(conn)
}

// SPEC-011 §3 scenario 2.
func TestUnaryServerInterceptorPropagatesSecurityContext(t *testing.T) {
	stub := &stubServer{}
	client := startTestServer(t, stub)

	want := &retrievalv1.SecurityContext{
		TenantId:     "tenant-42",
		UserId:       "user-7",
		AllowedRepos: []string{"repo-a", "repo-b"},
		AclGroups:    []string{"group-x"},
	}
	encoded, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), metadataKey, string(encoded))
	if _, err := client.GetNode(ctx, &retrievalv1.GetNodeRequest{NodeId: "n1"}); err != nil {
		t.Fatalf("GetNode: %v", err)
	}

	if !stub.gotOK {
		t.Fatalf("FromContext: ok = false, want true (SecurityContext atteso nei metadata)")
	}
	if stub.gotSC.GetTenantId() != want.GetTenantId() {
		t.Errorf("TenantId = %q, want %q", stub.gotSC.GetTenantId(), want.GetTenantId())
	}
	if stub.gotSC.GetUserId() != want.GetUserId() {
		t.Errorf("UserId = %q, want %q", stub.gotSC.GetUserId(), want.GetUserId())
	}
}

// SPEC-011 §3 scenario 3: nessun metadata -> handler comunque invocato,
// ok=false, nessun blocco (plumbing, non enforcement).
func TestUnaryServerInterceptorDoesNotBlockWhenAbsent(t *testing.T) {
	stub := &stubServer{}
	client := startTestServer(t, stub)

	if _, err := client.GetNode(context.Background(), &retrievalv1.GetNodeRequest{NodeId: "n1"}); err != nil {
		t.Fatalf("GetNode: %v (l'handler non deve essere bloccato dall'assenza del metadata)", err)
	}

	if stub.gotOK {
		t.Fatalf("FromContext: ok = true, want false (nessun metadata inviato)")
	}
}

// SPEC-011 §4 edge case: metadata presente ma non deserializzabile come
// protobuf valido -> trattato come assente, handler comunque invocato.
func TestUnaryServerInterceptorCorruptedMetadataTreatedAsAbsent(t *testing.T) {
	stub := &stubServer{}
	client := startTestServer(t, stub)

	// Wire type 7 non esiste (protobuf ne definisce 0-5): garantito non
	// deserializzabile, non lasciato al caso di un testo ASCII che per
	// coincidenza potrebbe decodificare come protobuf valido.
	corrupted := string([]byte{0xff, 0xff, 0xff})
	ctx := metadata.AppendToOutgoingContext(context.Background(), metadataKey, corrupted)
	if _, err := client.GetNode(ctx, &retrievalv1.GetNodeRequest{NodeId: "n1"}); err != nil {
		t.Fatalf("GetNode: %v (metadata corrotto non deve bloccare la richiesta)", err)
	}

	if stub.gotOK {
		t.Fatalf("FromContext: ok = true, want false (metadata corrotto trattato come assente)")
	}
}
