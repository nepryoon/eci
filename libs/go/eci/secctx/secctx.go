// Package secctx propaga il SecurityContext (tipo retrievalv1.SecurityContext,
// già generato da SPEC-002 — riusato, non ridefinito) via metadata gRPC.
// Fase 1 è "no security" per design (vedi ADD): questo package è solo
// plumbing (propagazione), non enforcement — nessun controllo su
// acl_groups/allowed_repos contro una risorsa.
package secctx

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
)

// metadataKey è la chiave dei metadata gRPC in ingresso/uscita. Il
// suffisso "-bin" è la convenzione gRPC per dati binari non-UTF8: il
// runtime gRPC applica automaticamente base64 encode/decode sul wire, il
// codice applicativo lavora sempre sui byte grezzi del protobuf serializzato.
const metadataKey = "eci-security-context-bin"

type contextKey struct{}

func extractIncoming(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	values := md.Get(metadataKey)
	if len(values) == 0 {
		return ctx
	}
	sc := &retrievalv1.SecurityContext{}
	if err := proto.Unmarshal([]byte(values[0]), sc); err != nil {
		fmt.Fprintf(os.Stderr,
			"eci/secctx: metadata %q presente ma non deserializzabile come SecurityContext, trattato come assente: %v\n",
			metadataKey, err)
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, sc)
}

// UnaryServerInterceptor estrae SecurityContext dai metadata gRPC in
// ingresso (chiave metadataKey, deserializzata come protobuf) e lo
// attacca al context.Context passato all'handler. Se il metadata è
// assente o malformato, l'handler riceve comunque il context (senza
// SecurityContext attaccato) — l'interceptor non blocca mai la richiesta
// (SPEC-011 §2/§4/§5: plumbing, non enforcement).
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		return handler(extractIncoming(ctx), req)
	}
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextServerStream) Context() context.Context { return s.ctx }

// StreamServerInterceptor is the streaming equivalent of
// UnaryServerInterceptor. It remains plumbing-only: missing or malformed
// metadata is represented as an absent SecurityContext and authorization is
// left to the separate Phase 6 PEP.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		return handler(srv, &contextServerStream{
			ServerStream: stream,
			ctx:          extractIncoming(stream.Context()),
		})
	}
}

// FromContext recupera il SecurityContext attaccato da
// UnaryServerInterceptor. ok=false se assente (nessun panic, nessun
// valore fittizio silenzioso).
func FromContext(ctx context.Context) (sc *retrievalv1.SecurityContext, ok bool) {
	sc, ok = ctx.Value(contextKey{}).(*retrievalv1.SecurityContext)
	return sc, ok
}
