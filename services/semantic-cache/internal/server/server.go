// Package server implementa SemanticCacheServer (SPEC-022/T2.3,
// SPEC-058/T6.4): Get/Put su Redis con chiave deterministica
// entity_id+ast_hash+logic_fingerprint+acl_scope autenticato.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/eci-project/eci/libs/go/eci/accessscope"
	semanticcachev1 "github.com/eci-project/eci/libs/go/eci/semanticcache/v1"
)

// Server implementa semanticcachev1.SemanticCacheServer.
// UnimplementedSemanticCacheServer embedded by value (non puntatore, per
// compatibilità forward-only del codice generato, stesso pattern di
// retrieval-engine/internal/server.Server).
type Server struct {
	semanticcachev1.UnimplementedSemanticCacheServer
	Redis *redis.Client
}

// Get — SPEC-022 §2/§3 scenari 1-4: lookup per chiave esatta. Un valore
// mai scritto (o scritto con una qualunque delle quattro componenti della
// chiave diversa) è un cache miss genuino (hit:false, nessun errore).
// Redis irraggiungibile è invece un errore gRPC Unavailable esplicito
// (§4): mai confuso con un miss silenzioso, che nasconderebbe un guasto
// infrastrutturale.
func (s *Server) Get(ctx context.Context, key *semanticcachev1.CacheKey) (*semanticcachev1.GetResponse, error) {
	ctx, observe := observeScope(ctx, "get")
	outcome := "error"
	defer func() { observe(outcome) }()
	if err := authorizeKey(ctx, key); err != nil {
		if status.Code(err) == codes.PermissionDenied {
			outcome = "deny"
		}
		return nil, err
	}
	raw, err := s.Redis.Get(ctx, redisKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		outcome = "allow"
		return &semanticcachev1.GetResponse{Hit: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis GET: %v", err)
	}

	value := &semanticcachev1.CacheValue{}
	if err := proto.Unmarshal(raw, value); err != nil {
		return nil, status.Errorf(codes.Internal, "deserializzazione CacheValue: %v", err)
	}
	outcome = "allow"
	return &semanticcachev1.GetResponse{Hit: true, Value: value}, nil
}

// Put — SPEC-022 §2 scenari 5/6: scrittura idempotente, TTL applicato
// nativamente da Redis (SET ... EX ttl_seconds, o SET senza EX se
// ttl_seconds == 0 — go-redis traduce un'expiration di durata zero in
// "nessuna scadenza", stesso comportamento). Il CacheValue è serializzato
// per intero (proto.Marshal): Get restituisce esattamente lo stesso valore
// scritto, non uno ricostruito a partire da un TTL residuo ricalcolato.
func (s *Server) Put(ctx context.Context, req *semanticcachev1.PutRequest) (*semanticcachev1.PutResponse, error) {
	ctx, observe := observeScope(ctx, "put")
	outcome := "error"
	defer func() { observe(outcome) }()
	if req == nil || req.GetKey() == nil || req.GetValue() == nil {
		return nil, status.Error(codes.InvalidArgument, "key e value sono obbligatori")
	}
	if err := authorizeKey(ctx, req.GetKey()); err != nil {
		if status.Code(err) == codes.PermissionDenied {
			outcome = "deny"
		}
		return nil, err
	}
	raw, err := proto.Marshal(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "serializzazione CacheValue: %v", err)
	}

	ttl := time.Duration(req.GetValue().GetTtlSeconds()) * time.Second
	if err := s.Redis.Set(ctx, redisKey(req.GetKey()), raw, ttl).Err(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis SET: %v", err)
	}
	outcome = "allow"
	return &semanticcachev1.PutResponse{}, nil
}

func authorizeKey(ctx context.Context, key *semanticcachev1.CacheKey) error {
	if key == nil {
		return status.Error(codes.InvalidArgument, "cache key obbligatoria")
	}
	scope, err := accessscope.FromContext(ctx)
	if err != nil {
		return status.Error(codes.PermissionDenied, "security scope non valido")
	}
	expected := accessscope.Fingerprint(scope)
	got := key.GetAclScope()
	if expected == "" || subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
		return status.Error(codes.PermissionDenied, "acl_scope non autorizzato")
	}
	return nil
}

// redisKey uses a v2 opaque digest over length-prefixed logical fields. This
// preserves the four-part contract while making delimiter collisions and raw
// scope disclosure impossible. Legacy scache:* entries are intentionally not
// reused after Phase 6 enforcement.
func redisKey(key *semanticcachev1.CacheKey) string {
	var encoded bytes.Buffer
	encoded.WriteString("eci-semantic-cache-key-v2")
	for _, value := range []string{
		key.GetEntityId(), key.GetAstHash(), key.GetLogicFingerprint(), key.GetAclScope(),
	} {
		_ = binary.Write(&encoded, binary.BigEndian, uint32(len([]byte(value))))
		encoded.WriteString(value)
	}
	digest := sha256.Sum256(encoded.Bytes())
	return fmt.Sprintf("scache:v2:%x", digest)
}
