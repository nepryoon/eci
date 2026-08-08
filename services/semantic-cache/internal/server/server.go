// Package server implementa SemanticCacheServer (SPEC-022, T2.3): Get/Put
// su Redis con chiave deterministica entity_id+ast_hash+logic_fingerprint+
// acl_scope (SPEC-022 §2).
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
	raw, err := s.Redis.Get(ctx, redisKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return &semanticcachev1.GetResponse{Hit: false}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis GET: %v", err)
	}

	value := &semanticcachev1.CacheValue{}
	if err := proto.Unmarshal(raw, value); err != nil {
		return nil, status.Errorf(codes.Internal, "deserializzazione CacheValue: %v", err)
	}
	return &semanticcachev1.GetResponse{Hit: true, Value: value}, nil
}

// Put — SPEC-022 §2 scenari 5/6: scrittura idempotente, TTL applicato
// nativamente da Redis (SET ... EX ttl_seconds, o SET senza EX se
// ttl_seconds == 0 — go-redis traduce un'expiration di durata zero in
// "nessuna scadenza", stesso comportamento). Il CacheValue è serializzato
// per intero (proto.Marshal): Get restituisce esattamente lo stesso valore
// scritto, non uno ricostruito a partire da un TTL residuo ricalcolato.
func (s *Server) Put(ctx context.Context, req *semanticcachev1.PutRequest) (*semanticcachev1.PutResponse, error) {
	raw, err := proto.Marshal(req.GetValue())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "serializzazione CacheValue: %v", err)
	}

	ttl := time.Duration(req.GetValue().GetTtlSeconds()) * time.Second
	if err := s.Redis.Set(ctx, redisKey(req.GetKey()), raw, ttl).Err(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "redis SET: %v", err)
	}
	return &semanticcachev1.PutResponse{}, nil
}

// redisKey — SPEC-022 §2: concatenazione deterministica dei quattro campi
// della chiave. Un campo vuoto (es. logic_fingerprint: "") non è un caso
// speciale: produce comunque una chiave valida e deterministica (§4).
func redisKey(key *semanticcachev1.CacheKey) string {
	return fmt.Sprintf("scache:%s:%s:%s:%s",
		key.GetEntityId(), key.GetAstHash(), key.GetLogicFingerprint(), key.GetAclScope())
}
