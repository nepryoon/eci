//go:build integration

// SPEC-022 §7 — test di integrazione: testcontainers Redis reale (non un
// mock), stesso principio di readiness reale già stabilito per
// Postgres/Neo4j/Kafka nei servizi precedenti (SPEC-016 per il pattern
// gRPC-su-TCP-loopback). Il server gRPC è istanziato per davvero (stesso
// main.go di produzione, meno l'observability bootstrap), un client gRPC
// reale gli parla via TCP loopback.
package server_test

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	semanticcachev1 "github.com/eci-project/eci/libs/go/eci/semanticcache/v1"
	"github.com/eci-project/eci/services/semantic-cache/internal/server"
)

func TestSemanticCacheServer(t *testing.T) {
	ctx := context.Background()
	rdb := startRedis(t, ctx)
	client := startServer(t, rdb)

	baseKey := &semanticcachev1.CacheKey{
		EntityId:         "test-entity-1",
		AstHash:          "test-ast-hash-1",
		LogicFingerprint: "fp-v1",
		AclScope:         "default",
	}
	baseValue := &semanticcachev1.CacheValue{
		EmbeddingRef: "qdrant:points/abc123",
		Summary:      "Valida che il carrello abbia abbastanza articoli.",
		ModelId:      "jina-code-embeddings-1.5b",
		EmbeddingDim: 1536,
		TtlSeconds:   3600,
	}

	// --- SPEC-022 §3 scenario 1: Get su chiave mai scritta -> hit:false,
	// nessun CacheValue. ---
	t.Run("Scenario1_GetMissOnUnwrittenKey", func(t *testing.T) {
		resp, err := client.Get(ctx, &semanticcachev1.CacheKey{
			EntityId:         "never-written",
			AstHash:          "never-written",
			LogicFingerprint: "never-written",
			AclScope:         "default",
		})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if resp.GetHit() {
			t.Errorf("Hit = true, want false")
		}
		if resp.GetValue() != nil {
			t.Errorf("Value = %+v, want nil", resp.GetValue())
		}
	})

	// --- SPEC-022 §3 scenario 2: Put seguito da Get con la STESSA chiave
	// esatta -> hit:true con lo stesso CacheValue scritto. ---
	t.Run("Scenario2_PutThenGetSameKeyHits", func(t *testing.T) {
		key := proto.Clone(baseKey).(*semanticcachev1.CacheKey)
		key.EntityId = "scenario2-entity"

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: baseValue}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		resp, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !resp.GetHit() {
			t.Fatalf("Hit = false, want true")
		}
		if !proto.Equal(resp.GetValue(), baseValue) {
			t.Errorf("Value = %+v, want %+v (lo stesso CacheValue scritto)", resp.GetValue(), baseValue)
		}
	})

	// --- SPEC-022 §3 scenario 3: stesso Put, Get con logic_fingerprint
	// DIVERSO -> hit:false. Verifica diretta che il fingerprint partecipi
	// davvero alla chiave. ---
	t.Run("Scenario3_DifferentLogicFingerprintMisses", func(t *testing.T) {
		key := proto.Clone(baseKey).(*semanticcachev1.CacheKey)
		key.EntityId = "scenario3-entity"

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: baseValue}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		differentFingerprint := proto.Clone(key).(*semanticcachev1.CacheKey)
		differentFingerprint.LogicFingerprint = "fp-v2-diverso"

		resp, err := client.Get(ctx, differentFingerprint)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if resp.GetHit() {
			t.Errorf("Hit = true, want false: logic_fingerprint diverso deve produrre un miss")
		}
	})

	// --- SPEC-022 §3 scenario 4: stesso Put, Get con acl_scope DIVERSO ->
	// hit:false. Verifica diretta dell'isolamento cross-ACL (ADD §2.6.3). ---
	t.Run("Scenario4_DifferentAclScopeMisses", func(t *testing.T) {
		key := proto.Clone(baseKey).(*semanticcachev1.CacheKey)
		key.EntityId = "scenario4-entity"

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: baseValue}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		differentAcl := proto.Clone(key).(*semanticcachev1.CacheKey)
		differentAcl.AclScope = "tenant-b"

		resp, err := client.Get(ctx, differentAcl)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if resp.GetHit() {
			t.Errorf("Hit = true, want false: acl_scope diverso deve produrre un miss (isolamento cross-ACL)")
		}
	})

	// --- SPEC-022 §3 scenario 5: Put con ttl_seconds basso (1), Get DOPO
	// la scadenza -> hit:false. Scadenza reale via Redis, non simulata. ---
	t.Run("Scenario5_ExpiresAfterLowTTL", func(t *testing.T) {
		key := proto.Clone(baseKey).(*semanticcachev1.CacheKey)
		key.EntityId = "scenario5-entity"
		shortLived := proto.Clone(baseValue).(*semanticcachev1.CacheValue)
		shortLived.TtlSeconds = 1

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: shortLived}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		// Prova di hit immediato: la chiave esiste ancora prima della
		// scadenza (altrimenti lo scenario non proverebbe nulla).
		immediate, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get immediato: %v", err)
		}
		if !immediate.GetHit() {
			t.Fatalf("Hit immediato = false, want true (la chiave non dovrebbe essere già scaduta)")
		}

		time.Sleep(1500 * time.Millisecond)

		expired, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get dopo scadenza: %v", err)
		}
		if expired.GetHit() {
			t.Errorf("Hit dopo scadenza TTL = true, want false")
		}
	})

	// --- SPEC-022 §3 scenario 6: Put con ttl_seconds:0, Get dopo
	// un'attesa superiore al TTL dello scenario 5 -> ancora hit:true. 0
	// significa genuinamente "nessuna scadenza". ---
	t.Run("Scenario6_ZeroTTLNeverExpires", func(t *testing.T) {
		key := proto.Clone(baseKey).(*semanticcachev1.CacheKey)
		key.EntityId = "scenario6-entity"
		noExpiry := proto.Clone(baseValue).(*semanticcachev1.CacheValue)
		noExpiry.TtlSeconds = 0

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: noExpiry}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		time.Sleep(1500 * time.Millisecond)

		resp, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !resp.GetHit() {
			t.Errorf("Hit = false, want true: ttl_seconds=0 deve significare nessuna scadenza")
		}
	})

	// --- SPEC-022 §4 edge case: campo della chiave vuoto (logic_fingerprint
	// "") -> nessun caso speciale, chiave valida e deterministica. ---
	t.Run("EdgeCase_EmptyKeyFieldIsOrdinaryNotSpecialCased", func(t *testing.T) {
		key := &semanticcachev1.CacheKey{
			EntityId:         "edge-empty-field",
			AstHash:          "edge-empty-field",
			LogicFingerprint: "",
			AclScope:         "default",
		}
		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: baseValue}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		resp, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !resp.GetHit() {
			t.Errorf("Hit = false, want true: una stringa vuota resta una chiave valida e deterministica")
		}
	})

	// --- SPEC-022 §4 edge case: ttl_seconds molto grande. Verificato
	// EMPIRICAMENTE (redis-cli diretto contro il container reale, non
	// assunto) che math.MaxUint32 secondi è ben ACCETTATO da Redis (il
	// vero overflow "invalid expire time" scatta solo per valori enormi,
	// dell'ordine di 10^16+ secondi, irraggiungibili da un campo uint32) —
	// il trigger letterale descritto in §4 non è riproducibile con questo
	// tipo di campo. Verificato invece che il valore massimo rappresentabile
	// non viene troncato silenziosamente (questa parte). La propagazione
	// generica di un errore Redis come errore gRPC esplicito, che è la
	// garanzia di fondo dietro questa riga della tabella, è la STESSA
	// verificata da EdgeCase_RedisUnavailable qui sotto (stesso codice,
	// stesso path `Set` -> errore -> Unavailable).
	t.Run("EdgeCase_MaxUint32TTLNotSilentlyTruncated", func(t *testing.T) {
		key := &semanticcachev1.CacheKey{
			EntityId:         "edge-max-ttl",
			AstHash:          "edge-max-ttl",
			LogicFingerprint: "edge-max-ttl",
			AclScope:         "default",
		}
		hugeTTL := proto.Clone(baseValue).(*semanticcachev1.CacheValue)
		hugeTTL.TtlSeconds = math.MaxUint32

		if _, err := client.Put(ctx, &semanticcachev1.PutRequest{Key: key, Value: hugeTTL}); err != nil {
			t.Fatalf("Put con ttl_seconds=MaxUint32: %v (atteso: nessun errore, Redis lo accetta)", err)
		}
		resp, err := client.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !resp.GetHit() {
			t.Fatalf("Hit = false, want true")
		}
		if resp.GetValue().GetTtlSeconds() != math.MaxUint32 {
			t.Errorf("TtlSeconds = %d, want %d (nessun troncamento silenzioso)", resp.GetValue().GetTtlSeconds(), uint32(math.MaxUint32))
		}
	})

	// --- SPEC-022 §4 edge case: Redis irraggiungibile -> errore gRPC
	// Unavailable esplicito su Get E Put, non un hit:false silenzioso. ---
	t.Run("EdgeCase_RedisUnavailable", func(t *testing.T) {
		// Client Redis dedicato verso un indirizzo che rifiuta la
		// connessione — non lo stesso Redis reale usato sopra.
		badRDB := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
		defer badRDB.Close()
		badClient := startServer(t, badRDB)

		key := &semanticcachev1.CacheKey{EntityId: "x", AstHash: "x", LogicFingerprint: "x", AclScope: "x"}

		getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := badClient.Get(getCtx, key); status.Code(err) != codes.Unavailable {
			t.Errorf("Get: status = %v, want Unavailable", err)
		}

		putCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel2()
		if _, err := badClient.Put(putCtx, &semanticcachev1.PutRequest{Key: key, Value: baseValue}); status.Code(err) != codes.Unavailable {
			t.Errorf("Put: status = %v, want Unavailable", err)
		}
	})
}

// ============================================================
// Harness.
// ============================================================

func startRedis(t *testing.T, ctx context.Context) *goredis.Client {
	t.Helper()
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("avvio container redis:7-alpine: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container redis: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}
	opts, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("goredis.ParseURL(%q): %v", connStr, err)
	}
	rdb := goredis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	return rdb
}

func startServer(t *testing.T, rdb *goredis.Client) semanticcachev1.SemanticCacheClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	semanticcachev1.RegisterSemanticCacheServer(srv, &server.Server{Redis: rdb})

	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return semanticcachev1.NewSemanticCacheClient(conn)
}
