//go:build integration

// SPEC-035 §3/§7 — test di integrazione: Kafka reale via testcontainers
// (stesso principio già stabilito ovunque in questo progetto, SPEC-015/
// 030/033/034) — backoff/DLQ sono comportamenti temporali e di
// instradamento reali, non verificabili con un mock (§7). Nessun Postgres/
// altro downstream qui: resilience.WithRetryAndDLQ è puramente un
// decoratore Kafka-a-Kafka, indipendente dalla logica applicativa dei
// quattro sink che lo useranno.
//
// Esecuzione manuale (richiede Docker):
// go test -tags=integration ./eci/resilience/... -run TestWithRetryAndDLQ -v
package resilience_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/eci-project/eci/libs/go/eci/resilience"
)

func TestWithRetryAndDLQ(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)

	t.Run("Scenario1_RetrySucceedsEventuallyNoDLQ", func(t *testing.T) {
		scenario1RetrySucceedsEventuallyNoDLQ(t, ctx, brokers)
	})
	t.Run("Scenario2_ExhaustedRetriesGoesToDLQWithMaxCount", func(t *testing.T) {
		scenario2ExhaustedRetriesGoesToDLQWithMaxCount(t, ctx, brokers)
	})
	t.Run("Scenario3_BackoffGrowsExponentiallyNotConstant", func(t *testing.T) {
		scenario3BackoffGrowsExponentiallyNotConstant(t, ctx, brokers)
	})
	t.Run("Scenario4_RetryCountIncrementsByExactlyOne", func(t *testing.T) {
		scenario4RetryCountIncrementsByExactlyOne(t, ctx, brokers)
	})
	t.Run("EdgeCase_NonNumericRetryHeaderTreatedAsZero", func(t *testing.T) {
		edgeCaseNonNumericRetryHeaderTreatedAsZero(t, ctx, brokers)
	})
	t.Run("EdgeCase_DLQPublishFailurePropagatesErrorOffsetNotCommitted", func(t *testing.T) {
		edgeCaseDLQPublishFailurePropagatesError(t, ctx, brokers)
	})
	t.Run("Security_ConsumerScopedRetryNormalizesOriginalTopic", func(t *testing.T) {
		securityConsumerScopedRetryNormalizesOriginalTopic(t, ctx, brokers)
	})
}

// ADR-0019 — il retry di produzione non richiede Write sul topic primario:
// viene pubblicato sul topic per-consumer e normalizzato prima della logica
// applicativa. Il topic primario non riceve una seconda copia.
func securityConsumerScopedRetryNormalizesOriginalTopic(t *testing.T, ctx context.Context, brokers []string) {
	primary := uniqueTopic(t, "scoped-retry")
	suffix := ".retry.embedding-worker"
	retryTopic := resilience.RetryTopic(primary, suffix)
	ensureTopics(t, ctx, brokers, primary, retryTopic)
	producer := newWriter(brokers)
	defer producer.Close()

	var innerTopics []string
	wrapped := resilience.WithRetryAndDLQ(
		resilience.Config{MaxRetries: 2, BackoffBase: 20 * time.Millisecond, RetryTopicSuffix: suffix},
		producer,
		func(_ context.Context, topic string, _ []byte, headers []kafka.Header) (resilience.Outcome, error) {
			innerTopics = append(innerTopics, topic)
			if resilience.RetryCount(headers) == 0 {
				return 0, errors.New("simulated first-attempt failure")
			}
			return resilience.OutcomeProcessed, nil
		},
	)

	if outcome, err := wrapped(ctx, primary, []byte("payload"), nil); err != nil || outcome != resilience.OutcomeRetried {
		t.Fatalf("first attempt = (%v, %v), want (OutcomeRetried, nil)", outcome, err)
	}

	retryReader := newReaderWithGroup(brokers, "scoped-retry-group", retryTopic)
	defer retryReader.Close()
	retry := fetchWithTimeout(t, ctx, retryReader)
	if retry.Topic != retryTopic {
		t.Fatalf("retry topic = %q, want %q", retry.Topic, retryTopic)
	}
	if outcome, err := wrapped(ctx, retry.Topic, retry.Value, retry.Headers); err != nil || outcome != resilience.OutcomeProcessed {
		t.Fatalf("retry attempt = (%v, %v), want (OutcomeProcessed, nil)", outcome, err)
	}
	if len(innerTopics) != 2 || innerTopics[0] != primary || innerTopics[1] != primary {
		t.Fatalf("inner topics = %v, want [%q %q]", innerTopics, primary, primary)
	}
	assertNoMessageArrives(t, ctx, brokers, primary)
}

// ============================================================
// Scenario 1 — fallisce al primo tentativo, ha successo al retry:
// nessun DLQ.
// ============================================================

func scenario1RetrySucceedsEventuallyNoDLQ(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "scenario1")
	ensureTopics(t, ctx, brokers, topic)
	producer := newWriter(brokers)
	defer producer.Close()

	produce(t, ctx, producer, topic, "k1", []byte("payload"), nil)

	cfg := resilience.Config{MaxRetries: 3, BackoffBase: 50 * time.Millisecond}
	inner := func(_ context.Context, _ string, _ []byte, headers []kafka.Header) (resilience.Outcome, error) {
		if resilience.RetryCount(headers) == 0 {
			return 0, errors.New("simulated failure on first attempt")
		}
		return 0, nil
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, producer, inner)

	reader := newReaderWithGroup(brokers, "scenario1-group", topic)
	defer reader.Close()

	msg1 := fetchWithTimeout(t, ctx, reader)
	if _, err := wrapped(ctx, msg1.Topic, msg1.Value, msg1.Headers); err != nil {
		t.Fatalf("wrapped (primo tentativo): %v", err)
	}
	if err := reader.CommitMessages(ctx, msg1); err != nil {
		t.Fatalf("commit primo tentativo: %v", err)
	}

	msg2 := fetchWithTimeout(t, ctx, reader)
	if got := resilience.RetryCount(msg2.Headers); got != 1 {
		t.Fatalf("retry-count del messaggio ripubblicato = %d, want 1", got)
	}
	outcome, err := wrapped(ctx, msg2.Topic, msg2.Value, msg2.Headers)
	if err != nil {
		t.Fatalf("wrapped (secondo tentativo): %v", err)
	}
	if outcome != resilience.OutcomeProcessed {
		t.Fatalf("outcome = %v, want OutcomeProcessed", outcome)
	}
	if err := reader.CommitMessages(ctx, msg2); err != nil {
		t.Fatalf("commit secondo tentativo: %v", err)
	}

	assertNoMessageArrives(t, ctx, brokers, topic+".DLQ")
}

// ============================================================
// Scenario 2 — fallisce SEMPRE: dopo esattamente MaxRetries tentativi
// compare sul topic {topic}.DLQ, con retry-count al valore massimo.
// ============================================================

func scenario2ExhaustedRetriesGoesToDLQWithMaxCount(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "scenario2")
	retrySuffix := ".retry.sink-graph"
	retryTopic := resilience.RetryTopic(topic, retrySuffix)
	// Il topic DLQ va creato esplicitamente QUI (confluent-local non ha
	// auto.create.topics.enable=true a livello di broker, stessa
	// deviazione già nota da SPEC-015 §10 — AllowAutoTopicCreation lato
	// produttore da solo NON basta contro QUESTO broker specifico,
	// verificato empiricamente: il primo publish verso un topic MAI
	// creato prima fallisce con "Unknown Topic Or Partition". Contro il
	// broker reale dello stack dev (apache/kafka:latest) l'auto-create è
	// invece abilitato a livello di broker, quindi in produzione il topic
	// DLQ nasce davvero implicitamente al primo publish, come da SPEC-035
	// §2 — qui nel test lo anticipiamo esplicitamente per lo stesso
	// motivo per cui lo fanno già tutti i topic "normali" in questo repo).
	ensureTopics(t, ctx, brokers, topic, retryTopic, topic+".DLQ")
	producer := newWriter(brokers)
	defer producer.Close()

	produce(t, ctx, producer, topic, "k2", []byte("payload"), nil)

	cfg := resilience.Config{MaxRetries: 2, BackoffBase: 30 * time.Millisecond, RetryTopicSuffix: retrySuffix}
	alwaysFails := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return 0, errors.New("simulated permanent failure")
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, producer, alwaysFails)

	primaryReader := newReaderWithGroup(brokers, "scenario2-primary-group", topic)
	defer primaryReader.Close()
	retryReader := newReaderWithGroup(brokers, "scenario2-retry-group", retryTopic)
	defer retryReader.Close()

	for i := 0; i < cfg.MaxRetries; i++ {
		reader := retryReader
		if i == 0 {
			reader = primaryReader
		}
		msg := fetchWithTimeout(t, ctx, reader)
		if got := resilience.RetryCount(msg.Headers); got != i {
			t.Fatalf("retry-count al giro %d = %d, want %d", i, got, i)
		}
		outcome, err := wrapped(ctx, msg.Topic, msg.Value, msg.Headers)
		if err != nil {
			t.Fatalf("wrapped al giro %d: %v", i, err)
		}
		if outcome != resilience.OutcomeRetried {
			t.Fatalf("outcome al giro %d = %v, want OutcomeRetried", i, outcome)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			t.Fatalf("commit al giro %d: %v", i, err)
		}
	}

	lastMsg := fetchWithTimeout(t, ctx, retryReader)
	if got := resilience.RetryCount(lastMsg.Headers); got != cfg.MaxRetries {
		t.Fatalf("retry-count dell'ultimo tentativo = %d, want %d", got, cfg.MaxRetries)
	}
	outcome, err := wrapped(ctx, lastMsg.Topic, lastMsg.Value, lastMsg.Headers)
	if err != nil {
		t.Fatalf("wrapped (esaurimento retry): %v", err)
	}
	if outcome != resilience.OutcomeDeadLettered {
		t.Fatalf("outcome = %v, want OutcomeDeadLettered", outcome)
	}
	if err := retryReader.CommitMessages(ctx, lastMsg); err != nil {
		t.Fatalf("commit finale: %v", err)
	}

	dlqReader := newReaderWithGroup(brokers, "scenario2-dlq-group", topic+".DLQ")
	defer dlqReader.Close()
	dlqMsg := fetchWithTimeout(t, ctx, dlqReader)
	if got := resilience.RetryCount(dlqMsg.Headers); got != cfg.MaxRetries {
		t.Fatalf("retry-count del messaggio in DLQ = %d, want %d (valore massimo)", got, cfg.MaxRetries)
	}
}

// ============================================================
// Scenario 3 — i tempi tra tentativi successivi crescono in modo
// coerente con backoff esponenziale, non costanti.
// ============================================================

func scenario3BackoffGrowsExponentiallyNotConstant(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "scenario3")
	ensureTopics(t, ctx, brokers, topic)
	producer := newWriter(brokers)
	defer producer.Close()

	produce(t, ctx, producer, topic, "k3", []byte("payload"), nil)

	base := 80 * time.Millisecond
	cfg := resilience.Config{MaxRetries: 3, BackoffBase: base}
	alwaysFails := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return 0, errors.New("simulated permanent failure")
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, producer, alwaysFails)

	reader := newReaderWithGroup(brokers, "scenario3-group", topic)
	defer reader.Close()

	gaps := make([]time.Duration, 0, cfg.MaxRetries)
	for i := 0; i < cfg.MaxRetries; i++ {
		msg := fetchWithTimeout(t, ctx, reader)
		start := time.Now()
		if _, err := wrapped(ctx, msg.Topic, msg.Value, msg.Headers); err != nil {
			t.Fatalf("wrapped al giro %d: %v", i, err)
		}
		// Il tempo trascorso DENTRO wrapped() è dominato dal sleep di
		// backoff (il publish di ripubblicazione è trascurabile in
		// confronto) — misura diretta del ritardo applicato, non solo del
		// gap tra fetch successivi (che includerebbe anche latenza di
		// poll/rebalance del consumer group, rumore non pertinente qui).
		gaps = append(gaps, time.Since(start))
		if err := reader.CommitMessages(ctx, msg); err != nil {
			t.Fatalf("commit al giro %d: %v", i, err)
		}
	}

	t.Logf("gaps misurati (attesi ~%v, ~%v, ~%v): %v", base, 2*base, 4*base, gaps)
	for i, gap := range gaps {
		wantMin := time.Duration(float64(base) * float64(int(1)<<i) * 0.5)
		if gap < wantMin {
			t.Fatalf("gap[%d] = %v, atteso almeno ~%v (backoff esponenziale base=%v, tentativo %d)", i, gap, wantMin, base, i)
		}
	}
	if gaps[1] <= gaps[0] {
		t.Fatalf("gap[1] (%v) deve essere maggiore di gap[0] (%v) — crescita non costante", gaps[1], gaps[0])
	}
	if gaps[2] <= gaps[1] {
		t.Fatalf("gap[2] (%v) deve essere maggiore di gap[1] (%v) — crescita non costante", gaps[2], gaps[1])
	}
}

// ============================================================
// Scenario 4 — retry-count incrementato esattamente di 1 ad ogni giro,
// mai un doppio incremento, mai un reset.
// ============================================================

func scenario4RetryCountIncrementsByExactlyOne(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "scenario4")
	ensureTopics(t, ctx, brokers, topic)
	producer := newWriter(brokers)
	defer producer.Close()

	produce(t, ctx, producer, topic, "k4", []byte("payload"), nil)

	cfg := resilience.Config{MaxRetries: 4, BackoffBase: 20 * time.Millisecond}
	alwaysFails := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return 0, errors.New("simulated permanent failure")
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, producer, alwaysFails)

	reader := newReaderWithGroup(brokers, "scenario4-group", topic)
	defer reader.Close()

	seen := make([]int, 0, cfg.MaxRetries)
	for i := 0; i < cfg.MaxRetries; i++ {
		msg := fetchWithTimeout(t, ctx, reader)
		seen = append(seen, resilience.RetryCount(msg.Headers))
		if _, err := wrapped(ctx, msg.Topic, msg.Value, msg.Headers); err != nil {
			t.Fatalf("wrapped al giro %d: %v", i, err)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			t.Fatalf("commit al giro %d: %v", i, err)
		}
	}
	for i, got := range seen {
		if got != i {
			t.Fatalf("retry-count osservati = %v, attesa la sequenza 0,1,2,...senza salti né reset (indice %d: got %d, want %d)", seen, i, got, i)
		}
	}
}

// ============================================================
// Edge case §4 — header di retry-count non numerico: trattato come 0.
// ============================================================

func edgeCaseNonNumericRetryHeaderTreatedAsZero(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "edge-nonnumeric")
	ensureTopics(t, ctx, brokers, topic)
	producer := newWriter(brokers)
	defer producer.Close()

	produce(t, ctx, producer, topic, "k5", []byte("payload"), []kafka.Header{
		{Key: resilience.RetryCountHeaderKey, Value: []byte("not-a-number")},
	})

	cfg := resilience.Config{MaxRetries: 2, BackoffBase: 20 * time.Millisecond}
	var sawRetryCount int
	inner := func(_ context.Context, _ string, _ []byte, headers []kafka.Header) (resilience.Outcome, error) {
		sawRetryCount = resilience.RetryCount(headers)
		return 0, errors.New("simulated failure")
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, producer, inner)

	reader := newReaderWithGroup(brokers, "edge-nonnumeric-group", topic)
	defer reader.Close()

	msg := fetchWithTimeout(t, ctx, reader)
	if _, err := wrapped(ctx, msg.Topic, msg.Value, msg.Headers); err != nil {
		t.Fatalf("wrapped: %v", err)
	}
	if sawRetryCount != 0 {
		t.Fatalf("retry-count con header non numerico = %d, want 0 (fail-safe, non blocca il messaggio)", sawRetryCount)
	}
	if err := reader.CommitMessages(ctx, msg); err != nil {
		t.Fatalf("commit: %v", err)
	}

	msg2 := fetchWithTimeout(t, ctx, reader)
	if got := resilience.RetryCount(msg2.Headers); got != 1 {
		t.Fatalf("retry-count del messaggio ripubblicato dopo un header corrotto = %d, want 1 (ricomincia il conteggio, non un crash)", got)
	}
}

// ============================================================
// Edge case §4 — il publish verso DLQ stesso fallisce: errore propagato
// esplicitamente, offset NON committato.
// ============================================================

func edgeCaseDLQPublishFailurePropagatesError(t *testing.T, ctx context.Context, brokers []string) {
	topic := uniqueTopic(t, "edge-dlqfail")
	ensureTopics(t, ctx, brokers, topic)
	realProducer := newWriter(brokers)
	defer realProducer.Close()

	produce(t, ctx, realProducer, topic, "k6", []byte("payload"), nil)

	// MaxRetries=0: il primo fallimento va GIÀ direttamente in DLQ
	// (nessun retry disponibile) — il primo publish tentato dal wrapper è
	// quindi già quello verso il topic DLQ, e punta a un indirizzo
	// irraggiungibile (nessun listener sulla porta 1, stesso principio già
	// usato per un downstream irraggiungibile in SPEC-030/033/034).
	unreachableProducer := &kafka.Writer{
		Addr:                   kafka.TCP("127.0.0.1:1"),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		MaxAttempts:            1,
	}
	defer unreachableProducer.Close()

	cfg := resilience.Config{MaxRetries: 0, BackoffBase: 10 * time.Millisecond}
	alwaysFails := func(context.Context, string, []byte, []kafka.Header) (resilience.Outcome, error) {
		return 0, errors.New("simulated failure")
	}
	wrapped := resilience.WithRetryAndDLQ(cfg, unreachableProducer, alwaysFails)

	reader := newReaderWithGroup(brokers, "edge-dlqfail-group", topic)
	defer reader.Close()

	msg := fetchWithTimeout(t, ctx, reader)
	wrapCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := wrapped(wrapCtx, msg.Topic, msg.Value, msg.Headers); err == nil {
		t.Fatal("atteso un errore quando il publish verso DLQ fallisce (Kafka irraggiungibile), ottenuto nil")
	}
	// NESSUN CommitMessages qui: l'offset non deve avanzare. Verifica
	// diretta: chiudendo e riaprendo un reader con lo STESSO group id, lo
	// stesso messaggio deve essere riconsegnato (redelivery), non perso.
	if err := reader.Close(); err != nil {
		t.Fatalf("chiusura reader (simulazione riavvio): %v", err)
	}
	reader2 := newReaderWithGroup(brokers, "edge-dlqfail-group", topic)
	defer reader2.Close()
	msg2 := fetchWithTimeout(t, ctx, reader2)
	if string(msg2.Value) != string(msg.Value) {
		t.Fatalf("atteso lo stesso messaggio in redelivery (offset non committato), ottenuto un valore diverso")
	}
}

// ============================================================
// Harness: Kafka (testcontainers, condiviso tra scenari — topic distinti
// per isolamento, stesso principio di scenario3 in
// services/sink-graph/internal/consumer/consumer_integration_test.go).
// ============================================================

func startKafka(t *testing.T, ctx context.Context) []string {
	t.Helper()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("avvio container kafka: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka Brokers: %v", err)
	}
	return brokers
}

func ensureTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka per creazione topic: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("kafka Controller: %v", err)
	}
	controllerAddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("dial kafka controller (%s): %v", controllerAddr, err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		t.Fatalf("CreateTopics %v: %v", topics, err)
	}
}

// newWriter costruisce il *kafka.Writer da passare a WithRetryAndDLQ:
// Topic VOLUTAMENTE lasciato vuoto (contratto richiesto da
// resilience.WithRetryAndDLQ — ogni Message specifica il proprio Topic,
// dato che il wrapper deve poter pubblicare sia sul topic originale sia
// su "{topic}.DLQ") — kafka-go richiede che Writer.Topic e Message.Topic
// siano mutuamente esclusivi, verificato nel sorgente del client.
// AllowAutoTopicCreation: true — funziona contro il broker reale dello
// stack dev (apache/kafka:latest, auto-create abilitato a livello di
// broker); verificato empiricamente qui che NON basta da solo contro
// confluent-local (auto.create.topics.enable=false a livello di broker,
// stessa deviazione già nota da SPEC-015 §10) — per questo i test che
// toccano un topic DLQ lo creano esplicitamente in anticipo (vedi
// scenario 2), stesso principio già in uso per i topic "normali" in
// questo repo.
// BatchTimeout basso — DEVIAZIONE scoperta scrivendo lo scenario 3: il
// default di kafka-go (1s) fa sì che ogni WriteMessages resti in attesa
// del flush del batch fino a un secondo intero anche per un solo
// messaggio, sporcando la misura dei tempi di backoff (osservato:
// ~1.08s/1.16s/1.32s invece di ~80/160/320ms attesi). Un valore basso
// qui è quindi necessario per isolare il ritardo REALE introdotto dal
// backoff di resilience.WithRetryAndDLQ da quello, indipendente,
// introdotto dal batching del producer stesso.
func newWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
}

func produce(t *testing.T, ctx context.Context, w *kafka.Writer, topic, key string, value []byte, headers []kafka.Header) {
	t.Helper()
	if err := w.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: value, Headers: headers}); err != nil {
		t.Fatalf("produzione messaggio sintetico su %s: %v", topic, err)
	}
}

func newReaderWithGroup(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    []string{topic},
		CommitInterval: 0,
	})
}

func fetchWithTimeout(t *testing.T, ctx context.Context, reader *kafka.Reader) kafka.Message {
	t.Helper()
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	msg, err := reader.FetchMessage(fetchCtx)
	if err != nil {
		t.Fatalf("FetchMessage: %v", err)
	}
	return msg
}

// assertNoMessageArrives verifica che NESSUN messaggio compaia sul topic
// dato entro un timeout breve — usato per confermare che uno scenario di
// solo-retry (mai DLQ) non abbia comunque pubblicato nulla sulla coda
// morta, anche se quel topic potrebbe non esistere affatto (il timeout
// del fetch è indistinguibile in questo caso, entrambi "nessun messaggio",
// che è esattamente l'esito atteso).
func assertNoMessageArrives(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()
	reader := newReaderWithGroup(brokers, "assert-empty-"+topic, topic)
	defer reader.Close()
	fetchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	msg, err := reader.FetchMessage(fetchCtx)
	if err == nil {
		t.Fatalf("atteso NESSUN messaggio su %s, trovato: %s", topic, string(msg.Value))
	}
}

func uniqueTopic(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("resilience-test-%s-%d", prefix, time.Now().UnixNano())
}
