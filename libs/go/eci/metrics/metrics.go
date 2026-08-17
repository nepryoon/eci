// Package metrics espone via /metrics (formato Prometheus) lo stato di
// salute dei quattro sink Kafka del progetto (SPEC-036, T3.3 parte 2/2) —
// componendo attorno al wrapper resilience.WithRetryAndDLQ (SPEC-035) già
// esistente, senza toccarne la logica. Non "lag" in senso stretto di
// offset (§0 della SPEC: kafka-go non lo riporta in modo affidabile per
// reader con GroupID, verificato — non presunto — contro un thread
// ufficiale del progetto), ma segnali di salute costruibili con certezza
// dal wrapper stesso: contatori per esito, timestamp dell'ultimo
// messaggio processato.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"

	"github.com/eci-project/eci/libs/go/eci/resilience"
)

// messagesProcessedTotal/lastProcessedTimestampSeconds registrati UNA
// VOLTA sul registry globale di default di prometheus/client_golang
// (stesso registry servito da Handler(), SPEC-036 §2) — promauto.NewX
// panica su una seconda registrazione della stessa metrica, motivo per
// cui restano var di package, mai ricreate per chiamata.
var (
	messagesProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "eci_messages_processed_total",
		Help: "Numero di messaggi Kafka processati da un sink, per esito finale (dopo retry/DLQ).",
	}, []string{"sink", "outcome"})

	lastProcessedTimestampSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "eci_last_processed_timestamp_seconds",
		Help: "Timestamp Unix dell'ultimo messaggio Kafka processato da un sink, indipendentemente dall'esito.",
	}, []string{"sink"})
)

// outcomeLabel traduce resilience.Outcome nell'etichetta stringa dichiarata
// da SPEC-036 §2 ({processed, retried, dead_lettered}, stessi valori di
// resilience.Outcome). Un valore fuori enum (non dovrebbe mai accadere per
// costruzione, resilience.Outcome ha solo tre varianti) diventa "unknown"
// — mai un panic su un'etichetta Prometheus, mai un valore vuoto.
func outcomeLabel(outcome resilience.Outcome) string {
	switch outcome {
	case resilience.OutcomeProcessed:
		return "processed"
	case resilience.OutcomeRetried:
		return "retried"
	case resilience.OutcomeDeadLettered:
		return "dead_lettered"
	default:
		return "unknown"
	}
}

// WithPrometheus avvolge una resilience.ProcessFunc GIÀ COMPOSTA con
// resilience.WithRetryAndDLQ (SPEC-036 §2) — lo strato PIÙ ESTERNO della
// composizione: l'outcome che vede è quello FINALE, dopo che retry/DLQ ha
// già agito. Aggiorna il gauge del timestamp SEMPRE, indipendentemente
// dall'esito o da un eventuale errore (§3 scenario 4: "indipendentemente
// dall'esito" — un tentativo è comunque avvenuto), poi incrementa il
// contatore per (sinkName, outcome).
func WithPrometheus(sinkName string, inner resilience.ProcessFunc) resilience.ProcessFunc {
	return func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (resilience.Outcome, error) {
		outcome, err := inner(ctx, topic, value, headers)
		lastProcessedTimestampSeconds.WithLabelValues(sinkName).Set(float64(time.Now().Unix()))
		messagesProcessedTotal.WithLabelValues(sinkName, outcomeLabel(outcome)).Inc()
		return outcome, err
	}
}

// Handler espone http.Handler per /metrics (wrapper sottile su
// promhttp.Handler(), stesso registry globale di prometheus/client_golang
// — SPEC-036 §2).
func Handler() http.Handler {
	return promhttp.Handler()
}
