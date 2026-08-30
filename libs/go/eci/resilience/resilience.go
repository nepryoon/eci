// Package resilience implementa retry con backoff esponenziale + DLQ per
// consumer Kafka at-least-once (SPEC-035, T3.3 parte 1/2): un decoratore
// attorno alla ProcessFunc di un sink esistente, senza riscriverne la
// logica applicativa. Lo stato del retry-count viaggia sull'header del
// messaggio stesso — non in Postgres — così un messaggio che fallisce
// SEMPRE (poison pill) non blocca la partizione per sempre: dopo
// MaxRetries tentativi finisce su un topic DLQ dedicato invece di essere
// ripubblicato all'infinito.
package resilience

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// RetryCountHeaderKey è il nome dell'header Kafka che porta il numero di
// tentativi già effettuati (SPEC-035 §2) — valore decimale ASCII, stessa
// convenzione già in uso per event_id/trace_id (header Kafka semplici, non
// un formato binario). Assente = tentativo 0.
const RetryCountHeaderKey = "x-eci-retry-count"

const (
	defaultMaxRetries  = 5
	defaultBackoffBase = time.Second
)

// Config configura WithRetryAndDLQ (SPEC-035 §2). Zero value -> default
// (MaxRetries=5, BackoffBase=1s, che produce i ritardi 1s,2s,4s,8s,16s).
type Config struct {
	MaxRetries       int
	BackoffBase      time.Duration
	RetryTopicSuffix string
	// ShouldRetry can reserve infrastructure-unavailability errors for the
	// caller, which must then leave the source offset uncommitted. Nil keeps
	// the historical behavior: every processing error is retried/DLQed.
	ShouldRetry func(error) bool
}

// RetryTopic returns a consumer-scoped retry topic. An empty suffix preserves
// the verified SPEC-035 same-topic behavior for compatibility; production
// consumers set a unique suffix so they never need Write on a shared primary.
func RetryTopic(topic, suffix string) string {
	if suffix == "" {
		return topic
	}
	return OriginalTopic(topic, suffix) + suffix
}

// OriginalTopic normalizes a message read from a scoped retry topic before
// invoking the existing ProcessFunc, whose topic allow-list remains unchanged.
func OriginalTopic(topic, suffix string) string {
	if suffix != "" && strings.HasSuffix(topic, suffix) {
		return strings.TrimSuffix(topic, suffix)
	}
	return topic
}

func (c Config) withDefaults() Config {
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = defaultBackoffBase
	}
	return c
}

// Outcome descrive cosa ha fatto IL WRAPPER per questo messaggio — non
// l'Outcome specifico del sink applicativo (ciascun sink ha il proprio
// tipo Outcome locale, es. OutcomeStored/OutcomeMerged/OutcomeDuplicate,
// non riutilizzabile qui perché `resilience` non conosce la semantica di
// nessun sink specifico). Non fissato esplicitamente da SPEC-035 §2 (che
// dichiara solo il tipo di ritorno `Outcome` senza enumerarne i valori) —
// scelta in implementazione, pensata per essere il punto di aggancio
// naturale delle metriche Prometheus di T3.3 parte 2/2 (quante
// elaborazioni dirette, quante ritentate, quante finite in DLQ).
type Outcome int

const (
	// OutcomeProcessed: la ProcessFunc interna ha avuto successo,
	// nessun retry necessario.
	OutcomeProcessed Outcome = iota
	// OutcomeRetried: la ProcessFunc interna ha fallito, il messaggio è
	// stato ripubblicato sul retry topic configurato con retry-count incrementato.
	OutcomeRetried
	// OutcomeDeadLettered: la ProcessFunc interna ha fallito con
	// retry-count già a MaxRetries, il messaggio è stato pubblicato su
	// "{topic}.DLQ".
	OutcomeDeadLettered
)

// ProcessFunc è il contratto che ciascun sink already espone (a meno del
// parametro Deps, specifico di ciascun servizio e catturato per closure
// dal chiamante prima di passare la funzione a WithRetryAndDLQ — SPEC-035
// §2: "nessuna modifica alla logica applicativa di ProcessMessage stessa
// in nessuno dei quattro servizi").
type ProcessFunc func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (Outcome, error)

// WithRetryAndDLQ avvolge inner (SPEC-035 §2): su successo, ritorna
// direttamente il risultato di inner. Se ShouldRetry rifiuta l'errore, lo
// propaga senza pubblicare e il chiamante deve lasciare l'offset non
// committato. Sugli altri errori legge il retry-count
// dagli header (assente o non numerico = 0, §4 edge case — fail-safe, mai
// un crash), applica un backoff ESPONENZIALE SINCRONO (`BackoffBase *
// 2^retryCount` — l'unico modo di produrre un ritardo reale osservabile
// senza stato esterno in Postgres/uno scheduler dedicato, dato il vincolo
// esplicito di §2 "via header sul messaggio stesso, non stato in
// Postgres": la funzione blocca il chiamante per la durata del backoff),
// poi:
//   - se retryCount < MaxRetries: ripubblica sul retry topic deterministico con
//     l'header incrementato di esattamente 1 (mai un doppio incremento,
//     §3 scenario 4) e ritorna (OutcomeRetried, nil) — l'offset
//     ORIGINALE viene commesso dal chiamante, il messaggio riappare più
//     avanti sullo stesso topic con lo stato di retry aggiornato;
//   - altrimenti: pubblica su "{topic}.DLQ" con l'header al valore
//     MASSIMO raggiunto (non ulteriormente incrementato) e ritorna
//     (OutcomeDeadLettered, nil) — stesso principio di "offset originale
//     comunque commesso".
//
// Se il publish di ripubblicazione/DLQ stesso fallisce (Kafka
// irraggiungibile, §4 edge case), l'errore propaga esplicitamente e
// l'offset NON va commesso in quel caso specifico — a differenza del
// percorso normale, qui non c'è un modo sicuro di "avanzare comunque"
// senza perdere il messaggio (né la copia originale né quella di
// retry/DLQ sarebbero mai state scritte).
//
// `producer`: Topic va lasciato VUOTO (ogni messaggio pubblicato qui
// specifica il proprio Topic, sia esso quello originale o "{topic}.DLQ" —
// kafka-go richiede che Writer.Topic e Message.Topic siano mutuamente
// esclusivi). Configurare un `BatchTimeout` basso è raccomandato: il
// default di kafka-go (1s) ritarda ogni publish fino a un secondo intero
// in attesa del flush del batch, sommandosi silenziosamente al backoff
// applicato qui sopra — scoperto scrivendo il test di integrazione di
// questo package (scenario 3, tempi osservati ~1s più lunghi
// dell'atteso), applicato di conseguenza anche ai producer reali dei
// quattro sink che usano questo wrapper.
func WithRetryAndDLQ(cfg Config, producer *kafka.Writer, inner ProcessFunc) ProcessFunc {
	cfg = cfg.withDefaults()

	return func(ctx context.Context, topic string, value []byte, headers []kafka.Header) (Outcome, error) {
		originalTopic := OriginalTopic(topic, cfg.RetryTopicSuffix)
		outcome, err := inner(ctx, originalTopic, value, headers)
		if err == nil {
			return outcome, nil
		}
		if cfg.ShouldRetry != nil && !cfg.ShouldRetry(err) {
			return outcome, err
		}

		retryCount := RetryCount(headers)

		if retryCount < cfg.MaxRetries {
			backoff := cfg.BackoffBase * time.Duration(uint64(1)<<uint(retryCount))
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return OutcomeRetried, fmt.Errorf("resilience: attesa di backoff interrotta: %w", ctx.Err())
			}

			retryHeaders := withRetryCountHeader(headers, retryCount+1)
			retryTopic := RetryTopic(originalTopic, cfg.RetryTopicSuffix)
			if pubErr := producer.WriteMessages(ctx, kafka.Message{
				Topic:   retryTopic,
				Value:   value,
				Headers: retryHeaders,
			}); pubErr != nil {
				return OutcomeRetried, fmt.Errorf("resilience: publish di retry su %q (tentativo %d): %w", retryTopic, retryCount+1, pubErr)
			}
			return OutcomeRetried, nil
		}

		dlqTopic := originalTopic + ".DLQ"
		dlqHeaders := withRetryCountHeader(headers, retryCount)
		if pubErr := producer.WriteMessages(ctx, kafka.Message{
			Topic:   dlqTopic,
			Value:   value,
			Headers: dlqHeaders,
		}); pubErr != nil {
			return OutcomeDeadLettered, fmt.Errorf("resilience: publish su DLQ %q: %w", dlqTopic, pubErr)
		}
		return OutcomeDeadLettered, nil
	}
}

// RetryCount legge RetryCountHeaderKey dagli header di un messaggio.
// Assente o non parsabile come intero non-negativo -> 0 (§4 edge case:
// fail-safe, non blocca il messaggio, ricomincia il conteggio — mai un
// crash su un header corrotto).
func RetryCount(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key != RetryCountHeaderKey {
			continue
		}
		n, err := strconv.Atoi(string(h.Value))
		if err != nil || n < 0 {
			return 0
		}
		return n
	}
	return 0
}

// withRetryCountHeader ritorna una COPIA di headers con RetryCountHeaderKey
// impostato a count — qualunque occorrenza precedente viene rimossa prima
// (mai un doppio header con lo stesso nome: la lettura leggerebbe sempre
// la prima occorrenza, lasciando un valore "fantasma" stantio dietro,
// invisibile ma comunque presente sul messaggio).
func withRetryCountHeader(headers []kafka.Header, count int) []kafka.Header {
	out := make([]kafka.Header, 0, len(headers)+1)
	for _, h := range headers {
		if h.Key == RetryCountHeaderKey {
			continue
		}
		out = append(out, h)
	}
	out = append(out, kafka.Header{
		Key:   RetryCountHeaderKey,
		Value: []byte(strconv.Itoa(count)),
	})
	return out
}
