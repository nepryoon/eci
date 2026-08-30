# SPEC-072 — Metadata outbox fail-closed condivisi
Stato: verified
Task-tree: T7.1a · Servizio: libs/go/eci · ADD: Modulo 1 §2.2.2–§2.2.4
Contratti: contracts/jsonschema/outbox-event.json, ADR-0025

## 1. Obiettivo

I consumer Go condividono un solo parser chiuso per l'identita' e l'operazione
autorevoli promosse dal CDC. Header mancanti, duplicati, non UTF-8, event ID
non UUID o operazioni ignote non possono raggiungere un effetto esterno.

## 2. Interfaccia

```go
package outboxmeta

type Operation string
const (
    OperationUpsert Operation = "UPSERT"
    OperationDelete Operation = "DELETE"
)
type Metadata struct { EventID string; Operation Operation }
func Parse(headers []kafka.Header) (Metadata, error)
```

## 3. Comportamento

1. **Dato** un solo `event_id` UUID e un solo `event_type=UPSERT`, **quando**
   Parse viene chiamato, **allora** ritorna metadata UPSERT.
2. **Dato** `event_type=DELETE`, **quando** Parse viene chiamato, **allora**
   ritorna metadata DELETE.
3. **Dato** uno dei due header assente, vuoto o duplicato, **quando** Parse
   viene chiamato, **allora** fallisce chiuso.
4. **Dato** un event ID non UUID canonico o operation ignota/non UTF-8,
   **quando** Parse viene chiamato, **allora** fallisce chiuso.
5. **Dato** trace header o metadata estranei, **quando** Parse viene chiamato,
   **allora** non diventano autorita' e non cambiano il risultato.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| stesso header due volte anche con valore uguale | errore permanente |
| UUID uppercase/non canonico | errore permanente |
| valore con byte UTF-8 invalido | errore permanente |
| ordine header diverso | stesso metadata |
| slice nil | errore permanente |

## 5. Non-goals

- Decodificare payload outbox o security scope.
- Decidere retry/DLQ/offset del singolo servizio.
- Interpretare trace context.
- Modificare i consumer in questa SPEC.

## 6. Vincoli dall'ADD

- Modulo 1 §2.2.2: CDC e' il boundary dell'envelope.
- Modulo 1 §2.2.3–§2.2.4: replay at-least-once e marker post-effetto.
- ADR-0025: solo `UPSERT|DELETE`; assente/duplicato/ignoto fail-closed.

## 7. Test plan

- Unit table-driven per entrambi gli happy path e tutte le forme malformate.
- Fuzz test deterministico: Parse non panic su header/byte arbitrari.
- `go test` e `go vet` sul modulo condiviso e aggregate repository.

## 8. Osservabilita'

La libreria non logga. I chiamanti possono esporre soltanto reason code chiusi,
mai valori degli header o ID evento.

## 9. Criteri di accettazione

- [x] Test nuovi osservati rossi per package assente.
- [x] `go test ./eci/outboxmeta`
- [x] `go vet ./eci/outboxmeta`
- [x] `task build && task lint && task test`
- [x] Nessun valore header compare in log/error restituito.

Evidenza aggiuntiva: fuzz `FuzzParseNeverPanics` per 2 secondi, 263.053
esecuzioni senza panic; errori pubblici ridotti alla sentinella costante
`invalid outbox metadata`.

## 10. Review avversariale pre-implementazione

Rifiutare i duplicati elimina ambiguita' first/last-wins sfruttabili durante
retry o forwarding. La canonicalita' UUID limita input e cardinalita'; gli
errori sono sentinelle chiuse e non includono dati attacker-controlled. Il
parser non considera il payload una fonte di operation o identity.
