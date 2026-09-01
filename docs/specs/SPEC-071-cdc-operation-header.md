# SPEC-071 — Operazione outbox nel boundary CDC
Stato: implemented
Task-tree: T7.1a · Servizio: deploy/compose, deploy/k8s · ADD: Modulo 1 §2.2.1–§2.2.4
Contratti: contracts/jsonschema/outbox-event.json, deploy/compose/debezium-outbox-connector.json

## 1. Obiettivo

Debezium promuove l'operazione canonica `event_type` e la coordinata monotona
`event_sequence` in header Kafka obbligatori, lasciando il payload invariato.
I consumer possono distinguere UPSERT/DELETE e rifiutare retry superati senza
inferire autorita' dal contenuto.

## 2. Interfaccia

```text
Kafka header: event_type = UTF-8("UPSERT" | "DELETE")
Kafka header: event_sequence = UTF-8(canonical positive int64, max 9223372036854775807)
Connector placement:
event_type:header:event_type,event_sequence:header:event_sequence
```

## 3. Comportamento

1. **Dato** un outbox UPSERT, **quando** EventRouter pubblica il record,
   **allora** l'header singolo `event_type` vale `UPSERT` e payload/topic/key
   restano invariati.
2. **Dato** un outbox DELETE, **quando** EventRouter pubblica il record,
   **allora** l'header singolo `event_type` vale `DELETE`.
3. **Dato** lo stack Compose, **quando** il connector viene registrato,
   **allora** event ID, trace ID ed event type sono tutti placement header.
4. **Dato** il chart Kubernetes renderizzato, **quando** CDC e' abilitato,
   **allora** usa la stessa placement esatta del template Compose.
5. **Dato** un payload tombstone, **quando** attraversa CDC, **allora** nessun
   testo, vettore, scope o secret viene copiato in nuovi header.
6. **Dato** un record reale, **quando** attraversa PostgreSQL/Debezium/Kafka,
   **allora** `event_sequence` corrisponde esattamente alla colonna identity.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `event_type` non ammesso | PostgreSQL CHECK rifiuta la riga prima del CDC |
| `trace_id` nullo | header trace opzionale; event type resta presente |
| riavvio/replay connector | stessa operazione deriva dalla riga canonica |
| config Compose/chart divergenti | test statico e render falliscono |
| sequence assente/non positiva/duplicata | consumer fail-closed |

## 5. Non-goals

- Interpretare DELETE dentro i sink.
- Cambiare topic, key o value Kafka.
- Aggiungere l'intero envelope PostgreSQL al valore Kafka.
- Registrare header provenienti dal payload applicativo.

## 6. Vincoli dall'ADD

- Modulo 1 §2.2.1–§2.2.2: PostgreSQL outbox e CDC restano l'unico percorso.
- Modulo 1 §2.2.3–§2.2.4: delivery at-least-once e consumer idempotenti.
- ADR-0025: `event_type` e' il discriminante canonico e deve essere promosso.

## 7. Test plan

- Unit repository: parsing JSON Compose e confronto esatto col template Helm.
- Integration testcontainers: UPSERT e DELETE reali, header singolo e valore
  esatto, usando il connector tracciato.
- Helm render/policy: configurazione CDC valida in ogni profilo supportato.

## 8. Osservabilita'

Nessuna nuova metrica o span. La sequence e' bounded e non contiene
coordinate di tenant, repository, path, contenuto o credenziali.

## 9. Criteri di accettazione

- [x] Test statico osservato rosso sulla configurazione corrente.
- [ ] Test integration CDC compilato e verde con Docker.
- [x] `task test`
- [x] `task k8s:validate`
- [ ] `task test:integration`
- [x] Compose e Helm contengono una placement identica e minimale.

Il test testcontainers verifica il record DELETE reale ed e' compilato nel
modulo integration. L'esecuzione resta intenzionalmente non dichiarata perche'
il daemon Docker locale non e' raggiungibile.

## 10. Review avversariale pre-implementazione

L'operazione deriva dalla colonna PostgreSQL protetta dal CHECK, non dal body
Kafka o da metadata caller-forged. La configurazione non espone payload o
coordinate di sicurezza, non introduce dual write e conserva replay e ordering
esistenti. Il sink dovra' comunque rifiutare header assente, duplicato o ignoto:
questa SPEC stabilisce soltanto il boundary autorevole che lo rende possibile.
