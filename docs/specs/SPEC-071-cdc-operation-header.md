# SPEC-071 — Operazione outbox nel boundary CDC
Stato: approved
Task-tree: T7.1a · Servizio: deploy/compose, deploy/k8s · ADD: Modulo 1 §2.2.1–§2.2.4
Contratti: contracts/jsonschema/outbox-event.json, deploy/compose/debezium-outbox-connector.json

## 1. Obiettivo

Debezium promuove l'operazione canonica `event_type` in un header Kafka
obbligatorio, lasciando il payload invariato. I consumer possono cosi'
distinguere UPSERT e DELETE senza inferirli dal contenuto e senza ampliare il
trust boundary.

## 2. Interfaccia

```text
Kafka header: event_type = UTF-8("UPSERT" | "DELETE")
Connector placement:
event_type:header:event_type
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

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `event_type` non ammesso | PostgreSQL CHECK rifiuta la riga prima del CDC |
| `trace_id` nullo | header trace opzionale; event type resta presente |
| riavvio/replay connector | stessa operazione deriva dalla riga canonica |
| config Compose/chart divergenti | test statico e render falliscono |

## 5. Non-goals

- Interpretare DELETE dentro i sink.
- Cambiare topic, key, value o schema outbox.
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

Nessuna nuova metrica o span. L'header ha cardinalita' chiusa e non contiene
coordinate di tenant, repository, path, contenuto o credenziali.

## 9. Criteri di accettazione

- [ ] Test statico osservato rosso sulla configurazione corrente.
- [ ] Test integration CDC compilato e verde con Docker.
- [ ] `task test`
- [ ] `task k8s:validate`
- [ ] `task test:integration`
- [ ] Compose e Helm contengono una placement identica e minimale.

## 10. Review avversariale pre-implementazione

L'operazione deriva dalla colonna PostgreSQL protetta dal CHECK, non dal body
Kafka o da metadata caller-forged. La configurazione non espone payload o
coordinate di sicurezza, non introduce dual write e conserva replay e ordering
esistenti. Il sink dovra' comunque rifiutare header assente, duplicato o ignoto:
questa SPEC stabilisce soltanto il boundary autorevole che lo rende possibile.
