# ADR-0021 — Deduplica degli eventi scoped per consumer

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D1, T1.3 / SPEC-015, T3.0 / SPEC-030,
T3.2 / SPEC-034, T7.1 / SPEC-062

## Contesto

Kafka consegna lo stesso evento a ogni consumer group indipendente. In
particolare `embedding-worker` e `sink-search` consumano entrambi
`outbox.event.CodeChunk`. La chiave primaria storica di `processed_events` era
il solo `event_id`: il primo consumer che registrava l'evento faceva apparire
la copia dell'altro come una redelivery. Il risultato dipendeva dall'ordine di
schedulazione e lasciava mancante o l'embedding o il documento OpenSearch.

Il `consumer_name` era gia' persistito, ma non partecipava al vincolo di
unicita'. L'ADD richiede sink idempotenti separati e attribuisce la loro
statefulness ai rispettivi consumer group; non richiede deduplica globale fra
bounded context diversi.

## Opzioni considerate

1. **Mantenere la chiave globale e assegnare un evento diverso a ogni sink.**
   Scartata: falsifica la provenance dell'evento CDC e richiede un fan-out
   writer aggiuntivo.
2. **Usare una tabella inbox separata per servizio.** Scartata: aggiunge DDL e
   manutenzione senza offrire isolamento ulteriore rispetto a una chiave
   composta.
3. **Chiave composta `(event_id, consumer_name)`.** Scelta: preserva la tabella
   condivisa e rende l'idempotenza congruente con il consumer group.

## Decisione

La migration `0006_consumer_scoped_processed_events` sostituisce la chiave
primaria globale con `(event_id, consumer_name)`. Tutti i consumer usano
esplicitamente la stessa coppia come conflict target. Una redelivery allo
stesso consumer resta un no-op; la prima consegna a un consumer distinto e'
nuova e deve essere elaborata.

`consumer_name` e' una costante del processo, mai un header Kafka o input
controllato dal payload. L'evento non puo' scegliere il proprio namespace di
deduplica.

## Conseguenze

- Il fan-out `CodeChunk` produce deterministicamente sia embedding sia indice
  full-text, indipendentemente dall'ordine dei consumer.
- Il numero di righe puo' crescere fino a una riga per coppia evento/consumer;
  la policy di purge dell'ADD resta necessaria.
- Nessun payload, ACL o contratto protobuf cambia.
- I consumer esistenti conservano la propria protezione at-least-once.

## Migrazione, rollback e sicurezza

L'upgrade della chiave e' compatibile con le righe storiche, che hanno al
massimo un consumer per `event_id`. La down migration rifiuta il rollback se
esistono eventi elaborati da piu' consumer: eliminare automaticamente una
riga perderebbe provenance di elaborazione e potrebbe alterare la sicurezza
della ripresa. L'operatore deve fermare i consumer e scegliere esplicitamente
una strategia di ricostruzione prima di ritentare un downgrade.

La modifica non amplia autorizzazioni: ogni consumer continua a usare la
propria identita' e il proprio nome compilato. Test DDL reali verificano che
consumer distinti possano registrare lo stesso evento e che una redelivery
della stessa coppia resti vietata; il regression test di sink-search simula
l'ordine embedding-worker-prima-di-sink-search.
