# SPEC-131 — Descriptor Envoy ristretto e deterministico
Stato: approved
Task-tree: T6.6 / repository completeness · Servizio: `deploy/envoy`, Taskfile · ADD: Modulo 3 §3.1
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto`

## 1. Obiettivo

Rendere `task envoy:descriptor` indipendente dall'aggiunta di API interne non
esposte dall'edge. Il descriptor contiene esclusivamente il contratto Retrieval
necessario alle route Envoy ed è verificato semanticamente oltre al clean diff.

## 2. Interfaccia

```text
task envoy:descriptor
deploy/envoy/retrieval.pb
python3 -m unittest tests.unit.envoy_descriptor.test_envoy_descriptor
```

## 3. Comportamento

1. **Dato** il repository corrente, **quando** si rigenera il descriptor,
   **allora** contiene `eci/retrieval/v1/retrieval.proto` e il servizio
   `eci.retrieval.v1.RetrievalEngine`.
2. **Dato** il descriptor edge, **quando** viene decodificato, **allora** non
   contiene orchestrator, verification, summarization o semantic-cache.
3. **Dato** un nuovo proto interno nel modulo Buf, **quando** gira il target,
   **allora** il suo contenuto non modifica `retrieval.pb`.
4. **Dato** il file checked-in aggiornato, **quando** il target gira due volte,
   **allora** i byte restano identici e il Git diff è vuoto.
5. **Dato** un descriptor vuoto o con servizio Retrieval assente, **quando**
   gira il test, **allora** fallisce senza affidarsi a stringhe binarie.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| path proto rinominato | Buf fallisce esplicitamente |
| API interna aggiunta | nessun cambiamento del descriptor edge |
| descriptor corrotto | parsing protobuf/test fallisce |
| servizio extra incluso | test di allow-list fallisce |

## 5. Non-goals

- Modificare proto, route Envoy o esposizione pubblica.
- Aggiungere API interne al transcoder o cambiare il bootstrap TLS.
- Dichiarare valida l'immagine Envoy senza il gate Docker separato.

## 6. Vincoli dall'ADD

- L'edge espone solo API esplicitamente instradate e autenticabili.
- I servizi interni non diventano pubblici per effetto del code generation.
- Gli artefatti generati sono riproducibili e verificabili.

## 7. Test plan

- Unit Python con decoder strutturale stdlib di `FileDescriptorSet` (campi
  file/package/service), senza dipendenza Python globale non dichiarata.
- Rigenerazione Buf path-scoped ripetuta e clean-diff.
- `task verify:generated`, full CPU/security/guard.

## 8. Osservabilità

Nessuna metrica runtime nuova. Il gate CI riporta solo inclusione/esclusione e
drift dell'artefatto, senza dati applicativi.

## 9. Criteri di accettazione

- [ ] Red iniziale osservato sul descriptor storico contenente semantic-cache.
- [ ] Test semantico verifica allow-list di file e servizi.
- [ ] Due rigenerazioni Buf producono SHA-256 identico e clean diff.
- [ ] `task verify:generated` e gate CPU/security/guard verdi.
