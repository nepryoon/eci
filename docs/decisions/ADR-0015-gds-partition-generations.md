# ADR-0015 — Generation fencing per risultati GDS ACL-scoped

Stato: accepted
Data: 2026-08-29
Decisione collegata: T6.7 / SPEC-061

## Contesto

SPEC-061 rende le proiezioni GDS specifiche per
`tenant_id`/`repo`/`acl_group` e conserva la stessa provenance sugli score.
Questo non basta quando la topologia cambia dopo la proiezione: uno score di un
nodo invariato può incorporare un nodo appena revocato. Rimuovere eager tutti i
risultati della partizione a ogni evento risolve lo stale read, ma visita e
blocca l'intera partizione per ogni nodo ingerito, con costo quadratico sui
grafi da milioni di nodi previsti dall'ADD. Inoltre rimane una race: un job GDS
può proiettare la generation precedente e riscriverla dopo l'invalidazione.

Il controllo deve essere deterministico, O(1) per mutazione, fail-closed e non
deve derivare autorità da prompt, LLM o input caller.

## Opzioni considerate

1. **Rimozione eager delle proprietà su ogni nodo della partizione.** Semplice,
   ma produce scansioni/write-lock non bounded per evento e non fissa la race
   con un write-back già in corso. Scartata.
2. **Timestamp o TTL sugli score.** Dipende dal clock, non dimostra quale
   topologia è stata analizzata e può accettare dati stale entro la finestra.
   Scartata.
3. **Generation replicata su tutti i `CodeNode`.** Il confronto è possibile ma
   incrementarla richiede ancora una riscrittura dell'intera partizione.
   Scartata.
4. **Nodo metadata per partizione con generation monotona e fencing al
   write-back.** Una mutazione incrementa un solo contatore; score e consumer
   confrontano la generation. È l'opzione scelta.

## Decisione

Ogni partizione ACL ha esattamente un nodo interno:

```cypher
(:GDSPartition {
  tenant_id: String,
  repo: String,
  acl_group: String,
  generation: Integer,
  write_lock: Integer
})
```

Il contratto D3 aggiunge un vincolo di unicità composito su
`(tenant_id, repo, acl_group)`. Il nodo è metadata control-plane: non è un
`CodeNode`, non entra in traversal, full-text, vector index o proiezioni GDS e
non è restituibile dalle API.

Ogni materializzazione `CodeNode` incrementa in O(1) la generation della
partizione nuova e, quando diversa, di quella precedente. Ogni mutazione di
relazione incrementa una volta ciascuna partizione degli endpoint. Gli update
avvengono nello stesso Cypher della mutazione, quindi non esiste una finestra
tra cambio dati e invalidazione. Placeholder senza security label non creano
partizioni.

Tutte le mutation seguono un ordine globale di lock: `CodeNode` in ordine
lessicografico di `id`, poi `GDSPartition` in ordine lessicografico di
`tenant_id`/`repo`/`acl_group`. Il sink acquisisce il write lock dei nodi in un
primo statement e legge gli scope soltanto in un secondo statement della
stessa explicit transaction. In questo modo un update ownership concorrente
non può lasciare al consumer uno snapshot pre-lock. Anche gli endpoint di una
relazione sono bloccati e poi riletti prima di derivare le partizioni. Le
partizioni distinte sono ordinate prima dei `MERGE`, così move opposti non
acquisiscono gli stessi lock in ordine inverso. Il driver ritenta bounded le
transazioni su errori transient Neo4j.

Il job GDS cattura la generation prima di discovery/proiezione. Al write-back
blocca prima tutti i `CodeNode` target in ordine di `id`, quindi acquisisce il
write lock sullo stesso `GDSPartition` incrementando `write_lock` e verifica
che la generation sia ancora identica. Questo rispetta lo stesso ordine
globale del sink ed evita deadlock nodo/partizione. Solo allora, nella stessa
transazione, scrive tutti gli score con `impact_generation` uguale alla
generation catturata. Se la generation è cambiata, un target manca o anche un
solo target non appartiene più allo scope, non scrive alcuno score e restituisce
un errore stale esplicito. La serializzazione produce due soli ordini possibili:

- write-back prima della mutazione: gli score vengono scritti, poi la mutazione
  incrementa la generation e li rende immediatamente invisibili;
- mutazione prima del write-back: il confronto generation fallisce e nessuno
  score viene scritto.

Il reranker usa uno score solo quando scope/provenance coincidono e
`CodeNode.impact_generation == GDSPartition.generation`. Proprietà fisicamente
stale possono restare fino a un nuovo calcolo o manutenzione, ma sono
deterministicamente invisibili e non richiedono scansioni hot-path.

## Sicurezza, failure e osservabilità

Il sink deriva le partizioni esclusivamente dalle label persistite prodotte
dalla pipeline autenticata; nessun testo utente/LLM costruisce generation o
scope. Nodo metadata assente, generation assente/non numerica, mismatch,
write-back parziale, lock/store failure e score legacy senza
`impact_generation` falliscono chiusi (`impact_score=0` al consumo o errore del
job). Il job logga solo outcome/conteggi, mai tenant/repo/ACL/node id. La
generation non è una metrica label né un token di autorizzazione: è un fence di
coerenza aggiuntivo alla verifica ACL.

## Compatibilità, migrazione e rollback

La modifica è additiva per il contratto Cypher e non cambia proto/API. Il DDL
idempotente crea il vincolo; le partizioni vengono create lazy dal sink o dal
primo job GDS con generation `0`. Score legacy privi di generation diventano
invisibili, come richiesto dal fail-closed, e vengono sostituiti dal successivo
run GDS. Non serve GPU né riscrittura iniziale del grafo.

Il rollback del codice può ignorare/rimuovere i nodi `GDSPartition` soltanto
dopo aver disabilitato il consumo degli score; tornare alla sola provenance o
all'invalidazione eager non è un rollback sicuro. Il vincolo additivo può
restare senza effetti. Nessuna modifica ADD è necessaria: la decisione applica
la prescrizione esistente di GDS per-ACL e revoca fail-closed.

## Review avversariale

Il pass avversariale ha verificato ownership change sovrapposti, endpoint che
cambiano scope durante una relation mutation, move fra partizioni in direzioni
opposte, edge e write-back concorrenti, partizione mancante, target spostato,
duplicazione concorrente, scope forgiato, datastore failure e costo su milioni
di nodi. Il vincolo composito impedisce due contatori per lo stesso scope;
l'ordine totale dei lock e la rilettura post-lock chiudono snapshot race e
deadlock; il confronto al consumo conserva defense in depth. Non vengono
introdotti traversal unbounded, write a viste da input LLM, default tenant,
fail-open, cambi di deadline/idempotenza o mescolanza fra garanzie
deterministiche e metriche probabilistiche.
