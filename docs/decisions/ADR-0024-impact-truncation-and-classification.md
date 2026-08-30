# ADR-0024 — Semantica completa e distinguibile di ImpactAnalysis

Stato: accepted
Data: 2026-08-30
Decisioni collegate: D4, D7, T4.2 / SPEC-042, ADR-0008

## Contesto

ADR-0008 ha riusato il contratto pubblico D7 per consegnare la prima
implementazione bounded e streaming di `ImpactAnalysis`, rinviando
esplicitamente `edge_types`, `direction`, `fanout_cap_per_hop`,
`min_impact_score`, `include_source_text`, il filtro multi-repository, la
catena completa del percorso e `ImpactKind`. Il runtime corrente accetta
quindi controlli pubblici senza applicarli. Inoltre mappa il cap totale
`max_nodes` su `truncated_by_fanout_cap`, sebbene il contratto e l'ADD
distinguano il cap totale da quello per-hop. Questa ambiguita' impedisce al
client di sapere quale bound ha reso incompleto il risultato.

L'ADD Modulo 2 richiede reverse reachability tipizzata, top-N per nodo a ogni
hop ordinato per priorita', score GDS, percorsi spiegabili e classificazione
`syntactic`/`behavioral`/`module-boundary`. Il contratto non trasporta una
descrizione del cambiamento proposto o l'esito del type-checker: la RPC puo'
classificare il meccanismo di propagazione osservato, non dichiarare se una
specifica modifica rompera' effettivamente la compilazione.

## Decisione

`ImpactAnalysis` applica tutti i controlli gia' pubblicati e mantiene tre
bounds indipendenti:

- `max_nodes` limita il totale emesso sull'intera traversata;
- `fanout_cap_per_hop` limita, separatamente per ogni nodo della frontiera,
  i suoi vicini autorizzati, ordinati per `impact_score DESC, node_id ASC`;
- `max_depth` limita i livelli BFS. Alla profondita' finale viene eseguito un
  probe bounded con gli stessi filtri per distinguere una frontiera terminale
  da una realmente troncata.

Si aggiunge in modo backward-compatible
`ImpactProgress.truncated_by_node_cap = 6`. I campi esistenti mantengono il
significato letterale: `truncated_by_fanout_cap` e' true solo se almeno un
nodo della frontiera aveva piu' vicini ammessi del cap per-node;
`truncated_by_depth` e' true solo se esiste almeno un vicino ammesso non
visitato oltre `max_depth`; il nuovo campo indica esclusivamente
`max_nodes`. Un consumer precedente ignora il nuovo tag senza cambiare
comportamento wire.

Una lista `edge_types` vuota usa i sei tipi D7
`CALLS|IMPLEMENTS|EXTENDS|OVERRIDES|DEPENDS_ON|IMPORTS`; una lista esplicita
accetta ogni valore `EdgeType` noto e rifiuta `UNSPECIFIED` o valori ignoti.
`direction=UNSPECIFIED` significa `REVERSE`. I filtri repository del body
restringono ulteriormente, senza mai ampliare, lo scope autenticato derivato
dai metadata. `min_impact_score` filtra prima dell'espansione. Il percorso
scelto e' il primo percorso minimo nell'ordine deterministico sopra e porta
tutti i tipi di arco.

`ImpactKind` classifica il meccanismo del percorso minimo con precedenza
conservativa:

1. `IMPORTS` o `DEPENDS_ON` => `MODULE_BOUNDARY`;
2. `CALLS`, `IMPLEMENTS`, `EXTENDS` o `OVERRIDES` => `BEHAVIORAL`;
3. le altre relazioni strutturali pubbliche => `SYNTACTIC`.

Questa classificazione non sostituisce il type-checker e non afferma che una
modifica ipotetica abbia cambiato firma. La Verification Layer resta il gate
deterministico finale.

`include_source_text=true` idrata a batch ogni livello dalla vista OpenSearch
con gli stessi filtri tenant/repository/ACL derivati dal contesto autenticato.
Il flag false non interroga OpenSearch; una dipendenza richiesta ma assente o
fallita produce errore esplicito e fail-closed.

## Sicurezza e limiti

Tipi di relazione sono interpolati in Cypher soltanto dopo mapping da enum a
una whitelist statica. Scope, repository, score, ID e bounds sono parametri.
Sono imposti massimi server-side a profondita', cap totale, fan-out e numero
di repository, oltre a validazione finita dello score, per impedire richieste
che eludano i bounds usando i massimi numerici protobuf. Ogni nodo attraversato
e ogni idratazione applicano tenant, repository e ACL prima di restituire
dati. Cancellazione e deadline del contesto gRPC restano propagate a Neo4j,
OpenSearch e invio stream.

## Conseguenze

- Il contratto cambia additivamente; binding Go e Python devono essere
  rigenerati e verificati con Buf e clean diff.
- I client possono distinguere senza euristiche i tre motivi di troncamento.
- La query esegue al massimo una espansione per livello e un probe finale;
  source text, se richiesto, usa una query batch per livello, non N query.
- La semantica rinviata da ADR-0008 e' superseded da questa decisione; la
  forma wire e la compatibilita' di ADR-0008 restano valide.

## Rollback

Il server puo' tornare alla logica precedente lasciando il tag 6 inutilizzato;
non esistono mutazioni canoniche o dati da migrare. Il campo pubblico non va
rimosso dopo la pubblicazione: un rollback del runtime deve continuare a
emetterlo col valore di default `false`.

## Review avversariale

La decisione non rende il body fonte di autorita', non introduce traversate
illimitate e non accetta frammenti Cypher dal chiamante. I probe usano gli
stessi filtri e bounds della traversata, quindi non diventano un canale di
esistenza cross-tenant. I tre segnali di troncamento non sono intercambiabili;
nessun esito incompleto e' presentato come completo. Source text passa dalla
vista autorizzata gia' usata da HybridSearch e non viene loggato o aggiunto a
telemetria.
