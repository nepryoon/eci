# ADR-0006 — Resolver su misura invece di stack-graphs per la risoluzione cross-file JavaScript
Stato: accettata · Data: 2026-08-09

## Contesto
L'ADD prescrive stack-graphs come meccanismo di name resolution
incrementale primario (Modulo 1 §1.4). Verificato con ricerca diretta (non
presunto): nessun supporto Go esistente nel progetto stack-graphs; le
regole TypeScript/JavaScript esistono ma sono descritte da un praticante
con esperienza diretta come "6.000 righe di DSL", il progetto come
"appassito sulla vite" da quando GitHub ha dismesso Precise Code Nav; un
issue aperto sul repository ufficiale mostra risoluzione cross-modulo non
funzionante per Python (il linguaggio meglio supportato dal progetto); un
secondo issue mostra un manutentore descrivere un problema di correttezza
("un riferimento che si risolve diversamente a seconda del contesto")
come strutturalmente non risolvibile con l'approccio attuale.

Il codice reale da analizzare in questo progetto è prevalentemente
C/C++/C#/Java/JavaScript/TypeScript — Go è solo il linguaggio-ponte della
pipeline di ingestion, non un target di analisi rappresentativo.

## Decisione
Per JavaScript/TypeScript, i cui moduli sono espliciti e basati su
percorso file (a differenza della visibilità implicita di Go, dove
stack-graphs avrebbe un ruolo più giustificato), si costruisce un resolver
su misura (SPEC-025, `services/ingestion/src/imports.rs` +
`services/ingestion/src/resolve.rs`) invece di adottare stack-graphs anche
per questa coppia di linguaggi. Un resolver su misura, per lo scope
dichiarato (named import con percorso relativo, nessuna verifica di
`export`, nessun `node_modules`), è più semplice da costruire
correttamente, capire per intero, e verificare con certezza — rispetto a
dipendere dal comportamento, a tratti documentato come inconsistente, di
una libreria esterna il cui stato di manutenzione è incerto.

## Conseguenze
Nessun supporto stack-graphs in questa fase del progetto: la risoluzione
cross-file JavaScript introdotta da SPEC-025 è un meccanismo interamente
proprietario di `services/ingestion`, non una configurazione di
stack-graphs. Lo scope è deliberatamente più ristretto di quanto
stack-graphs offrirebbe in teoria (nessun default/namespace import,
nessuna verifica di visibilità, nessuna risoluzione `node_modules`) — un
limite noto e dichiarato, non una parità di funzionalità mancata per
trascuratezza. Se in una fase successiva servisse una risoluzione più
completa (o per un linguaggio dove stack-graphs è meglio supportato),
questa decisione va rivalutata caso per caso, non estesa per default.
