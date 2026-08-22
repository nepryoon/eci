# SPEC-046 — Context packing "a U" (T4.5, ultimo task di Fase 4)
Stato: implemented
Task-tree: T4.5 (dip. T4.4, già chiuso) · Servizio: services/retrieval-engine (Go, estende T4.1/T4.4/SPEC-045) · ADD: Modulo 2 §2.4

## 1. Obiettivo
Impacchettare i risultati riordinati da T4.4 (`rerank.RankedNode`) in un contesto a budget di token limitato, organizzato in quattro sezioni con quote dedicate (Definizioni/Chiamanti-relazioni/Summary gerarchici/Sorgente integrale), deduplicato, con citazione di provenance obbligatoria per ogni frammento, ordinato "a U" (punteggio più alto alle due estremità, contesto secondario al centro) per mitigare il lost-in-the-middle (Liu et al. 2024, TACL). Ultimo task di Fase 4 — con questa, la fase è completa.

## 2. Interfaccia

**Package** `services/retrieval-engine/internal/packing` (nuovo, sorella di `rerank`/`hybridsearch`/`impactanalysis` — nessuna nuova infrastruttura esterna, opera interamente su dati già idratati da T4.1/T4.4/SPEC-045):
```go
func Pack(ranked []rerank.RankedNode, budget TokenBudget) PackedContext

type TokenBudget struct {
    Total                              int // default dichiarato: 8000
    DefinitionsFraction                float64 // default 0.4
    RelationsFraction                  float64 // default 0.2
    HierarchicalSummariesFraction      float64 // default 0.2 (riservata, mai consumata nella pratica — §5)
    FullSourceFraction                 float64 // default 0.2
    FullSourceTopK                     int     // default 3-5, dichiarato 4
    FullSourceImpactScoreThreshold     float64 // default dichiarato 0.5
}
```

**Conteggio token**: approssimazione `len(text)/4` (nessun vero tokenizzatore — il modello LLM non è ancora scelto, territorio Fase 5; dichiarato esplicitamente, non presentato come preciso).

**Sezione 1, Definizioni (quota alta, SEMPRE incluse per tutti i candidati deduplicati)**: `Name` + prima riga di `SourceText` come proxy di firma (nessuna estrazione di firma isolata esiste nella pipeline — stessa classe di gap già dichiarata in T4.3 per il cambio di firma).

**Sezione 2, Chiamanti/relazioni (quota media)**: rappresentazione compatta di `EdgeType`/`HopDistance` per candidato (`"<name> (<edge_type>, hop <n>)"`).

**Sezione 3, Summary gerarchici (quota RISERVATA ma strutturalmente vuota — Non-goal dichiarato)**: nessun meccanismo di riassunto a livello module/repo esiste nella pipeline oggi. La quota è riservata nel budget (coerente con la struttura a quattro sezioni di §2.4) ma il contenuto resta sempre vuoto — plumbing non enforcement, stesso principio già stabilito ripetutamente nel progetto.

**Sezione 4, Sorgente integrale (top-k, default k=4)**: criteri (a) e (c) dell'ADD implementati deterministicamente — (a) il nodo è il target diretto (`HopDistance==0`, se presente) O un dipendente con `ImpactScoreNorm` sopra soglia (`FullSourceImpactScoreThreshold`); (c) budget residuo nella quota dedicata. Criterio (b) dell'ADD ("la query richiede verifica a livello di riga/statement") dichiarato Non-goal esplicito — richiederebbe comprensione dell'intento della query in linguaggio naturale, infrastruttura assente da questa pipeline prima di Fase 5.

**Deduplicazione**: per `NodeID` (rete di sicurezza economica — la fusione RRF di T4.1 dovrebbe già garantirla prima che i candidati arrivino qui). Deduplicazione per embedding quasi-duplicato dichiarata Non-goal esplicito — richiederebbe il vettore grezzo di ciascun candidato per un confronto a coppie, mai esposto da `RetrievedNode` (che porta solo il punteggio di similarità con la query, non il vettore stesso).

**Citazione di provenance**: `repo/path:start_line-end_line@commit_sha`, costruita da `RetrievedNode.Provenance` quando presente. Quando assente (limite noto dichiarato in SPEC-041 §10 — sempre `nil` sui risultati della sola gamba vettoriale con i dati reali di `sink-vector`), il frammento è comunque incluso ma SENZA citazione fabbricata — un placeholder esplicito (`"provenance non disponibile"`), mai un valore inventato.

**Ordinamento "a U"**: candidati ordinati per `FinalScore` decrescente, poi interfogliati alternando fronte/retro della sequenza finale (il migliore va in prima posizione, il secondo in ultima, il terzo in seconda posizione, il quarto in penultima, ...) — punteggio più alto alle due estremità, punteggio più basso al centro.

## 3. Comportamento (scenari)

1. **Dato** un insieme di candidati con budget totale fisso, **quando** eseguo `Pack`, **allora** ciascuna delle quattro sezioni rispetta la propria quota (nessuna eccede la propria frazione del budget totale, entro l'approssimazione char/4 dichiarata).
2. **Dato** un candidato con `HopDistance==0` (il nodo di ingresso stesso), **quando** eseguo `Pack`, **allora** il suo `SourceText` è incluso per intero nella sezione Sorgente integrale (criterio (a), prima condizione).
3. **Dato** un candidato con `ImpactScoreNorm` sopra soglia ma budget della sezione Sorgente integrale esaurito, **quando** eseguo `Pack`, **allora** quel candidato NON riceve sorgente integrale (criterio (c) rispettato, non ignorato).
4. **Dato** un candidato con `Provenance` assente (`nil`), **quando** eseguo `Pack`, **allora** il frammento è comunque incluso, con un placeholder esplicito al posto della citazione, non un valore fabbricato.
5. **Dato** un insieme di N candidati con `FinalScore` distinti, **quando** ispeziono l'ordine finale del packing, **allora** il primo e l'ultimo elemento hanno i due `FinalScore` più alti, il centro ha i più bassi (verifica diretta dell'ordinamento "a U", non solo che tutti i candidati siano presenti).
6. **Dato** due candidati con lo stesso `NodeID` (non dovrebbe accadere dopo T4.1, ma verificato comunque come rete di sicurezza), **quando** eseguo `Pack`, **allora** solo uno dei due appare nel risultato finale.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Candidate set vuoto | `PackedContext` con tutte le sezioni vuote, nessun errore |
| Budget totale troppo piccolo per includere anche un solo candidato nella sezione Definizioni | Il candidato con `FinalScore` più alto è comunque incluso, anche se eccede leggermente la quota (mai una risposta completamente vuota per un budget troppo stretto — un frammento parziale è meglio di nessun frammento) |

## 5. Non-goals
Criterio (b) dell'ADD per sorgente integrale (comprensione dell'intento della query, §2). Deduplicazione per embedding quasi-duplicato (§2). Contenuto reale della sezione Summary gerarchici (§2, quota riservata ma vuota). Vero conteggio token specifico per un modello (approssimazione char/4 dichiarata, §2). Nessuna integrazione con l'agente/LLM reale (Fase 5, fuori scope — questa SPEC produce il contesto impacchettato, non lo consuma).

## 6. Vincoli dall'ADD
Modulo 2 §2.4 — quattro sezioni con quote, criterio sorgente-vs-summary (parzialmente implementato, §2), deduplicazione (parzialmente implementata, §2), provenance obbligatoria, ordinamento "a U" (Liu et al. 2024, TACL).

## 7. Test plan
Unitari puri (nessuna infrastruttura reale necessaria — `Pack` opera su `[]rerank.RankedNode` costruiti a mano, stesso principio già stabilito per `rerankCore`/T4.4).

## 8. Osservabilità
Nessun requisito nuovo.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verificati con evidenza diretta, in particolare lo scenario 5 (l'ordinamento "a U" verificato posizione per posizione, non solo "tutti presenti"): `internal/packing/pack_test.go`, `TestPack_OrderIsUShaped` — 6 candidati a punteggio distinto, verificato `Order[i]` esattamente contro la sequenza attesa `[n0,n2,n4,n5,n3,n1]`, non solo l'insieme. Scenari 1-4/6 e i due edge case altrettanto diretti (vedi test dedicati, TDD: scritti e verificati falliti — pacchetto inesistente — prima dell'implementazione).
- [x] Edge case tabella §4 verificati esplicitamente: candidate set vuoto (`TestPack_EmptyCandidateSet`); budget totale troppo piccolo per la sezione Definizioni (`TestPack_TinyBudgetStillIncludesTopCandidateInDefinitions` — il candidato a punteggio più alto resta incluso anche a quota-Definizioni nominale zero).
- [x] Nessuna regressione sui test esistenti di T4.1/T4.2/T4.4/SPEC-045: intera suite `services/retrieval-engine` (unitari + integrazione, `-p 1`) rieseguita verde insieme ai nuovi test — `packing` non modifica nessun file esistente (nuovo package puro), quindi nessuna sorpresa attesa, verificato comunque per esplicita richiesta.
- [x] Con questa SPEC chiusa, Fase 4 è dichiarata completa per intero.

## 10. Deviazioni rispetto alla SPEC

1. **Sezione 2 (Chiamanti/relazioni): formato `"<name> (hop <n>)"`, SENZA
   `<edge_type>`** — nessuna SPEC precedente ha mai aggiunto un campo
   `EdgeType` a `hybridsearch.RetrievedNode`/`rerank.RankedNode` (verificato
   con `grep` su tutto il pacchetto prima di scrivere codice, non presunto):
   `GraphTraversal` (T4.1) usa un'unica query Cypher a lunghezza variabile
   (`*1..maxDepth`), non un BFS livello-per-livello come
   `impactanalysis`/T4.2 — l'UNICO punto della pipeline dove un "tipo
   d'arco dell'ultimo hop" è mai stato tracciato, ma è un type
   (`impactanalysis.ImpactNode`) completamente distinto da
   `RetrievedNode`, mai collegato. Estendere `GraphTraversal` per tracciare
   l'edge type avrebbe richiesto riscriverlo come BFS livello-per-livello
   (una modifica strutturale a T4.1, esplicitamente fuori scope — §5 non
   lo vieta testualmente ma nessuno scenario lo richiede, e la SPEC stessa
   in altri punti vieta modifiche a T4.1/T4.2/T4.4). Stesso principio già
   applicato in SPEC-044 §10 (testo del candidato limitato a `node_id` per
   assenza di un campo — qui: componente del formato omessa per lo stesso
   motivo, mai un valore fabbricato).

2. **`PackedContext.Order []string`, campo non elencato esplicitamente
   nell'interfaccia dichiarata da SPEC-046 §2** (che specifica solo la
   firma di `Pack` e i campi di `TokenBudget`, non la forma completa di
   `PackedContext`): necessario per rendere lo scenario 5 ("ispeziono
   l'ordine finale del packing") verificabile direttamente — la sequenza
   "a U" dell'INTERO candidate set deduplicato, indipendente da quali
   sezioni un candidato è riuscito a raggiungere sotto vincolo di budget.
   Ogni sezione (`Definitions`/`Relations`/`FullSource`) presenta i propri
   `Fragment` nello stesso ordine relativo di `Order`.

3. **Budget scarso: priorità di ammissione per `FinalScore` decrescente,
   presentazione finale per posizione "a U"** — due concetti distinti,
   deliberatamente disaccoppiati: CHI riceve il budget scarso (sezioni
   Definizioni/Relazioni/Sorgente integrale) segue sempre l'ordine di
   punteggio (il più alto ha sempre priorità), MAI l'ordine "a U" — che
   descrive solo l'arrangiamento di PRESENTAZIONE finale (mitigazione
   lost-in-the-middle), un problema ortogonale a "chi entra nel budget".
   Non esplicitato in questi termini da SPEC-046 §2, ma l'unica lettura
   che rende scenario 3 (priorità di budget) e scenario 5 (ordine di
   presentazione) coerenti insieme.

4. **Strategia di troncamento greedy: arresto al primo candidato che non
   entra (mai "salta e prova il prossimo più piccolo")**, identica per
   tutte e tre le sezioni budget-vincolate (Definizioni/Relazioni/Sorgente
   integrale) — stesso principio di troncamento rigido già usato altrove
   nel progetto (`top_k` in T4.1/T4.4: mai un bin-packing ottimizzante).
   L'eccezione "sempre almeno il primo" (edge case §4 riga 2) è applicata
   SOLO a Definizioni, il suo scope letterale nella tabella — Relazioni e
   Sorgente integrale possono restare vuote se il budget è troppo stretto
   fin dal primo candidato (nessuno scenario lo vieta).

5. **`FullSourceTopK` applicato come cap sui candidati ELEGGIBILI
   (criterio (a)) in ordine di priorità `FinalScore`, PRIMA di applicare
   il budget (criterio (c))** — SPEC-046 §2 elenca (a) e (c) come criteri
   distinti senza specificarne l'ordine di applicazione; applicarli in
   quest'ordine (prima il filtro di eleggibilità+cap count, poi il
   budget) è la lettura più naturale del testo ("(a) ... O ... ; (c)
   budget residuo").

6. **Separatore `": "` tra `Name` e la prima riga di `SourceText`
   (Definizioni) e formato `"%s (hop %d)"` (Relazioni)**: nessun
   separatore/formato esplicito dichiarato da SPEC-046 §2 oltre
   all'esempio per Relazioni (che include comunque `edge_type`, omesso
   qui per la deviazione 1) — scelte ragionevoli, non desunte da
   nient'altro nel progetto.

7. **Il conteggio token di un `Fragment` include SIA `Text` SIA
   `Citation`** (`tokenCount(Text) + tokenCount(Citation)`): la citazione
   di provenance è parte di ciò che verrebbe effettivamente incluso nel
   prompt LLM (§1: "citazione di provenance obbligatoria per ogni
   frammento") — esclusa dal conteggio, il budget dichiarato sarebbe
   sistematicamente sottostimato rispetto a quanto genuinamente
   impacchettato.
