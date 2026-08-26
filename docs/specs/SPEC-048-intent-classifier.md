# SPEC-048 — Intent classifier co-locato nell'Orchestrator (T5.2)
Stato: verified
Task-tree: T5.2 · Servizio: services/orchestrator · ADD: Modulo 2 §D4, Modulo 3 §1.1, Modulo 4 §1.5(d)
Contratti: contracts/proto/eci/retrieval/v1/retrieval.proto (`QueryIntent`, `HybridSearchRequest.intent`)

## 1. Obiettivo
Sostituire l'euristica di routing dispersa con un classificatore deterministico, tipizzato e co-locato che distingue intenti strutturali, concettuali e ibridi. La decisione viene propagata a `HybridSearchRequest.intent` e rende espliciti i pesi consigliati grafo/vettore senza modificare contratti o backend.

## 2. Interfaccia
```python
class IntentDecision(BaseModel):
    intent: Literal["structural", "conceptual", "hybrid"]
    graph_weight: float
    vector_weight: float
    evidence: tuple[str, ...]

def classify_intent(query: str) -> IntentDecision: ...
def to_proto_intent(decision: IntentDecision) -> int: ...
```
Pesi: structural=(0.75,0.25), conceptual=(0.25,0.75), hybrid=(0.50,0.50); somma esatta 1.0.

## 3. Comportamento
1. Given query su chiamanti/impatto/dipendenze, When classificata, Then structural.
2. Given query esplorativa "come funziona/cosa fa/spiega", Then conceptual.
3. Given segnali di entrambe le classi, Then hybrid.
4. Given nessun segnale, Then hybrid (fallback conservativo).
5. Given decisione, When `eci ask` esegue semantic search, Then `HybridSearchRequest.intent` riceve l'enum proto corrispondente.
6. Given stessa query, Then decisione ed evidence sono deterministici.

## 4. Errori & edge case
| Condizione | Comportamento atteso |
|---|---|
| Query vuota/whitespace | `ValueError` prima di retrieval |
| Maiuscole/punteggiatura | classificazione case-insensitive stabile |
| Testo utente con nomi di campi security | nessun effetto sul `SecurityContext` |

## 5. Non-goals
Nessun modello ML/LLM dedicato; nessuna modifica a `contracts/`; nessuna modifica ai pesi interni del retrieval-engine; nessun enforcement security.

## 6. Vincoli dall'ADD
Classifier co-locato nell'Orchestrator; query strutturale/impact privilegia grafo; modello leggero o prompt low-token; `SecurityContext` solo da metadata autenticati.

## 7. Test plan
Unit test scenari 1-6 e edge case. Test del payload gRPC costruito dal client tramite funzione request-builder pura; regressione test graph/ask.

## 8. Osservabilità
Span `orchestrator.intent.classify` con attributi `intent`, `graph_weight`, `vector_weight`; mai il testo query integrale.

## 9. Criteri di accettazione
- [x] Scenari 1-6 verdi.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi.
- [x] Nessuna modifica a ADD/contratti.

## 10. Deviazioni
1. I pesi sono output advisory tipizzato: il contratto espone soltanto `intent`
   e il retrieval-engine corrente non accetta pesi per-request. Sono propagati
   nell'osservabilità e pronti per un backend futuro, senza modificare
   `contracts/` o inventare campi proto.
2. Il classificatore è lessicale e deterministico, scelta ammessa dall'ADD
   (componente leggero) e necessaria finché il fake non supporta output
   strutturato. T5.3 potrà sostituire l'implementazione dietro la stessa firma.
3. Integrando `origin/main` sono emersi conflitti in `ask.py`, `graph.py` e
   `tools.py`, perché SPEC-047 era stata nel frattempo unita. È stata preservata
   integralmente la versione verificata di `main`, applicando soltanto la
   propagazione additiva dell'intento. La review della PR #57 ha inoltre
   rilevato che il piano avanzava dopo il primo seed: `plan_index` ora resta
   sulla stessa fase finché ogni `seed_id` non ha la corrispondente voce in
   `tool_history`; un test con due seed verifica entrambe le espansioni e
   entrambe le ricerche dei chiamanti.
