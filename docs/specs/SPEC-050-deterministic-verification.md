# SPEC-050 — Verification deterministica dei claim (T5.4)
Stato: verified
Task-tree: T5.4 · Servizio: services/verification · ADD: Modulo 2 §3.1-3.4, Modulo 3 §1.2
Contratti: `contracts/proto/eci/retrieval/v1/retrieval.proto` (CodeNode, CodeRelation e Provenance; invariato)

## 1. Obiettivo
Implementare il gate stateless che verifica una risposta già estratta in claim atomici contro ground truth deterministica e ri-parsa ogni snippet con Tree-sitter. Il risultato classifica errori e decide approvazione, correzione annotata o rigenerazione/degrado entro un budget bounded.

## 2. Interfaccia
```python
class EvidenceStore(Protocol):
    def symbol(self, node_id: str) -> SymbolEvidence | None: ...
    def relation_exists(self, source_id: str, target_id: str, edge_type: str, max_depth: int) -> bool: ...

class SyntaxVerifier(Protocol):
    def is_valid(self, language: str, source: str) -> bool: ...

class Verifier:
    def verify(self, candidate: CandidateAnswer, *, attempt: int = 0) -> VerificationResult: ...
```
`CandidateAnswer` contiene `symbol_claims`, `relation_claims`, `citations` e `snippets` strutturati. Nessun parsing probabilistico di testo libero avviene nel gate.

## 3. Comportamento
1. Given claim e prove coerenti, when `verify`, then outcome `approved` e nessun issue.
2. Given simbolo assente, then issue `symbol-hallucination` e outcome `regenerated` con feedback deterministico.
3. Given relazione assente, then issue `relation-nonexistent`; la profondità richiesta è bounded dal massimo configurato.
4. Given citation stale ma simbolo esistente, then citation corretta dalla provenance corrente, annotazione presente e outcome `corrected`.
5. Given snippet Tree-sitter invalido, then issue `syntax-invalid` e outcome `regenerated`.
6. Given più errori, then tutti gli stadi sono eseguiti e gli issue hanno ordine stabile per stadio e input.
7. Given `attempt < max_regenerations`, errore sostanziale richiede rigenerazione; esaurito il budget, outcome `degraded` senza approvare claim non verificati.
8. Given stesso input e store, due verifiche producono un risultato identico e non mutano il candidato.

## 4. Errori & edge case
| Condizione | Comportamento |
|---|---|
| ID vuoto, righe invalide, snippet/language vuoti | validazione Pydantic fallisce prima dei check |
| Evidence store fallisce | `VerificationBackendError`, mai approval permissivo |
| linguaggio Tree-sitter non supportato | issue `syntax-invalid` con dettaglio `unsupported-language` |
| profondità relazione oltre limite | clamp al limite configurato |
| `max_regenerations` fuori 2-3 | configurazione rifiutata |

## 5. Non-goals
Estrazione LLM da prosa, API gRPC nuova, ACL (T6.5), SCA/Semgrep opzionale, correzione di simboli o relazioni, integrazione nell'orchestrator.

## 6. Vincoli dall'ADD
Gate solo deterministico; graph/provenance/parser sono gli oracoli. `stale-citation` è correggibile; symbol/relation/syntax richiedono rigenerazione. Loop massimo 2-3; servizio Python stateless; budget operativo 4s. Il gate non scrive alcuno store.

## 7. Test plan
Unit test con store in-memory deterministico e parser Tree-sitter reale per scenari 1-8. Test di errore backend, linguaggio ignoto, clamp della profondità e modelli invalidi. Nessuno stub di Tree-sitter.

## 8. Osservabilità
Span `verification.verify` con attributi `verification.outcome`, `verification.attempt`, `verification.issue_count`; eventi `verification.issue` con solo classe/stadio (mai source o testo risposta).

## 9. Criteri di accettazione
- [x] Scenari 1-8 verdi con Tree-sitter reale.
- [x] `task build`, `task lint`, `task test`, `task guard` verdi.
- [x] `git diff -- docs/add contracts` vuoto.

## 10. Deviazioni
1. Questa SPEC espone un core Python in-process e un `EvidenceStore` tipizzato,
   non un endpoint gRPC: il repository non contiene un contratto Verification e
   crearne uno senza ADR violerebbe la gerarchia delle fonti. L'adapter di rete e
   il wiring nell'orchestrator richiedono una SPEC contrattuale successiva.
2. L'estrazione dei claim è l'input strutturato del gate, non una seconda chiamata
   LLM: il gate resta quindi interamente deterministico come prescritto dall'ADD.
3. Lo stadio SCA opzionale e l'ACL delle citazioni restano rispettivamente non-goal
   e T6.5. La sintassi usa grammatiche reali di `tree-sitter-language-pack`; un
   linguaggio non disponibile fallisce chiuso come `syntax-invalid`.
