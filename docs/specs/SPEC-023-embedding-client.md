# SPEC-023 — Embedding: embedder-fake + client configurabile (T2.4)
Stato: verified
Task-tree: T2.4 (quarto task di Fase 2) · Nuovo: fakes/embedder-fake (Python, stesso pattern di fakes/vllm-fake) + services/ingestion (Rust, nuovo client) · ADD: Modulo 1 §1.6.3 (embedding_ref), Modulo 4 (tech stack)
Contratti: nessuno sotto contracts/; modifica dichiarata a `deploy/compose/docker-compose.yml` (nuovo profilo Docker Compose `gpu`, non parte di `task up` di default)

## 1. Obiettivo
Due parti, entrambe necessarie perché T2.4 sia genuinamente completabile senza dipendere da hardware GPU per i test automatici: (a) `embedder-fake`, un fake deterministico per gli embedding — **correzione dichiarata**: T0.9/SPEC-017 lo aveva rimandato a "Fase 4, nessun consumatore oggi", ma il testo di T2.4 ("client con fallback al fake") lo richiede già qui, un rimando prematuro non corretto nella sostanza; (b) un client Rust in `services/ingestion` verso l'API nativa di TEI (Text Embeddings Inference), configurabile per puntare al fake in sviluppo/test o al vero `jina-code-embeddings-1.5b` (1536 dimensioni di default, verificato — non improvvisato) quando una GPU è disponibile.

## 2. Interfaccia

**`embedder-fake`** (`fakes/embedder-fake/`, stesso scheletro Python/FastAPI di `fakes/vllm-fake`, SPEC-017): replica l'API **nativa** di TEI (non quella OpenAI-compatibile opzionale — è quella che un client puntato su un vero TEI userebbe di default):
```
POST /embed
{"inputs": "testo o lista di testi"}
→ [[float, float, ...]]  (un vettore per ogni input, stesso ordine)
```
**Determinismo**: `SHA-256(testo)` esteso (ri-hashing concatenato finché non si hanno abbastanza byte) a 1536 float normalizzati in `[-1, 1]` — stesso testo produce sempre lo stesso vettore, testi diversi producono vettori diversi. **1536 dimensioni fisse** (il default reale del modello — nessun supporto Matryoshka/troncamento nel fake: non è il suo scopo, il fake serve a testare "il client parla correttamente con qualcosa che risponde come TEI", non a replicare la flessibilità dimensionale del modello reale).

**Client Rust** (`services/ingestion/src/embedding.rs`, nuovo modulo):
```rust
pub struct EmbeddingClient { base_url: String }

pub fn embed(client: &EmbeddingClient, text: &str) -> Result<Vec<f32>, EmbeddingError>;
```
Chiama `POST {base_url}/embed` (stesso endpoint nativo TEI, identico contro fake o reale — nessuna differenza di codice, solo di `base_url` configurato). `base_url` da `eci_common::config::env_or_default("EMBEDDING_SERVICE_URL", "http://localhost:8002")` (porta dedicata, non in conflitto con `vllm-fake` :8001 né con nessun altro servizio già in ascolto).

**"Fallback al fake"**: non failover automatico a runtime (nessuna logica che rileva un'assenza di GPU e commuta da sola) — la stessa filosofia già stabilita per `vllm-fake` in T1.5/SPEC-018: un URL configurabile che punta al fake in sviluppo/CI, al vero TEI quando una GPU è disponibile. Il client stesso non sa né gli interessa quale dei due ci sia dietro `base_url` — stesso contratto HTTP in entrambi i casi, per costruzione.

**Profilo Docker Compose `gpu`** (`deploy/compose/docker-compose.yml`, modifica dichiarata): nuovo servizio `tei-embedding`, immagine `ghcr.io/huggingface/text-embeddings-inference:latest-cuda` (variante GPU ufficiale), `HF_MODEL_ID=jinaai/jina-code-embeddings-1.5b`, `profiles: ["gpu"]` (Docker Compose nativo — non avviato da `task up` di default, richiede `docker compose --profile gpu up` esplicito). **Non verificato con un avvio reale in questa SPEC** (nessuna GPU garantita disponibile sulla macchina di sviluppo al momento dell'implementazione — vedi §5 Non-goals): la configurazione è scritta e sintatticamente validata (`docker compose config`), non avviata end-to-end.

## 3. Comportamento (scenari)

1. **Dato** lo stesso testo inviato due volte a `embedder-fake`, **quando** confronto le due risposte, **allora** sono identiche byte per byte.
2. **Dato** due testi diversi, **quando** li invio a `embedder-fake`, **allora** i vettori risultanti sono diversi.
3. **Dato** una qualunque risposta di `embedder-fake`, **quando** ne controllo la lunghezza, **allora** è esattamente 1536.
4. **Dato** il client Rust puntato su `embedder-fake` reale (non mockato) in esecuzione, **quando** chiamo `embed("qualche testo")`, **allora** ricevo `Ok(vec)` con `vec.len() == 1536`, corrispondente esattamente a quanto `embedder-fake` avrebbe risposto per quello stesso testo (verificabile chiamando direttamente l'endpoint HTTP in parallelo nello stesso test e confrontando).
5. **Dato** il client Rust puntato su un indirizzo che non risponde, **quando** chiamo `embed(...)`, **allora** ricevo `Err(...)` esplicito, non un panic né un vettore vuoto silenzioso.
6. **Manuale, non automatizzato** (richiede GPU reale — vedi §5): il client Rust puntato sul vero `tei-embedding` (profilo `gpu` avviato a mano) restituisce un vettore di 1536 elementi per un testo di prova, confermando che il client parla correttamente anche con l'implementazione reale, non solo col fake.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Testo vuoto (`""`) inviato a `embedder-fake` | Nessun caso speciale — `SHA-256("")` è comunque un valore ben definito, produce un vettore deterministico valido come qualunque altro input |
| Risposta HTTP con status diverso da 200 (es. 500) dal servizio dietro `base_url` | Il client Rust ritorna `Err` esplicito col codice di stato incluso nel messaggio, non tenta di parsare un body di errore come se fosse un vettore |
| Risposta 200 ma JSON malformato o non un array di float | `Err` esplicito di deserializzazione, non un panic |

## 5. Non-goals
Nessun avvio reale del profilo `gpu` in questa SPEC (nessuna GPU garantita disponibile — la configurazione è scritta e validata sintatticamente, non eseguita end-to-end; se una GPU diventa disponibile in una sessione futura, verificare lo scenario 6 allora). Nessun wiring del client dentro `parse_file`/l'output di `services/ingestion` verso un consumatore reale — stesso principio già applicato in T2.2/T2.3 (si costruisce la capacità, si rimanda il collegamento a quando esisterà davvero chi la userà, presumibilmente quando la Semantic Cache di T2.3 avrà un primo consumatore). Nessun supporto Matryoshka/troncamento dimensionale nel fake. Nessun failover automatico runtime fake↔reale.

## 6. Vincoli dall'ADD
Modulo 1 §1.6.3: `embedding_ref`/`embedding_dim` come campi del value della Semantic Cache — questa SPEC produce il MECCANISMO che li popolerebbe (un client che restituisce un vettore e la sua dimensione), non ancora il collegamento a quei campi (T2.3 non ha consumatori, vedi Non-goals). Modulo 4: TEI + `jina-code-1.5b` come stack di embedding raccomandato — nome esatto e dimensione verificati (`jina-code-embeddings-1.5b`, 1536 dim di default), non presunti dal nome abbreviato del task-tree.

## 7. Test plan
`embedder-fake`: test HTTP diretti, stesso pattern di `fakes/vllm-fake` (SPEC-017 §7). Client Rust: test con `embedder-fake` reale avviato come processo (stesso pattern di sottoprocesso reale già usato in `services/orchestrator/conftest.py`, SPEC-018), non un mock HTTP — per lo scenario 4, verifica diretta di corrispondenza col fake reale, non un valore atteso hard-coded che potrebbe divergere silenziosamente dall'implementazione del fake.

## 8. Osservabilità
Nessun requisito nuovo oltre quanto già stabilito per `services/ingestion` (T1.1).

## 9. Criteri di accettazione
- [x] Scenari 1-5 verificati con evidenza diretta.
- [x] Scenario 6 esplicitamente segnalato come non eseguito se nessuna GPU è disponibile al momento dell'implementazione — non saltato in silenzio, dichiarato come tale nel report.
- [x] Edge case tabella §4 tutti verificati.
- [x] `docker compose config` (validazione sintattica, non avvio) confermato pulito sul nuovo profilo `gpu`.

## 10. Implementazione — deviazioni

**Scenario 6: NON ESEGUITO.** Nessuna GPU disponibile in questa sessione
(verificato esplicitamente: `nvidia-smi` assente dal PATH, nessun
`/dev/nvidia*`). Il profilo `gpu` è scritto e validato solo
sintatticamente (`docker compose config` e `docker compose --profile gpu
config`, entrambi puliti; `docker compose --profile gpu config --services`
conferma che `tei-embedding` compare SOLO col profilo esplicito, mai nel
set di default di `task up`). Nessun avvio reale, nessuna verifica contro
il vero `tei-embedding`. Da eseguire in una sessione futura con accesso a
GPU, come previsto esplicitamente da §5 Non-goals.

`fakes/embedder-fake`: stesso scheletro di `fakes/vllm-fake` (FastAPI,
venv proprio, non cablato in `task build`/`task lint`/`task test` —
stesso trattamento già riservato a `vllm-fake`, nessuno script di questi
è tra i file toccabili di questa SPEC). 10 test HTTP diretti (`TestClient`,
nessuna dipendenza esterna), verificati fail-first sostituendo
temporaneamente `_deterministic_vector` con un placeholder piatto (4/10
test failiscono correttamente, poi tutti passano con l'implementazione
reale). Un test aggiuntivo oltre agli scenari dichiarati
(`test_vector_matches_manual_sha256_extension_reference`) ricalcola
indipendentemente la stessa estensione SHA-256 descritta in §2 e la
confronta byte per byte con la risposta del fake — non solo "esce un
vettore deterministico", ma verifica la FORMULA esatta.

Client Rust in `services/ingestion/src/embedding.rs`:
`EmbeddingClient::new(base_url: String)` aggiunto oltre alla firma
letterale di §2 (che dichiara solo il campo `base_url: String` sul
struct, non un costruttore) — necessario per costruire il valore, non
una reinterpretazione del contratto. `embed` usa `ureq` (blocking, coerente
con lo stile sincrono già esistente in `services/ingestion`, che non ha
un runtime async: `postgres::Client` è a sua volta sincrono). Test unitari
per lo scenario 5 e i due edge case di errore HTTP usano un server
TCP/HTTP minimale scritto a mano (un solo `accept`, risposta grezza fissa)
per ottenere risposte che `embedder-fake` non produrrebbe mai per
costruzione (500, corpo malformato) — non un mock del client, un vero
server a cui il client parla via rete, semplicemente non `embedder-fake`
stesso. Verificati fail-first (implementazione placeholder che ritorna
sempre `Ok(vec![0.0; 1536])`: 4/4 test falliscono correttamente prima
dell'implementazione reale).

Il test dello scenario 4 (`services/ingestion/tests/embedding_integration_test.rs`)
avvia `embedder-fake` come vero sottoprocesso `uvicorn` (bootstrap del venv
se assente, replicando in Rust lo stesso identico principio già usato da
`services/orchestrator/orchestrator/conftest.py` per `vllm-fake`, SPEC-018)
e confronta il vettore ottenuto dal client con una chiamata HTTP grezza
allo stesso endpoint nello stesso test, come richiesto esplicitamente da
§3 scenario 4. **Gated con `#[ignore]`**, stessa deviazione già dichiarata
in `persist_integration_test.rs` (SPEC-014 §10): Cargo non ha un analogo
diretto del build tag Go `-tags=integration`; eseguito manualmente due
volte con `cargo test --test embedding_integration_test -- --ignored`
(prima fallimento contro il placeholder, poi verde contro l'implementazione
reale, poi una seconda run per confermare il riuso del venv) — non cablato
in `Taskfile.yml` (`test:integration` elenca solo comandi Go/Rust già
esistenti; lo stesso gap preesiste già per `persist_integration_test.rs`
stesso, che NON è lì cablato — `Taskfile.yml` non è tra i file toccabili
dichiarati per questa SPEC).

**Nuova dipendenza Rust, versione esatta** (da `Cargo.toml`/`Cargo.lock`):
`ureq = "3.3.0"` (feature `json`). Comportamento di errore verificato
EMPIRICAMENTE (non presunto dalla documentazione) prima di scrivere il
client: connessione rifiutata → `Err(ureq::Error::Io(...))`; status
non-2xx → `Err(ureq::Error::StatusCode(code))` (il cui `Display` include
già il codice, es. `"http status: 500"` — soddisfa direttamente l'edge
case §4 "codice di stato incluso nel messaggio"); corpo 200 ma non
deserializzabile in `Vec<Vec<f32>>` → `Err(ureq::Error::Json(...))` da
`read_json`, mai un panic.

Nessun'altra deviazione. Nessun wiring in `parse_file`, nessun supporto
Matryoshka, nessun failover automatico fake↔reale — tutto come da §5
Non-goals.
