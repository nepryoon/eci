# SPEC-017 — vllm-fake: LLM fake OpenAI-compatible, risposte deterministiche (T0.9, scope rivisto)
Stato: verified
Task-tree: T0.9 (recuperato dopo Fase 0 — scope rivisto, vedi §1) · Servizio: fakes/vllm-fake (Python, directory seedata ma vuota da Step 1-3) · ADD: Modulo 4 (Tech Stack — vLLM come backend LLM reale in Fase 5)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Popolare `fakes/vllm-fake` con un servizio HTTP minimale, compatibile con l'API OpenAI Chat Completions (lo stesso contratto che un vLLM reale espone), con risposte **deterministiche**: lo stesso prompt produce sempre la stessa risposta, byte per byte — necessario per T1.5 (orchestrator v0), che chiama un LLM per produrre la risposta finale, e per T1.6 (test E2E), che deve poter asserire risultati esatti senza la non-determinismo di un LLM reale.

**Scope ridotto rispetto al T0.9 originale**: `retrieval-stub` non costruito (superato da T1.4, il retrieval-engine reale esiste già — uno stub sarebbe un downgrade). `embedder-fake` rimandato (serve solo alla gamba vettoriale, Fase 4 — nessun consumatore oggi). Resta solo `vllm-fake`, l'unico dei tre genuinamente necessario a T1.5.

## 2. Interfaccia

Servizio HTTP Python (`fakes/vllm-fake/`, riusa `libs/py/eci_core` per config/OTel dove pertinente — stesso pattern di orchestrator, entrambi Python).

**`POST /v1/chat/completions`** — stesso shape di richiesta/risposta dell'API OpenAI Chat Completions (il contratto che un vLLM reale espone), sufficiente per essere chiamato da un client OpenAI-compatibile standard senza adattamenti:

Richiesta (campi rilevanti, il resto ignorato senza errore — un client reale ne invia altri, es. `temperature`, che qui non hanno effetto dato che la risposta è deterministica per design):
```json
{"model": "...", "messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}]}
```

Risposta:
```json
{
  "id": "fake-<primi 16 char di sha256(concatenazione di tutti i messages[].content)>",
  "object": "chat.completion",
  "model": "vllm-fake",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "[vllm-fake risposta deterministica] <content del PIÙ RECENTE messaggio con role=\"user\">"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": <conteggio parole di tutti i messages>, "completion_tokens": <conteggio parole della risposta>, "total_tokens": <somma>}
}
```

**Meccanismo deterministico, deliberatamente semplice**: il contenuto della risposta è l'**eco letterale** dell'ultimo messaggio `user`, prefissato da un marcatore chiaro (`[vllm-fake risposta deterministica]`) — non un tentativo di sintetizzare/riassumere il prompt. Se il template di prompt di T1.5 incorpora nomi/id dei nodi recuperati nel messaggio utente (atteso, dato che T1.5 deve produrre "risposta con provenance"), quegli stessi nomi/id compaiono letteralmente nella risposta del fake — verificabile da T1.6 con un controllo di sottostringa, senza che `vllm-fake` debba capire la struttura del prompt di nessun chiamante.

`usage`: conteggio parole (split su spazi), non vera tokenizzazione — approssimazione dichiarata, sufficiente per un campo che i client si aspettano popolato ma che qui non deve essere accurato.

## 3. Comportamento (scenari)

1. **Dato** una richiesta con un solo messaggio `user` "Chi chiama Validate?", **quando** chiamo `/v1/chat/completions`, **allora** la risposta contiene esattamente `"[vllm-fake risposta deterministica] Chi chiama Validate?"`.
2. **Dato** la STESSA identica richiesta inviata due volte, **quando** confronto le due risposte, **allora** sono byte-per-byte identiche, incluso il campo `id` (stesso hash, stesso input).
3. **Dato** due richieste con contenuto `user` diverso, **quando** le confronto, **allora** gli `id` sono diversi (hash diversi) e il contenuto della risposta riflette il rispettivo messaggio.
4. **Dato** una richiesta con più messaggi (`system` + più turni `user`/`assistant`), **quando** chiamo l'endpoint, **allora** la risposta echeggia SOLO l'ultimo messaggio `user`, non l'intera conversazione.
5. **Dato** un campo extra non gestito nella richiesta (es. `temperature: 0.7`), **quando** chiamo l'endpoint, **allora** la richiesta viene comunque processata correttamente (nessun errore per campi sconosciuti ignorati).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Richiesta senza nessun messaggio `role="user"` (solo `system`) | Errore esplicito (400), non un comportamento indefinito — un client reale non dovrebbe mai omettere un messaggio utente, ma se succede va segnalato chiaramente |
| Richiesta con `messages` assente o vuoto | Errore esplicito (400) |
| Corpo della richiesta non JSON valido | Errore esplicito (400), stesso trattamento di un client OpenAI-compatibile reale |

## 5. Non-goals
Nessuna vera intelligenza/generazione — il fake esiste per essere prevedibile, non utile come risposta reale. Nessun supporto streaming (`stream: true` nella richiesta OpenAI reale) — walking skeleton, T1.5 non lo richiede. Nessun `embedder-fake`/`retrieval-stub` (vedi §1). Nessuna autenticazione/rate limiting (un fake locale di sviluppo, non esposto oltre lo stack dev).

## 6. Vincoli dall'ADD
Modulo 4: vLLM è il backend LLM reale previsto per Fase 5, API OpenAI-compatibile — questo fake replica esattamente quel contratto (stesso path `/v1/chat/completions`, stesso shape request/response) così che T1.5 possa essere scritto oggi contro un'interfaccia stabile, e il passaggio a un vLLM reale in Fase 5 richieda solo un cambio di URL, non di codice.

## 7. Test plan
Test HTTP diretti (nessun testcontainers necessario — il servizio stesso è l'oggetto sotto test, nessuna dipendenza esterna). Scenari 1-5 verificabili con richieste dirette al servizio in esecuzione locale.

## 8. Osservabilità
Non applicabile in modo significativo (servizio di test/sviluppo) — uso base di `libs/py/eci_core.observability` per coerenza con gli altri servizi Python, non un requisito critico qui.

## 9. Criteri di accettazione
- [x] Scenario 1: eco letterale corretto con prefisso marcatore.
- [x] Scenario 2: determinismo verificato con confronto diretto di due risposte identiche (sia via `TestClient` in-process sia via HTTP reale contro il servizio avviato con `uvicorn`, vedi §10).
- [x] Scenario 3: input diversi producono id/contenuto diversi.
- [x] Scenario 4: solo l'ultimo messaggio user viene echeggiato, non l'intera conversazione.
- [x] Scenario 5: campi extra ignorati senza errore.
- [x] Edge case tabella §4 verificati esplicitamente (400 sui tre casi elencati) — sia nei test automatici sia con `curl` diretto contro il servizio realmente in esecuzione.

## 10. Deviazioni rispetto alla SPEC

1. **Framework HTTP: FastAPI + Uvicorn** — non specificato dalla SPEC
   ("servizio HTTP minimale"), scelto per l'idiomaticità con un'API
   OpenAI-compatible (validazione via Pydantic, `TestClient` per i test
   diretti di §7 senza bisogno di un socket reale) e per la buona
   integrazione con `libs/py/eci_core` (già dipende da `pydantic`).
   Versioni esatte risolte: `fastapi==0.141.1`, `uvicorn==0.52.1`,
   `starlette==1.3.1` (indiretta), `httpx==0.28.1`.

2. **Parsing del body manuale, non un parametro Pydantic tipizzato
   sull'endpoint**: FastAPI risponderebbe `422 Unprocessable Entity` sui
   fallimenti di validazione automatica (JSON malformato, campo mancante),
   ma SPEC-017 §4 richiede esplicitamente `400` sui tre casi elencati —
   implementato leggendo il body grezzo e validandolo a mano
   (`json.loads` con `except JSONDecodeError`, poi controlli espliciti su
   `messages`/`role=="user"`), così il codice di stato è sotto controllo
   diretto invece che affidato al comportamento di default del framework.
   Aggiunto anche un controllo non esplicitamente in tabella ma dello
   stesso spirito: un body che è JSON valido ma non un oggetto (es. un
   array) produce comunque 400, non un errore 500 non gestito.

3. **`httpx` aggiunta come dipendenza diretta** (non "extra"): richiesta
   da `starlette.testclient.TestClient` per i test HTTP diretti di §7.
   Osservato un warning di deprecazione Starlette ("Using `httpx` with
   `starlette.testclient` is deprecated; install `httpx2` instead") — non
   un errore, i test passano; non migrato a `httpx2` in questa sessione
   (pacchetto di transizione non ancora usato altrove nel repo, nessun
   precedente da seguire).

4. **`eci_core` non dichiarato come dipendenza in `pyproject.toml`**:
   verificato che NESSUno scaffold Python del repo (`orchestrator`,
   `verification`, `summarization`) dichiara `eci_core` come dipendenza —
   non esiste un meccanismo di "path dependency" cross-modulo stabilito
   per Python in questo monorepo (a differenza di Go, che ha `replace` in
   `go.mod`). Il venv di sviluppo/test installa entrambi i pacchetti
   editable nello stesso ambiente: `pip install -e ../../libs/py -e .`
   — un passo manuale in più, non automatizzato da `pyproject.toml`
   stesso, coerente con l'assenza dello stesso meccanismo negli altri
   scaffold Python.

5. **Ambiente locale: `python3-venv` non installato, fallback a
   `virtualenv`** — stesso fallback già implementato in
   `scripts/task-build.sh` (`ensure_venv`), qui eseguito a mano
   (`python3 -m virtualenv .venv`) dato che `fakes/` non è wired in quello
   script. Non un problema del codice, un'osservazione sull'host.

6. **Algoritmo esatto per `id` e `usage`, reso esplicito dove la SPEC
   lascia margine di lettura**: concatenazione dei `content` di TUTTI i
   messaggi SENZA separatore (lettura letterale di "concatenazione");
   `prompt_tokens` = somma del conteggio parole di OGNI messaggio
   (`system`+`user`+`assistant`, non solo `user` — "tutti i messages");
   `completion_tokens` = conteggio parole dell'INTERA risposta, marcatore
   incluso (è il contenuto del messaggio, non solo la parte echeggiata).
   Nessuna di queste scelte è testata per un valore specifico diverso da
   quello prodotto da questa stessa implementazione (i test verificano
   consistenza/determinismo, non un numero magico), quindi restano scelte
   implementative dichiarate, non deviazioni di comportamento osservabile
   dai criteri di accettazione.

7. **Non wired in `Taskfile.yml`/`scripts/task-*.sh`**: il perimetro file
   di questa sessione è `fakes/vllm-fake` e questa SPEC — stesso
   precedente di SPEC-008/014/015/016. `fakes/` non è menzionato in
   nessuno script `task-*.sh` esistente (verificato: nessun riferimento a
   "fakes" in `Taskfile.yml` o `scripts/*.sh` prima di questa SPEC).

8. **Verifica end-to-end reale, non solo `TestClient`**: oltre agli 11
   test automatici (`pytest`, tutti verdi, `ruff check` pulito), il
   servizio è stato avviato per davvero con `uvicorn` e interrogato via
   `curl` reale — determinismo (stessa richiesta due volte, risposte
   byte-per-byte identiche) e tutti e tre gli edge case 400 di §4
   riconfermati contro il processo realmente in esecuzione, non solo
   in-process via `TestClient`.
