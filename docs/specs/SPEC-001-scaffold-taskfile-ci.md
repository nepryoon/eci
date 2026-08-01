# SPEC-001 — Scaffold monorepo, Taskfile, CI baseline
Stato: implemented
Task-tree: T0.1 · Servizio: root (nessun service applicativo) · ADD: struttura repo (Playbook §1)
Contratti: nessuno

## 1. Obiettivo
Creare lo scheletro di directory del monorepo, un `Taskfile.yml` con l'interfaccia comandi completa (target reali dove possibile, placeholder espliciti altrove) e una pipeline CI che esegue lint/test sui tre linguaggi e applica il guard su `contracts/`/`docs/add/`. Nessuna logica di prodotto in questo task: solo infrastruttura di repo.

## 2. Interfaccia

Struttura di directory da creare (vuote salvo README/placeholder minimi):
```
eci/
├── CLAUDE.md  Taskfile.yml  .gitignore
├── docs/{add,specs,decisions,runbooks}/
├── contracts/{proto,jsonschema,cypher,sql,events}/   # già popolate (Step 2)
├── services/{ingestion,sink-graph,sink-vector,sink-search,
│             retrieval-engine,orchestrator,verification,
│             llm-gateway,semantic-cache,summarization}/
├── libs/{go,py,rust}/
├── fakes/
├── deploy/{compose,k8s}/
├── tests/{integration,e2e,fixtures,golden}/
├── scripts/guard.sh
└── .github/workflows/ci.yml
```

Ogni cartella in `services/` contiene per ora solo un file minimo che rende il linguaggio "buildabile":
- Go (`sink-graph`, `sink-vector`, `sink-search`, `retrieval-engine`, `llm-gateway`, `semantic-cache`): `go.mod` (module `github.com/<org>/eci/services/<nome>`) + `main.go` con `func main() {}`.
- Python (`orchestrator`, `verification`, `summarization`): `pyproject.toml` minimale (nome, versione, Python ≥3.11) + `__init__.py` vuoto.
- Rust (`ingestion`): `Cargo.toml` minimale + `src/main.rs` con `fn main() {}`.

`Taskfile.yml` — target e comportamento atteso:

| Target | Comportamento in questo task | Comportamento finale (task che lo completa) |
|---|---|---|
| `up` | stampa `"not implemented yet — see T0.6"`, exit 0 | avvia `deploy/compose` |
| `down` | idem | ferma lo stack compose |
| `build` | `go build ./...` in ogni modulo Go, `cargo build` in ingestion, `pip install -e .` per i moduli Python — deve passare da subito sullo scaffold | invariato |
| `lint` | `go vet ./...` (Go), `cargo clippy` (Rust), `ruff check .` (Python) su ogni modulo presente | invariato |
| `test` | `go test ./...`, `cargo test`, `pytest` — devono girare (anche a vuoto) senza errori | invariato |
| `test:integration` | stampa `"not implemented yet — see T0.7"`, exit 0 | testcontainers |
| `proto:gen` | stampa `"not implemented yet — see T0.2"`, exit 0 | buf + grpc_tools codegen |
| `proto:breaking` | stampa `"not implemented yet — see T0.2"`, exit 0 | buf breaking check |
| `schema:gen` | stampa `"not implemented yet — see T0.3"`, exit 0 | datamodel-code-generator |
| `db:migrate` / `db:migrate:down` | stampa `"not implemented yet — see T0.5"`, exit 0 | golang-migrate su Postgres |
| `db:neo4j:migrate` | stampa `"not implemented yet — see T0.4"`, exit 0 | runner Cypher |
| `guard` | ESEGUE `scripts/guard.sh` (vedi sotto) — reale da subito | invariato |

`scripts/guard.sh` — logica esatta:
1. Determina il branch base: variabile d'ambiente `BASE_REF`, default `origin/main`.
2. `CHANGED=$(git diff --name-status "$BASE_REF"...HEAD)`.
3. Se nessuna riga di `CHANGED` ha path che inizia per `contracts/` o `docs/add/` → exit 0.
4. Altrimenti, verifica che almeno una riga di `CHANGED` abbia status `A` (added) e path che inizia per `docs/decisions/` → se sì, exit 0; se no, exit 1 stampando l'elenco dei file protetti toccati e il messaggio: `"Modifiche a contracts/ o docs/add/ richiedono un ADR in docs/decisions/ nello stesso commit."`.

## 3. Comportamento (scenari)

1. **Dato** un checkout pulito, **quando** eseguo `task --list`, **allora** vedo tutti i target della tabella con una riga di descrizione ciascuno.
2. **Dato** lo scaffold Go/Python/Rust minimo, **quando** eseguo `task build && task lint && task test`, **allora** tutti passano con exit 0 (anche se non testano nulla di significativo).
3. **Dato** una PR che modifica un file sotto `contracts/` senza aggiungere nulla in `docs/decisions/`, **quando** eseguo `task guard` (o la CI la esegue), **allora** fallisce con l'elenco dei file protetti toccati.
4. **Dato** la stessa PR ma con l'aggiunta di `docs/decisions/ADR-0001-qualcosa.md` nello stesso diff, **quando** eseguo `task guard`, **allora** passa.
5. **Dato** una PR che NON tocca `contracts/` né `docs/add/`, **quando** eseguo `task guard`, **allora** passa sempre, indipendentemente da `docs/decisions/`.
6. **Dato** il repo, **quando** eseguo un target placeholder (es. `task up`), **allora** l'output contiene esplicitamente la stringa `"not implemented yet"` e il riferimento al task che lo completerà, con exit code 0 (non deve far fallire la CI).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `BASE_REF` non risolvibile (es. shallow clone senza `origin/main`) | `guard.sh` fallisce con messaggio esplicito che chiede un `git fetch` più profondo, non un falso "passa" |
| Nessun modulo Go/Python/Rust ancora popolato in un sottodirectory | `task lint`/`task test` non deve fallire per "nessun file trovato": skip esplicito con messaggio, non errore |
| File spostato (rename) sotto `contracts/` | `git diff --name-status` marca rename con `R100 old new`: `guard.sh` deve trattarlo come tocco a `contracts/` (controlla sia old che new path) |
| Python di sistema "externally managed" (PEP 668) senza `python3-venv`/`ensurepip`, o necessità di isolare le dipendenze per servizio | `build`/`lint`/`test` non installano mai nel Python di sistema: per ogni servizio Python creano/riusano un virtualenv dedicato in `services/<nome>/.venv/` (fallback a `virtualenv` se `python3 -m venv` non è disponibile) e puntano `pip`/`ruff`/`pytest` all'interprete di quel venv |

## 5. Non-goals
Non implementare in questo task: compose, codegen proto/schema, migration DB, testcontainers. Sono placeholder intenzionali fino ai task che li completano (tabella §2).

## 6. Vincoli dall'ADD
Nessun vincolo architetturale diretto (è infrastruttura di repo). Vincolo di processo dal Playbook §1: `contracts/` e `docs/add/` sono read-only per l'AI salvo ADR nello stesso commit — è l'invariante che questo task rende meccanico.

## 7. Test plan
- Unit: test su `scripts/guard.sh` con un repo git temporaneo fixture (3 casi: nessun tocco a file protetti; tocco senza ADR; tocco con ADR) — script di test in bash o un piccolo harness Python che crea commit sintetici in una repo temp.
- CI: il workflow stesso è il test end-to-end — una PR di prova che tocca `contracts/README.md` senza ADR deve fallire visibilmente in GitHub Actions.

## 8. Osservabilità
N/A (infrastruttura di build, non runtime).

## 9. Criteri di accettazione
- [x] `task --list` mostra tutti i target della tabella.
- [x] `task build && task lint && task test` verdi su repo pulito.
- [x] `scripts/guard.sh` supera i 3 casi di test descritti in §7 (implementati come 7 casi, vedi deviazioni).
- [ ] CI GitHub Actions verde su un PR "vuota" (nessun tocco a file protetti). — non verificabile da questa sessione (nessun push/PR eseguito), vedi deviazioni.
- [ ] CI GitHub Actions rossa su una PR di prova che tocca `contracts/` senza ADR, verde se si aggiunge l'ADR. — idem.
- [x] Nessun target placeholder fa fallire la CI (tutti exit 0 con messaggio esplicito).

## Deviazioni rispetto alla SPEC

1. **Ambiente locale privo di toolchain**: la macchina di sviluppo non aveva `go`,
   `cargo`, `ruff` né `task` installati, e non c'è accesso sudo/pip di sistema.
   Installati tutti in user-space (`~/.local/go`, `~/.cargo`, `~/.local/bin`)
   previa conferma esplicita dell'utente. Per Rust è servito anche un linker C:
   estratti i pacchetti Debian `gcc-15`/`binutils`/`libc6-dev` (senza root, via
   `apt-get download` + `dpkg-deb -x`) in `~/.local/opt/gcc-15`, referenziato da
   `~/.cargo/config.toml`. Questo setup è locale alla macchina (non nel repo) e
   non serve in CI, dove i runner GitHub Actions hanno già i toolchain standard.

2. **`pip install -e .` e PEP 668 → virtualenv dedicato per servizio**: il
   Python di sistema su questa macchina è "externally managed" (niente
   `python3-venv`/`ensurepip` disponibili senza sudo). Anziché installare nel
   Python di sistema (con o senza `--break-system-packages`, workaround
   iniziale poi abbandonato), `scripts/task-build.sh`/`task-lint.sh`/
   `task-test.sh` creano/riusano un virtualenv dedicato per servizio in
   `services/<nome>/.venv/` (`python3 -m venv`, con fallback a `virtualenv` se
   `ensurepip` non è disponibile) e puntano `pip`/`ruff`/`pytest` all'interprete
   di quel venv. `.venv/` è in `.gitignore`. Questo evita del tutto il problema
   PEP 668 ed isola le dipendenze fra servizi; funziona identico in CI.

3. **`pytest` con 0 test raccolti restituisce exit code 5** (non 0): trattato
   esplicitamente come skip in `scripts/task-test.sh`, altrimenti `task test`
   fallirebbe sullo scaffold vuoto in contraddizione con lo scenario §3.2.

4. **Modulo Go/Rust/Python non popolato**: la SPEC richiede lo skip esplicito
   in `task lint`/`task test`; ho generalizzato la stessa guardia anche a
   `task build`, per coerenza (evita lo stesso tipo di falso negativo).

5. **`services/<nome>/go.mod`**: l'org placeholder usata è
   `github.com/eci-project/eci/services/<nome>` — nessun org name era
   specificato in SPEC/ADD; da rinominare con un `sed` globale quando l'org
   reale sarà decisa (nessun impatto funzionale nello scaffold).

6. **Struttura extra non nell'albero di directory di SPEC-001 §2**: aggiunto
   `tests/unit/guard/` (non elencato tra `tests/{integration,e2e,fixtures,golden}/`)
   per ospitare i test unitari di `scripts/guard.sh` richiesti da §7, dato che
   non esiste un'altra collocazione indicata dalla SPEC per test unitari
   (non-integration, non-e2e) su file root come `scripts/guard.sh`.

7. **`contracts/{proto,sql,events}/` non creati**: la SPEC elenca
   `contracts/{proto,jsonschema,cypher,sql,events}/` come "già popolate (Step
   2)"; nel repo reale solo `contracts/jsonschema/` e `contracts/cypher/`
   esistono. `contracts/` è read-only per l'AI per invariante di CLAUDE.md
   (richiede ADR): non ho creato le sottodirectory mancanti, che restano di
   competenza dei task T0.2 (proto)/T0.3 (eventi)/T0.5 (sql).

8. **Verifica CI reale non eseguita**: non è stato aperto alcun push/PR da
   questa sessione, quindi gli ultimi due criteri di accettazione (verde su PR
   vuota, rosso→verde su tocco `contracts/` con/senza ADR) sono verificati solo
   per costruzione (stessa logica di `scripts/guard.sh` già coperta da 7 test
   unitari) e non tramite un'esecuzione reale di GitHub Actions.
