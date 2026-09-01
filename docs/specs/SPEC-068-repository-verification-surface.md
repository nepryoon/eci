# SPEC-068 — Superficie di verifica completa del monorepo
Stato: implemented
Task-tree: cross-cutting T0.1/T7.5 · Servizio: scripts, Taskfile e CI · ADD: registro D1–D9; Modulo 4 §§2–3
Contratti: nessuna modifica; protezione di `contracts/` e `docs/ADD_Enterprise_Code_Intelligence_consolidato.md`

## 1. Obiettivo

Rendere build, lint e test aggregati una rappresentazione completa e deterministica dei moduli realmente presenti nel monorepo. Separare i test CPU/unitari dai test che richiedono Docker senza eliminare o saltare questi ultimi, proteggere il vero ADD consolidato e rendere esplicite in Taskfile e CI le superfici E2E, interop, fake, security e codegen.

## 2. Interfaccia

```text
task build
task lint
task test
task test:integration
task test:e2e
task test:interop
task test:fakes
task test:security
task verify:generated
task verify:ci
```

Gli script aggregati scoprono i manifest tracciati (`go.mod`, `Cargo.toml`, `pyproject.toml`) nelle directory di produzione supportate; una allow-list esplicita riguarda soltanto fixture/test intenzionalmente non produttivi e deve essere verificata da un test di inventario.

## 3. Comportamento

1. **Dato** un nuovo modulo Go, Rust o Python tracciato in `services/`, `tools/`, `libs/` o `fakes/`, **quando** gira il test dell'inventario, **allora** il modulo compare in build, lint e test oppure in una esclusione motivata e testata.
2. **Dato** un host senza daemon Docker, **quando** eseguo `task test`, **allora** tutti i test unitari/CPU girano e il comando termina verde senza tentare di aprire il socket Docker.
3. **Dato** un host con Docker, **quando** eseguo `task test:integration`, **allora** i test Keycloak, OPA, orchestrator/Neo4j e ogni altra suite Docker-backed restano invocati esplicitamente.
4. **Dato** il file ADD reale già tracciato, **quando** viene modificato, cancellato o rinominato senza un nuovo ADR nello stesso diff, **allora** `task guard` fallisce e nomina il file.
5. **Dato** ciascun fake Python e `libs/rust/eci-common`, **quando** eseguo gli aggregati pertinenti, **allora** installazione/build, lint e test vengono eseguiti davvero.
6. **Dato** `tests/e2e`, `tests/interop` e le suite security, **quando** ispeziono Taskfile e CI, **allora** ciascuna ha un target nominato e una chiamata CI; nessuna è raggiunta solo incidentalmente.
7. **Dato** i generatori proto, schema ed Envoy, **quando** eseguo `task verify:generated`, **allora** ogni generatore gira e un diff limitato agli output generati deve restare vuoto.
8. **Dato** l'insieme dei gate CPU/CI, **quando** eseguo `task verify:ci`, **allora** fallisce al primo gate rosso e include build, lint, unit, guard, proto, codegen, Kubernetes, fake, interop, E2E-smoke, security-smoke, eval-smoke e load-smoke quando tali target esistono.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Manifest tracciato non classificato | fallimento esplicito con path del manifest |
| Modulo privo di test | build/lint restano obbligatori; il test segnala chiaramente "no test files" senza fingere copertura |
| Docker non disponibile in `task test:integration` | fallimento esplicito con diagnosi del daemon, mai skip o successo fittizio |
| Output codegen modificato | `verify:generated` fallisce mostrando il diff ristretto |
| ADD rinominato fuori dal path corrente | guard considera sia sorgente sia destinazione del rename |
| Toolchain mancante | errore immediato con nome/versione richiesta, nessun download non verificato |

## 5. Non-goals

Non implementa funzionalità runtime, contratti, tombstone, autoscaling, osservabilità, evaluation o load test. Non rende Docker opzionale per le suite d'integrazione: le separa soltanto dal gate unitario CPU. Non modifica il contenuto dell'ADD.

## 6. Vincoli dall'ADD

- Il registro D1–D9 impone che contratti e deliverable generati restino sincronizzati.
- Modulo 1 §2.2.4 richiede prova di idempotenza dei consumer; i test d'integrazione non possono essere eliminati.
- Modulo 3 §§2.1–2.5 richiede enforcement fail-closed; le suite security devono avere un percorso aggregato esplicito.
- Modulo 4 §§2–3 richiede manifest Kubernetes validabili e osservabilità verificabile.

## 7. Test plan

- Unit: test Python dell'inventario manifest/target e casi sintetici di `scripts/guard.sh`.
- Unit/CPU: eseguire `task test` con `DOCKER_HOST` puntato a un socket inesistente e verificare che nessuna suite integration venga raccolta.
- Statico: analisi Taskfile/CI per tutti i target nominati.
- Integration: con Docker disponibile, eseguire due volte i test idempotenti già aggregati; nessuna modifica delle loro asserzioni.
- Codegen: eseguire i tre generatori e `git diff --exit-code` sugli output.

## 8. Osservabilità

Nessuna nuova operazione runtime. Gli aggregati stampano il path/modulo prima di ogni comando e conservano exit code non-zero; non stampano variabili ambiente, credenziali o contenuti sorgente.

## 9. Criteri di accettazione

- [x] Test di inventario prima rosso sui moduli oggi omessi.
- [x] Test guard prima rosso sul vero ADD consolidato.
- [x] `task build`, `task lint` e `task test` verdi senza daemon Docker.
- [ ] `task test:integration` conserva tutte le suite Docker-backed ed è eseguito in CI (wiring completo; esecuzione locale bloccata dal daemon assente).
- [x] Fake, E2E, interop e security hanno target e CI espliciti.
- [x] `task verify:generated` verde e senza diff.
- [x] `task k8s:validate` verde.
- [x] `task guard` verde sul diff accompagnato da questa SPEC e senza modifiche al contenuto dell'ADD.

## 10. Evidenza di implementazione

- Fail-first osservato: 1 fallimento guard e 4 fallimenti inventario/Taskfile/CI.
- `task build`, `task lint`, `task test`, `task test:fakes`, `task test:security`, `bash scripts/task-interop.sh`, `task verify:generated`, `task guard` e `task k8s:validate`: exit 0.
- L'inventario ora copre 13 moduli Go, 2 Rust e 7 Python, inclusi i tre fake, `eci-common`, `gc-postgres` e `reconcile`.
- `task test:integration` e `task test:e2e` falliscono subito e chiaramente quando Docker non è disponibile; nessun test viene convertito in skip. Il job CI completo è presente ma non viene dichiarato verde senza un'esecuzione remota osservata.
