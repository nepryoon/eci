# SPEC-010 — libs/rust: bootstrap OTel + config loader (T0.8, parte Rust)
Stato: implemented
Task-tree: T0.8 (split per linguaggio — Rust, primo dei tre) · Servizio: libs/rust/eci-common · ADD: Modulo 4 §3.1 (Observability & Agent Tracing, OTel end-to-end)
Contratti: nessuno (nessun file sotto contracts/ toccato)

## 1. Obiettivo
Costruire il crate Rust condiviso `libs/rust/eci-common`, con (a) bootstrap OTel — tracer provider + exporter stdout/console, pronto a passare a un backend OTLP reale in Fase 7 cambiando solo l'exporter — e (b) un config loader minimo (env var con default). Sono le due fondamenta che il servizio di ingestion (T1.1/T1.2) userà da subito, e che ogni futuro servizio Rust riuserà senza duplicarle. Interceptor gRPC e propagazione/estrazione di header Kafka sono esplicitamente fuori scope qui — vedi §5, coprti invece da SPEC-011 (Go) dove servono davvero.

## 2. Interfaccia

Nuovo crate `libs/rust/eci-common/` (membro del workspace Cargo del repo, `Cargo.toml` proprio).

**Modulo `observability`**:
```rust
/// Inizializza il TracerProvider OTel con un exporter stdout/console.
/// Va chiamata una sola volta all'avvio del processo. Ritorna un guard
/// che deve restare vivo per tutta la durata del processo (il suo Drop
/// esegue lo shutdown pulito dell'exporter) — tipicamente tenuto in una
/// variabile locale a main().
pub fn init_tracing(service_name: &str) -> TracingGuard;

/// Legge il trace_id (128 bit, formato esadecimale W3C Trace Context,
/// 32 caratteri) dallo span attivo nel contesto corrente. None se non
/// c'è nessuno span attivo — mai un panic, mai una stringa vuota silenziosa.
pub fn current_trace_id_hex() -> Option<String>;
```
Dipendenze: `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-stdout` (versioni correnti — verificare al momento dell'implementazione, stesso trattamento già riservato alle immagini Docker `:latest` in SPEC-006/007: usare l'ultima versione compatibile, annotare quale si è risolta). Integrazione con la crate `tracing` (macro `#[tracing::instrument]`, `tracing::info_span!` ecc.) tramite `tracing-opentelemetry`, così il codice applicativo usa le macro standard dell'ecosistema Rust e non l'API OTel a basso livello direttamente.

**Modulo `config`**:
```rust
/// Legge la variabile d'ambiente `key`; se assente, ritorna `default`.
/// Stesso pattern già usato in tools/migrate-neo4j (Go, envOrDefault),
/// qui in Rust per uso da ingestion e futuri servizi Rust.
pub fn env_or_default(key: &str, default: &str) -> String;
```

## 3. Comportamento (scenari)

1. **Dato** `init_tracing("ingestion")` chiamato all'avvio, **quando** genero uno span (es. `tracing::info_span!("ingest_commit")`) attorno a un blocco di codice, **allora** lo span viene stampato su stdout in formato leggibile — conferma diretta che l'exporter funziona, non solo che compila.
2. **Dato** uno span attivo, **quando** chiamo `current_trace_id_hex()` dall'interno di quello span, **allora** ottengo una stringa esadecimale di esattamente 32 caratteri, non vuota.
3. **Dato** nessuno span attivo (chiamata fuori da qualunque span), **quando** chiamo `current_trace_id_hex()`, **allora** ottengo `None`.
4. **Dato** `env_or_default("ECI_REPO_PATH", "./sample-repo")` con la variabile d'ambiente NON impostata, **allora** ritorna `"./sample-repo"`; **dato** la stessa variabile impostata a un valore esplicito, **allora** ritorna quel valore.
5. **Dato** due span annidati (uno dentro l'altro), **quando** chiamo `current_trace_id_hex()` in ciascuno, **allora** ottengo lo **stesso** trace_id in entrambi (il trace_id è una proprietà del trace, non del singolo span — gli span annidati condividono lo stesso trace_id, solo lo span_id cambia).

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| `init_tracing` chiamata due volte nello stesso processo | Non deve panicare — comportamento accettabile: la seconda chiamata è no-op, oppure sovrascrive il provider globale con un warning nei log; specificare quale delle due nell'implementazione, non lasciarlo indefinito |
| Variabile d'ambiente impostata a stringa vuota (`ECI_REPO_PATH=""`) esplicitamente | Trattata come **valore impostato** (stringa vuota), non come "assente" — `env_or_default` ritorna `""`, non il default. Comportamento standard delle API env di Rust, da non alterare con logica custom che confonderebbe "vuoto" con "assente" |
| Il processo termina senza che il guard di `init_tracing` venga droppato correttamente (es. panic non gestito) | Gli span in buffer potrebbero non essere flush-ati sull'exporter stdout — comportamento noto e accettato a questo stadio (nessun backend reale da perdere), da rivalutare quando arriva un exporter OTLP reale in Fase 7 |

## 5. Non-goals
Nessun interceptor gRPC: il servizio di ingestion (T1.1/T1.2) non è un server né un client gRPC in questo walking skeleton — non fa chiamate ad altri servizi interni. Nessuna propagazione o estrazione di header Kafka: l'ingestion non consuma né produce messaggi Kafka direttamente — il `trace_id` generato qui viene scritto nella colonna `trace_id` della riga `outbox` (tramite `current_trace_id_hex()`, consumato dal codice applicativo di T1.2), non passato via header a questo stadio. Nessun exporter OTLP reale: rimandato a Fase 7, quando esisterà un backend (Tempo) a cui esportare — qui solo stdout/console, per rendere gli span visibili durante lo sviluppo senza richiedere infrastruttura aggiuntiva.

## 6. Vincoli dall'ADD
Modulo 4 §3.1: propagazione del trace context, span per ogni operazione rilevante. Questa SPEC costruisce solo la fondazione lato Rust (generazione trace_id, span locali) — la propagazione cross-servizio completa (gRPC metadata, header Kafka) arriva con SPEC-011/012 e con le fasi successive.

## 7. Test plan
`cargo test` nel crate `libs/rust/eci-common`: test standard Rust per gli scenari 2/3/4/5 (deterministici, nessuna dipendenza esterna — verificabili leggendo il contesto OTel in-process). Scenario 1 verificato per ispezione manuale dell'output stdout durante l'implementazione (non automatizzabile in modo pulito senza parsing dell'output — accettabile per una SPEC di bootstrap).

## 8. Osservabilità
Questa SPEC stessa costruisce la fondazione dell'osservabilità per i servizi Rust — non applicabile ricorsivamente.

## 9. Criteri di accettazione
- [x] `init_tracing` produce output leggibile su stdout per uno span reale (scenario 1) — verificato con `cargo run --example manual_scenario1` (`examples/manual_scenario1.rs`): output completo con `TraceId`/`SpanId`, nome span, attributi (`commit_sha`), evento (`elaborazione commit avviata`); il `trace_id` stampato manualmente da `current_trace_id_hex()` coincide esattamente col `TraceId` riportato dall'exporter.
- [x] `current_trace_id_hex()` ritorna una stringa esadecimale di 32 caratteri dentro uno span, `None` fuori da uno span (scenari 2/3) — `cargo test`, verde.
- [x] `env_or_default` rispetta default/override correttamente, incluso il caso stringa-vuota-esplicita (scenario 4, edge case tabella §4) — `cargo test`, verde.
- [x] Span annidati condividono lo stesso trace_id (scenario 5) — `cargo test`, verde.
- [x] `cargo test` verde su tutti gli scenari automatizzabili — 4/4 test passati (`config::tests` × 3, `observability::tests` × 1), `cargo clippy --all-targets -- -D warnings` pulito.

## 10. Deviazioni rispetto alla SPEC

1. **Versioni esatte risolte** (verificate al momento dell'implementazione,
   come da istruzione — stesso trattamento delle immagini Docker `:latest`
   in SPEC-006/007): `opentelemetry` 0.32.0, `opentelemetry_sdk` 0.32.1,
   `opentelemetry-stdout` 0.32.0, `tracing` 0.1.44, `tracing-subscriber`
   0.3.23, `tracing-opentelemetry` 0.33.0. API verificata leggendo il
   sorgente reale delle crate scaricate (non assunta da versioni precedenti
   note, che divergono parecchio: `opentelemetry-rust` ha avuto churn
   significativo, es. `SdkTracerProvider` — non più solo `TracerProvider` —
   e `Resource::builder()`).

2. **Doppio `init_tracing`: no-op con warning su stderr** (non
   sovrascrittura del provider globale) — una delle due opzioni esplicitamente
   sanzionate da §4. Implementato con
   `tracing_subscriber::util::SubscriberInitExt::try_init()`: se il
   subscriber globale `tracing` è già impostato, `try_init()` ritorna `Err`
   senza panicare; in quel caso si stampa un warning esplicito su stderr e
   si ritorna comunque un `TracingGuard` valido (contiene un
   `SdkTracerProvider` proprio, mai collegato al subscriber globale già
   attivo — il suo `Drop` fa comunque uno shutdown pulito del proprio
   provider inerte, nessun effetto sul subscriber realmente in uso).

3. **`eci-common` non è (ancora) membro di un workspace Cargo del repo**:
   §2 lo descrive come "membro del workspace Cargo del repo", ma **non
   esiste un `Cargo.toml` di workspace alla root** — solo
   `services/ingestion/Cargo.toml`, anch'esso un crate standalone, non
   parte di alcun workspace. Creare un workspace root è fuori dal
   perimetro file assegnato a questa sessione (solo
   `libs/rust/eci-common` e questa SPEC). Il crate è quindi standalone
   (proprio `Cargo.lock`, buildabile/testabile con `cargo test` dentro
   `libs/rust/eci-common/`) — stesso pattern già adottato per i moduli Go
   standalone di questo repo (`tools/migrate-neo4j`,
   `tests/integration/postgres_ddl`, `tests/integration/outbox_cdc`, nessuno
   dei quali è in un `go.work`). Diventare un vero workspace member è
   lasciato a quando/se si crea un `Cargo.toml` di workspace alla root.

4. **`Cargo.lock` committato**: pratica già in uso in questo repo per
   `services/ingestion/Cargo.lock` (crate Rust standalone, non libreria
   pubblicata) — stessa scelta qui, per riproducibilità del build.

5. **`cargo fmt --check` non eseguibile**: `rustfmt` non è installato nel
   toolchain di questa sandbox (`error: 'cargo-fmt' is not installed`).
   Formattazione verificata a mano (un solo refuso di indentazione
   corretto manualmente); `cargo clippy --all-targets -- -D warnings`
   eseguito con successo come controllo di qualità alternativo.

6. **Esempio `examples/manual_scenario1.rs`** aggiunto per la verifica
   manuale dello scenario 1 (richiesta esplicitamente da questa sessione,
   non dalla SPEC): non è un test automatico, resta un artefatto eseguibile
   con `cargo run --example manual_scenario1` per chiunque voglia
   riverificare l'output stdout in futuro, invece di una verifica
   "usa e getta" non riproducibile.
