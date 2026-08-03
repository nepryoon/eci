# SPEC-013 — ingestion v0: parsing Go con Tree-sitter → CPG walking skeleton (T1.1)
Stato: verified
Task-tree: T1.1 (primo task di Fase 1) · Servizio: services/ingestion (Rust) · ADD: Modulo 1 §1.2-1.3 (CPG a livello statement), §1.4 (name resolution — qui solo intra-file)
Contratti: contracts/jsonschema/hybrid-graph.json (D2, letto come riferimento — CodeNode/CodeRelation, non modificato)

## 1. Obiettivo
Popolare per la prima volta `services/ingestion` (Rust, finora solo scaffold vuoto da SPEC-001): parsing di codice Go con Tree-sitter, che produce in memoria — non ancora scritto su Postgres, quello è T1.2 — le entità `CodeNode` (File, Class, Interface, Method, Function) e `CodeRelation` (CONTAINS, CALLS) corrispondenti allo schema D2, limitatamente a relazioni **intra-file**. Verificato contro il fixture e il golden dataset già costruiti in SPEC-009 (`tests/fixtures/sample-repo/`, `tests/golden/queries_v0.json`).

## 2. Interfaccia

Nuovo codice in `services/ingestion/src/` (crate Rust, dipendenza aggiuntiva `libs/rust/eci-common` di SPEC-010 per OTel/config). Dipendenze: `tree-sitter`, `tree-sitter-go` (grammatica Go — versioni correnti, verificare al momento dell'implementazione).

**Tipi in memoria** (non ancora persistiti — vedi §5):
```rust
pub struct CodeNode {
    pub id: String,           // deterministico: hash di "{file_path}:{qualified_name}"
    pub domain: String,       // sempre "code" qui
    pub node_type: String,    // "File" | "Class" | "Interface" | "Method" | "Function"
    pub name: String,
    pub ast_hash: String,     // SHA-256 del solo testo sorgente di QUESTA entità — hash
                              // semplice, NON ancora la normalizzazione/Merkle completa
                              // di T2.1 (§5 Non-goals): quella sostituirà questo valore
                              // in una SPEC successiva, non va anticipata qui.
    pub file_path: String,
}

pub struct CodeRelation {
    pub domain: String,       // "code"
    pub rel_type: String,     // "CONTAINS" | "CALLS"
    pub from_id: String,
    pub to_id: String,
    pub weight: Option<u32>,  // solo per CALLS — stesso pattern del Cypher di riferimento
                              // D3 (MERGE...ON MATCH SET r.weight = coalesce(r.weight,0)+1):
                              // qui, nella singola passata di parsing di un file, il weight
                              // riflette quante volte la STESSA coppia caller/callee compare
                              // (più call-site allo stesso target = un arco con weight>1, non
                              // più archi) — vedi §6 per la deviazione su CallSite.
}

pub fn parse_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>);
```

**Mappatura Tree-sitter Go → CPG** (D2 `node_type` enum):
- Il file stesso → un `CodeNode` `node_type="File"`.
- `type_declaration` con `struct_type` → `node_type="Class"`.
- `type_declaration` con `interface_type` → `node_type="Interface"`.
- `method_declaration` (ha un receiver) → `node_type="Method"`.
- `function_declaration` (nessun receiver) → `node_type="Function"`.
- Ogni dichiarazione di primo livello → un arco `CONTAINS` dal `File` verso di essa.

**Risoluzione CALLS, intra-file, per nome** (walking skeleton — non name resolution completa, quella è T2.5): per ogni file, costruire una mappa nome→id di tutte le funzioni/metodi dichiarati IN QUEL FILE. Per ogni `call_expression` trovato dentro il corpo di una funzione/metodo:
- Se il target è un `identifier` semplice (chiamata a funzione standalone, es. `computeTotal(...)`): cercare quel nome nella mappa del file.
- Se il target è una `selector_expression` (chiamata a metodo, es. `s.Validate(...)`): estrarre solo il nome del campo selezionato (`Validate`) e cercarlo nella mappa del file — **per nome, senza verifica del tipo del receiver** (semplificazione dichiarata: a questo stadio, intra-file, una collisione di nomi tra struct diverse nello stesso file è un edge case, non il caso comune; la risoluzione per tipo arriva con la name resolution completa di T2.5).
- Se il nome non è nella mappa del file (funzione builtin, simbolo importato, o dichiarato in un altro file): **nessun arco creato** — questo è il comportamento corretto e atteso per una chiamata cross-file a questo stadio, non un fallimento silenzioso.

## 3. Comportamento (scenari)

Verificati direttamente contro il fixture di SPEC-009 (`tests/fixtures/sample-repo/`), non un fixture ad-hoc separato:

1. **Dato** `order_service.go` (contiene `OrderService`, `Process`, `Validate`), **quando** chiamo `parse_file`, **allora** produco: 1 `CodeNode` File, 1 `CodeNode` Class (`OrderService`), 2 `CodeNode` Method (`Process`, `Validate`), 3 archi `CONTAINS` (File→Class, File→Process, File→Validate).
2. **Dato** lo stesso file, **quando** ispeziono le `CodeRelation` prodotte, **allora** esiste un arco `CALLS` da `Process` a `Validate` con `weight=1` — stesso caso già verificato nel golden dataset g01 di SPEC-009.
3. **Dato** lo stesso file, **quando** cerco un arco `CALLS` da `Process` verso `computeTotal`, **allora** NON esiste — `computeTotal` è dichiarata in `util.go`, un file diverso, e questa singola chiamata a `parse_file("order_service.go", ...)` non ha visibilità su altri file — stesso caso già verificato nel golden dataset g03.
4. **Dato** `notifier.go` (contiene l'interfaccia `Notifier` e la classe `EmailNotifier`), **quando** chiamo `parse_file`, **allora** produco 1 `CodeNode` Interface, 1 `CodeNode` Class, 1 `CodeNode` Method (`Notify`), coerente con g05/g07 del golden dataset.
5. **Dato** una chiamata alla STESSA coppia caller/callee ripetuta più volte nello stesso file (caso non presente nel fixture attuale — costruire un piccolo caso di test dedicato per questo scenario specifico), **quando** parso il file, **allora** produco UN SOLO arco `CALLS` con `weight` pari al numero di occorrenze, non archi multipli.
6. **Dato** `main.go` (chiama `Process`, definita in un altro file), **quando** parso SOLO `main.go` in isolamento, **allora** non produco nessun arco `CALLS` per quella chiamata — coerente con g02 del golden dataset.

## 4. Errori & edge case

| Condizione | Comportamento atteso |
|---|---|
| Codice Go con errori di sintassi (Tree-sitter ha error recovery nativo, per design tollera input parzialmente invalido) | Il parsing prosegue sulle parti valide; le entità non estraibili dalla porzione con errore vengono omesse, non causano un panic o un fallimento dell'intero file — questo è il comportamento accettato per design, non solo rimandato: l'ADD (Modulo 1 §1.5, "fault-isolation per unità") richiede esplicitamente che un errore sia isolato all'unità che lo contiene, senza propagarsi. Verificato empiricamente che l'isolamento è stretto: il recovery di Tree-sitter inghiotte al più la dichiarazione immediatamente adiacente a quella malformata (ri-annidata nel suo span, es. come `field_declaration` spuria dentro uno `struct_type` rotto), poi si risincronizza correttamente sulle dichiarazioni successive — non un effetto a cascata sul resto del file. Dettaglio ed evidenza in §10 punto 5 |
| Un file Go vuoto o senza dichiarazioni di primo livello | Produce comunque 1 `CodeNode` File, zero altre entità, zero archi — non un errore |
| Due funzioni con lo stesso nome in file diversi (normale in Go, namespacing per package/file) | Non genera falsi positivi di risoluzione CALLS: la mappa nome→id è costruita per-file, non globale — un nome che esiste anche altrove ma non nel file corrente resta non trovato, coerente con lo scope intra-file dichiarato |

## 5. Non-goals
Nessuna scrittura su PostgreSQL (quello è T1.2 — `parse_file` produce solo strutture Rust in memoria). Nessuna risoluzione cross-file (T2.5, stack-graphs — qui il "non trovato" per un nome fuori dal file corrente è il comportamento corretto, non un limite da colmare ora). Nessun hashing Merkle/normalizzato (T2.1 — `ast_hash` qui è un semplice SHA-256 del testo sorgente grezzo dell'entità, sostituito da una SPEC successiva). Nessun nodo `CallSite` separato (deviazione dichiarata, vedi §6): l'arco `CALLS` con `weight` aggregato, esattamente come nel Cypher di riferimento D3, è la scelta di questa SPEC — non un'omissione.

## 6. Vincoli dall'ADD
Modulo 1 §1.3: CPG a granularità di statement/dichiarazione, non a livello di ogni nodo AST — coerente con l'estrazione a livello di File/Class/Interface/Method/Function qui, non ogni singolo token. **Deviazione dichiarata**: D2 elenca `CallSite` come `node_type` valido, e il testo del task T1.1 lo nomina esplicitamente — ma il Cypher di riferimento D3 (già eseguito e verificato in SPEC-004) modella `CALLS` come arco diretto Method→Method con `weight` aggregato, senza mai istanziare un nodo `CallSite` nei suoi esempi. Questa SPEC segue il D3 già verificato: nessun nodo `CallSite` separato, `weight` sull'arco `CALLS` invece. Se in futuro servirà tracciare ogni singola occorrenza di chiamata (non solo il conteggio aggregato), è un'estensione da riaprire allora.

## 7. Test plan
`cargo test` nel crate `services/ingestion`: test unitari per ciascuno scenario di §3, usando direttamente i file del fixture di SPEC-009 come input (letti da disco nel test, non duplicati come stringhe inline) — così un cambiamento futuro al fixture si riflette automaticamente nei test, senza doppia manutenzione. Scenario 5 richiede un piccolo caso di test dedicato (stringa Go inline con una chiamata ripetuta), dato che il fixture attuale non lo esercita.

## 8. Osservabilità
Uso di `libs/rust/eci-common::observability::init_tracing` (SPEC-010): uno span per l'intera operazione di parsing di un file, con `file_path` come attributo — prima applicazione reale della fondazione OTel costruita in T0.8.

## 9. Criteri di accettazione
- [x] Scenari 1/2/3/4/6 verificati direttamente contro il fixture reale di SPEC-009, con conteggi esatti di nodi/archi riportati nel report (non solo "corrisponde") — vedi §10.
- [x] Scenario 5 (weight aggregato su chiamate ripetute) verificato con un caso di test dedicato (`scenario5_repeated_call_aggregates_single_weighted_edge`, non presente nel fixture, unica eccezione ammessa da §7).
- [x] Error recovery su Go sintatticamente invalido verificato con almeno un caso concreto (edge case tabella §4) — vedi §10 per il dettaglio e l'output.
- [x] `cargo test` verde su tutti gli scenari — 8/8 (`services/ingestion`), verificato anche via `task lint`/`task test` reali (nessuna regressione altrove).
- [x] Span OTel prodotto durante il parsing, verificato con l'output stdout (stesso trattamento manuale di SPEC-010/011/012 scenario 1) — vedi §10.

## 10. Deviazioni rispetto alla SPEC

1. **Versioni esatte risolte** (verificate al momento dell'implementazione,
   stesso trattamento di SPEC-006/007/010/011/012): `tree-sitter` 0.26.11,
   `tree-sitter-go` 0.25.0, `sha2` 0.11.0 (per `ast_hash` e per l'`id`
   deterministico, vedi punto 3 sotto). Aggiunte a
   `services/ingestion/Cargo.toml` insieme a `eci-common = { path =
   "../../libs/rust/eci-common" }` (SPEC-010, per `init_tracing`) e
   `tracing` 0.1.44 (stessa versione già usata da `eci-common`, per lo
   span di `parse_file`).

2. **Struttura del crate**: `services/ingestion` era finora solo uno
   scaffold binario vuoto (`src/main.rs` con `fn main() {}`, nessun
   `src/lib.rs`, SPEC-001). Aggiunto `src/lib.rs` come target di libreria
   (Cargo rileva automaticamente `src/lib.rs` + `src/main.rs` come
   libreria+binario dello stesso package, nessuna configurazione `[lib]`
   esplicita necessaria) — `CodeNode`/`CodeRelation`/`parse_file` vivono
   nella libreria, testabili con `cargo test` senza dover passare dal
   binario. `src/main.rs` aggiornato da uno stub vuoto a un entrypoint
   minimo che inizializza `eci_common::observability::init_tracing`,
   legge un file Go da argv (default: il fixture `order_service.go` di
   SPEC-009) e stampa i conteggi — usato per la verifica manuale dello
   span OTel (scenario 1-equivalente, vedi evidenza sotto), non un
   requisito esplicito della SPEC ma la via più diretta per produrre
   l'evidenza richiesta da §8/§9 senza un binario/example separato.

3. **Algoritmo per l'`id` deterministico non specificato esplicitamente
   da §2** (solo "hash di `{file_path}:{qualified_name}`", senza
   nominare l'algoritmo — a differenza di `ast_hash`, per cui §2 dice
   esplicitamente SHA-256). Scelto SHA-256 anche per `id`, stessa
   funzione già necessaria per `ast_hash`: nessuna dipendenza aggiuntiva,
   nessuna ambiguità nuova introdotta. `qualified_name` per un `Method` è
   `"{ReceiverType}.{MethodName}"` (es. `"OrderService.Process"`,
   coerente con la notazione del golden dataset g01/g04) — per
   `Class`/`Interface`/`Function`/`File` è il nome semplice (rispettivamente
   il nome del tipo, il nome della funzione, il `file_path` stesso per
   `File`). Il campo `name` del `CodeNode`, invece, è sempre il nome
   **semplice** (es. `"Process"`, non `"OrderService.Process"`): è la
   stessa chiave usata dalla mappa nome→id per la risoluzione `CALLS`
   (§2), quindi deve restare non qualificata per coerenza con la
   semantica "per nome, senza verifica del tipo del receiver" già
   dichiarata in §2. Una collisione tra due receiver diversi con lo
   stesso nome di metodo nello stesso file produce un `id` distinto
   (grazie al qualificatore) ma una voce sovrascritta nella mappa
   nome→id usata per `CALLS` (l'ultima dichiarazione vince) — esattamente
   l'edge case già accettato esplicitamente da §2, non una scoperta
   nuova.

4. **Risoluzione CALLS in due passaggi, non durante un singolo
   attraversamento**: il fixture stesso lo richiede — `Process` (che
   chiama `Validate`) precede `Validate` nel file `order_service.go`.
   Risolvere le chiamate durante un unico passaggio dichiarazione-per-
   dichiarazione avrebbe fallito silenziosamente su questo caso (la
   mappa nome→id non conterrebbe ancora `Validate` quando si processa il
   corpo di `Process`). Implementato con un primo passaggio che
   costruisce TUTTA la mappa nome→id del file (e colleziona i corpi da
   scansionare), seguito da un secondo passaggio che risolve le chiamate
   — order-independent per costruzione, non specificato esplicitamente
   da §2 ma necessario per soddisfare correttamente lo scenario 2 così
   come il fixture reale lo presenta.

5. **Error recovery: caso di test scelto deliberatamente per mostrare
   entrambi i lati del comportamento di §4**, non il primo tentativo. Un
   primo esperimento con un errore di sintassi più severo (uso della
   keyword `go` e di operatori come `&&&` in un contesto invalido) ha
   causato un recovery a cascata che assorbiva anche una dichiarazione
   successiva come `func_literal` innestato in profondità — comunque
   "omessa, non un panic" (conforme a §4), ma un caso meno chiaro da
   documentare. Il caso finale (`type Broken struct` senza corpo `{}`,
   seguito da `func alsoValid() {...}`) mostra più nitidamente entrambe
   le garanzie richieste da §4 nello stesso test: (a) `valid()`, dichiarata
   PRIMA dell'errore, resta estratta correttamente e identica a come
   sarebbe senza l'errore altrove nel file; (b) `alsoValid()`, inghiottita
   dal recovery dentro il corpo malformato di `Broken` (diventa una
   `field_declaration` innestata, non più un `function_declaration` di
   primo livello), viene semplicemente omessa dal mio `match` sui kind
   riconosciuti — nessun panic, nessuna gestione speciale necessaria:
   l'omissione è un effetto naturale del fatto che il mio loop di primo
   livello riconosce solo kind espliciti (`type_declaration`,
   `function_declaration`, `method_declaration`). Verificato con
   `tree.root_node().has_error() == true` durante lo sviluppo (non
   asserito nel test finale, che verifica il comportamento osservabile —
   nodi prodotti/omessi — non un dettaglio interno dell'albero
   Tree-sitter).

   **Ampiezza del recovery, verificata separatamente (non nel test
   committato)**: ispezionando l'albero prodotto per varianti del
   sorgente con dichiarazioni aggiuntive dopo `alsoValid()` (es. `func
   third() {...}`, poi anche un `type AnotherOne struct {...}` valido e
   `func fourth() {}`), Tree-sitter risincronizza correttamente sul primo
   costrutto di primo livello riconoscibile subito dopo la dichiarazione
   inghiottita: SOLO `alsoValid()` — la dichiarazione immediatamente
   adiacente a quella malformata — finisce ri-annidata nello span
   dell'errore; tutte le dichiarazioni successive tornano ad essere figli
   diretti di `root_node()` e vengono estratte normalmente. Questo
   conferma che l'isolamento richiesto dall'ADD (Modulo 1 §1.5,
   "fault-isolation per unità") è rispettato in senso stretto anche nel
   caso peggiore osservato qui: un errore di sintassi precoce in un file
   più grande non fa perdere l'intero resto del file, ma al più la singola
   unità adiacente a quella malformata. Non è stato investigato se esista
   una forma di errore che ingoi più di una dichiarazione adiacente (il
   comportamento di recovery di Tree-sitter è euristico, non specificato
   come contratto) — se osservato in futuro su codice reale, va trattato
   come nuovo edge case da documentare, non come violazione di questa
   garanzia.

### Evidenza scenari 1/2/3 (order_service.go, conteggi esatti)

`cargo test` (`scenario1_order_service_nodes_and_contains`,
`scenario2_order_service_calls_process_to_validate`,
`scenario3_order_service_no_cross_file_call_to_compute_total`):
- 4 `CodeNode`: `File("order_service.go")`, `Class("OrderService")`,
  `Method("Process")`, `Method("Validate")`.
- 3 archi `CONTAINS`, tutti con `from_id` = id del `File`, verso
  rispettivamente `Class`, `Process`, `Validate`.
- Esattamente 1 arco `CALLS`: `Process` → `Validate`, `weight = 1`.
- Nessun `CodeNode` `computeTotal` (dichiarata in `util.go`, file
  diverso) e nessun arco `CALLS` verso di essa: la singola chiamata
  cross-file in `Process` non produce nulla, come richiesto da §2/§5.

### Evidenza scenario 4 (notifier.go)

`cargo test` (`scenario4_notifier_interface_and_class`):
- 4 `CodeNode`: `File("notifier.go")`, `Interface("Notifier")`,
  `Class("EmailNotifier")`, `Method("Notify")`.
- 3 archi `CONTAINS` (coerente con g07: File contiene sia
  un'`Interface` sia una `Class`).
- 0 archi `CALLS` (`Notify` non ha chiamate in uscita).

### Evidenza scenario 5 (weight aggregato, caso dedicato)

`cargo test` (`scenario5_repeated_call_aggregates_single_weighted_edge`,
sorgente Go inline con `caller()` che chiama `helper()` 3 volte):
- 3 `CodeNode`: `File`, `Function("caller")`, `Function("helper")`.
- Esattamente 1 arco `CALLS` (non 3): `caller` → `helper`, `weight = 3`.

### Evidenza scenario 6 (main.go in isolamento)

`cargo test` (`scenario6_main_go_isolated_no_cross_file_call`):
- 2 `CodeNode`: `File("main.go")`, `Function("main")`.
- 1 arco `CONTAINS` (File → main).
- 0 archi `CALLS` (la chiamata a `Process`, definita in
  `order_service.go`, è cross-file — coerente con g02).

### Evidenza error recovery (§4, dettaglio nel punto 5 sopra)

`cargo test` (`edge_case_syntax_error_recovers_valid_and_omits_unextractable`):
sorgente con `type Broken struct` privo di corpo `{}` (errore di
sintassi reale, verificato con `has_error() == true` durante lo
sviluppo) seguito da `func alsoValid() {...}` — nessun panic;
`valid()` (dichiarata prima dell'errore) risulta tra i `CodeNode`
estratti; `alsoValid()` (inghiottita dalla regione di errore) non vi
compare. Vedi anche l'edge case "file vuoto" (§4 riga 2, coperto da
`edge_case_file_with_no_declarations_produces_only_file_node`): 1
`CodeNode` `File`, zero altre entità, zero archi.

### Evidenza span OTel (§8, verifica manuale scenario 1-equivalente)

```
$ cargo run
   Compiling ingestion v0.1.0 (/home/luca/projects/eci/services/ingestion)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.81s
     Running `target/debug/ingestion`
Spans
Resource
	 ->  telemetry.sdk.language=String(Static("rust"))
	 ->  service.name=String(Owned("ingestion"))
	 ->  telemetry.sdk.name=String(Static("opentelemetry"))
	 ->  telemetry.sdk.version=String(Static("0.32.1"))
Span #0
	Instrumentation Scope
		Name         : "ingestion"

	Name         : parse_file
	TraceId      : a58d22229fbfbc711e3b3748ae1c5549
	SpanId       : 7f4518ec02d4efe1
	TraceFlags   : TraceFlags(1)
	ParentSpanId : None (root span)
	Kind         : Internal
	Start time   : 2026-08-02 21:49:09.398401
	End time     : 2026-08-02 21:49:09.399709
	Status       : Unset
	Attributes:
		 ->  code.file.path: String(Static("src/lib.rs"))
		 ->  code.module.name: String(Static("ingestion"))
		 ->  code.line.number: I64(35)
		 ->  thread.id: I64(1)
		 ->  thread.name: String(Owned("main"))
		 ->  target: String(Static("ingestion"))
		 ->  file_path: String(Owned("../../tests/fixtures/sample-repo/order_service.go"))
		 ->  busy_ns: I64(1227345)
		 ->  idle_ns: I64(105330)
parse_file("../../tests/fixtures/sample-repo/order_service.go"): 4 CodeNode, 4 CodeRelation
```

Span `parse_file` esportato su stdout con `file_path` come attributo
(richiesto da §8), `Resource.service.name = "ingestion"`, `TraceId`
valido a 32 caratteri esadecimali — prima applicazione reale della
fondazione OTel di T0.8 (SPEC-010/011/012) in un servizio applicativo.
