//! SPEC-029 §7 — test di integrazione: `postgres:17` via testcontainers,
//! migration reali applicate col CLI `migrate`, stesso pattern di
//! `tests/persist_integration_test.rs` (SPEC-014) — qui per il cablaggio
//! del chunking cAST (T2.2, SPEC-021) dentro `parse_*_file_full` e
//! `persist_parsed_file` (SPEC-029). Il comportamento DELETE+INSERT
//! transazionale su `code_chunk` è esattamente il tipo di cosa che un test
//! puramente unitario non verificherebbe realisticamente (§7).
//!
//! Esecuzione manuale (richiede Docker e il binario `migrate` sul PATH):
//! `cargo test --test chunk_persist_integration_test -- --ignored --test-threads=1`

use ingestion::chunking::CodeChunk;
use ingestion::{
    parse_file_full, persist_parsed_file as persist_scoped, CodeNode, CodeRelation,
    scoped_node_id, IngestionScope, PersistError, PersistSummary,
};
use postgres::{Client, NoTls};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;

const DB_USER: &str = "eci";
const DB_PASSWORD: &str = "eci-test-password-1234";
const DB_NAME: &str = "eci";

fn default_scope() -> IngestionScope {
    IngestionScope::new("tenant-test", "sample-repo", "developers").unwrap()
}

fn persisted_id(parser_id: &str) -> String {
    scoped_node_id(&default_scope(), parser_id)
}

fn persist_parsed_file(
    client: &mut Client,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<PersistSummary, PersistError> {
    let scope = default_scope();
    persist_scoped(client, &scope, nodes, relations, chunks)
}

fn start_migrated_postgres() -> (Container<PostgresImage>, Client) {
    if which("migrate").is_none() {
        panic!(
            "binario 'migrate' non trovato sul PATH: go install -tags 'postgres' \
             github.com/golang-migrate/migrate/v4/cmd/migrate@latest, poi verifica \
             che $(go env GOPATH)/bin sia sul PATH (stesso requisito di \
             tests/persist_integration_test.rs, SPEC-014)."
        );
    }

    let container = PostgresImage::default()
        .with_db_name(DB_NAME)
        .with_user(DB_USER)
        .with_password(DB_PASSWORD)
        .with_tag("17")
        .start()
        .expect("avvio container postgres:17");

    let host = container.get_host().expect("host container");
    let port = container
        .get_host_port_ipv4(5432)
        .expect("porta pubblicata 5432/tcp");
    let dsn =
        format!("postgres://{DB_USER}:{DB_PASSWORD}@{host}:{port}/{DB_NAME}?sslmode=disable");

    run_migrate_cli(&dsn, "up");

    let client = Client::connect(&dsn, NoTls).expect("connessione al postgres di test");
    (container, client)
}

fn run_migrate_cli(dsn: &str, direction: &str) {
    let migrations_dir = format!(
        "{}/../../contracts/sql/migrations",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut args = vec![
        "-source".to_string(),
        format!("file://{migrations_dir}"),
        "-database".to_string(),
        dsn.to_string(),
        direction.to_string(),
    ];
    if direction == "down" {
        args.push("1".to_string());
    }
    let output = std::process::Command::new("migrate")
        .args(&args)
        .output()
        .expect("esecuzione del CLI 'migrate'");
    assert!(
        output.status.success(),
        "migrate {direction} fallito:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn which(bin: &str) -> Option<String> {
    let path = std::env::var("PATH").ok()?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|candidate| candidate.is_file())
        .map(|p| p.display().to_string())
}

fn read_fixture(name: &str) -> String {
    let path = format!(
        "{}/../../tests/fixtures/sample-repo/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
}

fn read_js_fixture(name: &str) -> String {
    let path = format!(
        "{}/../../tests/fixtures/sample-repo-js/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
}

fn count(client: &mut Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .unwrap_or_else(|e| panic!("count({table}): {e}"))
        .get(0)
}

fn chunk_count_for_entity(client: &mut Client, entity_id: &str) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM code_chunk WHERE entity_id = $1",
            &[&entity_id],
        )
        .unwrap_or_else(|e| panic!("count(code_chunk) per entity_id={entity_id}: {e}"))
        .get(0)
}

fn find_node_id(nodes: &[ingestion::CodeNode], node_type: &str, name: &str) -> String {
    nodes
        .iter()
        .find(|n| n.node_type == node_type && n.name == name)
        .unwrap_or_else(|| panic!("nodo {node_type} {name:?} non trovato in {nodes:?}"))
        .id
        .clone()
}

// --- SPEC-029 §3 scenari 1, 2, 4 — sequenziali sullo stesso DB (stesso
// principio di persist_integration_test.rs): Validate (order_service.go)
// sta in un solo chunk col budget di default (1500).
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-029 §7); esclusa da cargo test di default"]
fn chunk_persist_scenarios_1_2_4_validate_single_chunk_and_outbox() {
    let (_container, mut client) = start_migrated_postgres();

    let _tracing_guard = eci_common::observability::init_tracing("ingestion-chunk-persist-test");
    let outer_span = tracing::info_span!("scenario1_persist_chunks_order_service");
    let _enter = outer_span.enter();
    let want_trace_id = eci_common::observability::current_trace_id_hex()
        .expect("trace_id atteso Some dentro lo span attivo di questo test");

    // --- Scenario 1: primo persist, un solo code_chunk per Validate. ---
    let source = read_fixture("order_service.go");
    let (nodes, relations, chunks) = parse_file_full("order_service.go", &source);
    let validate_id = find_node_id(&nodes, "Method", "Validate");
    let persisted_validate_id = persisted_id(&validate_id);

    let validate_chunks: Vec<&CodeChunk> = chunks.iter().filter(|c| c.entity_id == validate_id).collect();
    assert_eq!(
        validate_chunks.len(),
        1,
        "precondizione: Validate deve produrre un solo CodeChunk col budget di default: {validate_chunks:?}"
    );

    let summary = persist_parsed_file(&mut client, nodes.clone(), relations.clone(), &chunks)
        .expect("persist_parsed_file scenario 1");
    assert!(
        summary.outbox_rows_written > 0,
        "scenario 1/4: outbox_rows_written deve includere anche le righe CodeChunk"
    );

    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_validate_id),
        1,
        "scenario 1: esattamente una riga code_chunk per Validate"
    );

    // --- Scenario 4: riga outbox aggregate_type='CodeChunk' con lo stesso trace_id della tx. ---
    let chunk_outbox_rows: Vec<(String, Option<String>)> = client
        .query(
            "SELECT aggregate_id, trace_id FROM outbox WHERE aggregate_type = 'CodeChunk'",
            &[],
        )
        .expect("query outbox CodeChunk")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert!(
        !chunk_outbox_rows.is_empty(),
        "scenario 4: attesa almeno una riga outbox con aggregate_type='CodeChunk'"
    );
    assert!(
        chunk_outbox_rows.iter().all(|(_, tid)| *tid == Some(want_trace_id.clone())),
        "scenario 4: tutte le righe outbox CodeChunk devono condividere il trace_id della transazione: {chunk_outbox_rows:?}"
    );

    // --- SPEC-032 §3 scenario 1: il payload outbox del CodeChunk di
    // Validate include provenance:{"path": "order_service.go"}, coerente
    // col provenance del CodeNode di Validate stesso. ---
    let validate_chunk_payload: serde_json::Value = client
        .query_one(
            "SELECT payload FROM outbox WHERE aggregate_type = 'CodeChunk' AND payload->>'entity_id' = $1",
            &[&persisted_validate_id],
        )
        .expect("query outbox CodeChunk per il chunk di Validate")
        .get(0);
    assert_eq!(
        validate_chunk_payload.get("provenance"),
        Some(&serde_json::json!({
            "path": "order_service.go",
            "tenant_id": "tenant-test",
            "repo": "sample-repo",
            "acl_group": "developers"
        })),
        "SPEC-032 scenario 1: provenance del CodeChunk di Validate deve combaciare col path del suo CodeNode: {validate_chunk_payload:?}"
    );

    // --- Scenario 2: ri-parso e ri-persisto SENZA modifiche -> il conteggio resta lo stesso. ---
    let (nodes2, relations2, chunks2) = parse_file_full("order_service.go", &source);
    let _summary2 = persist_parsed_file(&mut client, nodes2, relations2, &chunks2)
        .expect("persist_parsed_file scenario 2");
    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_validate_id),
        1,
        "scenario 2: il conteggio code_chunk per Validate deve restare 1 (vecchie cancellate, nuove identiche inserite, non duplicate)"
    );
}

// --- SPEC-029 §3 scenario 3: NUMERO diverso di chunk tra un parse e
// l'altro (budget artificialmente basso, come suggerito esplicitamente
// dalla SPEC) -> il conteggio riflette il NUOVO numero, non la somma. DB
// indipendente (manipola una env var globale al processo, stesso
// principio già adottato in chunking.rs — nessun altro test la legge in
// parallelo grazie a --test-threads=1, SPEC-029 §7).
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-029 §7); esclusa da cargo test di default"]
fn chunk_persist_scenario3_repersist_with_different_chunk_count_replaces_not_sums() {
    let (_container, mut client) = start_migrated_postgres();

    let source = read_fixture("order_service.go");

    // Primo parse+persist: budget molto basso -> Validate si spezza in
    // più chunk.
    std::env::set_var("CHUNK_BUDGET_CHARS_GO", "20");
    let (nodes, relations, chunks) = parse_file_full("order_service.go", &source);
    let validate_id = find_node_id(&nodes, "Method", "Validate");
    let persisted_validate_id = persisted_id(&validate_id);
    let first_count = chunks.iter().filter(|c| c.entity_id == validate_id).count();
    assert!(
        first_count > 1,
        "precondizione: budget 20 deve spezzare Validate in più di un chunk, ottenuti {first_count}"
    );
    persist_parsed_file(&mut client, nodes, relations, &chunks)
        .expect("persist_parsed_file scenario 3 (prima versione)");
    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_validate_id),
        first_count as i64,
        "dopo il primo persist, il conteggio deve combaciare col numero di chunk prodotti"
    );

    // Secondo parse+persist: budget generoso -> Validate torna a essere UN
    // solo chunk. Il conteggio finale deve essere 1 (sostituito), non
    // first_count + 1 (sommato).
    std::env::set_var("CHUNK_BUDGET_CHARS_GO", "1500");
    let (nodes2, relations2, chunks2) = parse_file_full("order_service.go", &source);
    let second_count = chunks2.iter().filter(|c| c.entity_id == validate_id).count();
    assert_eq!(second_count, 1, "precondizione: budget generoso -> un solo chunk");
    persist_parsed_file(&mut client, nodes2, relations2, &chunks2)
        .expect("persist_parsed_file scenario 3 (seconda versione)");

    std::env::remove_var("CHUNK_BUDGET_CHARS_GO");

    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_validate_id),
        1,
        "scenario 3: il conteggio deve riflettere il NUOVO numero di chunk (1), non la somma (first_count + 1)"
    );
}

// --- SPEC-029 §3 scenario 5: entità JS/TS (non solo Go) -> stesso
// comportamento, verifica diretta che il cablaggio non sia specifico a un
// solo linguaggio.
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-029 §7); esclusa da cargo test di default"]
fn chunk_persist_scenario5_js_entity_same_behavior_as_go() {
    let (_container, mut client) = start_migrated_postgres();

    let source = read_js_fixture("util.js");
    let (nodes, relations, chunks) = parse_file_full("util.js", &source);
    let compute_total_id = find_node_id(&nodes, "Function", "computeTotal");
    let persisted_compute_total_id = persisted_id(&compute_total_id);

    let entity_chunks: Vec<&CodeChunk> = chunks.iter().filter(|c| c.entity_id == compute_total_id).collect();
    assert_eq!(
        entity_chunks.len(),
        1,
        "computeTotal deve produrre un solo CodeChunk col budget di default: {entity_chunks:?}"
    );

    persist_parsed_file(&mut client, nodes, relations, &chunks)
        .expect("persist_parsed_file scenario 5 (JS)");

    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_compute_total_id),
        1,
        "scenario 5: il cablaggio deve funzionare identico per un'entità JS"
    );
}

// --- SPEC-029 §4 edge case: entità con zero figli/conteggio zero (source
// Go completamente vuoto -> il solo File node, radice senza figli) deve
// comunque produrre UNA riga code_chunk (char_count 0), non zero righe.
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-029 §7); esclusa da cargo test di default"]
fn chunk_persist_edge_case_zero_count_entity_writes_single_chunk_row() {
    let (_container, mut client) = start_migrated_postgres();

    let (nodes, relations, chunks) = parse_file_full("empty.go", "");
    assert_eq!(nodes.len(), 1, "precondizione: source vuoto -> solo il File node");
    let file_id = nodes[0].id.clone();
    let persisted_file_id = persisted_id(&file_id);
    assert_eq!(
        chunks.len(),
        1,
        "precondizione: il File con radice senza figli deve produrre esattamente un CodeChunk: {chunks:?}"
    );
    assert_eq!(chunks[0].char_count, 0);

    persist_parsed_file(&mut client, nodes, relations, &chunks)
        .expect("persist_parsed_file edge case zero-count");

    assert_eq!(
        chunk_count_for_entity(&mut client, &persisted_file_id),
        1,
        "edge case: una riga code_chunk deve comunque essere scritta, non un caso speciale saltato"
    );
}

// --- SPEC-029 §4 edge case: un errore a metà transazione (qui: un chunk
// con entity_id che non corrisponde a NESSUN code_node, violazione FK)
// deve fare rollback dell'INTERA transazione — nessuna scrittura parziale
// di code_node/code_relation/code_chunk/outbox, stesso principio già
// stabilito in persist_integration_test.rs scenario 4 (SPEC-014).
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-029 §7); esclusa da cargo test di default"]
fn chunk_persist_edge_case_rollback_on_forced_mid_transaction_chunk_failure() {
    let (_container, mut client) = start_migrated_postgres();

    let source = read_fixture("main.go");
    let (nodes, relations, _chunks) = parse_file_full("main.go", &source);

    let broken_chunks = vec![CodeChunk {
        entity_id: "scenario-edge-entity-id-non-esistente".to_string(),
        chunk_index: 0,
        text: "qualunque".to_string(),
        char_count: 9,
    }];

    let result = persist_parsed_file(&mut client, nodes, relations, &broken_chunks);
    assert!(
        result.is_err(),
        "il DELETE/INSERT su code_chunk con entity_id inesistente deve fallire per violazione FK"
    );

    assert_eq!(count(&mut client, "code_node"), 0, "rollback completo: nessun CodeNode residuo");
    assert_eq!(count(&mut client, "code_relation"), 0, "rollback completo: nessuna CodeRelation residua");
    assert_eq!(count(&mut client, "code_chunk"), 0, "rollback completo: nessun CodeChunk residuo");
    assert_eq!(count(&mut client, "outbox"), 0, "rollback completo: nessuna riga outbox residua");
}
