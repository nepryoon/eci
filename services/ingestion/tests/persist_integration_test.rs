//! SPEC-014 §7 — test di integrazione: `postgres:17` via testcontainers,
//! migration reali applicate col CLI `migrate` (lo stesso invocato da
//! `task db:migrate`), stesso pattern di
//! `tests/integration/postgres_ddl` (SPEC-005) e
//! `tests/integration/outbox_cdc` (SPEC-008) — qui in Rust, non Go,
//! perché `persist_parsed_file` vive in `services/ingestion` (crate
//! Rust). Gated con `#[ignore]` (SPEC-014 §10 — deviazione: Cargo non
//! ha un analogo diretto del build tag Go `-tags=integration`: i
//! dev-dependencies non supportano `optional`/feature-gating, quindi
//! `testcontainers`/`testcontainers-modules` vengono comunque compilati
//! da un `cargo test` semplice; `#[ignore]` è l'idioma Rust per escludere
//! comunque l'ESECUZIONE di default, che è la proprietà che conta per
//! `task test` — SPEC-001 §4 — nessuna dipendenza da Docker lì).
//!
//! Esecuzione manuale (richiede Docker e il binario `migrate` sul PATH):
//! `cargo test --test persist_integration_test -- --ignored --test-threads=1`
//! (`--test-threads=1`: i due `#[test]` avviano ciascuno il proprio
//! container Postgres indipendente — nessun bisogno di parallelismo qui,
//! ed evita di sommare il carico di due container Docker in parallelo).
//!
//! **Nota sui conteggi attesi (SPEC-014 §10, deviazione dichiarata)**:
//! §3 scenario 1 della SPEC dichiara "4 nodi, 1 relazione" per
//! `parse_file("order_service.go", ...)" — verificato empiricamente (e
//! coerente coi conteggi già asseriti nei test di SPEC-013, "verified")
//! che l'output reale è 4 nodi E 4 RELAZIONI (3 CONTAINS + 1 CALLS, non
//! 1): `scenario1_order_service_nodes_and_contains` in
//! `services/ingestion/src/lib.rs` attesta `contains.len() == 3`,
//! `scenario2_order_service_calls_process_to_validate` attesta 1 CALLS.
//! I conteggi qui sotto usano il valore reale (4 relazioni, quindi
//! `outbox_rows_written = 8`, non 5) — un `PersistSummary{relations_replaced: 1}`
//! sarebbe stato semplicemente falso rispetto a quello che `parse_file`
//! produce davvero.

use ingestion::{
    parse_file, persist_parsed_file as persist_scoped, CodeNode, CodeRelation,
    IngestionScope, PersistError, PersistSummary,
};
use postgres::{Client, NoTls};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;

const DB_USER: &str = "eci";
const DB_PASSWORD: &str = "eci-test-password-1234";
const DB_NAME: &str = "eci";

fn persist_parsed_file(
    client: &mut Client,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[ingestion::chunking::CodeChunk],
) -> Result<PersistSummary, PersistError> {
    let scope = IngestionScope::new("tenant-test", "sample-repo", "developers").unwrap();
    persist_scoped(client, &scope, nodes, relations, chunks)
}

/// Avvia `postgres:17` (stessa immagine di SPEC-005/SPEC-008, non il tag
/// default `11-alpine` del modulo `testcontainers-modules`) e applica le
/// migration reali di `contracts/sql/migrations` col CLI `migrate` prima
/// di ritornare una connessione pronta all'uso.
fn start_migrated_postgres() -> (Container<PostgresImage>, Client) {
    if which("migrate").is_none() {
        panic!(
            "binario 'migrate' non trovato sul PATH: go install -tags 'postgres' \
             github.com/golang-migrate/migrate/v4/cmd/migrate@latest, poi verifica \
             che $(go env GOPATH)/bin sia sul PATH (stesso requisito di \
             tests/integration/postgres_ddl, SPEC-005)."
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

fn count(client: &mut Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .unwrap_or_else(|e| panic!("count({table}): {e}"))
        .get(0)
}

/// Tuple `(rel_type, from_id, to_id, id)` di TUTTE le righe `code_relation`
/// correnti, ordinate deterministicamente — usato per confrontare
/// l'insieme logico delle relazioni (deve restare identico su run
/// ripetuti, scenario 2) separatamente dall'identità fisica delle righe
/// (deve cambiare, §4).
fn relation_rows(client: &mut Client) -> Vec<(String, String, String, String)> {
    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "SELECT rel_type, from_id, to_id, id::text FROM code_relation",
            &[],
        )
        .expect("query code_relation")
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    rows.sort();
    rows
}

// SPEC-014 §3 scenari 1, 2, 3, 5 — sequenziali sullo STESSO DB: ognuno si
// costruisce sullo stato lasciato dal precedente, esattamente come
// descritto in §3 ("la SECONDA volta", "una seconda versione del
// sorgente").
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-014 §7); esclusa da cargo test di default"]
fn persist_parsed_file_scenarios_1_2_3_5() {
    let (_container, mut client) = start_migrated_postgres();

    // init_tracing DEVE precedere la creazione dello span: installa il
    // layer OTel sul subscriber globale (SPEC-010) — uno span costruito
    // PRIMA non avrebbe un contesto OTel valido agganciato.
    let _tracing_guard = eci_common::observability::init_tracing("ingestion-persist-test");
    let outer_span = tracing::info_span!("scenario1_persist_order_service");
    let _enter = outer_span.enter();
    let want_trace_id = eci_common::observability::current_trace_id_hex()
        .expect("trace_id atteso Some dentro lo span attivo di questo test");

    // --- Scenario 1: DB vuoto, primo persist. ---
    let source = read_fixture("order_service.go");
    let (nodes, relations) = parse_file("order_service.go", &source);
    assert_eq!(nodes.len(), 4, "precondizione: 4 CodeNode da parse_file (SPEC-013 §3 scenario 1)");
    assert_eq!(
        relations.len(),
        4,
        "precondizione: 4 CodeRelation da parse_file (3 CONTAINS + 1 CALLS, SPEC-013 §3 scenari 1/2 — vedi nota in testa a questo file su SPEC-014 §3 scenario 1)"
    );

    let summary = persist_parsed_file(&mut client, nodes.clone(), relations.clone(), &[])
        .expect("persist_parsed_file scenario 1");

    assert_eq!(
        summary,
        PersistSummary {
            nodes_upserted: 4,
            relations_replaced: 4,
            outbox_rows_written: 8,
        },
        "scenario 1: conteggi del summary"
    );
    assert_eq!(count(&mut client, "code_node"), 4, "scenario 1: code_node");
    assert_eq!(
        count(&mut client, "code_relation"),
        4,
        "scenario 1: code_relation"
    );
    assert_eq!(count(&mut client, "outbox"), 8, "scenario 1: outbox");

    // --- Scenario 5: trace_id propagato, stesso per tutte le 8 righe. ---
    let trace_ids: Vec<Option<String>> = client
        .query("SELECT DISTINCT trace_id FROM outbox", &[])
        .expect("query trace_id distinti")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        trace_ids,
        vec![Some(want_trace_id.clone())],
        "scenario 5: tutte le righe outbox devono condividere lo stesso trace_id non-NULL \
         dello span attivo al momento della chiamata"
    );

    let rows_after_first = relation_rows(&mut client);

    // --- Scenario 2: stesso file, ripersistito senza modifiche. ---
    let summary2 = persist_parsed_file(&mut client, nodes.clone(), relations.clone(), &[])
        .expect("persist_parsed_file scenario 2");
    assert_eq!(
        summary2,
        PersistSummary {
            nodes_upserted: 4,
            relations_replaced: 4,
            outbox_rows_written: 8,
        },
        "scenario 2: conteggi del summary (identici allo scenario 1, nessun 'nessun cambiamento' rilevato, §5)"
    );
    assert_eq!(
        count(&mut client, "code_node"),
        4,
        "scenario 2: code_node NON deve duplicare (stessi id, ON CONFLICT)"
    );
    assert_eq!(
        count(&mut client, "code_relation"),
        4,
        "scenario 2: code_relation resta 4 righe (sostituite, non accumulate)"
    );

    let rows_after_second = relation_rows(&mut client);
    let logical_after_first: Vec<(String, String, String)> = rows_after_first
        .iter()
        .map(|(rt, f, t, _id)| (rt.clone(), f.clone(), t.clone()))
        .collect();
    let logical_after_second: Vec<(String, String, String)> = rows_after_second
        .iter()
        .map(|(rt, f, t, _id)| (rt.clone(), f.clone(), t.clone()))
        .collect();
    assert_eq!(
        logical_after_first, logical_after_second,
        "scenario 2: l'insieme LOGICO delle relazioni (rel_type, from_id, to_id) deve restare identico"
    );
    let ids_after_first: std::collections::HashSet<&String> =
        rows_after_first.iter().map(|(_, _, _, id)| id).collect();
    let ids_after_second: std::collections::HashSet<&String> =
        rows_after_second.iter().map(|(_, _, _, id)| id).collect();
    assert!(
        ids_after_first.is_disjoint(&ids_after_second),
        "scenario 2: ogni riga code_relation deve avere un nuovo id UUID fisicamente diverso, \
         comportamento accettato (SPEC-014 §4 — nessun vincolo di unicità naturale su code_relation): \
         prima={rows_after_first:?} dopo={rows_after_second:?}"
    );
    assert_eq!(
        count(&mut client, "outbox"),
        16,
        "scenario 2: 8 nuove righe outbox si sommano alle 8 dello scenario 1 (ogni run produce eventi, §5 non-goals)"
    );

    // --- Scenario 3: seconda versione del sorgente, Process non chiama più Validate. ---
    let source_without_call = source.replace("s.Validate(prices)", "nil");
    assert_ne!(
        source_without_call, source,
        "precondizione del test: la sostituzione deve aver trovato e modificato la call originale"
    );
    let (nodes3, relations3) = parse_file("order_service.go", &source_without_call);
    assert_eq!(
        relations3.len(),
        3,
        "precondizione del parsing: solo le 3 CONTAINS restano, l'arco CALLS deve sparire dal parsing stesso"
    );
    assert!(
        relations3.iter().all(|r| r.rel_type != "CALLS"),
        "precondizione del parsing: nessun arco CALLS atteso dalla nuova versione del sorgente"
    );
    let summary3 = persist_parsed_file(&mut client, nodes3, relations3, &[])
        .expect("persist_parsed_file scenario 3");
    assert_eq!(
        summary3.relations_replaced, 3,
        "scenario 3: 3 CodeRelation da persistere (solo le CONTAINS, la CALLS è sparita dal sorgente)"
    );
    assert_eq!(
        count(&mut client, "code_relation"),
        3,
        "scenario 3: l'arco CALLS precedente deve sparire, nessuna riga orfana (DELETE prima di INSERT)"
    );
    let remaining_rel_types: std::collections::HashSet<String> = relation_rows(&mut client)
        .into_iter()
        .map(|(rt, _, _, _)| rt)
        .collect();
    assert_eq!(
        remaining_rel_types,
        std::collections::HashSet::from(["CONTAINS".to_string()]),
        "scenario 3: le righe rimaste devono essere ESATTAMENTE le 3 CONTAINS, nessuna CALLS residua"
    );
}

// SPEC-014 §3 scenario 4 — DB indipendente: forziamo un fallimento reale
// a metà transazione (una CodeRelation con `to_id` inesistente, che viola
// la FK `code_relation.to_id REFERENCES code_node(id)` di SPEC-005) e
// verifichiamo che NESSUNA delle tre tabelle riporti modifiche parziali.
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-014 §7); esclusa da cargo test di default"]
fn persist_parsed_file_scenario4_rollback_on_forced_mid_transaction_failure() {
    let (_container, mut client) = start_migrated_postgres();

    let source = read_fixture("main.go");
    let (nodes, _relations) = parse_file("main.go", &source);

    // Relazione fabbricata deliberatamente rotta: `to_id` non corrisponde
    // a NESSUN CodeNode appena upsertato (né a nessuno già nel DB, che è
    // vuoto) — la FK su code_relation.to_id la respinge a metà
    // transazione, DOPO che i CodeNode e la prima riga outbox sono già
    // stati eseguiti (non committati) nella stessa transazione.
    let broken_relations = vec![CodeRelation {
        domain: "code".to_string(),
        rel_type: "CALLS".to_string(),
        from_id: nodes[0].id.clone(),
        to_id: "scenario4-to-id-non-esistente".to_string(),
        weight: Some(1),
    }];

    let result = persist_parsed_file(&mut client, nodes, broken_relations, &[]);
    assert!(
        result.is_err(),
        "scenario 4: persist_parsed_file deve fallire per violazione FK su to_id"
    );

    assert_eq!(
        count(&mut client, "code_node"),
        0,
        "scenario 4: rollback completo, nessun CodeNode residuo (nemmeno il File, upsertato PRIMA del fallimento)"
    );
    assert_eq!(
        count(&mut client, "code_relation"),
        0,
        "scenario 4: rollback completo, nessuna CodeRelation residua"
    );
    assert_eq!(
        count(&mut client, "outbox"),
        0,
        "scenario 4: rollback completo, nessuna riga outbox residua (nemmeno quelle scritte per i CodeNode, PRIMA del fallimento sulla relazione)"
    );
}

// T6.3 regression: parser identities are repository-local. Persisting the
// same logical file for two authenticated ownership scopes must not let the
// second writer relabel or delete the first scope's canonical rows.
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-057); esclusa da cargo test di default"]
fn same_parser_ids_remain_isolated_across_tenants() {
    let (_container, mut client) = start_migrated_postgres();
    let source = read_fixture("order_service.go");
    let (nodes, relations) = parse_file("order_service.go", &source);
    let tenant_a = IngestionScope::new("tenant-a", "shared-repo", "developers").unwrap();
    let tenant_b = IngestionScope::new("tenant-b", "shared-repo", "developers").unwrap();

    persist_scoped(&mut client, &tenant_a, nodes.clone(), relations.clone(), &[])
        .expect("persist tenant-a");
    persist_scoped(&mut client, &tenant_b, nodes, relations, &[])
        .expect("persist tenant-b");

    assert_eq!(count(&mut client, "code_node"), 8, "four nodes per tenant");
    assert_eq!(count(&mut client, "code_relation"), 8, "four relations per tenant");

    let rows = client
        .query(
            "SELECT id, provenance->>'tenant_id' FROM code_node ORDER BY id, provenance->>'tenant_id'",
            &[],
        )
        .expect("read scoped nodes");
    let tenant_a_ids: std::collections::HashSet<String> = rows
        .iter()
        .filter(|row| row.get::<_, String>(1) == "tenant-a")
        .map(|row| row.get(0))
        .collect();
    let tenant_b_ids: std::collections::HashSet<String> = rows
        .iter()
        .filter(|row| row.get::<_, String>(1) == "tenant-b")
        .map(|row| row.get(0))
        .collect();
    assert_eq!(tenant_a_ids.len(), 4);
    assert_eq!(tenant_b_ids.len(), 4);
    assert!(tenant_a_ids.is_disjoint(&tenant_b_ids));
}
