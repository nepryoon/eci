//! SPEC-027 §2 "Persistenza" — test di integrazione: `postgres:17` via
//! testcontainers, migration reali applicate col CLI `migrate` (stesso
//! pattern di `tests/persist_integration_test.rs`, SPEC-014). Non
//! richiesto esplicitamente da SPEC-027 §7 (che elenca solo
//! `detect_renames`/`reconcile_renamed_file`), ma `persist_lineage`
//! implementa comunque la garanzia transazionale descritta in §2
//! ("o tutte le righe di un rename sono scritte, o nessuna") — stessa
//! garanzia di `persist_parsed_file`, quindi verificata con lo stesso
//! rigore.
//!
//! Esecuzione manuale (richiede Docker e il binario `migrate` sul PATH):
//! `cargo test --test lineage_persist_integration_test -- --ignored --test-threads=1`

use ingestion::lineage::{persist_lineage, LineageLink};
use postgres::{Client, NoTls};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;

const DB_USER: &str = "eci";
const DB_PASSWORD: &str = "eci-test-password-1234";
const DB_NAME: &str = "eci";

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

fn count(client: &mut Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .unwrap_or_else(|e| panic!("count({table}): {e}"))
        .get(0)
}

fn link(old_node_id: &str, new_node_id: &str, ast_hash: &str) -> LineageLink {
    LineageLink {
        old_node_id: old_node_id.to_string(),
        new_node_id: new_node_id.to_string(),
        ast_hash: ast_hash.to_string(),
    }
}

// Happy path: N link di un rename scritti in blocco, `reason` di default
// 'rename' applicato dalla colonna (SPEC-027 §2).
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-027 §2); esclusa da cargo test di default"]
fn persist_lineage_writes_all_links_in_one_transaction() {
    let (_container, mut client) = start_migrated_postgres();

    let links = vec![
        link(
            &"a".repeat(64),
            &"b".repeat(64),
            &"c".repeat(64),
        ),
        link(
            &"d".repeat(64),
            &"e".repeat(64),
            &"f".repeat(64),
        ),
    ];

    let written = persist_lineage(&mut client, &links).expect("persist_lineage happy path");

    assert_eq!(written, 2, "due link scritti");
    assert_eq!(count(&mut client, "lineage"), 2, "due righe lineage");

    let reasons: Vec<String> = client
        .query("SELECT DISTINCT reason FROM lineage", &[])
        .expect("query reason")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        reasons,
        vec!["rename".to_string()],
        "default 'reason' applicato dalla colonna (SPEC-027 §2)"
    );
}

// Rollback: un `old_node_id`/`new_node_id`/`ast_hash` più lungo di 64
// caratteri viola il tipo colonna CHAR(64) a metà batch — nessuna delle
// righe precedenti nella stessa chiamata deve restare committata
// (SPEC-027 §2, stesso principio di SPEC-014 §3 scenario 4).
#[test]
#[ignore = "richiede Docker + 'migrate' sul PATH (SPEC-027 §2); esclusa da cargo test di default"]
fn persist_lineage_rolls_back_completely_on_forced_mid_transaction_failure() {
    let (_container, mut client) = start_migrated_postgres();

    let links = vec![
        link(&"a".repeat(64), &"b".repeat(64), &"c".repeat(64)),
        link("too-long-".repeat(10).as_str(), &"e".repeat(64), &"f".repeat(64)),
    ];

    let result = persist_lineage(&mut client, &links);

    assert!(
        result.is_err(),
        "old_node_id troppo lungo per CHAR(64) deve far fallire la transazione"
    );
    assert_eq!(
        count(&mut client, "lineage"),
        0,
        "rollback completo: nessuna riga residua, nemmeno la prima valida"
    );
}
