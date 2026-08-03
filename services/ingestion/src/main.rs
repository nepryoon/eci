//! Entrypoint di `services/ingestion`: parsa un file Go passato come
//! argomento (o il fixture di SPEC-009 di default), poi persiste il
//! risultato su PostgreSQL dentro un'unica transazione ACID (SPEC-014,
//! T1.2 — `persist_parsed_file`). Inizializza l'OTel bootstrap di
//! `eci-common` (SPEC-010) prima del parsing, così lo span aperto da
//! `parse_file` E quello aperto da `persist_parsed_file` vengono
//! esportati su stdout, e le righe `outbox` prodotte condividono il
//! `trace_id` di questa esecuzione (SPEC-014 §8).
//!
//! DSN letto da `POSTGRES_DSN` via `eci_common::config::env_or_default`
//! (SPEC-014 §2 — "stesso pattern già stabilito"), stesso default già
//! usato da `task db:migrate` in `Taskfile.yml` per lo stack compose
//! locale.

fn main() {
    let _guard = eci_common::observability::init_tracing("ingestion");

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "../../tests/fixtures/sample-repo/order_service.go".to_string());
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("lettura di {path}: {e}"));

    let (nodes, relations) = ingestion::parse_file(&path, &source);
    println!(
        "parse_file({path:?}): {} CodeNode, {} CodeRelation",
        nodes.len(),
        relations.len()
    );

    let dsn = eci_common::config::env_or_default(
        "POSTGRES_DSN",
        "postgres://eci:eci-dev-only@localhost:5432/eci?sslmode=disable",
    );
    let mut client = postgres::Client::connect(&dsn, postgres::NoTls)
        .unwrap_or_else(|e| panic!("connessione a Postgres ({dsn}): {e}"));

    let summary = ingestion::persist_parsed_file(&mut client, nodes, relations)
        .unwrap_or_else(|e| panic!("persist_parsed_file({path:?}): {e}"));
    println!(
        "persist_parsed_file({path:?}): {} nodi upsert, {} relazioni sostituite, {} righe outbox",
        summary.nodes_upserted, summary.relations_replaced, summary.outbox_rows_written
    );
}
