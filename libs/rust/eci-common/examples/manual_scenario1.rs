//! SPEC-010 §3 scenario 1 — verifica manuale (non automatizzabile in modo
//! pulito senza parsing dell'output, §7): `init_tracing` deve produrre
//! output leggibile su stdout per uno span reale, non solo compilare.
//!
//! Uso: `cargo run --example manual_scenario1`

fn main() {
    let _guard = eci_common::observability::init_tracing("ingestion");

    let span = tracing::info_span!("ingest_commit", commit_sha = "abc1234");
    let _enter = span.enter();
    tracing::info!("elaborazione commit avviata");

    if let Some(trace_id) = eci_common::observability::current_trace_id_hex() {
        eprintln!("[manual check] trace_id dentro lo span: {trace_id}");
    } else {
        eprintln!("[manual check] ERRORE: nessun trace_id trovato dentro uno span attivo");
    }
}
