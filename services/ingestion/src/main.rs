//! Entrypoint minimale di `services/ingestion` (SPEC-013): parsa un file Go
//! passato come argomento (o il fixture di SPEC-009 di default) e stampa i
//! conteggi di CodeNode/CodeRelation prodotti. Inizializza l'OTel bootstrap
//! di `eci-common` (SPEC-010) prima del parsing, cosi' lo span aperto da
//! `parse_file` viene esportato su stdout — prima applicazione reale della
//! fondazione OTel costruita in T0.8 (SPEC-013 §8).

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
}
