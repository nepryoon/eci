//! Entrypoint SPEC-067. Default is the authenticated long-running worker. The
//! historical one-shot path remains an explicit local diagnostic subcommand.

fn run_oneshot(path: String) {
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("source read failed: {error}"));
    let (nodes, relations, chunks) = ingestion::parse_file_full(&path, &source);
    let dsn = std::env::var("POSTGRES_DSN")
        .unwrap_or_else(|_| panic!("POSTGRES_DSN is required and has no security default"));
    let mut client = postgres::Client::connect(&dsn, postgres::NoTls)
        .unwrap_or_else(|error| panic!("PostgreSQL connection failed: {error}"));
    let required = |name: &str| {
        std::env::var(name)
            .unwrap_or_else(|_| panic!("{name} is required and has no security default"))
    };
    let scope = ingestion::IngestionScope::new(
        required("ECI_TENANT_ID"),
        required("ECI_REPOSITORY"),
        required("ECI_ACL_GROUP"),
    )
    .expect("invalid ingestion scope");
    ingestion::persist_parsed_file(&mut client, &scope, nodes, relations, &chunks)
        .expect("one-shot persistence failed");
}

fn main() {
    std::panic::set_hook(Box::new(|_| eprintln!("ingestion internal panic")));
    let _guard = eci_common::observability::init_tracing("ingestion");
    let mut args = std::env::args().skip(1);
    match args.next() {
        Some(mode) if mode == "oneshot" => {
            let path = args.next().expect("usage: ingestion oneshot <path>");
            run_oneshot(path);
            return;
        }
        Some(_) => {
            eprintln!("ingestion configuration error: unsupported command");
            std::process::exit(2);
        }
        None => {}
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|_| panic!("Tokio runtime initialization failed"));
    if let Err(error) = runtime.block_on(ingestion::runtime::run()) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
