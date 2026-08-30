//! SPEC-067 / T7.1a red-first contract tests for the authenticated long-running
//! ingestion worker. Integration replay coverage lives in the same named test
//! target and is executed explicitly by `task test:integration` once Docker is
//! available.

use ingestion::worker::{
    command_message_key, parse_authenticated_command, source_object_key, CommandErrorKind,
};
use ingestion::{parse_file_full, persist_ingestion_command, CommandOutcome};
use postgres::{Client, NoTls};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;

fn valid_payload() -> Vec<u8> {
    br#"{
        "schema_version":"1",
        "command_id":"018f0806-3d73-7a8f-b5a5-c4b25f9d4701",
        "commit_sha":"32cd8e17643358a7a4307c92dfb3a025d59045f4",
        "path":"src/order service.go",
        "source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "source_size_bytes":4096
    }"#
    .to_vec()
}

fn valid_headers() -> Vec<(String, Vec<u8>)> {
    vec![
        ("eci-tenant-id".into(), b"tenant-a".to_vec()),
        ("eci-repository".into(), b"orders".to_vec()),
        ("eci-acl-group".into(), b"engineering".to_vec()),
    ]
}

#[test]
fn checked_in_command_contract_is_closed_and_matches_runtime_limits() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../contracts/jsonschema/ingestion-file-command.json"
    );
    let schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read command contract"))
            .expect("parse command contract");
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["schema_version"]["const"], "1");
    assert_eq!(schema["properties"]["path"]["maxLength"], 1024);
    assert_eq!(
        schema["properties"]["source_size_bytes"]["maximum"],
        16 * 1024 * 1024
    );
    assert_eq!(
        schema["required"],
        serde_json::json!([
            "schema_version",
            "command_id",
            "commit_sha",
            "path",
            "source_sha256",
            "source_size_bytes"
        ])
    );
    for forbidden in [
        "tenant_id",
        "repository",
        "acl_group",
        "url",
        "bucket",
        "key",
    ] {
        assert!(schema["properties"].get(forbidden).is_none());
    }
}

#[test]
fn authenticated_headers_are_the_only_scope_authority() {
    let (scope, command) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024)
            .expect("valid authenticated command");
    assert_eq!(scope.tenant_id(), "tenant-a");
    assert_eq!(scope.repository(), "orders");
    assert_eq!(scope.acl_group(), "engineering");
    assert_eq!(command.path(), "src/order service.go");

    let body_with_scope = br#"{
        "schema_version":"1",
        "command_id":"018f0806-3d73-7a8f-b5a5-c4b25f9d4701",
        "commit_sha":"32cd8e17643358a7a4307c92dfb3a025d59045f4",
        "path":"src/order_service.go",
        "source_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "source_size_bytes":4096,
        "tenant_id":"tenant-b"
    }"#;
    let err = parse_authenticated_command(body_with_scope, &valid_headers(), 16 * 1024 * 1024)
        .expect_err("closed schema must reject body-controlled scope");
    assert_eq!(err.kind(), CommandErrorKind::InvalidPayload);
    assert!(!err.to_string().contains("tenant-b"));
}

#[test]
fn missing_duplicate_or_malformed_scope_fails_closed() {
    let cases = [
        vec![
            ("eci-repository".into(), b"orders".to_vec()),
            ("eci-acl-group".into(), b"engineering".to_vec()),
        ],
        vec![
            ("eci-tenant-id".into(), b"tenant-a".to_vec()),
            ("eci-tenant-id".into(), b"tenant-b".to_vec()),
            ("eci-repository".into(), b"orders".to_vec()),
            ("eci-acl-group".into(), b"engineering".to_vec()),
        ],
        vec![
            ("eci-tenant-id".into(), b"tenant-a\nforged".to_vec()),
            ("eci-repository".into(), b"orders".to_vec()),
            ("eci-acl-group".into(), b"engineering".to_vec()),
        ],
    ];
    for headers in cases {
        let err = parse_authenticated_command(&valid_payload(), &headers, 16 * 1024 * 1024)
            .expect_err("invalid scope headers must fail");
        assert_eq!(err.kind(), CommandErrorKind::InvalidScope);
    }
}

#[test]
fn paths_identifiers_and_size_are_strictly_bounded() {
    for (needle, replacement) in [
        ("src/order service.go", "../secret.go"),
        ("src/order service.go", "/etc/passwd"),
        ("src/order service.go", "src\\windows.go"),
        ("src/order service.go", "src/readme.txt"),
        (
            "32cd8e17643358a7a4307c92dfb3a025d59045f4",
            "32CD8E17643358A7A4307C92DFB3A025D59045F4",
        ),
    ] {
        let payload = String::from_utf8(valid_payload())
            .unwrap()
            .replace(needle, replacement);
        assert!(parse_authenticated_command(
            payload.as_bytes(),
            &valid_headers(),
            16 * 1024 * 1024
        )
        .is_err());
    }

    let err = parse_authenticated_command(&valid_payload(), &valid_headers(), 1024)
        .expect_err("configured source limit must be authoritative");
    assert_eq!(err.kind(), CommandErrorKind::SourceTooLarge);
}

#[test]
fn object_key_and_partition_key_are_deterministic_and_non_disclosing() {
    let (scope, command) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let object_key = source_object_key(&scope, &command);
    assert!(object_key.starts_with("sources/v1/"));
    assert!(object_key.ends_with("/src/order%20service.go"));
    assert!(object_key.contains(command.commit_sha()));
    assert!(!object_key.contains("tenant-a"));
    assert!(!object_key.contains("orders"));

    let key = command_message_key(&scope, &command);
    assert_eq!(key.len(), 64);
    assert!(key.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(key, command_message_key(&scope, &command));
}

#[test]
#[ignore = "requires Docker and the migrate CLI; run through task test:integration"]
fn command_receipt_is_atomic_idempotent_and_conflict_closed() {
    let (_container, mut client) = start_migrated_postgres();
    let (scope, command) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let source = "package orders\n\nfunc Process() {}\n";
    let (nodes, relations, chunks) = parse_file_full(command.path(), source);

    let first = persist_ingestion_command(
        &mut client,
        &scope,
        &command,
        nodes.clone(),
        relations.clone(),
        &chunks,
    )
    .expect("first apply");
    assert!(matches!(first, CommandOutcome::Applied(_)));
    let counts = canonical_counts(&mut client);
    assert_eq!(counts.4, 1, "one receipt");

    let replay = persist_ingestion_command(
        &mut client,
        &scope,
        &command,
        nodes.clone(),
        relations.clone(),
        &chunks,
    )
    .expect("identical replay");
    assert_eq!(replay, CommandOutcome::Duplicate);
    assert_eq!(canonical_counts(&mut client), counts);

    let conflicting = String::from_utf8(valid_payload())
        .unwrap()
        .replace(&"a".repeat(64), &"b".repeat(64));
    let (_, conflicting) =
        parse_authenticated_command(conflicting.as_bytes(), &valid_headers(), 16 * 1024 * 1024)
            .unwrap();
    let error =
        persist_ingestion_command(&mut client, &scope, &conflicting, nodes, relations, &chunks)
            .expect_err("same command id with a different fingerprint must fail");
    assert!(error.is_command_id_conflict());
    assert_eq!(canonical_counts(&mut client), counts);

    let provenance: serde_json::Value = client
        .query_one("SELECT provenance FROM code_node LIMIT 1", &[])
        .unwrap()
        .get(0);
    assert_eq!(provenance["tenant_id"], "tenant-a");
    assert_eq!(provenance["repo"], "orders");
    assert_eq!(provenance["acl_group"], "engineering");
    assert_eq!(provenance["commit_sha"], command.commit_sha());
    assert_eq!(provenance["path"], command.path());
    assert!(provenance["ingested_at"].as_str().unwrap().ends_with('Z'));
}

fn canonical_counts(client: &mut Client) -> (i64, i64, i64, i64, i64) {
    let count = |client: &mut Client, table: &str| -> i64 {
        client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .unwrap()
            .get(0)
    };
    (
        count(client, "code_node"),
        count(client, "code_relation"),
        count(client, "code_chunk"),
        count(client, "outbox"),
        count(client, "ingestion_command_receipt"),
    )
}

fn start_migrated_postgres() -> (Container<PostgresImage>, Client) {
    let container = PostgresImage::default()
        .with_db_name("eci")
        .with_user("eci")
        .with_password("eci-test-password-1234")
        .with_tag("17")
        .start()
        .expect("start postgres:17");
    let host = container.get_host().unwrap();
    let port = container.get_host_port_ipv4(5432).unwrap();
    let dsn = format!("postgres://eci:eci-test-password-1234@{host}:{port}/eci?sslmode=disable");
    let migrations = format!(
        "file://{}/../../contracts/sql/migrations",
        env!("CARGO_MANIFEST_DIR")
    );
    let output = std::process::Command::new("migrate")
        .args(["-source", &migrations, "-database", &dsn, "up"])
        .output()
        .expect("migrate CLI");
    assert!(
        output.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let client = Client::connect(&dsn, NoTls).expect("connect migrated postgres");
    (container, client)
}
