//! SPEC-067 / T7.1a red-first contract tests for the authenticated long-running
//! ingestion worker. Integration replay coverage lives in the same named test
//! target and is executed explicitly by `task test:integration` once Docker is
//! available.

use ingestion::runtime::postgres_runtime_schema_ready;
use ingestion::worker::{
    command_message_key, command_message_key_matches, parse_authenticated_command,
    source_object_key, CommandErrorKind, FileOperation,
};
use ingestion::{
    inspect_ingestion_command_receipt, parse_file_full, persist_ingestion_command,
    persist_ingestion_delete_command, CommandOutcome, CommandReceiptStatus,
};
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

fn payload_with_path(path: &str) -> Vec<u8> {
    let mut payload: serde_json::Value =
        serde_json::from_slice(&valid_payload()).expect("valid test payload");
    payload["path"] = serde_json::Value::String(path.to_owned());
    serde_json::to_vec(&payload).expect("serialize path test payload")
}

fn payload_with_command_id(command_id: &str) -> Vec<u8> {
    let mut payload: serde_json::Value =
        serde_json::from_slice(&valid_payload()).expect("valid test payload");
    payload["command_id"] = serde_json::Value::String(command_id.to_owned());
    serde_json::to_vec(&payload).expect("serialize command ID test payload")
}

fn valid_delete_payload() -> Vec<u8> {
    br#"{
        "schema_version":"1",
        "operation":"DELETE",
        "command_id":"018f0806-3d73-7a8f-b5a5-c4b25f9d4702",
        "commit_sha":"42cd8e17643358a7a4307c92dfb3a025d59045f4",
        "path":"src/order service.go"
    }"#
    .to_vec()
}

#[test]
fn legacy_upsert_and_delete_are_closed_distinct_commands() {
    let (_, upsert) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024)
            .expect("legacy upsert");
    assert_eq!(upsert.operation(), FileOperation::Upsert);
    assert!(upsert.source_sha256().is_some());

    let (scope, delete) =
        parse_authenticated_command(&valid_delete_payload(), &valid_headers(), 16 * 1024 * 1024)
            .expect("authenticated delete");
    assert_eq!(delete.operation(), FileOperation::Delete);
    assert_eq!(delete.source_sha256(), None);
    assert_eq!(
        command_message_key(&scope, &delete),
        command_message_key(&scope, &upsert)
    );

    let delete_with_source = String::from_utf8(valid_delete_payload())
        .unwrap()
        .replace(
            "\n    }",
            ",\n        \"source_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\n        \"source_size_bytes\":0\n    }",
        );
    assert!(parse_authenticated_command(
        delete_with_source.as_bytes(),
        &valid_headers(),
        16 * 1024 * 1024
    )
    .is_err());
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
        serde_json::json!(["schema_version", "command_id", "commit_sha", "path"])
    );
    assert_eq!(
        schema["properties"]["operation"]["enum"],
        serde_json::json!(["UPSERT", "DELETE"])
    );
    assert_eq!(
        schema["oneOf"][0]["required"],
        serde_json::json!(["source_sha256", "source_size_bytes"])
    );
    assert_eq!(schema["oneOf"][1]["properties"]["source_sha256"], false);
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
fn command_id_matches_json_schema_uuid_string_representation() {
    let canonical_uppercase = "018F0806-3D73-7A8F-B5A5-C4B25F9D4701";
    parse_authenticated_command(
        &payload_with_command_id(canonical_uppercase),
        &valid_headers(),
        16 * 1024 * 1024,
    )
    .expect("JSON Schema UUID hex digits are case-insensitive");

    for invalid in [
        "018f08063d737a8fb5a5c4b25f9d4701",
        "{018f0806-3d73-7a8f-b5a5-c4b25f9d4701}",
        "urn:uuid:018f0806-3d73-7a8f-b5a5-c4b25f9d4701",
    ] {
        let error = parse_authenticated_command(
            &payload_with_command_id(invalid),
            &valid_headers(),
            16 * 1024 * 1024,
        )
        .expect_err("non-hyphenated UUID spellings are outside the JSON Schema format");
        assert_eq!(error.kind(), CommandErrorKind::InvalidPayload);
    }
}

#[test]
fn path_length_matches_json_schema_unicode_semantics_and_s3_key_limit() {
    let multibyte = format!("src/{}.go", "é".repeat(600));
    let (scope, command) = parse_authenticated_command(
        &payload_with_path(&multibyte),
        &valid_headers(),
        16 * 1024 * 1024,
    )
    .expect("600 multibyte code points are contract-valid");
    let object_key = source_object_key(&scope, &command);
    assert!(
        object_key.len() <= 1024,
        "S3 object keys are at most 1024 bytes"
    );
    assert!(
        !object_key.contains('é'),
        "object key does not disclose the path"
    );

    let exact_boundary = format!("{}.go", "界".repeat(1021));
    parse_authenticated_command(
        &payload_with_path(&exact_boundary),
        &valid_headers(),
        16 * 1024 * 1024,
    )
    .expect("JSON Schema maxLength=1024 counts Unicode code points");

    let beyond_boundary = format!("{}.go", "界".repeat(1022));
    let error = parse_authenticated_command(
        &payload_with_path(&beyond_boundary),
        &valid_headers(),
        16 * 1024 * 1024,
    )
    .expect_err("1025 code points exceed the contract boundary");
    assert_eq!(error.kind(), CommandErrorKind::InvalidPath);
}

#[test]
fn object_key_and_partition_key_are_deterministic_and_non_disclosing() {
    let (scope, command) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let object_key = source_object_key(&scope, &command).expect("upsert source key");
    assert!(object_key.starts_with("sources/v1/"));
    assert_eq!(object_key.len(), 181);
    assert!(object_key.contains(command.commit_sha()));
    assert!(!object_key.contains("tenant-a"));
    assert!(!object_key.contains("orders"));
    assert!(!object_key.contains("order service.go"));

    let key = command_message_key(&scope, &command);
    assert_eq!(key.len(), 64);
    assert!(key.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(key, command_message_key(&scope, &command));
    assert!(command_message_key_matches(
        &scope,
        &command,
        Some(key.as_bytes())
    ));
    assert!(!command_message_key_matches(&scope, &command, None));
    assert!(!command_message_key_matches(
        &scope,
        &command,
        Some(b"forged-partition-key")
    ));
}

#[test]
#[ignore = "requires Docker and the migrate CLI; run through task test:integration"]
fn command_receipt_is_atomic_idempotent_and_conflict_closed() {
    let (_container, mut client, runtime_dsn) = start_migrated_postgres();
    let (scope, command) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let source = "package orders\n\nfunc Process() {}\n";
    let (nodes, relations, chunks) = parse_file_full(command.path(), source);

    assert_eq!(
        inspect_ingestion_command_receipt(&mut client, &scope, &command)
            .expect("new command receipt lookup"),
        CommandReceiptStatus::New
    );

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
    assert_eq!(
        inspect_ingestion_command_receipt(&mut client, &scope, &command)
            .expect("completed command receipt lookup"),
        CommandReceiptStatus::Duplicate
    );

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
    assert_eq!(
        inspect_ingestion_command_receipt(&mut client, &scope, &conflicting)
            .expect("conflicting command receipt lookup"),
        CommandReceiptStatus::Conflict
    );
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

    client
        .batch_execute(
            "CREATE ROLE ingestion_runtime_probe LOGIN PASSWORD 'probe-test-password';
             GRANT CONNECT ON DATABASE eci TO ingestion_runtime_probe;
             GRANT USAGE ON SCHEMA public TO ingestion_runtime_probe;
             GRANT SELECT, INSERT ON ingestion_command_receipt TO ingestion_runtime_probe;
             GRANT SELECT, INSERT, UPDATE, DELETE ON code_node TO ingestion_runtime_probe;
             GRANT SELECT, INSERT, DELETE ON code_relation, code_chunk TO ingestion_runtime_probe;
             GRANT SELECT, DELETE ON code_embedding TO ingestion_runtime_probe;
             GRANT DELETE ON lineage TO ingestion_runtime_probe;
             GRANT INSERT ON outbox TO ingestion_runtime_probe;",
        )
        .expect("create least-privilege runtime probe role");
    let probe_dsn = runtime_dsn.replace(
        "eci:eci-test-password-1234@",
        "ingestion_runtime_probe:probe-test-password@",
    );
    let mut probe = Client::connect(&probe_dsn, NoTls).expect("connect runtime probe role");
    assert!(postgres_runtime_schema_ready(&mut probe));
    client
        .batch_execute("REVOKE SELECT ON ingestion_command_receipt FROM ingestion_runtime_probe")
        .expect("revoke required receipt permission");
    assert!(!postgres_runtime_schema_ready(&mut probe));
}

#[test]
#[ignore = "requires Docker and the migrate CLI; run through task test:integration"]
fn file_delete_is_atomic_idempotent_and_scope_isolated() {
    let (_container, mut client, _) = start_migrated_postgres();
    let (scope, upsert) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let source = "package orders\n\nfunc Process() {}\n";
    let (nodes, relations, chunks) = parse_file_full(upsert.path(), source);
    persist_ingestion_command(&mut client, &scope, &upsert, nodes, relations, &chunks)
        .expect("seed file to delete");

    client
        .execute(
            "INSERT INTO code_embedding (chunk_id, vector, model_id, embedding_dim)
             SELECT id, ARRAY[0.1]::real[], 'delete-test', 1 FROM code_chunk LIMIT 1",
            &[],
        )
        .expect("seed embedding");
    let target_id: String = client
        .query_one(
            "SELECT id FROM code_node WHERE provenance->>'tenant_id'='tenant-a' LIMIT 1",
            &[],
        )
        .unwrap()
        .get(0);
    let external_id = "f".repeat(64);
    client
        .execute(
            "INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
             VALUES ($1, 'code', 'Function', 'External', $2,
               '{\"tenant_id\":\"tenant-a\",\"repo\":\"orders\",\"acl_group\":\"engineering\",\"path\":\"src/external.go\"}'::jsonb)",
            &[&external_id, &"e".repeat(64)],
        )
        .unwrap();
    client
        .execute(
            "INSERT INTO code_relation (domain, rel_type, from_id, to_id)
             VALUES ('code', 'CALLS', $1, $2)",
            &[&external_id, &target_id],
        )
        .expect("seed incoming cross-file relation");

    let foreign_payload = String::from_utf8(valid_payload()).unwrap().replace(
        "018f0806-3d73-7a8f-b5a5-c4b25f9d4701",
        "018f0806-3d73-7a8f-b5a5-c4b25f9d4799",
    );
    let mut foreign_headers = valid_headers();
    foreign_headers[0].1 = b"tenant-b".to_vec();
    let (foreign_scope, foreign_upsert) = parse_authenticated_command(
        foreign_payload.as_bytes(),
        &foreign_headers,
        16 * 1024 * 1024,
    )
    .unwrap();
    let (nodes, relations, chunks) = parse_file_full(foreign_upsert.path(), source);
    persist_ingestion_command(
        &mut client,
        &foreign_scope,
        &foreign_upsert,
        nodes,
        relations,
        &chunks,
    )
    .expect("seed same path in foreign tenant");

    let (_, delete) =
        parse_authenticated_command(&valid_delete_payload(), &valid_headers(), 16 * 1024 * 1024)
            .unwrap();
    let outcome = persist_ingestion_delete_command(&mut client, &scope, &delete)
        .expect("apply canonical delete");
    let CommandOutcome::Applied(summary) = outcome else {
        panic!("first delete must apply")
    };
    assert!(summary.outbox_rows_written > 0);

    let scoped_count = |client: &mut Client, tenant: &str| -> i64 {
        client
            .query_one(
                "SELECT count(*) FROM code_node
                 WHERE provenance->>'tenant_id'=$1 AND provenance->>'path'=$2",
                &[&tenant, &delete.path()],
            )
            .unwrap()
            .get(0)
    };
    assert_eq!(scoped_count(&mut client, "tenant-a"), 0);
    assert!(scoped_count(&mut client, "tenant-b") > 0);
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM code_relation WHERE from_id=$1 OR to_id=$1",
                &[&target_id],
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    let delete_types: Vec<(String, i64)> = client
        .query(
            "SELECT aggregate_type, count(*) FROM outbox
             WHERE event_type='DELETE' GROUP BY aggregate_type ORDER BY aggregate_type",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert!(delete_types.iter().any(|(kind, _)| kind == "CodeNode"));
    assert!(delete_types.iter().any(|(kind, _)| kind == "CodeRelation"));
    assert!(delete_types.iter().any(|(kind, _)| kind == "CodeChunk"));
    assert!(delete_types.iter().any(|(kind, _)| kind == "CodeEmbedding"));
    let invalid_tombstones: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox WHERE event_type='DELETE' AND (
               payload->'provenance'->>'tenant_id' <> 'tenant-a'
               OR payload->'provenance'->>'repo' <> 'orders'
               OR payload ? 'text' OR payload ? 'vector')",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(invalid_tombstones, 0);
    let receipt = client
        .query_one(
            "SELECT operation, source_sha256 IS NULL FROM ingestion_command_receipt WHERE command_id=$1",
            &[&delete.command_id()],
        )
        .unwrap();
    assert_eq!(receipt.get::<_, String>(0), "DELETE");
    assert!(receipt.get::<_, bool>(1));

    let counts = canonical_counts(&mut client);
    assert_eq!(
        persist_ingestion_delete_command(&mut client, &scope, &delete).unwrap(),
        CommandOutcome::Duplicate
    );
    assert_eq!(canonical_counts(&mut client), counts);

    let absent_payload = String::from_utf8(valid_delete_payload())
        .unwrap()
        .replace(
            "018f0806-3d73-7a8f-b5a5-c4b25f9d4702",
            "018f0806-3d73-7a8f-b5a5-c4b25f9d4703",
        )
        .replace("src/order service.go", "src/absent.go");
    let (_, absent) = parse_authenticated_command(
        absent_payload.as_bytes(),
        &valid_headers(),
        16 * 1024 * 1024,
    )
    .unwrap();
    let outbox_before: i64 = client
        .query_one("SELECT count(*) FROM outbox", &[])
        .unwrap()
        .get(0);
    let CommandOutcome::Applied(summary) =
        persist_ingestion_delete_command(&mut client, &scope, &absent).unwrap()
    else {
        panic!("absent delete must still apply receipt")
    };
    assert_eq!(summary.outbox_rows_written, 0);
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM outbox", &[])
            .unwrap()
            .get::<_, i64>(0),
        outbox_before
    );
}

#[test]
#[ignore = "requires Docker and the migrate CLI; run through task test:integration"]
fn file_delete_rolls_back_canonical_rows_outbox_and_receipt_together() {
    let (_container, mut client, _) = start_migrated_postgres();
    let (scope, upsert) =
        parse_authenticated_command(&valid_payload(), &valid_headers(), 16 * 1024 * 1024).unwrap();
    let (nodes, relations, chunks) =
        parse_file_full(upsert.path(), "package orders\n\nfunc Process() {}\n");
    persist_ingestion_command(&mut client, &scope, &upsert, nodes, relations, &chunks)
        .expect("seed canonical file");
    let counts_before = canonical_counts(&mut client);

    client
        .batch_execute(
            "CREATE FUNCTION reject_delete_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
               BEGIN
                 IF NEW.operation = 'DELETE' THEN
                   RAISE EXCEPTION 'forced receipt failure';
                 END IF;
                 RETURN NEW;
               END;
             $$;
             CREATE TRIGGER reject_delete_receipt
               BEFORE INSERT ON ingestion_command_receipt
               FOR EACH ROW EXECUTE FUNCTION reject_delete_receipt();",
        )
        .expect("install deterministic receipt failpoint");
    let (_, delete) =
        parse_authenticated_command(&valid_delete_payload(), &valid_headers(), 16 * 1024 * 1024)
            .unwrap();

    assert!(persist_ingestion_delete_command(&mut client, &scope, &delete).is_err());
    assert_eq!(canonical_counts(&mut client), counts_before);
    assert!(matches!(
        inspect_ingestion_command_receipt(&mut client, &scope, &delete).unwrap(),
        CommandReceiptStatus::New
    ));
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

fn start_migrated_postgres() -> (Container<PostgresImage>, Client, String) {
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
    (container, client, dsn)
}
