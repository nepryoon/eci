//! Persistenza PostgreSQL di `CodeNode`/`CodeRelation` (SPEC-014, T1.2):
//! `persist_parsed_file` prende l'output in memoria di `parse_file` (T1.1) e
//! lo scrive dentro un'UNICA transazione ACID — upsert idempotente dei nodi,
//! sostituzione (delete+insert) delle relazioni emananti da questo file, e
//! una riga `outbox` per ciascuna entità toccata (SPEC-014 §2).

use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::chunking::CodeChunk;
use crate::{CodeNode, CodeRelation};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestionScope {
    tenant_id: String,
    repo: String,
    acl_group: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeError;

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingestion security scope is missing or invalid")
    }
}

impl std::error::Error for ScopeError {}

impl IngestionScope {
    pub fn new(
        tenant_id: impl Into<String>,
        repo: impl Into<String>,
        acl_group: impl Into<String>,
    ) -> Result<Self, ScopeError> {
        let value = Self {
            tenant_id: tenant_id.into(),
            repo: repo.into(),
            acl_group: acl_group.into(),
        };
        if [&value.tenant_id, &value.repo, &value.acl_group]
            .iter()
            .any(|v| !valid_scope_value(v))
        {
            return Err(ScopeError);
        }
        Ok(value)
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    pub fn repo(&self) -> &str {
        &self.repo
    }
    pub fn acl_group(&self) -> &str {
        &self.acl_group
    }
}

fn valid_scope_value(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= 256
        && !value.chars().any(char::is_control)
}

/// Convert a parser-local node identity into the canonical persisted identity.
/// Tenant and repository are immutable ownership dimensions; ACL is excluded
/// deliberately so an ACL change updates/revokes the existing object instead
/// of leaving an old-scope copy readable. Length-prefixing prevents delimiter
/// ambiguity between otherwise valid scope values.
pub fn scoped_node_id(scope: &IngestionScope, parser_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eci-node-id-v2");
    for component in [scope.tenant_id(), scope.repo(), parser_id] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PersistSummary {
    pub nodes_upserted: usize,
    pub relations_replaced: usize,
    pub outbox_rows_written: usize,
}

/// Errore di persistenza: wrapper sottile su `postgres::Error` (SPEC-014
/// §4 — fail-fast, nessun retry automatico, nessuna categorizzazione
/// ulteriore in questa SPEC).
#[derive(Debug)]
enum PersistErrorKind {
    Database(postgres::Error),
    CommandIdConflict,
    InvalidCommandData,
}

#[derive(Debug)]
pub struct PersistError(PersistErrorKind);

impl PersistError {
    pub fn is_command_id_conflict(&self) -> bool {
        matches!(self.0, PersistErrorKind::CommandIdConflict)
    }

    pub fn is_invalid_command_data(&self) -> bool {
        matches!(self.0, PersistErrorKind::InvalidCommandData)
    }
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            PersistErrorKind::Database(error) => write!(f, "persist_parsed_file: {error}"),
            PersistErrorKind::CommandIdConflict => {
                write!(f, "persist_ingestion_command: command id conflict")
            }
            PersistErrorKind::InvalidCommandData => {
                write!(f, "persist_ingestion_command: invalid command data")
            }
        }
    }
}

impl std::error::Error for PersistError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            PersistErrorKind::Database(error) => Some(error),
            PersistErrorKind::CommandIdConflict | PersistErrorKind::InvalidCommandData => None,
        }
    }
}

impl From<postgres::Error> for PersistError {
    fn from(err: postgres::Error) -> Self {
        PersistError(PersistErrorKind::Database(err))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandOutcome {
    Applied(PersistSummary),
    Duplicate,
}

/// Read-only preflight for an already durable command receipt. This is an
/// optimization and classification boundary before object I/O; the advisory
/// lock in `persist_ingestion_command` remains the authoritative concurrency
/// check for commands that are still new here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandReceiptStatus {
    New,
    Duplicate,
    Conflict,
}

pub fn inspect_ingestion_command_receipt(
    client: &mut postgres::Client,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
) -> Result<CommandReceiptStatus, PersistError> {
    let row = client.query_opt(
        "SELECT fingerprint FROM ingestion_command_receipt WHERE command_id = $1",
        &[&command.command_id()],
    )?;
    Ok(match row {
        None => CommandReceiptStatus::New,
        Some(row) => {
            let existing: String = row.get(0);
            if existing == crate::worker::command_fingerprint(scope, command) {
                CommandReceiptStatus::Duplicate
            } else {
                CommandReceiptStatus::Conflict
            }
        }
    })
}

struct CommitProvenance<'a> {
    commit_sha: &'a str,
    path: &'a str,
    ingested_at: &'a str,
}

/// Persiste `nodes`/`relations` (output di `parse_file`) su PostgreSQL
/// dentro una singola transazione (SPEC-014 §2): `tx.commit()` solo se
/// TUTTE le operazioni riescono; qualunque errore intermedio propaga via
/// `?` e la transazione va in rollback automaticamente al `Drop` (nessun
/// `commit()` mai chiamato — comportamento della libreria `postgres`,
/// documentato sul suo stesso `Transaction`, non reimplementato qui).
pub fn persist_parsed_file(
    client: &mut postgres::Client,
    scope: &IngestionScope,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<PersistSummary, PersistError> {
    let _span = tracing::info_span!("persist_parsed_file").entered();
    // Catturato UNA VOLTA dentro lo span di questa funzione (non per ogni
    // riga outbox): tutte le righe outbox prodotte da questa chiamata
    // condividono lo stesso trace_id, come richiesto da SPEC-014 §3
    // scenario 5. `None` se nessun OTel TracerProvider è mai stato
    // inizializzato nel processo (SPEC-014 §4, edge case, comportamento
    // già verificato in SPEC-010 su `current_trace_id_hex` stesso — non
    // ri-testato qui).
    let trace_id = eci_common::observability::current_trace_id_hex();

    let mut tx = client.transaction()?;
    let summary = persist_parsed_file_in_transaction(
        &mut tx, scope, nodes, relations, chunks, &trace_id, None,
    )?;
    tx.commit()?;
    Ok(summary)
}

/// SPEC-067: receipt and canonical writes share one PostgreSQL transaction.
/// The advisory transaction lock serializes the same command id so concurrent
/// redeliveries observe either the completed receipt or a clean first apply.
pub fn persist_ingestion_command(
    client: &mut postgres::Client,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<CommandOutcome, PersistError> {
    let span = tracing::info_span!(
        "ingestion.command.persist",
        ingestion.operation = "upsert",
        ingestion.outcome = tracing::field::Empty
    );
    let _entered = span.enter();
    let result = persist_ingestion_command_inner(client, scope, command, nodes, relations, chunks);
    let outcome = match &result {
        Ok(CommandOutcome::Applied(_)) => "applied",
        Ok(CommandOutcome::Duplicate) => "duplicate",
        Err(_) => "failed",
    };
    span.record("ingestion.outcome", outcome);
    result
}

fn persist_ingestion_command_inner(
    client: &mut postgres::Client,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
) -> Result<CommandOutcome, PersistError> {
    if command.operation() != crate::worker::FileOperation::Upsert {
        return Err(PersistError(PersistErrorKind::InvalidCommandData));
    }
    if !parsed_command_data_is_valid(command.path(), &nodes, chunks) {
        return Err(PersistError(PersistErrorKind::InvalidCommandData));
    }
    let trace_id = eci_common::observability::current_trace_id_hex();
    let fingerprint = crate::worker::command_fingerprint(scope, command);
    let command_id = command.command_id();
    let mut tx = client.transaction()?;
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&command_id.to_string()],
    )?;
    if let Some(row) = tx.query_opt(
        "SELECT fingerprint FROM ingestion_command_receipt WHERE command_id = $1",
        &[&command_id],
    )? {
        let existing: String = row.get(0);
        if existing == fingerprint {
            tx.commit()?;
            return Ok(CommandOutcome::Duplicate);
        }
        return Err(PersistError(PersistErrorKind::CommandIdConflict));
    }

    let ingested_at: String = tx
        .query_one(
            "SELECT to_char(transaction_timestamp() AT TIME ZONE 'UTC',\
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
            &[],
        )?
        .get(0);
    let canonical_scope = scope.ingestion_scope();
    let provenance = CommitProvenance {
        commit_sha: command.commit_sha(),
        path: command.path(),
        ingested_at: &ingested_at,
    };
    let summary = persist_parsed_file_in_transaction(
        &mut tx,
        &canonical_scope,
        nodes,
        relations,
        chunks,
        &trace_id,
        Some(&provenance),
    )?;
    insert_command_receipt(&mut tx, scope, command, &fingerprint)?;
    tx.commit()?;
    Ok(CommandOutcome::Applied(summary))
}

/// Apply an authenticated file removal and its projection tombstones in one
/// canonical PostgreSQL transaction (SPEC-070 / ADR-0025).
pub fn persist_ingestion_delete_command(
    client: &mut postgres::Client,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
) -> Result<CommandOutcome, PersistError> {
    let span = tracing::info_span!(
        "ingestion.command.persist",
        ingestion.operation = "delete",
        ingestion.outcome = tracing::field::Empty
    );
    let _entered = span.enter();
    let result = persist_ingestion_delete_command_inner(client, scope, command);
    let outcome = match &result {
        Ok(CommandOutcome::Applied(_)) => "applied",
        Ok(CommandOutcome::Duplicate) => "duplicate",
        Err(_) => "failed",
    };
    span.record("ingestion.outcome", outcome);
    result
}

fn persist_ingestion_delete_command_inner(
    client: &mut postgres::Client,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
) -> Result<CommandOutcome, PersistError> {
    if command.operation() != crate::worker::FileOperation::Delete {
        return Err(PersistError(PersistErrorKind::InvalidCommandData));
    }
    let trace_id = eci_common::observability::current_trace_id_hex();
    let fingerprint = crate::worker::command_fingerprint(scope, command);
    let command_id = command.command_id();
    let mut tx = client.transaction()?;
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        &[&command_id.to_string()],
    )?;
    if let Some(row) = tx.query_opt(
        "SELECT fingerprint FROM ingestion_command_receipt WHERE command_id = $1",
        &[&command_id],
    )? {
        let existing: String = row.get(0);
        if existing == fingerprint {
            tx.commit()?;
            return Ok(CommandOutcome::Duplicate);
        }
        return Err(PersistError(PersistErrorKind::CommandIdConflict));
    }

    let node_rows = tx.query(
        "SELECT id, node_type FROM code_node
         WHERE provenance->>'tenant_id' = $1
           AND provenance->>'repo' = $2
           AND provenance->>'acl_group' = $3
           AND provenance->>'path' = $4
         ORDER BY id FOR UPDATE",
        &[
            &scope.tenant_id(),
            &scope.repository(),
            &scope.acl_group(),
            &command.path(),
        ],
    )?;
    let node_ids: Vec<String> = node_rows.iter().map(|row| row.get(0)).collect();
    let provenance = serde_json::json!({
        "tenant_id": scope.tenant_id(),
        "repo": scope.repository(),
        "acl_group": scope.acl_group(),
        "path": command.path(),
        "commit_sha": command.commit_sha(),
    });

    let relation_rows = tx.query(
        "SELECT id::text, rel_type, from_id, to_id
         FROM code_relation
         WHERE from_id = ANY($1) OR to_id = ANY($1)
         ORDER BY id FOR UPDATE",
        &[&node_ids],
    )?;
    let chunk_rows = tx.query(
        "SELECT id, entity_id FROM code_chunk
         WHERE entity_id = ANY($1) ORDER BY id FOR UPDATE",
        &[&node_ids],
    )?;
    let chunk_ids: Vec<uuid::Uuid> = chunk_rows.iter().map(|row| row.get(0)).collect();
    let embedding_rows = tx.query(
        "SELECT e.id::text, c.entity_id
         FROM code_embedding e JOIN code_chunk c ON c.id = e.chunk_id
         WHERE e.chunk_id = ANY($1) ORDER BY e.id FOR UPDATE",
        &[&chunk_ids],
    )?;

    let mut outbox_rows_written = 0usize;
    for row in &embedding_rows {
        let id: String = row.get(0);
        let entity_id: String = row.get(1);
        insert_delete_outbox(
            &mut tx,
            "CodeEmbedding",
            &id,
            serde_json::json!({"id": id, "entity_id": entity_id, "provenance": provenance}),
            &trace_id,
        )?;
        outbox_rows_written += 1;
    }
    for row in &chunk_rows {
        let id = row.get::<_, uuid::Uuid>(0).to_string();
        let entity_id: String = row.get(1);
        insert_delete_outbox(
            &mut tx,
            "CodeChunk",
            &id,
            serde_json::json!({"id": id, "entity_id": entity_id, "provenance": provenance}),
            &trace_id,
        )?;
        outbox_rows_written += 1;
    }
    for row in &relation_rows {
        let id: String = row.get(0);
        let rel_type: String = row.get(1);
        let from_id: String = row.get(2);
        let to_id: String = row.get(3);
        insert_delete_outbox(
            &mut tx,
            "CodeRelation",
            &id,
            serde_json::json!({
                "id": id, "rel_type": rel_type, "from_id": from_id,
                "to_id": to_id, "provenance": provenance
            }),
            &trace_id,
        )?;
        outbox_rows_written += 1;
    }
    for row in &node_rows {
        let id: String = row.get(0);
        let node_type: String = row.get(1);
        insert_delete_outbox(
            &mut tx,
            "CodeNode",
            &id,
            serde_json::json!({
                "id": id, "ext": {"node_type": node_type}, "provenance": provenance
            }),
            &trace_id,
        )?;
        outbox_rows_written += 1;
    }

    tx.execute(
        "DELETE FROM code_embedding WHERE chunk_id = ANY($1)",
        &[&chunk_ids],
    )?;
    tx.execute(
        "DELETE FROM code_chunk WHERE entity_id = ANY($1)",
        &[&node_ids],
    )?;
    tx.execute(
        "DELETE FROM code_relation WHERE from_id = ANY($1) OR to_id = ANY($1)",
        &[&node_ids],
    )?;
    tx.execute(
        "DELETE FROM lineage
         WHERE old_node_id::text = ANY($1) OR new_node_id::text = ANY($1)",
        &[&node_ids],
    )?;
    tx.execute(
        "UPDATE code_node SET supersedes = NULL WHERE supersedes = ANY($1)",
        &[&node_ids],
    )?;
    tx.execute("DELETE FROM code_node WHERE id = ANY($1)", &[&node_ids])?;
    insert_command_receipt(&mut tx, scope, command, &fingerprint)?;
    tx.commit()?;

    Ok(CommandOutcome::Applied(PersistSummary {
        nodes_upserted: 0,
        relations_replaced: 0,
        outbox_rows_written,
    }))
}

fn insert_delete_outbox(
    tx: &mut postgres::Transaction<'_>,
    aggregate_type: &str,
    aggregate_id: &str,
    payload: serde_json::Value,
    trace_id: &Option<String>,
) -> Result<(), PersistError> {
    tx.execute(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
         VALUES ($1, $2, 'DELETE', $3, $4)",
        &[&aggregate_type, &aggregate_id, &payload, trace_id],
    )?;
    Ok(())
}

fn insert_command_receipt(
    tx: &mut postgres::Transaction<'_>,
    scope: &crate::worker::AuthenticatedCommitScope,
    command: &crate::worker::IngestionFileCommand,
    fingerprint: &str,
) -> Result<(), PersistError> {
    tx.execute(
        "INSERT INTO ingestion_command_receipt
         (command_id, fingerprint, tenant_id, repository, commit_sha, path, source_sha256, operation)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &command.command_id(),
            &fingerprint,
            &scope.tenant_id(),
            &scope.repository(),
            &command.commit_sha(),
            &command.path(),
            &command.source_sha256(),
            &command.operation().as_str(),
        ],
    )?;
    Ok(())
}

fn parsed_command_data_is_valid(
    command_path: &str,
    nodes: &[CodeNode],
    chunks: &[CodeChunk],
) -> bool {
    let mut node_ids = HashSet::with_capacity(nodes.len());
    if nodes.is_empty()
        || nodes
            .iter()
            .any(|node| node.file_path != command_path || !node_ids.insert(node.id.as_str()))
    {
        return false;
    }

    let mut chunk_keys = HashSet::with_capacity(chunks.len());
    chunks.iter().all(|chunk| {
        node_ids.contains(chunk.entity_id.as_str())
            && chunk_keys.insert((chunk.entity_id.as_str(), chunk.chunk_index))
    })
}

fn persist_parsed_file_in_transaction(
    tx: &mut postgres::Transaction<'_>,
    scope: &IngestionScope,
    nodes: Vec<CodeNode>,
    relations: Vec<CodeRelation>,
    chunks: &[CodeChunk],
    trace_id: &Option<String>,
    commit: Option<&CommitProvenance<'_>>,
) -> Result<PersistSummary, PersistError> {
    let mut nodes_upserted = 0usize;
    let mut outbox_rows_written = 0usize;

    for node in &nodes {
        let node_id = scoped_node_id(scope, &node.id);
        // Le etichette sono parte della provenance transazionale e provengono
        // soltanto dall'ingestion context validato (ADR-0010). Non sono mai
        // ricavate dal path, dal payload o da un consumer downstream.
        let provenance = provenance_json(scope, &node.file_path, commit);

        tx.execute(
            "INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
             VALUES ($1, 'code', $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE SET
               node_type = EXCLUDED.node_type,
               name = EXCLUDED.name,
               ast_hash = EXCLUDED.ast_hash,
               provenance = EXCLUDED.provenance,
               updated_at = now()",
            &[
                &node_id,
                &node.node_type,
                &node.name,
                &node.ast_hash,
                &provenance,
            ],
        )?;
        nodes_upserted += 1;

        // Payload outbox conforme a D2 CodeNode (hybrid-graph.json):
        // `node_type` va sotto `ext` per domain="code" (codeExtension),
        // non come campo top-level — a differenza della colonna SQL
        // `code_node.node_type`, che è una colonna scalare dedicata (DDL
        // SPEC-005, non un riflesso diretto di `ext`).
        let payload = serde_json::json!({
            "id": node_id,
            "domain": node.domain,
            "name": node.name,
            "ast_hash": node.ast_hash,
            "provenance": provenance,
            "ext": { "node_type": node.node_type, "language": "go" },
        });
        tx.execute(
            "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
             VALUES ('CodeNode', $1, 'UPSERT', $2, $3)",
            &[&node_id, &payload, &trace_id],
        )?;
        outbox_rows_written += 1;
    }

    // Scope del DELETE: SOLO from_id, mai to_id (SPEC-014 §2) — id di
    // TUTTI i nodi appena prodotti da questo file, File incluso (le sue
    // CONTAINS hanno from_id = id del File).
    let file_node_ids_owned: Vec<String> = nodes
        .iter()
        .map(|node| scoped_node_id(scope, &node.id))
        .collect();
    let file_node_ids: Vec<&str> = file_node_ids_owned.iter().map(String::as_str).collect();
    tx.execute(
        "DELETE FROM code_relation WHERE domain = 'code' AND from_id = ANY($1)",
        &[&file_node_ids],
    )?;

    let mut relations_replaced = 0usize;
    for relation in &relations {
        let from_id = scoped_node_id(scope, &relation.from_id);
        let to_id = scoped_node_id(scope, &relation.to_id);
        // NUMERIC via rust_decimal (SPEC-014 §10 — dipendenza aggiunta):
        // la colonna `code_relation.weight` è NUMERIC (DDL SPEC-005); il
        // driver `postgres` non lega direttamente un intero Rust a un
        // parametro NUMERIC (mismatch di OID rilevato da `ToSql::accepts`
        // a runtime, non un cast implicito lato client) — `Decimal`
        // implementa `ToSql` per l'OID numeric esatto, verificato contro
        // il sorgente del crate prima di introdurlo.
        let weight = relation.weight.map(Decimal::from);
        let row = tx.query_one(
            "INSERT INTO code_relation (domain, rel_type, from_id, to_id, weight, provenance)
             VALUES ('code', $1, $2, $3, $4, $5)
             RETURNING id::text",
            &[
                &relation.rel_type,
                &from_id,
                &to_id,
                &weight,
                &provenance_json(scope, commit.map_or("", |value| value.path), commit),
            ],
        )?;
        let relation_id: String = row.get(0);
        relations_replaced += 1;

        let payload = serde_json::json!({
            "id": relation_id,
            "domain": relation.domain,
            "rel_type": relation.rel_type,
            "from_id": from_id,
            "to_id": to_id,
            "weight": relation.weight,
            "provenance": provenance_json(scope, commit.map_or("", |value| value.path), commit),
        });
        tx.execute(
            "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
             VALUES ('CodeRelation', $1, 'UPSERT', $2, $3)",
            &[&relation_id, &payload, &trace_id],
        )?;
        outbox_rows_written += 1;
    }

    // Scope del DELETE: SOLO entity_id tra i nodi appena prodotti da questo
    // file (SPEC-029 §2 — stesso pattern già stabilito per code_relation
    // sopra, non un ON CONFLICT: il numero di chunk di un'entità può
    // cambiare tra un parse e l'altro).
    tx.execute(
        "DELETE FROM code_chunk WHERE entity_id = ANY($1)",
        &[&file_node_ids],
    )?;

    // Lookup in memoria file_path per id (SPEC-032 §2): nessuna nuova
    // query, `nodes` è già ricevuto nella STESSA chiamata di `chunks`.
    let file_path_by_node_id: HashMap<String, &str> = nodes
        .iter()
        .map(|n| (scoped_node_id(scope, &n.id), n.file_path.as_str()))
        .collect();

    for chunk in chunks {
        let entity_id = scoped_node_id(scope, &chunk.entity_id);
        let row = tx.query_one(
            "INSERT INTO code_chunk (entity_id, chunk_index, text, char_count)
             VALUES ($1, $2, $3, $4)
             RETURNING id::text",
            &[
                &entity_id,
                &(chunk.chunk_index as i32),
                &chunk.text,
                &(chunk.char_count as i32),
            ],
        )?;
        let chunk_id: String = row.get(0);

        let mut payload = serde_json::json!({
            "id": chunk_id,
            "entity_id": entity_id,
            "chunk_index": chunk.chunk_index,
            "text": chunk.text,
            "char_count": chunk.char_count,
        });
        // SPEC-032 §2/§4: se il CodeNode dell'entità non è nel batch
        // corrente (non dovrebbe accadere per costruzione), `provenance`
        // resta semplicemente omesso dal payload — mai un panic, mai un
        // valore fabbricato.
        if let Some(file_path) = file_path_by_node_id.get(&entity_id) {
            payload["provenance"] = provenance_json(scope, file_path, commit);
        }
        tx.execute(
            "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
             VALUES ('CodeChunk', $1, 'UPSERT', $2, $3)",
            &[&chunk_id, &payload, &trace_id],
        )?;
        outbox_rows_written += 1;
    }

    Ok(PersistSummary {
        nodes_upserted,
        relations_replaced,
        outbox_rows_written,
    })
}

fn provenance_json(
    scope: &IngestionScope,
    legacy_path: &str,
    commit: Option<&CommitProvenance<'_>>,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "tenant_id": scope.tenant_id(),
        "repo": scope.repo(),
        "acl_group": scope.acl_group(),
    });
    if !legacy_path.is_empty() {
        value["path"] = serde_json::Value::String(legacy_path.to_owned());
    }
    if let Some(commit) = commit {
        value["path"] = serde_json::Value::String(commit.path.to_owned());
        value["commit_sha"] = serde_json::Value::String(commit.commit_sha.to_owned());
        value["ingested_at"] = serde_json::Value::String(commit.ingested_at.to_owned());
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_parser_entities_and_chunks_are_invalid_command_data() {
        let source = "function duplicate() {}\nfunction duplicate() {}\n";
        let (nodes, _relations, chunks) = crate::parse_file_full_private("duplicate.js", source);
        assert!(
            nodes
                .iter()
                .enumerate()
                .any(|(index, node)| nodes[..index].iter().any(|prior| prior.id == node.id)),
            "fixture must reproduce duplicate deterministic parser IDs"
        );
        assert!(!parsed_command_data_is_valid(
            "duplicate.js",
            &nodes,
            &chunks,
        ));
    }
}
