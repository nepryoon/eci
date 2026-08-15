//! Persistenza PostgreSQL di `CodeNode`/`CodeRelation` (SPEC-014, T1.2):
//! `persist_parsed_file` prende l'output in memoria di `parse_file` (T1.1) e
//! lo scrive dentro un'UNICA transazione ACID — upsert idempotente dei nodi,
//! sostituzione (delete+insert) delle relazioni emananti da questo file, e
//! una riga `outbox` per ciascuna entità toccata (SPEC-014 §2).

use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::chunking::CodeChunk;
use crate::{CodeNode, CodeRelation};

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
pub struct PersistError(postgres::Error);

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "persist_parsed_file: {}", self.0)
    }
}

impl std::error::Error for PersistError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<postgres::Error> for PersistError {
    fn from(err: postgres::Error) -> Self {
        PersistError(err)
    }
}

/// Persiste `nodes`/`relations` (output di `parse_file`) su PostgreSQL
/// dentro una singola transazione (SPEC-014 §2): `tx.commit()` solo se
/// TUTTE le operazioni riescono; qualunque errore intermedio propaga via
/// `?` e la transazione va in rollback automaticamente al `Drop` (nessun
/// `commit()` mai chiamato — comportamento della libreria `postgres`,
/// documentato sul suo stesso `Transaction`, non reimplementato qui).
pub fn persist_parsed_file(
    client: &mut postgres::Client,
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

    let mut nodes_upserted = 0usize;
    let mut outbox_rows_written = 0usize;

    for node in &nodes {
        // Provenance minimale (SPEC-014 §10 — deviazione dichiarata):
        // solo `path`, unico dato disponibile all'interfaccia di questa
        // funzione. `repo`/`commit_sha`/`ingested_at`, richiesti dalla
        // definizione "provenance" di D2 (hybrid-graph.json), non sono
        // ricavabili da `CodeNode` così come definito in SPEC-013 — non
        // fabbricati con placeholder finti.
        let provenance = serde_json::json!({ "path": node.file_path });

        tx.execute(
            "INSERT INTO code_node (id, domain, node_type, name, ast_hash, provenance)
             VALUES ($1, 'code', $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE SET
               node_type = EXCLUDED.node_type,
               name = EXCLUDED.name,
               ast_hash = EXCLUDED.ast_hash,
               updated_at = now()",
            &[
                &node.id,
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
            "id": node.id,
            "domain": node.domain,
            "name": node.name,
            "ast_hash": node.ast_hash,
            "provenance": provenance,
            "ext": { "node_type": node.node_type, "language": "go" },
        });
        tx.execute(
            "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
             VALUES ('CodeNode', $1, 'UPSERT', $2, $3)",
            &[&node.id, &payload, &trace_id],
        )?;
        outbox_rows_written += 1;
    }

    // Scope del DELETE: SOLO from_id, mai to_id (SPEC-014 §2) — id di
    // TUTTI i nodi appena prodotti da questo file, File incluso (le sue
    // CONTAINS hanno from_id = id del File).
    let file_node_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    tx.execute(
        "DELETE FROM code_relation WHERE domain = 'code' AND from_id = ANY($1)",
        &[&file_node_ids],
    )?;

    let mut relations_replaced = 0usize;
    for relation in &relations {
        // NUMERIC via rust_decimal (SPEC-014 §10 — dipendenza aggiunta):
        // la colonna `code_relation.weight` è NUMERIC (DDL SPEC-005); il
        // driver `postgres` non lega direttamente un intero Rust a un
        // parametro NUMERIC (mismatch di OID rilevato da `ToSql::accepts`
        // a runtime, non un cast implicito lato client) — `Decimal`
        // implementa `ToSql` per l'OID numeric esatto, verificato contro
        // il sorgente del crate prima di introdurlo.
        let weight = relation.weight.map(Decimal::from);
        let row = tx.query_one(
            "INSERT INTO code_relation (domain, rel_type, from_id, to_id, weight)
             VALUES ('code', $1, $2, $3, $4)
             RETURNING id::text",
            &[
                &relation.rel_type,
                &relation.from_id,
                &relation.to_id,
                &weight,
            ],
        )?;
        let relation_id: String = row.get(0);
        relations_replaced += 1;

        let payload = serde_json::json!({
            "id": relation_id,
            "domain": relation.domain,
            "rel_type": relation.rel_type,
            "from_id": relation.from_id,
            "to_id": relation.to_id,
            "weight": relation.weight,
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
    let file_path_by_node_id: HashMap<&str, &str> = nodes
        .iter()
        .map(|n| (n.id.as_str(), n.file_path.as_str()))
        .collect();

    for chunk in chunks {
        let row = tx.query_one(
            "INSERT INTO code_chunk (entity_id, chunk_index, text, char_count)
             VALUES ($1, $2, $3, $4)
             RETURNING id::text",
            &[
                &chunk.entity_id,
                &(chunk.chunk_index as i32),
                &chunk.text,
                &(chunk.char_count as i32),
            ],
        )?;
        let chunk_id: String = row.get(0);

        let mut payload = serde_json::json!({
            "id": chunk_id,
            "entity_id": chunk.entity_id,
            "chunk_index": chunk.chunk_index,
            "text": chunk.text,
            "char_count": chunk.char_count,
        });
        // SPEC-032 §2/§4: se il CodeNode dell'entità non è nel batch
        // corrente (non dovrebbe accadere per costruzione), `provenance`
        // resta semplicemente omesso dal payload — mai un panic, mai un
        // valore fabbricato.
        if let Some(file_path) = file_path_by_node_id.get(chunk.entity_id.as_str()) {
            payload["provenance"] = serde_json::json!({ "path": file_path });
        }
        tx.execute(
            "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, trace_id)
             VALUES ('CodeChunk', $1, 'UPSERT', $2, $3)",
            &[&chunk_id, &payload, &trace_id],
        )?;
        outbox_rows_written += 1;
    }

    tx.commit()?;

    Ok(PersistSummary {
        nodes_upserted,
        relations_replaced,
        outbox_rows_written,
    })
}
