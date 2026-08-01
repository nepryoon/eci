# ADR-0003 — Split di contracts/cypher/schema.cypher in schema.cypher + examples.cypher

Stato: accettata · Data: 2026-08-02

## Contesto
schema.cypher (D3) conteneva sia il DDL (constraint/indici) sia blocchi MERGE
di query di esempio per i servizi applicativi. Il migration runner (T0.4)
richiede di eseguire solo il DDL; i MERGE di esempio sono stati spostati in
examples.cypher (query di reference per T1.3/T4.1, non eseguite dalla
migration).

## Decisione
schema.cypher contiene ora solo i blocchi CREATE CONSTRAINT/INDEX. I blocchi
MERGE sono stati spostati verbatim in examples.cypher con intestazione di
riferimento. Nessun contenuto alterato, solo redistribuito tra due file.

## Conseguenze
Il migration runner (tools/migrate-neo4j) opera esclusivamente su
schema.cypher.
