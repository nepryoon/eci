# ADR-0005 — Rimozione del property type constraint emb_vector_type

Stato: accettata · Data: 2026-08-01

## Contesto
`schema.cypher` (D3) includeva un property type constraint su
`:CodeNode.embedding`:

```
CREATE CONSTRAINT emb_vector_type IF NOT EXISTS
FOR (n:CodeNode) REQUIRE n.embedding IS :: VECTOR<FLOAT32>(1536);
```

Eseguendo `TestMigrationAgainstRealNeo4j` contro `neo4j:5-community`
(risolto a Neo4j 5.26.28), la creazione fallisce con un errore di sintassi:
`Invalid input 'VECTOR': expected 'ARRAY', 'LIST', ...` — il parser non
riconosce affatto il tipo `VECTOR<FLOAT32>(1536)`.

Diagnosi: `VECTOR<TYPE>(DIMENSION)` come tipo di proprietà è sintassi
Cypher 25-only, introdotta in Neo4j 2025.10 (nuovo schema di versioning
CalVer). Non è disponibile sulla linea 5.x (`neo4j:5-community`, usata nei
test), dove `CYPHER 25` non è nemmeno un valore di versione valido
(`cypher-shell` risponde: "25 is not a valid option for cypher version.
Valid options are: 5"). Inoltre, per documentazione ufficiale Neo4j,
memorizzare valori come tipo `VECTOR` nativo (a differenza di una generica
`LIST<FLOAT>`) è supportato solo in Enterprise Edition o su Aura, mai in
Community — indipendentemente dalla versione. Il problema quindi non si
risolverebbe passando a un'immagine 2025.10+ se resta Community.

## Decisione
Il constraint di tipo `emb_vector_type` è rimosso da
`contracts/cypher/schema.cypher`. `CREATE VECTOR INDEX code_embeddings`
resta invariato e continua a funzionare — verificato che la creazione
dell'indice vettoriale nativo (1536 dimensioni, similarity `cosine`) non
richiede il constraint di tipo ed è disponibile su Community. La ricerca
per similarità non è impattata.

Si perde solo l'enforcement schema-level che la proprietà `embedding` sia
esattamente `VECTOR<FLOAT32>(1536)` anziché una `LIST<FLOAT>` generica
(o qualunque altro valore). La dimensionalità corretta resta responsabilità
applicativa (sink writer), coerente con l'approccio già adottato in
[ADR-0004](ADR-0004-rimuovi-existence-constraint-neo4j.md) per
id/domain non-null.

## Conseguenze
Nessun impatto sulla ricerca vettoriale né sull'indice `code_embeddings`.
Da riconsiderare in Fase 6 se si adotta Neo4j Enterprise su una versione
CalVer 2025.10+ (allora `VECTOR<FLOAT32>(1536)` come property type
constraint tornerebbe disponibile e potrebbe essere reintrodotto).
