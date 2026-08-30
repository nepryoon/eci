//! `ingestion` — parsing Go con Tree-sitter -> CPG walking skeleton
//! (SPEC-013, T1.1) più persistenza PostgreSQL transazionale (SPEC-014,
//! T1.2) più hashing Merkle bottom-up di `ast_hash` (SPEC-020, T2.1,
//! modulo [`hashing`], che sostituisce il placeholder `sha256_hex(source)`
//! di T1.1) più `doc_hash` (modulo [`hashing`]) e chunking cAST configurabile
//! (SPEC-021, T2.2, modulo [`chunking`]) più estrazione CPG JavaScript
//! (SPEC-024, T2.5 parte 1/3): `parse_file` dispatcha per estensione del
//! path (`.go` -> `parse_go_file`, `.js` -> `parse_js_file`, entrambe
//! funzioni interne con lo stesso contratto di output). `parse_file`
//! produce `CodeNode`/`CodeRelation` in memoria; `persist_parsed_file`
//! (modulo [`persist`]) li scrive dentro un'unica transazione ACID (nodi
//! upsert, relazioni sostituite, righe outbox).

use std::collections::HashMap;

use tree_sitter::Node;

pub mod chunking;
pub mod embedding;
pub mod hashing;
pub mod imports;
pub mod lineage;
pub mod persist;
pub mod resolve;
pub mod runtime;
pub mod worker;
pub use persist::{
    persist_ingestion_command, persist_parsed_file, scoped_node_id, CommandOutcome, IngestionScope,
    PersistError, PersistSummary, ScopeError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeNode {
    pub id: String,
    pub domain: String,
    pub node_type: String,
    pub name: String,
    pub ast_hash: String,
    pub file_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeRelation {
    pub domain: String,
    pub rel_type: String,
    pub from_id: String,
    pub to_id: String,
    pub weight: Option<u32>,
}

/// Dispatch per linguaggio (SPEC-024 §2, prima volta che serve — fino a
/// T2.4 `services/ingestion` era implicitamente solo-Go): l'estensione
/// del path determina la strada, nessun altro meccanismo (nessuno
/// sniffing del contenuto). Stesso span `parse_file` di sempre, con un
/// attributo `language` in più (§8) per distinguere le due strade.
pub fn parse_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>) {
    let language = match std::path::Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
    {
        Some("go") => "go",
        Some("js") => "javascript",
        Some("ts") => "typescript",
        other => panic!(
            "parse_file({file_path:?}): estensione non supportata ({other:?}) — \
             il dispatch per linguaggio è limitato a .go/.js/.ts (SPEC-024/026 §2). \
             .tsx NON è .ts: cade qui, comportamento esplicitamente verificato da SPEC-026 §4."
        ),
    };
    let _span =
        tracing::info_span!("parse_file", file_path = file_path, language = language).entered();

    match language {
        "go" => parse_go_file(file_path, source),
        "javascript" => parse_js_file(file_path, source),
        "typescript" => parse_ts_file(file_path, source),
        _ => unreachable!("language già validato sopra"),
    }
}

/// Variante "completa" di [`parse_file`] (SPEC-029 §2): stesso dispatch
/// per estensione, in più espone i `CodeChunk` prodotti dal cablaggio del
/// chunking cAST (SPEC-021/T2.2) dentro ciascuna variante `_full` per
/// linguaggio — il "futuro chiamante" a cui T2.2 §10 rimandava. Scarta
/// `unresolved` (SPEC-025/026, non pertinente qui) esattamente come
/// [`parse_js_file`]/[`parse_ts_file`] già fanno.
pub fn parse_file_full(
    file_path: &str,
    source: &str,
) -> (Vec<CodeNode>, Vec<CodeRelation>, Vec<chunking::CodeChunk>) {
    parse_file_full_with_telemetry(file_path, source, true)
}

/// Runtime-only parsing path for authenticated repository content. It keeps
/// `file_path` in canonical provenance but deliberately excludes it from span
/// attributes, as required by SPEC-067's observability boundary.
pub(crate) fn parse_file_full_private(
    file_path: &str,
    source: &str,
) -> (Vec<CodeNode>, Vec<CodeRelation>, Vec<chunking::CodeChunk>) {
    parse_file_full_with_telemetry(file_path, source, false)
}

fn parse_file_full_with_telemetry(
    file_path: &str,
    source: &str,
    expose_path_in_span: bool,
) -> (Vec<CodeNode>, Vec<CodeRelation>, Vec<chunking::CodeChunk>) {
    let language = match std::path::Path::new(file_path)
        .extension()
        .and_then(|e| e.to_str())
    {
        Some("go") => "go",
        Some("js") => "javascript",
        Some("ts") => "typescript",
        other => panic!(
            "parse_file_full({file_path:?}): estensione non supportata ({other:?}) — \
             il dispatch per linguaggio è limitato a .go/.js/.ts (SPEC-024/026 §2)."
        ),
    };
    let span = if expose_path_in_span {
        tracing::info_span!(
            "parse_file_full",
            file_path = file_path,
            language = language
        )
    } else {
        tracing::info_span!("parse_file_full", language = language)
    };
    let _span = span.entered();

    match language {
        "go" => parse_go_file_full(file_path, source),
        "javascript" => {
            let (nodes, relations, _unresolved, chunks) = parse_js_file_full(file_path, source);
            (nodes, relations, chunks)
        }
        "typescript" => {
            let (nodes, relations, _unresolved, chunks) = parse_ts_file_full(file_path, source);
            (nodes, relations, chunks)
        }
        _ => unreachable!("language già validato sopra"),
    }
}

/// Parsa il sorgente Go di un singolo file e produce i `CodeNode`/
/// `CodeRelation` corrispondenti (SPEC-013 §2): un `CodeNode` `File`,
/// un `CodeNode` per ogni `Class`/`Interface`/`Method`/`Function` di
/// primo livello, un arco `CONTAINS` da `File` verso ciascuno, e archi
/// `CALLS` risolti per nome **solo intra-file** (§2/§5 — nessuna
/// risoluzione cross-file, quella è T2.5/SPEC-024, e nemmeno lì:
/// SPEC-024 stessa introduce solo l'estrazione JS, la risoluzione
/// cross-file resta un sotto-task successivo, parte 2/3).
fn parse_go_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>) {
    let (nodes, relations, _chunks) = parse_go_file_full(file_path, source);
    (nodes, relations)
}

/// Variante "completa" di [`parse_go_file`] (SPEC-029 §2, nuova — non
/// esisteva prima): stesso identico algoritmo di estrazione, in più
/// accumula i `CodeChunk` prodotti da [`chunking::chunk_entity`] per
/// ciascuna entità, chiamato nello stesso punto in cui il suo nodo
/// Tree-sitter e il suo id sono già in mano. A differenza di
/// `parse_js_file_full`/`parse_ts_file_full` non esiste un concetto di
/// chiamate "unresolved" per Go (SPEC-013/§2 — risoluzione CALLS
/// interamente intra-file, nessun resolver cross-file per questo
/// linguaggio): il terzo elemento è direttamente `Vec<CodeChunk>`, non un
/// quarto dopo un `UnresolvedCalls` che qui non avrebbe alcun consumatore
/// (deviazione dichiarata da SPEC-029 §2, vedi nota a fondo SPEC).
pub(crate) fn parse_go_file_full(
    file_path: &str,
    source: &str,
) -> (Vec<CodeNode>, Vec<CodeRelation>, Vec<chunking::CodeChunk>) {
    let budget = chunking::chunk_budget_chars("go");
    let mut chunks: Vec<chunking::CodeChunk> = Vec::new();

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_go::LANGUAGE.into())
        .expect("grammatica tree-sitter-go valida");
    // tree-sitter ha error recovery nativo (SPEC-013 §4): anche con
    // errori di sintassi, `parse` ritorna sempre un albero (mai `None`
    // per una grammatica non-None come questa). Verificato ispezionando
    // l'albero prodotto per il caso di test dedicato: quando manca un
    // punto di sincronizzazione esplicito (es. `type Broken struct` senza
    // il corpo `{}`), Tree-sitter non isola l'errore in un nodo ERROR
    // sibling che si possa semplicemente saltare — ri-annida il testo
    // sorgente successivo (fino al prossimo punto in cui riesce a
    // risincronizzarsi) DENTRO lo span del nodo malformato stesso, come
    // figli spuri (es. una `function_declaration` successiva diventa una
    // `field_declaration` innestata nello `struct_type` rotto). Per
    // questo il loop sui figli di primo livello sotto, che riconosce solo
    // kind espliciti ("type_declaration", "function_declaration",
    // "method_declaration"), la omette naturalmente: non perché un nodo
    // ERROR viene incontrato e ignorato, ma perché a livello di root la
    // dichiarazione inghiottita non esiste più come proprio kind — è
    // diventata un dettaglio interno del nodo malformato adiacente.
    let tree = parser.parse(source, None).expect("tree-sitter parse");
    let root = tree.root_node();

    let mut nodes = Vec::new();
    let mut relations = Vec::new();

    let file_id = make_id(file_path, file_path);
    nodes.push(CodeNode {
        id: file_id.clone(),
        domain: "code".to_string(),
        node_type: "File".to_string(),
        name: file_path.to_string(),
        ast_hash: hashing::merkle_hash(root, source.as_bytes()),
        file_path: file_path.to_string(),
    });
    push_entity_chunks(root, source, &file_id, budget, &mut chunks);

    // Mappa nome-semplice -> id, per-file (SPEC-013 §2): usata per
    // risolvere CALLS. Una collisione tra due receiver diversi con lo
    // stesso nome di metodo nello stesso file è un edge case accettato
    // (§2): l'ultima dichiarazione incontrata vince, non un panic.
    let mut name_to_id: HashMap<String, String> = HashMap::new();
    // Corpi da scansionare per CALLS in un secondo passaggio, DOPO che
    // name_to_id contiene TUTTE le dichiarazioni del file: una funzione
    // può chiamarne un'altra dichiarata più avanti nel file (come
    // Process -> Validate nel fixture, dove Process precede Validate).
    let mut callable_bodies: Vec<(String, Node)> = Vec::new();

    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        match child.kind() {
            "type_declaration" => {
                let mut spec_cursor = child.walk();
                for spec in child
                    .children(&mut spec_cursor)
                    .filter(|c| c.kind() == "type_spec")
                {
                    let (Some(name_node), Some(type_node)) = (
                        spec.child_by_field_name("name"),
                        spec.child_by_field_name("type"),
                    ) else {
                        continue;
                    };
                    let node_type = match type_node.kind() {
                        "struct_type" => "Class",
                        "interface_type" => "Interface",
                        _ => continue,
                    };
                    let name = text(name_node, source);
                    let id = make_id(file_path, &name);
                    nodes.push(CodeNode {
                        id: id.clone(),
                        domain: "code".to_string(),
                        node_type: node_type.to_string(),
                        name: name.clone(),
                        ast_hash: hashing::merkle_hash(spec, source.as_bytes()),
                        file_path: file_path.to_string(),
                    });
                    push_entity_chunks(spec, source, &id, budget, &mut chunks);
                    name_to_id.insert(name, id.clone());
                    relations.push(CodeRelation {
                        domain: "code".to_string(),
                        rel_type: "CONTAINS".to_string(),
                        from_id: file_id.clone(),
                        to_id: id,
                        weight: None,
                    });
                }
            }
            "method_declaration" | "function_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let simple_name = text(name_node, source);
                let (node_type, qualified_name) = if child.kind() == "method_declaration" {
                    let receiver_type = child
                        .child_by_field_name("receiver")
                        .and_then(|r| receiver_type_name(r, source))
                        .unwrap_or_default();
                    ("Method", format!("{receiver_type}.{simple_name}"))
                } else {
                    ("Function", simple_name.clone())
                };
                let id = make_id(file_path, &qualified_name);
                nodes.push(CodeNode {
                    id: id.clone(),
                    domain: "code".to_string(),
                    node_type: node_type.to_string(),
                    name: simple_name.clone(),
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &id, budget, &mut chunks);
                name_to_id.insert(simple_name, id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: id.clone(),
                    weight: None,
                });
                if let Some(body) = child.child_by_field_name("body") {
                    callable_bodies.push((id, body));
                }
            }
            _ => {}
        }
    }

    for (caller_id, body) in callable_bodies {
        let mut call_counts: HashMap<String, u32> = HashMap::new();
        collect_calls(body, source, &name_to_id, &mut call_counts);
        for (callee_id, weight) in call_counts {
            relations.push(CodeRelation {
                domain: "code".to_string(),
                rel_type: "CALLS".to_string(),
                from_id: caller_id.clone(),
                to_id: callee_id,
                weight: Some(weight),
            });
        }
    }

    (nodes, relations, chunks)
}

/// Parsa il sorgente JavaScript di un singolo file (SPEC-024 §2): stesso
/// contratto di output di [`parse_go_file`], adattato alla struttura
/// reale di JavaScript (`class_declaration` contiene realmente i propri
/// `method_definition` come figli — CONTAINS Class->Method deriva
/// direttamente dall'albero, non un escamotage). Nessuna risoluzione
/// cross-file (§5), nessuna estrazione di function expression/arrow
/// function assegnate a variabile (§2, Non-goal dichiarato).
fn parse_js_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>) {
    let (nodes, relations, _unresolved, _chunks) = parse_js_file_full(file_path, source);
    (nodes, relations)
}

/// Variante "completa" di [`parse_js_file`] che espone anche le chiamate
/// rimaste irrisolte a livello intra-file (SPEC-025 §2: "stesso
/// meccanismo già esistente in SPEC-013/SPEC-024... qui riusato, non
/// duplicato") — stesso identico algoritmo, in più raccoglie
/// (caller_id, nome_chiamato, weight) per ogni `call_expression` il cui
/// nome non risolve localmente, invece di scartarlo silenziosamente. In
/// più (SPEC-029 §2) accumula i `CodeChunk` per ciascuna entità, come
/// quarto elemento. `parse_js_file` resta il contratto pubblico invariato
/// di SPEC-024 (ne scarta terzo e quarto elemento). `pub(crate)`:
/// consumata da [`resolve`] per costruire l'input di
/// `resolve_cross_file_calls`.
pub(crate) fn parse_js_file_full(
    file_path: &str,
    source: &str,
) -> (
    Vec<CodeNode>,
    Vec<CodeRelation>,
    resolve::UnresolvedCalls,
    Vec<chunking::CodeChunk>,
) {
    let budget = chunking::chunk_budget_chars("javascript");
    let mut chunks: Vec<chunking::CodeChunk> = Vec::new();

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_javascript::LANGUAGE.into())
        .expect("grammatica tree-sitter-javascript valida");
    // Stesso error recovery nativo di Tree-sitter già sfruttato per Go
    // (SPEC-013 §4): `parse` ritorna sempre un albero. Verificato
    // empiricamente (SPEC-024 §10) che una `class` con corpo non chiuso
    // resta comunque un `class_declaration` di primo livello proprio (a
    // differenza del caso Go analogo, dove la dichiarazione successiva
    // veniva inghiottita SENZA restare un proprio kind riconoscibile) —
    // qui è la porzione interna al corpo malformato che diventa un
    // `method_definition` spurio, non la dichiarazione esterna stessa.
    let tree = parser.parse(source, None).expect("tree-sitter parse");
    let root = tree.root_node();

    let mut nodes = Vec::new();
    let mut relations = Vec::new();

    let file_id = make_id(file_path, file_path);
    nodes.push(CodeNode {
        id: file_id.clone(),
        domain: "code".to_string(),
        node_type: "File".to_string(),
        name: file_path.to_string(),
        ast_hash: hashing::merkle_hash(root, source.as_bytes()),
        file_path: file_path.to_string(),
    });
    push_entity_chunks(root, source, &file_id, budget, &mut chunks);

    let mut name_to_id: HashMap<String, String> = HashMap::new();
    let mut callable_bodies: Vec<(String, Node)> = Vec::new();

    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        // `export function f() {}`/`export class C {}` avvolgono la
        // dichiarazione in un `export_statement` (campo `declaration`) —
        // verificato empiricamente (SPEC-025 §10): il costrutto `export`
        // stesso non è tracciato (SPEC-025 §2, nessuna verifica di
        // visibilità), ma la dichiarazione avvolta deve comunque essere
        // estratta esattamente come se non fosse esportata, altrimenti
        // `export function computeTotal` sparirebbe del tutto
        // dall'estrazione — regressione reale su SPEC-024, non
        // ipotetica, scoperta estendendo il fixture per SPEC-025 §3.
        let child = if child.kind() == "export_statement" {
            child.child_by_field_name("declaration").unwrap_or(child)
        } else {
            child
        };
        match child.kind() {
            "class_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let class_name = text(name_node, source);
                let class_id = make_id(file_path, &class_name);
                nodes.push(CodeNode {
                    id: class_id.clone(),
                    domain: "code".to_string(),
                    node_type: "Class".to_string(),
                    name: class_name.clone(),
                    // Nodo dell'INTERA class_declaration (nome + body):
                    // a differenza di Go (dove il "nodo in mano" per una
                    // Class, `type_spec`, non contiene i Method,
                    // strutturalmente separati — SPEC-020 §10), qui
                    // `class_body` contiene realmente i `method_definition`
                    // come figli, quindi l'hash della Class si propaga
                    // correttamente fino a includerli (SPEC-024 §2).
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &class_id, budget, &mut chunks);
                name_to_id.insert(class_name.clone(), class_id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: class_id.clone(),
                    weight: None,
                });

                if let Some(class_body) = child.child_by_field_name("body") {
                    let mut member_cursor = class_body.walk();
                    for member in class_body
                        .children(&mut member_cursor)
                        .filter(|m| m.kind() == "method_definition")
                    {
                        let Some(method_name_node) = member.child_by_field_name("name") else {
                            continue;
                        };
                        let simple_name = text(method_name_node, source);
                        let qualified_name = format!("{class_name}.{simple_name}");
                        let method_id = make_id(file_path, &qualified_name);
                        nodes.push(CodeNode {
                            id: method_id.clone(),
                            domain: "code".to_string(),
                            node_type: "Method".to_string(),
                            name: simple_name.clone(),
                            ast_hash: hashing::merkle_hash(member, source.as_bytes()),
                            file_path: file_path.to_string(),
                        });
                        push_entity_chunks(member, source, &method_id, budget, &mut chunks);
                        name_to_id.insert(simple_name, method_id.clone());
                        // Class->Method: derivabile direttamente
                        // dall'albero (SPEC-024 §2), non un escamotage
                        // come sarebbe stato necessario per Go.
                        relations.push(CodeRelation {
                            domain: "code".to_string(),
                            rel_type: "CONTAINS".to_string(),
                            from_id: class_id.clone(),
                            to_id: method_id.clone(),
                            weight: None,
                        });
                        if let Some(body) = member.child_by_field_name("body") {
                            callable_bodies.push((method_id, body));
                        }
                    }
                }
            }
            "function_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let simple_name = text(name_node, source);
                let id = make_id(file_path, &simple_name);
                nodes.push(CodeNode {
                    id: id.clone(),
                    domain: "code".to_string(),
                    node_type: "Function".to_string(),
                    name: simple_name.clone(),
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &id, budget, &mut chunks);
                name_to_id.insert(simple_name, id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: id.clone(),
                    weight: None,
                });
                if let Some(body) = child.child_by_field_name("body") {
                    callable_bodies.push((id, body));
                }
            }
            _ => {}
        }
    }

    let mut unresolved = Vec::new();
    for (caller_id, body) in callable_bodies {
        let mut call_counts: HashMap<String, u32> = HashMap::new();
        let mut unresolved_counts: HashMap<String, u32> = HashMap::new();
        collect_calls_js(
            body,
            source,
            &name_to_id,
            &mut call_counts,
            &mut unresolved_counts,
        );
        for (callee_id, weight) in call_counts {
            relations.push(CodeRelation {
                domain: "code".to_string(),
                rel_type: "CALLS".to_string(),
                from_id: caller_id.clone(),
                to_id: callee_id,
                weight: Some(weight),
            });
        }
        for (callee_name, weight) in unresolved_counts {
            unresolved.push(resolve::UnresolvedCall {
                caller_id: caller_id.clone(),
                callee_name,
                weight,
            });
        }
    }

    (nodes, relations, unresolved, chunks)
}

/// Parsa il sorgente TypeScript di un singolo file (SPEC-026 §2): stesso
/// pattern di [`parse_js_file`], con due novità assolute rispetto a
/// Go/JS — `Interface` (`interface_declaration`, con `Method` anche
/// senza corpo per i suoi `method_signature`) e archi `IMPLEMENTS`/
/// `EXTENDS` dalle clausole `implements`/`extends`, risolti SOLO
/// intra-file (§2/§5). CALLS/CONTAINS di base riusano lo stesso
/// meccanismo di `parse_js_file` (kind Tree-sitter verificati
/// empiricamente coincidenti, SPEC-026 §10).
fn parse_ts_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>) {
    let (nodes, relations, _unresolved, _chunks) = parse_ts_file_full(file_path, source);
    (nodes, relations)
}

/// Variante "completa" di [`parse_ts_file`], stesso ruolo di
/// [`parse_js_file_full`]: espone anche le chiamate rimaste irrisolte a
/// livello intra-file, consumate da [`resolve::resolve_cross_file_calls`]
/// (riusato invariato da SPEC-025, SPEC-026 §2/§10), più (SPEC-029 §2) i
/// `CodeChunk` per ciascuna entità come quarto elemento.
pub(crate) fn parse_ts_file_full(
    file_path: &str,
    source: &str,
) -> (
    Vec<CodeNode>,
    Vec<CodeRelation>,
    resolve::UnresolvedCalls,
    Vec<chunking::CodeChunk>,
) {
    let budget = chunking::chunk_budget_chars("typescript");
    let mut chunks: Vec<chunking::CodeChunk> = Vec::new();

    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
        .expect("grammatica tree-sitter-typescript valida");
    let tree = parser.parse(source, None).expect("tree-sitter parse");
    let root = tree.root_node();

    let mut nodes = Vec::new();
    let mut relations = Vec::new();

    let file_id = make_id(file_path, file_path);
    nodes.push(CodeNode {
        id: file_id.clone(),
        domain: "code".to_string(),
        node_type: "File".to_string(),
        name: file_path.to_string(),
        ast_hash: hashing::merkle_hash(root, source.as_bytes()),
        file_path: file_path.to_string(),
    });
    push_entity_chunks(root, source, &file_id, budget, &mut chunks);

    let mut name_to_id: HashMap<String, String> = HashMap::new();
    let mut callable_bodies: Vec<(String, Node)> = Vec::new();
    // (from_id, rel_type, nome_del_target): risolto contro name_to_id
    // SOLO dopo che l'intero file è stato scansionato (stesso principio
    // "due passaggi" già usato per CALLS in SPEC-013/SPEC-024 — un
    // riferimento a un tipo dichiarato più avanti nel file deve comunque
    // risolvere). Un nome non trovato resta semplicemente senza arco:
    // SOLO intra-file per costruzione (SPEC-026 §2/§5), non un errore.
    let mut heritage_refs: Vec<(String, &'static str, String)> = Vec::new();

    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        // Stesso unwrap di `export_statement` di SPEC-025 §10 (verificato
        // empiricamente identico anche per la grammatica TypeScript:
        // `export function`/`export class`/`export interface` avvolgono
        // tutte allo stesso modo, campo `declaration`).
        let child = if child.kind() == "export_statement" {
            child.child_by_field_name("declaration").unwrap_or(child)
        } else {
            child
        };
        match child.kind() {
            "interface_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let interface_name = text(name_node, source);
                let interface_id = make_id(file_path, &interface_name);
                nodes.push(CodeNode {
                    id: interface_id.clone(),
                    domain: "code".to_string(),
                    node_type: "Interface".to_string(),
                    name: interface_name.clone(),
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &interface_id, budget, &mut chunks);
                name_to_id.insert(interface_name.clone(), interface_id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: interface_id.clone(),
                    weight: None,
                });

                // `interface X extends Y, Z {}` -> `extends_type_clause`
                // figlio diretto, campo `type` RIPETUTO (uno per target) —
                // verificato empiricamente (SPEC-026 §10).
                let mut iface_cursor = child.walk();
                if let Some(extends_clause) = child
                    .children(&mut iface_cursor)
                    .find(|c| c.kind() == "extends_type_clause")
                {
                    let mut type_cursor = extends_clause.walk();
                    for target in extends_clause.children_by_field_name("type", &mut type_cursor) {
                        heritage_refs.push((interface_id.clone(), "EXTENDS", text(target, source)));
                    }
                }

                // `property_signature` (es. `readonly id: string;`) non è
                // un Method: nessun nodo prodotto, nessun caso speciale
                // scritto apposta — il filtro sotto lo esclude
                // semplicemente non riconoscendolo (SPEC-026 §4 edge case).
                if let Some(body) = child.child_by_field_name("body") {
                    let mut member_cursor = body.walk();
                    for member in body
                        .children(&mut member_cursor)
                        .filter(|m| m.kind() == "method_signature")
                    {
                        let Some(method_name_node) = member.child_by_field_name("name") else {
                            continue;
                        };
                        let simple_name = text(method_name_node, source);
                        let qualified_name = format!("{interface_name}.{simple_name}");
                        let method_id = make_id(file_path, &qualified_name);
                        nodes.push(CodeNode {
                            id: method_id.clone(),
                            domain: "code".to_string(),
                            node_type: "Method".to_string(),
                            name: simple_name.clone(),
                            ast_hash: hashing::merkle_hash(member, source.as_bytes()),
                            file_path: file_path.to_string(),
                        });
                        push_entity_chunks(member, source, &method_id, budget, &mut chunks);
                        name_to_id.insert(simple_name, method_id.clone());
                        relations.push(CodeRelation {
                            domain: "code".to_string(),
                            rel_type: "CONTAINS".to_string(),
                            from_id: interface_id.clone(),
                            to_id: method_id.clone(),
                            weight: None,
                        });
                        // `method_signature` non ha MAI un campo `body`
                        // (nessuna implementazione, §2): nessun
                        // `callable_bodies` entry, nessun CALLS possibile
                        // da una firma sola.
                    }
                }
            }
            "class_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let class_name = text(name_node, source);
                let class_id = make_id(file_path, &class_name);
                nodes.push(CodeNode {
                    id: class_id.clone(),
                    domain: "code".to_string(),
                    node_type: "Class".to_string(),
                    name: class_name.clone(),
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &class_id, budget, &mut chunks);
                name_to_id.insert(class_name.clone(), class_id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: class_id.clone(),
                    weight: None,
                });

                // `class_heritage` figlio diretto (non un campo), a sua
                // volta contenitore di `extends_clause` (campo `value`,
                // kind `identifier` — non `type_identifier`: asimmetria
                // reale rispetto a `implements_clause`, verificata
                // empiricamente, SPEC-026 §10) e/o `implements_clause`
                // (figli `type_identifier` ripetuti, senza field name).
                let mut class_cursor = child.walk();
                if let Some(heritage) = child
                    .children(&mut class_cursor)
                    .find(|c| c.kind() == "class_heritage")
                {
                    let mut heritage_cursor = heritage.walk();
                    for clause in heritage.children(&mut heritage_cursor) {
                        match clause.kind() {
                            "extends_clause" => {
                                if let Some(value) = clause.child_by_field_name("value") {
                                    heritage_refs.push((
                                        class_id.clone(),
                                        "EXTENDS",
                                        text(value, source),
                                    ));
                                }
                            }
                            "implements_clause" => {
                                let mut impl_cursor = clause.walk();
                                for target in clause
                                    .children(&mut impl_cursor)
                                    .filter(|c| c.kind() == "type_identifier")
                                {
                                    heritage_refs.push((
                                        class_id.clone(),
                                        "IMPLEMENTS",
                                        text(target, source),
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }

                if let Some(class_body) = child.child_by_field_name("body") {
                    let mut member_cursor = class_body.walk();
                    for member in class_body
                        .children(&mut member_cursor)
                        .filter(|m| m.kind() == "method_definition")
                    {
                        let Some(method_name_node) = member.child_by_field_name("name") else {
                            continue;
                        };
                        let simple_name = text(method_name_node, source);
                        let qualified_name = format!("{class_name}.{simple_name}");
                        let method_id = make_id(file_path, &qualified_name);
                        nodes.push(CodeNode {
                            id: method_id.clone(),
                            domain: "code".to_string(),
                            node_type: "Method".to_string(),
                            name: simple_name.clone(),
                            ast_hash: hashing::merkle_hash(member, source.as_bytes()),
                            file_path: file_path.to_string(),
                        });
                        push_entity_chunks(member, source, &method_id, budget, &mut chunks);
                        name_to_id.insert(simple_name, method_id.clone());
                        relations.push(CodeRelation {
                            domain: "code".to_string(),
                            rel_type: "CONTAINS".to_string(),
                            from_id: class_id.clone(),
                            to_id: method_id.clone(),
                            weight: None,
                        });
                        if let Some(body) = member.child_by_field_name("body") {
                            callable_bodies.push((method_id, body));
                        }
                    }
                }
            }
            "function_declaration" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let simple_name = text(name_node, source);
                let id = make_id(file_path, &simple_name);
                nodes.push(CodeNode {
                    id: id.clone(),
                    domain: "code".to_string(),
                    node_type: "Function".to_string(),
                    name: simple_name.clone(),
                    ast_hash: hashing::merkle_hash(child, source.as_bytes()),
                    file_path: file_path.to_string(),
                });
                push_entity_chunks(child, source, &id, budget, &mut chunks);
                name_to_id.insert(simple_name, id.clone());
                relations.push(CodeRelation {
                    domain: "code".to_string(),
                    rel_type: "CONTAINS".to_string(),
                    from_id: file_id.clone(),
                    to_id: id.clone(),
                    weight: None,
                });
                if let Some(body) = child.child_by_field_name("body") {
                    callable_bodies.push((id, body));
                }
            }
            _ => {}
        }
    }

    for (from_id, rel_type, target_name) in heritage_refs {
        if let Some(to_id) = name_to_id.get(&target_name) {
            relations.push(CodeRelation {
                domain: "code".to_string(),
                rel_type: rel_type.to_string(),
                from_id,
                to_id: to_id.clone(),
                weight: None,
            });
        }
        // else: non risolvibile intra-file -> nessun arco (SPEC-026 §2/§5,
        // §4 edge case "referenzia un nome NON definito nello stesso file").
    }

    let mut unresolved = Vec::new();
    for (caller_id, body) in callable_bodies {
        let mut call_counts: HashMap<String, u32> = HashMap::new();
        let mut unresolved_counts: HashMap<String, u32> = HashMap::new();
        // Riuso diretto di `collect_calls_js` (SPEC-026 §2/§10): i kind
        // Tree-sitter coinvolti (`call_expression`, `member_expression`,
        // `identifier`, `property_identifier`) sono stati verificati
        // empiricamente identici tra le due grammatiche prima di
        // scrivere questa funzione — nessun codice nuovo necessario.
        collect_calls_js(
            body,
            source,
            &name_to_id,
            &mut call_counts,
            &mut unresolved_counts,
        );
        for (callee_id, weight) in call_counts {
            relations.push(CodeRelation {
                domain: "code".to_string(),
                rel_type: "CALLS".to_string(),
                from_id: caller_id.clone(),
                to_id: callee_id,
                weight: Some(weight),
            });
        }
        for (callee_name, weight) in unresolved_counts {
            unresolved.push(resolve::UnresolvedCall {
                caller_id: caller_id.clone(),
                callee_name,
                weight,
            });
        }
    }

    (nodes, relations, unresolved, chunks)
}

/// Cammina ricorsivamente il corpo di una funzione/metodo JS cercando
/// `call_expression`: risolve il nome del target (identifier semplice,
/// es. `computeTotal(...)`, o il campo `property` di una
/// `member_expression` per le chiamate a metodo, es. `this.validate(...)`/
/// `obj.metodo(...)` -> `validate`/`metodo` — equivalente JS della
/// `selector_expression` di Go) contro `name_to_id`, SOLO nomi dichiarati
/// in questo stesso file (SPEC-024 §2, stesso limite di SPEC-013 §2).
/// `new X(...)` produce un nodo `new_expression`, non `call_expression`:
/// non entra in questo percorso per costruzione, non per un filtro
/// esplicito (verificato empiricamente, SPEC-024 §10).
///
/// Un nome non risolto localmente (`name_to_id` non lo contiene) non
/// viene più scartato in silenzio (a differenza della variante Go/§T1.1
/// originale): finisce in `unresolved_counts`, consumato da
/// [`resolve::resolve_cross_file_calls`] per la risoluzione cross-file
/// (SPEC-025 §2) — stesso meccanismo di attraversamento, riusato per
/// entrambi gli scopi in un solo passaggio.
fn collect_calls_js(
    node: Node,
    source: &str,
    name_to_id: &HashMap<String, String>,
    call_counts: &mut HashMap<String, u32>,
    unresolved_counts: &mut HashMap<String, u32>,
) {
    if node.kind() == "call_expression" {
        if let Some(function_node) = node.child_by_field_name("function") {
            let callee_name = match function_node.kind() {
                "identifier" => Some(text(function_node, source)),
                "member_expression" => function_node
                    .child_by_field_name("property")
                    .map(|f| text(f, source)),
                _ => None,
            };
            if let Some(name) = callee_name {
                if let Some(id) = name_to_id.get(&name) {
                    *call_counts.entry(id.clone()).or_insert(0) += 1;
                } else {
                    *unresolved_counts.entry(name).or_insert(0) += 1;
                }
            }
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_calls_js(child, source, name_to_id, call_counts, unresolved_counts);
    }
}

/// Estrae il nome del tipo receiver da un `parameter_list` di receiver
/// (es. `(s *OrderService)` o `(s OrderService)`), spogliando il
/// `pointer_type` se presente. `None` se la struttura non è quella
/// attesa (nessun panic su input inatteso).
fn receiver_type_name(receiver_param_list: Node, source: &str) -> Option<String> {
    let mut cursor = receiver_param_list.walk();
    let param_decl = receiver_param_list
        .children(&mut cursor)
        .find(|c| c.kind() == "parameter_declaration")?;
    let type_node = param_decl.child_by_field_name("type")?;
    let resolved = if type_node.kind() == "pointer_type" {
        type_node.named_child(0)?
    } else {
        type_node
    };
    Some(text(resolved, source))
}

/// Cammina ricorsivamente il corpo di una funzione/metodo cercando
/// `call_expression`: risolve il nome del target (identifier semplice, o
/// il campo di una selector_expression per le chiamate a metodo, es.
/// `s.Validate(...)` -> `Validate`) contro `name_to_id` — SOLO nomi
/// dichiarati in questo stesso file (SPEC-013 §2). Un nome non trovato
/// (builtin, importato, o cross-file) non produce nessun arco: questo è
/// il comportamento corretto atteso, non un'omissione (§2/§4). Le
/// occorrenze ripetute della stessa coppia caller/callee si aggregano in
/// `call_counts` (SPEC-013 §3 scenario 5): un arco CALLS con weight
/// aggregato, non archi multipli.
fn collect_calls(
    node: Node,
    source: &str,
    name_to_id: &HashMap<String, String>,
    call_counts: &mut HashMap<String, u32>,
) {
    if node.kind() == "call_expression" {
        if let Some(function_node) = node.child_by_field_name("function") {
            let callee_name = match function_node.kind() {
                "identifier" => Some(text(function_node, source)),
                "selector_expression" => function_node
                    .child_by_field_name("field")
                    .map(|f| text(f, source)),
                _ => None,
            };
            if let Some(id) = callee_name.and_then(|name| name_to_id.get(&name)) {
                *call_counts.entry(id.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_calls(child, source, name_to_id, call_counts);
    }
}

fn text(node: Node, source: &str) -> String {
    node.utf8_text(source.as_bytes()).unwrap_or("").to_string()
}

/// Cablaggio comune (SPEC-029 §2) usato da tutte e tre le varianti
/// `parse_*_file_full`: chunka `node` (il nodo Tree-sitter dell'entità
/// appena costruita) e accumula il risultato in `chunks`, popolando
/// `entity_id` con l'id reale — lasciato vuoto da `chunk_entity` stessa
/// (T2.2 §10, "da un futuro chiamante": questo lo è).
fn push_entity_chunks(
    node: Node,
    source: &str,
    entity_id: &str,
    budget: usize,
    chunks: &mut Vec<chunking::CodeChunk>,
) {
    for mut chunk in chunking::chunk_entity(node, source.as_bytes(), budget) {
        chunk.entity_id = entity_id.to_string();
        chunks.push(chunk);
    }
}

/// Id deterministico: SHA-256 esadecimale di `"{file_path}:{qualified_name}"`
/// (SPEC-013 §2). Per i `Method`, `qualified_name` è
/// `"{ReceiverType}.{MethodName}"` (disambigua id tra receiver diversi
/// con lo stesso nome di metodo nello stesso file — la risoluzione CALLS
/// per nome invece ignora deliberatamente il receiver, vedi §2).
fn make_id(file_path: &str, qualified_name: &str) -> String {
    sha256_hex(&format!("{file_path}:{qualified_name}"))
}

fn sha256_hex(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Legge un file del fixture di SPEC-009 direttamente da disco (non
    /// duplicato come stringa Go inline, SPEC-013 §7) — un cambiamento
    /// futuro al fixture si riflette automaticamente in questi test.
    fn read_fixture(name: &str) -> String {
        let path = format!(
            "{}/../../tests/fixtures/sample-repo/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
    }

    fn node_types(nodes: &[CodeNode]) -> Vec<(&str, &str)> {
        nodes
            .iter()
            .map(|n| (n.node_type.as_str(), n.name.as_str()))
            .collect()
    }

    fn find_id<'a>(nodes: &'a [CodeNode], node_type: &str, name: &str) -> &'a str {
        nodes
            .iter()
            .find(|n| n.node_type == node_type && n.name == name)
            .unwrap_or_else(|| panic!("nodo {node_type} {name:?} non trovato in {nodes:?}"))
            .id
            .as_str()
    }

    // SPEC-013 §3 scenario 1.
    #[test]
    fn scenario1_order_service_nodes_and_contains() {
        let source = read_fixture("order_service.go");
        let (nodes, relations) = parse_file("order_service.go", &source);

        assert_eq!(
            nodes.len(),
            4,
            "attesi 4 CodeNode (File, Class, 2 Method), ottenuti: {:?}",
            node_types(&nodes)
        );
        let types = node_types(&nodes);
        assert!(types.contains(&("File", "order_service.go")));
        assert!(types.contains(&("Class", "OrderService")));
        assert!(types.contains(&("Method", "Process")));
        assert!(types.contains(&("Method", "Validate")));

        let contains: Vec<_> = relations
            .iter()
            .filter(|r| r.rel_type == "CONTAINS")
            .collect();
        assert_eq!(
            contains.len(),
            3,
            "attesi 3 archi CONTAINS (File->Class, File->Process, File->Validate), ottenuti: {contains:?}"
        );
        let file_id = find_id(&nodes, "File", "order_service.go");
        let class_id = find_id(&nodes, "Class", "OrderService");
        let process_id = find_id(&nodes, "Method", "Process");
        let validate_id = find_id(&nodes, "Method", "Validate");
        let contains_targets: HashSet<&str> = contains.iter().map(|r| r.to_id.as_str()).collect();
        assert!(contains.iter().all(|r| r.from_id == file_id));
        assert_eq!(
            contains_targets,
            HashSet::from([class_id, process_id, validate_id])
        );
    }

    // SPEC-013 §3 scenario 2.
    #[test]
    fn scenario2_order_service_calls_process_to_validate() {
        let source = read_fixture("order_service.go");
        let (nodes, relations) = parse_file("order_service.go", &source);

        let process_id = find_id(&nodes, "Method", "Process").to_string();
        let validate_id = find_id(&nodes, "Method", "Validate").to_string();

        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(
            calls.len(),
            1,
            "atteso ESATTAMENTE 1 arco CALLS (Process->Validate; Process->computeTotal è cross-file, scenario 3), ottenuti: {calls:?}"
        );
        let call = calls[0];
        assert_eq!(call.from_id, process_id);
        assert_eq!(call.to_id, validate_id);
        assert_eq!(call.weight, Some(1));
    }

    // SPEC-013 §3 scenario 3.
    #[test]
    fn scenario3_order_service_no_cross_file_call_to_compute_total() {
        let source = read_fixture("order_service.go");
        let (nodes, relations) = parse_file("order_service.go", &source);

        // computeTotal è dichiarata in util.go: parse_file su
        // order_service.go da solo non deve produrre NESSUN CodeNode né
        // arco CALLS che la riguardi.
        assert!(
            !nodes.iter().any(|n| n.name == "computeTotal"),
            "computeTotal è dichiarata in util.go, non deve comparire parsando solo order_service.go"
        );
        let process_id = find_id(&nodes, "Method", "Process").to_string();
        assert!(
            !relations.iter().any(|r| r.rel_type == "CALLS"
                && r.from_id == process_id
                && !nodes.iter().any(|n| n.id == r.to_id && n.name == "Validate")),
            "nessun arco CALLS da Process verso un target diverso da Validate (computeTotal è cross-file)"
        );
    }

    // SPEC-013 §3 scenario 4.
    #[test]
    fn scenario4_notifier_interface_and_class() {
        let source = read_fixture("notifier.go");
        let (nodes, relations) = parse_file("notifier.go", &source);

        assert_eq!(
            nodes.len(),
            4,
            "attesi 4 CodeNode (File, Interface, Class, Method), ottenuti: {:?}",
            node_types(&nodes)
        );
        let types = node_types(&nodes);
        assert!(types.contains(&("File", "notifier.go")));
        assert!(types.contains(&("Interface", "Notifier")));
        assert!(types.contains(&("Class", "EmailNotifier")));
        assert!(types.contains(&("Method", "Notify")));

        let contains_count = relations
            .iter()
            .filter(|r| r.rel_type == "CONTAINS")
            .count();
        assert_eq!(contains_count, 3, "attesi 3 archi CONTAINS (g07)");
        let calls_count = relations.iter().filter(|r| r.rel_type == "CALLS").count();
        assert_eq!(calls_count, 0, "Notify non ha chiamate in uscita");
    }

    // SPEC-013 §3 scenario 5 — caso non presente nel fixture, costruito
    // ad-hoc (unica eccezione ammessa esplicitamente da §7).
    #[test]
    fn scenario5_repeated_call_aggregates_single_weighted_edge() {
        let source = r#"package main

func helper() {}

func caller() {
	helper()
	helper()
	helper()
}
"#;
        let (nodes, relations) = parse_file("repeated.go", source);

        assert_eq!(nodes.len(), 3, "attesi 3 CodeNode (File, 2 Function)");
        let caller_id = find_id(&nodes, "Function", "caller").to_string();
        let helper_id = find_id(&nodes, "Function", "helper").to_string();

        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(
            calls.len(),
            1,
            "atteso UN SOLO arco CALLS aggregato, non uno per occorrenza: {calls:?}"
        );
        assert_eq!(calls[0].from_id, caller_id);
        assert_eq!(calls[0].to_id, helper_id);
        assert_eq!(
            calls[0].weight,
            Some(3),
            "3 occorrenze di helper() dentro caller()"
        );
    }

    // SPEC-013 §3 scenario 6.
    #[test]
    fn scenario6_main_go_isolated_no_cross_file_call() {
        let source = read_fixture("main.go");
        let (nodes, relations) = parse_file("main.go", &source);

        assert_eq!(nodes.len(), 2, "attesi 2 CodeNode (File, Function main)");
        let types = node_types(&nodes);
        assert!(types.contains(&("File", "main.go")));
        assert!(types.contains(&("Function", "main")));

        assert_eq!(
            relations
                .iter()
                .filter(|r| r.rel_type == "CONTAINS")
                .count(),
            1,
            "atteso 1 arco CONTAINS (File->main)"
        );
        assert_eq!(
            relations.iter().filter(|r| r.rel_type == "CALLS").count(),
            0,
            "Process è cross-file (order_service.go): nessun arco CALLS atteso (g02)"
        );
    }

    // SPEC-013 §4 edge case: file senza dichiarazioni di primo livello.
    #[test]
    fn edge_case_file_with_no_declarations_produces_only_file_node() {
        let (nodes, relations) = parse_file("empty.go", "package main\n");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_type, "File");
        assert!(relations.is_empty());
    }

    // SPEC-013 §4 edge case: codice Go sintatticamente invalido. Il source
    // qui sotto è REALMENTE invalido (compilazione Go fallirebbe: `struct`
    // senza corpo `{}`) — verificato con `tree.root_node().has_error()`
    // durante lo sviluppo. Dimostra ENTRAMBI i lati del comportamento
    // atteso: (a) le dichiarazioni valide che precedono l'errore restano
    // intatte ("valid"), (b) una dichiarazione che il recovery di
    // Tree-sitter inghiotte dentro la regione malformata ("alsoValid",
    // assorbita come field_declaration dentro il corpo tronco di
    // `type Broken struct`) viene omessa, non causa panic né corrompe il
    // resto del file.
    #[test]
    fn edge_case_syntax_error_recovers_valid_and_omits_unextractable() {
        let source = r#"package main

func valid() {
	helper()
}

type Broken struct

func alsoValid() {
	valid()
}
"#;
        let (nodes, _relations) = parse_file("broken.go", source);

        assert_eq!(
            nodes.iter().filter(|n| n.node_type == "File").count(),
            1,
            "il File node va comunque prodotto nonostante l'errore di sintassi"
        );
        assert!(
            nodes.iter().any(|n| n.node_type == "Function" && n.name == "valid"),
            "\"valid\" precede l'errore di sintassi, deve restare estratta correttamente: {nodes:?}"
        );
        assert!(
            !nodes.iter().any(|n| n.name == "alsoValid"),
            "\"alsoValid\" viene inghiottita dalla regione di errore di \
             \"type Broken struct\" (nessun corpo `{{}}`): omessa, non un panic — comportamento atteso (§4): {nodes:?}"
        );
    }

    // ============================================================
    // SPEC-024 §3/§4 — estrazione CPG JavaScript (T2.5, parte 1/3),
    // mirror diretto degli scenari di T1.1/SPEC-013 sopra, stessa
    // numerazione concettuale, fixture in tests/fixtures/sample-repo-js/.
    // ============================================================
    mod js_tests {
        use super::*;

        fn read_js_fixture(name: &str) -> String {
            let path = format!(
                "{}/../../tests/fixtures/sample-repo-js/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
        }

        // SPEC-024 §3 scenario 1.
        #[test]
        fn scenario1_order_service_nodes_and_contains() {
            let source = read_js_fixture("order_service.js");
            let (nodes, relations) = parse_file("order_service.js", &source);

            assert_eq!(
                nodes.len(),
                5,
                "attesi 5 CodeNode (File, Class, 3 Method: constructor/process/validate), ottenuti: {:?}",
                node_types(&nodes)
            );
            let types = node_types(&nodes);
            assert!(types.contains(&("File", "order_service.js")));
            assert!(types.contains(&("Class", "OrderService")));
            assert!(types.contains(&("Method", "constructor")));
            assert!(types.contains(&("Method", "process")));
            assert!(types.contains(&("Method", "validate")));

            let contains: Vec<_> = relations
                .iter()
                .filter(|r| r.rel_type == "CONTAINS")
                .collect();
            assert_eq!(
                contains.len(),
                4,
                "attesi 4 archi CONTAINS (File->Class, Class->constructor, Class->process, Class->validate), ottenuti: {contains:?}"
            );
            let file_id = find_id(&nodes, "File", "order_service.js");
            let class_id = find_id(&nodes, "Class", "OrderService");
            let constructor_id = find_id(&nodes, "Method", "constructor");
            let process_id = find_id(&nodes, "Method", "process");
            let validate_id = find_id(&nodes, "Method", "validate");

            let file_contains: Vec<_> = contains.iter().filter(|r| r.from_id == file_id).collect();
            assert_eq!(file_contains.len(), 1, "atteso un solo CONTAINS da File");
            assert_eq!(file_contains[0].to_id, class_id);

            // Class->Method: tre archi, uno per metodo — NUOVO rispetto a
            // Go, verificato esplicitamente qui (SPEC-024 §3 scenario 1).
            let class_contains: HashSet<&str> = contains
                .iter()
                .filter(|r| r.from_id == class_id)
                .map(|r| r.to_id.as_str())
                .collect();
            assert_eq!(
                class_contains,
                HashSet::from([constructor_id, process_id, validate_id])
            );
        }

        // SPEC-024 §3 scenario 2.
        #[test]
        fn scenario2_process_calls_validate_intra_class() {
            let source = read_js_fixture("order_service.js");
            let (nodes, relations) = parse_file("order_service.js", &source);

            let process_id = find_id(&nodes, "Method", "process").to_string();
            let validate_id = find_id(&nodes, "Method", "validate").to_string();

            let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
            assert_eq!(
                calls.len(),
                1,
                "atteso ESATTAMENTE 1 arco CALLS (process->validate; process->computeTotal è cross-file, scenario 3), ottenuti: {calls:?}"
            );
            assert_eq!(calls[0].from_id, process_id);
            assert_eq!(calls[0].to_id, validate_id);
            assert_eq!(calls[0].weight, Some(1));
        }

        // SPEC-024 §3 scenario 3.
        #[test]
        fn scenario3_no_cross_file_call_to_compute_total() {
            let source = read_js_fixture("order_service.js");
            let (nodes, relations) = parse_file("order_service.js", &source);

            assert!(
                !nodes.iter().any(|n| n.name == "computeTotal"),
                "computeTotal è dichiarata in util.js, non deve comparire parsando solo order_service.js"
            );
            let process_id = find_id(&nodes, "Method", "process").to_string();
            assert!(
                !relations.iter().any(|r| r.rel_type == "CALLS"
                    && r.from_id == process_id
                    && !nodes.iter().any(|n| n.id == r.to_id && n.name == "validate")),
                "nessun arco CALLS da process verso un target diverso da validate (computeTotal è cross-file)"
            );
        }

        // SPEC-024 §3 scenario 4.
        #[test]
        fn scenario4_util_compute_total_function() {
            let source = read_js_fixture("util.js");
            let (nodes, relations) = parse_file("util.js", &source);

            assert_eq!(
                nodes.len(),
                2,
                "attesi 2 CodeNode (File, Function computeTotal), ottenuti: {:?}",
                node_types(&nodes)
            );
            let types = node_types(&nodes);
            assert!(types.contains(&("File", "util.js")));
            assert!(types.contains(&("Function", "computeTotal")));

            assert_eq!(
                relations
                    .iter()
                    .filter(|r| r.rel_type == "CONTAINS")
                    .count(),
                1,
                "atteso 1 arco CONTAINS (File->computeTotal)"
            );
            assert_eq!(
                relations.iter().filter(|r| r.rel_type == "CALLS").count(),
                0,
                "prices.reduce(...) chiama un metodo su un builtin, non una dichiarazione del file: nessun arco CALLS atteso"
            );
        }

        // SPEC-024 §3 scenario 5.
        #[test]
        fn scenario5_main_isolated_no_cross_file_or_instance_call() {
            let source = read_js_fixture("main.js");
            let (nodes, relations) = parse_file("main.js", &source);

            assert_eq!(
                nodes.len(),
                2,
                "attesi 2 CodeNode (File, Function main), ottenuti: {:?}",
                node_types(&nodes)
            );
            let types = node_types(&nodes);
            assert!(types.contains(&("File", "main.js")));
            assert!(types.contains(&("Function", "main")));

            assert_eq!(
                relations
                    .iter()
                    .filter(|r| r.rel_type == "CONTAINS")
                    .count(),
                1,
                "atteso 1 arco CONTAINS (File->main)"
            );
            assert_eq!(
                relations.iter().filter(|r| r.rel_type == "CALLS").count(),
                0,
                "service.process(...) è una chiamata su un'istanza, non risolvibile per nome in questo file; \
                 new OrderService(2) è un new_expression, non un call_expression, nessuno dei due produce CALLS"
            );
        }

        // SPEC-024 §3 scenario 6: error recovery, stesso principio di T1.1
        // (SPEC-013 §4) — caso dedicato inline (non nel fixture, stesso
        // principio già ammesso per Go, SPEC-013 §7). Verificato con
        // `has_error()` che il source sia realmente malformato (una
        // `class` con corpo non chiuso, `MISSING "}"` riportato da
        // Tree-sitter) durante lo sviluppo — solo l'affermazione POSITIVA
        // di §3 scenario 6 è verificata qui ("le porzioni non affette
        // restano estraibili"), non ipotesi su come Tree-sitter tratti
        // internamente la porzione malformata (comportamento diverso e
        // più complesso di quello Go, non rilevante per questo scenario).
        #[test]
        fn scenario6_syntax_error_recovers_valid_declarations() {
            let source = r#"function valid() {
  helper();
}

class Broken {

function alsoValid() {
  valid();
}
"#;
            let (nodes, _relations) = parse_file("broken.js", source);

            assert_eq!(
                nodes.iter().filter(|n| n.node_type == "File").count(),
                1,
                "il File node va comunque prodotto nonostante l'errore di sintassi"
            );
            assert!(
                nodes.iter().any(|n| n.node_type == "Function" && n.name == "valid"),
                "\"valid\" precede l'errore di sintassi, deve restare estratta correttamente: {nodes:?}"
            );
        }

        // --- SPEC-024 §4 edge case: funzione nidificata dentro un'altra
        // funzione (non top-level, non dentro una classe) — non estratta
        // come Function, nessun crash. ---
        #[test]
        fn edge_case_nested_function_not_extracted() {
            let source = r#"function outer() {
  function inner() {}
  inner();
}
"#;
            let (nodes, _relations) = parse_file("nested.js", source);
            assert!(
                nodes
                    .iter()
                    .any(|n| n.node_type == "Function" && n.name == "outer"),
                "outer deve essere estratta: {nodes:?}"
            );
            assert!(
                !nodes.iter().any(|n| n.name == "inner"),
                "inner è nidificata, non deve produrre un proprio CodeNode: {nodes:?}"
            );
        }

        // --- SPEC-024 §4 edge case: metodo statico, stesso trattamento
        // di un Method ordinario. ---
        #[test]
        fn edge_case_static_method_extracted_as_ordinary_method() {
            let source = r#"class Foo {
  static bar() {}
}
"#;
            let (nodes, _relations) = parse_file("static.js", source);
            let bar = nodes
                .iter()
                .find(|n| n.name == "bar")
                .unwrap_or_else(|| panic!("bar non trovato: {nodes:?}"));
            assert_eq!(
                bar.node_type, "Method",
                "static non deve cambiare node_type"
            );
        }

        // --- SPEC-024 §4 edge case: getter/setter, stesso trattamento di
        // un Method ordinario. ---
        #[test]
        fn edge_case_getter_setter_extracted_as_ordinary_methods() {
            let source = r#"class Foo {
  get baz() { return 1; }
  set baz(v) {}
}
"#;
            let (nodes, _relations) = parse_file("accessors.js", source);
            let methods: Vec<_> = nodes.iter().filter(|n| n.name == "baz").collect();
            assert_eq!(
                methods.len(),
                2,
                "getter e setter devono produrre due CodeNode distinti: {nodes:?}"
            );
            assert!(methods.iter().all(|n| n.node_type == "Method"));
        }

        // Verifica diretta del dispatch: l'estensione .js instrada
        // sull'estrazione JS reale (non su un risultato vuoto/sbagliato).
        #[test]
        fn dispatch_routes_js_extension_correctly() {
            let source = read_js_fixture("util.js");
            let (nodes, _relations) = parse_file("util.js", &source);
            assert!(
                nodes
                    .iter()
                    .any(|n| n.node_type == "Function" && n.name == "computeTotal"),
                "il dispatch su .js deve produrre l'estrazione JS reale: {nodes:?}"
            );
        }

        #[test]
        #[should_panic(expected = "estensione non supportata")]
        fn dispatch_panics_on_unsupported_extension() {
            let _ = parse_file("mystery.py", "print('hello')");
        }
    }

    // ============================================================
    // SPEC-026 §3/§4 — estrazione + risoluzione TypeScript (T2.5, parte
    // 3/3), fixture in tests/fixtures/sample-repo-ts/.
    // ============================================================
    mod ts_tests {
        use super::*;
        use crate::imports::extract_imports;
        use crate::resolve::resolve_cross_file_calls;
        use std::path::PathBuf;

        fn read_ts_fixture(name: &str) -> String {
            let path = format!(
                "{}/../../tests/fixtures/sample-repo-ts/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
        }

        fn parse_ts_tree(source: &str) -> tree_sitter::Tree {
            let mut parser = tree_sitter::Parser::new();
            parser
                .set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())
                .expect("grammatica tree-sitter-typescript valida");
            parser.parse(source, None).expect("tree-sitter parse")
        }

        fn build_ts_file_entry(
            path: &str,
            source: &str,
        ) -> (
            PathBuf,
            Vec<CodeNode>,
            Vec<crate::imports::ImportBinding>,
            crate::resolve::UnresolvedCalls,
        ) {
            let tree = parse_ts_tree(source);
            let imports = extract_imports(&tree, source.as_bytes());
            let (nodes, _relations, unresolved, _chunks) = parse_ts_file_full(path, source);
            (PathBuf::from(path), nodes, imports, unresolved)
        }

        // --- SPEC-026 §3 scenario 1. ---
        #[test]
        fn scenario1_notifier_interface_and_class_distinct_method_ids() {
            let source = read_ts_fixture("notifier.ts");
            let (nodes, relations) = parse_file("notifier.ts", &source);

            let types = node_types(&nodes);
            assert!(types.contains(&("File", "notifier.ts")));
            assert!(types.contains(&("Interface", "Notifier")));
            assert!(types.contains(&("Class", "EmailNotifier")));
            let notify_methods: Vec<_> = nodes.iter().filter(|n| n.name == "notify").collect();
            assert_eq!(
                notify_methods.len(),
                2,
                "due Method 'notify' distinti (Interface + Class): {nodes:?}"
            );
            assert_ne!(
                notify_methods[0].id, notify_methods[1].id,
                "stesso nome semplice ma qualified_name diverso (Notifier.notify vs EmailNotifier.notify) -> id diverso"
            );
            assert!(notify_methods.iter().all(|n| n.node_type == "Method"));

            let interface_notify_id = find_id(&nodes, "Method", "notify");
            let _ = interface_notify_id; // esistenza già verificata sopra

            let contains: Vec<_> = relations
                .iter()
                .filter(|r| r.rel_type == "CONTAINS")
                .collect();
            let interface_id = find_id(&nodes, "Interface", "Notifier");
            let class_id = find_id(&nodes, "Class", "EmailNotifier");
            assert!(
                contains.iter().any(|r| r.from_id == interface_id),
                "Interface->Method(notify) CONTAINS atteso: {contains:?}"
            );
            assert!(
                contains.iter().any(|r| r.from_id == class_id),
                "Class->Method(notify) CONTAINS atteso: {contains:?}"
            );
        }

        // --- SPEC-026 §3 scenario 2. ---
        #[test]
        fn scenario2_email_notifier_implements_notifier() {
            let source = read_ts_fixture("notifier.ts");
            let (nodes, relations) = parse_file("notifier.ts", &source);

            let class_id = find_id(&nodes, "Class", "EmailNotifier");
            let interface_id = find_id(&nodes, "Interface", "Notifier");

            let implements: Vec<_> = relations
                .iter()
                .filter(|r| r.rel_type == "IMPLEMENTS")
                .collect();
            assert_eq!(implements.len(), 1, "ottenuti: {implements:?}");
            assert_eq!(implements[0].from_id, class_id);
            assert_eq!(implements[0].to_id, interface_id);
        }

        // --- SPEC-026 §3 scenario 3: cross-file CALLS, resolve_cross_file_calls
        // riusata invariata da SPEC-025. ---
        #[test]
        fn scenario3_cross_file_calls_process_to_compute_total_via_reused_resolver() {
            let order_service_src = read_ts_fixture("order_service.ts");
            let util_src = read_ts_fixture("util.ts");

            let order_service = build_ts_file_entry("order_service.ts", &order_service_src);
            let util = build_ts_file_entry("util.ts", &util_src);
            let order_service_nodes = order_service.1.clone();
            let util_nodes = util.1.clone();

            let relations = resolve_cross_file_calls(&[order_service, util]);

            let process_id = find_id(&order_service_nodes, "Method", "process");
            let compute_total_id = find_id(&util_nodes, "Function", "computeTotal");

            let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
            assert_eq!(calls.len(), 1, "ottenuti: {calls:?}");
            assert_eq!(calls[0].from_id, process_id);
            assert_eq!(calls[0].to_id, compute_total_id);
        }

        // --- SPEC-026 §3 scenario 4. ---
        #[test]
        fn scenario4_process_calls_validate_intra_file() {
            let source = read_ts_fixture("order_service.ts");
            let (nodes, relations) = parse_file("order_service.ts", &source);

            let process_id = find_id(&nodes, "Method", "process").to_string();
            let validate_id = find_id(&nodes, "Method", "validate").to_string();

            let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
            assert_eq!(
                calls.len(),
                1,
                "atteso solo process->validate (process->computeTotal è cross-file, scenario 3): {calls:?}"
            );
            assert_eq!(calls[0].from_id, process_id);
            assert_eq!(calls[0].to_id, validate_id);
        }

        // --- SPEC-026 §3 scenario 5: le annotazioni di tipo non
        // contaminano l'estrazione del nome. ---
        #[test]
        fn scenario5_type_annotations_do_not_contaminate_names() {
            let source = read_ts_fixture("order_service.ts");
            let (nodes, _relations) = parse_file("order_service.ts", &source);

            let types = node_types(&nodes);
            assert!(types.contains(&("Class", "OrderService")), "{types:?}");
            assert!(types.contains(&("Method", "process")), "{types:?}");
            assert!(types.contains(&("Method", "validate")), "{types:?}");
            assert!(types.contains(&("Method", "constructor")), "{types:?}");
            // Nessun nome dovrebbe contenere frammenti di tipo (es.
            // "number", "[]", ":") — verifica diretta, non solo assenza
            // di crash.
            for (_, name) in &types {
                assert!(
                    !name.contains("number") && !name.contains(':'),
                    "il nome {name:?} sembra contaminato da un'annotazione di tipo"
                );
            }
        }

        // --- SPEC-026 §4 edge case: implements/extends verso un nome non
        // definito nello stesso file -> nessun arco prodotto. ---
        #[test]
        fn edge_case_implements_extends_cross_file_reference_produces_no_edge() {
            let source = r#"class EmailNotifier implements Notifier {
  notify(message: string): void {}
}
"#;
            let (nodes, relations) = parse_file("standalone.ts", source);
            assert!(
                !nodes.iter().any(|n| n.name == "Notifier"),
                "Notifier non è definita in questo file: nessun CodeNode atteso"
            );
            assert!(
                !relations.iter().any(|r| r.rel_type == "IMPLEMENTS"),
                "nessun arco IMPLEMENTS: Notifier non è risolvibile intra-file: {relations:?}"
            );
        }

        // --- SPEC-026 §4 edge case: property signature in un'interfaccia
        // non estratta come Method. ---
        #[test]
        fn edge_case_interface_property_signature_not_extracted() {
            let source = r#"interface Foo {
  readonly id: string;
  bar(): void;
}
"#;
            let (nodes, _relations) = parse_file("foo.ts", source);
            assert!(
                !nodes.iter().any(|n| n.name == "id"),
                "property_signature 'id' non deve produrre un CodeNode: {nodes:?}"
            );
            assert!(
                nodes
                    .iter()
                    .any(|n| n.node_type == "Method" && n.name == "bar"),
                "method_signature 'bar' deve comunque essere estratto: {nodes:?}"
            );
        }

        // --- SPEC-026 §4 edge case: file .tsx -> comportamento
        // esplicitamente verificato, non lasciato implicito. .tsx non
        // combacia con l'estensione .ts: cade nel ramo "estensione non
        // supportata" già esistente da SPEC-024, stesso comportamento di
        // qualunque altra estensione sconosciuta. ---
        #[test]
        #[should_panic(expected = "estensione non supportata")]
        fn edge_case_tsx_file_falls_into_unsupported_extension_panic() {
            let _ = parse_file("component.tsx", "const x: number = 1;");
        }
    }
}
