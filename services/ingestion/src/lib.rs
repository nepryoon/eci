//! `ingestion` — parsing Go con Tree-sitter -> CPG walking skeleton
//! (SPEC-013, T1.1) più persistenza PostgreSQL transazionale (SPEC-014,
//! T1.2) più hashing Merkle bottom-up di `ast_hash` (SPEC-020, T2.1,
//! modulo [`hashing`], che sostituisce il placeholder `sha256_hex(source)`
//! di T1.1) più `doc_hash` (modulo [`hashing`]) e chunking cAST configurabile
//! (SPEC-021, T2.2, modulo [`chunking`]). `parse_file` produce
//! `CodeNode`/`CodeRelation` in memoria; `persist_parsed_file` (modulo
//! [`persist`]) li scrive dentro un'unica transazione ACID (nodi upsert,
//! relazioni sostituite, righe outbox).

use std::collections::HashMap;

use tree_sitter::Node;

pub mod chunking;
pub mod embedding;
pub mod hashing;
pub mod persist;
pub use persist::{persist_parsed_file, PersistError, PersistSummary};

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

/// Parsa il sorgente Go di un singolo file e produce i `CodeNode`/
/// `CodeRelation` corrispondenti (SPEC-013 §2): un `CodeNode` `File`,
/// un `CodeNode` per ogni `Class`/`Interface`/`Method`/`Function` di
/// primo livello, un arco `CONTAINS` da `File` verso ciascuno, e archi
/// `CALLS` risolti per nome **solo intra-file** (§2/§5 — nessuna
/// risoluzione cross-file, quella è T2.5).
pub fn parse_file(file_path: &str, source: &str) -> (Vec<CodeNode>, Vec<CodeRelation>) {
    let _span = tracing::info_span!("parse_file", file_path = file_path).entered();

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

    (nodes, relations)
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

        let contains: Vec<_> = relations.iter().filter(|r| r.rel_type == "CONTAINS").collect();
        assert_eq!(
            contains.len(),
            3,
            "attesi 3 archi CONTAINS (File->Class, File->Process, File->Validate), ottenuti: {contains:?}"
        );
        let file_id = find_id(&nodes, "File", "order_service.go");
        let class_id = find_id(&nodes, "Class", "OrderService");
        let process_id = find_id(&nodes, "Method", "Process");
        let validate_id = find_id(&nodes, "Method", "Validate");
        let contains_targets: HashSet<&str> =
            contains.iter().map(|r| r.to_id.as_str()).collect();
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

        let contains_count = relations.iter().filter(|r| r.rel_type == "CONTAINS").count();
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
        assert_eq!(calls[0].weight, Some(3), "3 occorrenze di helper() dentro caller()");
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
            relations.iter().filter(|r| r.rel_type == "CONTAINS").count(),
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
}
