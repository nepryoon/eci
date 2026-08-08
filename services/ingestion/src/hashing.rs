//! Merkle SHA-256 bottom-up hashing (SPEC-020, T2.1), estende il
//! placeholder `sha256_hex(source)` di T1.1 (SPEC-013 §5 Non-goals).
//!
//! Formula esatta da ADD Modulo 1 §1.6.1 / SPEC-020 §2:
//! `H(node) = SHA256(normalize(kind) || normalize(text) [se foglia] ||
//! H(child_1)..H(child_n) [se interno])`. Normalizzazione (§1.6.2 /
//! SPEC-020 §2 punto 3): whitespace mai incluso (gratis via testo dei nodi
//! foglia Tree-sitter), commenti saltati per intero, identificatori
//! identity-preserving per default salvo variabili locali dichiarate con
//! `:=`/`var` dentro il corpo di una Method/Function (alpha-renamed a
//! `LOCAL_N` posizionale).

use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};
use tree_sitter::Node;

/// Calcola l'hash Merkle di `node` (File/Class/Interface/Method/Function —
/// il nodo Tree-sitter già in mano durante il traversal esistente di
/// `parse_file`, SPEC-020 §2). I corpi di Method/Function annidati dentro
/// `node` (es. quando `node` è la radice del File) ricevono ciascuno la
/// propria mappa di variabili locali, scoperta fresca per ognuno.
pub fn merkle_hash(node: Node, source: &[u8]) -> String {
    hash_node(node, source, &HashMap::new())
}

fn hash_node(node: Node, source: &[u8], locals: &HashMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(node.kind().as_bytes());

    if node.child_count() == 0 {
        let raw = node_text(node, source);
        let normalized = if node.kind() == "identifier" {
            locals.get(raw.as_str()).cloned().unwrap_or(raw)
        } else {
            raw
        };
        hasher.update(normalized.as_bytes());
    } else {
        // Un metodo/funzione ha la propria mappa di variabili locali,
        // valida SOLO dentro il proprio field "body" (SPEC-020 §2 punto
        // 3: "DENTRO il corpo") — nome dichiarato, receiver, parametri e
        // tipo di ritorno restano identity-preserving anche se il loro
        // testo letterale coincide per caso con una variabile locale
        // interna (nessun caso del genere nei test, ma la separazione è
        // strutturalmente più fedele al testo della SPEC di applicare la
        // mappa all'intero sottoalbero della dichiarazione).
        let body_field = if matches!(node.kind(), "method_declaration" | "function_declaration") {
            node.child_by_field_name("body")
        } else {
            None
        };
        let body_locals = body_field.map(|body| collect_local_names(body, source));

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            // Commenti: saltati per intero, non contribuiscono né come
            // foglia né propagando un hash verso l'alto (SPEC-020 §2
            // punto 2).
            if child.kind() == "comment" {
                continue;
            }
            let child_locals = match (&body_field, &body_locals) {
                (Some(body), Some(map)) if body.id() == child.id() => map,
                _ => locals,
            };
            hasher.update(hash_node(child, source, child_locals).as_bytes());
        }
    }

    hex(hasher.finalize().as_slice())
}

/// Raccoglie, in ordine di prima dichiarazione, i nomi delle variabili
/// locali dichiarate con `:=` (`short_var_declaration`) o `var`
/// (`var_declaration`) dentro `body`, e li mappa a placeholder posizionali
/// deterministici `LOCAL_0`, `LOCAL_1`, ... (SPEC-020 §2 punto 3).
fn collect_local_names(body: Node, source: &[u8]) -> HashMap<String, String> {
    let mut order: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    collect_local_names_rec(body, source, &mut order, &mut seen);
    order
        .into_iter()
        .enumerate()
        .map(|(i, name)| (name, format!("LOCAL_{i}")))
        .collect()
}

fn collect_local_names_rec(
    node: Node,
    source: &[u8],
    order: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    match node.kind() {
        "comment" => return,
        "short_var_declaration" => {
            if let Some(left) = node.child_by_field_name("left") {
                let mut cursor = left.walk();
                for id in left.named_children(&mut cursor) {
                    if id.kind() == "identifier" {
                        record_name(node_text(id, source), order, seen);
                    }
                }
            }
        }
        "var_declaration" => {
            let mut cursor = node.walk();
            for spec in node
                .named_children(&mut cursor)
                .filter(|c| c.kind() == "var_spec")
            {
                let mut name_cursor = spec.walk();
                for id in spec.children_by_field_name("name", &mut name_cursor) {
                    record_name(node_text(id, source), order, seen);
                }
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_local_names_rec(child, source, order, seen);
    }
}

fn record_name(name: String, order: &mut Vec<String>, seen: &mut HashSet<String>) {
    if seen.insert(name.clone()) {
        order.push(name);
    }
}

fn node_text(node: Node, source: &[u8]) -> String {
    node.utf8_text(source).unwrap_or("").to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Cattura il commento di documentazione immediatamente precedente `node`
/// (SPEC-021 §2, convenzione Go): uno o più nodi `comment` consecutivi
/// nei fratelli precedenti di `node`, senza riga vuota né tra l'ultimo
/// commento e `node` né tra i commenti stessi. `None` se non ce n'è
/// nessuno con queste caratteristiche immediatamente prima. Il testo
/// hashato è lo slice grezzo di sorgente dal primo all'ultimo commento
/// trovato: preserva esattamente il whitespace originale tra loro (una
/// docstring è prosa, non codice — non ha senso normalizzarla come
/// `ast_hash`, SPEC-021 §2).
pub fn doc_hash(node: Node, source: &[u8]) -> Option<String> {
    let mut comments: Vec<Node> = Vec::new();
    let mut expected_end_row = node.start_position().row;
    let mut current = node.prev_sibling();

    while let Some(sibling) = current {
        if sibling.kind() != "comment" {
            break;
        }
        // Nessuna riga vuota tra questo commento e ciò che lo segue
        // (l'ultimo commento raccolto finora, o `node` stesso al primo
        // giro): la riga finale del commento dev'essere esattamente la
        // riga precedente.
        if sibling.end_position().row + 1 != expected_end_row {
            break;
        }
        expected_end_row = sibling.start_position().row;
        comments.push(sibling);
        current = sibling.prev_sibling();
    }

    if comments.is_empty() {
        return None;
    }

    // `comments` è stato accumulato dal più vicino al più lontano da
    // `node`: il primo per posizione nel sorgente è l'ultimo raccolto.
    let first = *comments.last().unwrap();
    let last = *comments.first().unwrap();
    let span = &source[first.start_byte()..last.end_byte()];

    let mut hasher = Sha256::new();
    hasher.update(span);
    Some(hex(hasher.finalize().as_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> tree_sitter::Tree {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_go::LANGUAGE.into())
            .expect("grammatica tree-sitter-go valida");
        parser.parse(source, None).expect("tree-sitter parse")
    }

    fn read_fixture(name: &str) -> String {
        let path = format!(
            "{}/../../tests/fixtures/sample-repo/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
    }

    /// Trova la `function_declaration`/`method_declaration` con questo
    /// nome dichiarato (campo "name") a livello top-level del file.
    fn find_callable<'a>(root: Node<'a>, source: &str, name: &str) -> Node<'a> {
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if matches!(child.kind(), "function_declaration" | "method_declaration") {
                if let Some(name_node) = child.child_by_field_name("name") {
                    if node_text(name_node, source.as_bytes()) == name {
                        return child;
                    }
                }
            }
        }
        panic!("callable {name:?} non trovato a top-level");
    }

    /// Trova il `type_spec` (Class/Interface) con questo nome dichiarato.
    fn find_type_spec<'a>(root: Node<'a>, source: &str, name: &str) -> Node<'a> {
        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "type_declaration" {
                let mut spec_cursor = child.walk();
                for spec in child
                    .children(&mut spec_cursor)
                    .filter(|c| c.kind() == "type_spec")
                {
                    if let Some(name_node) = spec.child_by_field_name("name") {
                        if node_text(name_node, source.as_bytes()) == name {
                            return spec;
                        }
                    }
                }
            }
        }
        panic!("type_spec {name:?} non trovato a top-level");
    }

    // --- SPEC-020 §3 scenario 1: whitespace/indentazione non cambia
    // l'hash di NESSUNA entità (File, Class, entrambi i Method). ---
    #[test]
    fn scenario1_whitespace_and_indentation_do_not_change_any_entity_hash() {
        let original = read_fixture("order_service.go");
        // Stessa semantica, whitespace/indentazione completamente diversi:
        // tab -> spazi, righe vuote extra, allineamento differente.
        let reformatted = r#"package main


import "errors"


type OrderService struct {
        MinItems int
}


func (s *OrderService) Process(prices []float64) (float64, error) {
        if err := s.Validate(prices); err != nil {
                return 0, err
        }
        return computeTotal(prices), nil
}



func (s *OrderService) Validate(prices []float64) error {
    if len(prices) < s.MinItems {
        return errors.New("too few items")
    }
    return nil
}
"#;

        let tree_a = parse(&original);
        let tree_b = parse(reformatted);
        let root_a = tree_a.root_node();
        let root_b = tree_b.root_node();

        assert_eq!(
            merkle_hash(root_a, original.as_bytes()),
            merkle_hash(root_b, reformatted.as_bytes()),
            "File hash deve restare identico a whitespace/indentazione diversi"
        );
        assert_eq!(
            merkle_hash(
                find_type_spec(root_a, &original, "OrderService"),
                original.as_bytes()
            ),
            merkle_hash(
                find_type_spec(root_b, reformatted, "OrderService"),
                reformatted.as_bytes()
            ),
            "Class hash deve restare identico a whitespace/indentazione diversi"
        );
        for method in ["Process", "Validate"] {
            assert_eq!(
                merkle_hash(
                    find_callable(root_a, &original, method),
                    original.as_bytes()
                ),
                merkle_hash(
                    find_callable(root_b, reformatted, method),
                    reformatted.as_bytes()
                ),
                "Method({method}) hash deve restare identico a whitespace/indentazione diversi"
            );
        }
    }

    // --- SPEC-020 §3 scenario 2: aggiungere un commento dentro il corpo
    // di Validate non cambia merkle_hash(Validate). ---
    #[test]
    fn scenario2_adding_comment_inside_body_does_not_change_hash() {
        let original = read_fixture("order_service.go");
        let with_comment = original.replace(
            "if len(prices) < s.MinItems {",
            "// nuovo commento esplicativo\n\tif len(prices) < s.MinItems {",
        );
        assert_ne!(original, with_comment, "il replace deve aver avuto effetto");

        let tree_a = parse(&original);
        let tree_b = parse(&with_comment);

        let hash_a = merkle_hash(
            find_callable(tree_a.root_node(), &original, "Validate"),
            original.as_bytes(),
        );
        let hash_b = merkle_hash(
            find_callable(tree_b.root_node(), &with_comment, "Validate"),
            with_comment.as_bytes(),
        );
        assert_eq!(hash_a, hash_b, "un commento aggiunto nel corpo non deve cambiare l'hash");
    }

    // --- SPEC-020 §3 scenario 3: rinominare una variabile locale
    // dichiarata con `:=` non cambia l'hash. order_service.go/Validate non
    // ha variabili locali proprie: caso dedicato inline (§7, eccezione
    // esplicitamente ammessa). ---
    #[test]
    fn scenario3_renaming_local_var_declared_with_walrus_does_not_change_hash() {
        let original = r#"package main

func Validate(prices []float64) error {
	total := 0.0
	for _, p := range prices {
		total += p
	}
	if total < 0 {
		return nil
	}
	return nil
}
"#;
        let renamed = r#"package main

func Validate(prices []float64) error {
	sum := 0.0
	for _, p := range prices {
		sum += p
	}
	if sum < 0 {
		return nil
	}
	return nil
}
"#;
        let tree_a = parse(original);
        let tree_b = parse(renamed);
        let hash_a = merkle_hash(
            find_callable(tree_a.root_node(), original, "Validate"),
            original.as_bytes(),
        );
        let hash_b = merkle_hash(
            find_callable(tree_b.root_node(), renamed, "Validate"),
            renamed.as_bytes(),
        );
        assert_eq!(
            hash_a, hash_b,
            "rinominare una variabile locale dichiarata con := non deve cambiare l'hash"
        );
    }

    // --- SPEC-020 §3 scenario 4: rinominare il METODO Validate stesso
    // (nome dichiarato) CAMBIA l'hash — identity-preserving per il nome
    // dichiarato, non alpha-renamed. ---
    #[test]
    fn scenario4_renaming_declared_method_name_changes_hash() {
        let original = read_fixture("order_service.go");
        let renamed = original.replace(
            "func (s *OrderService) Validate(prices []float64) error {",
            "func (s *OrderService) CheckValid(prices []float64) error {",
        );
        assert_ne!(original, renamed, "il replace deve aver avuto effetto");

        let tree_a = parse(&original);
        let tree_b = parse(&renamed);
        let hash_a = merkle_hash(
            find_callable(tree_a.root_node(), &original, "Validate"),
            original.as_bytes(),
        );
        let hash_b = merkle_hash(
            find_callable(tree_b.root_node(), &renamed, "CheckValid"),
            renamed.as_bytes(),
        );
        assert_ne!(
            hash_a, hash_b,
            "rinominare il nome dichiarato del metodo deve cambiare l'hash (identity-preserving)"
        );
    }

    // --- SPEC-020 §3 scenario 5: cambiare un operatore di confronto reale
    // dentro Validate cambia merkle_hash(Validate) e merkle_hash(File),
    // mentre merkle_hash(Process) (non toccato) resta identico.
    //
    // DEVIAZIONE ESPLICITA dal testo letterale dello scenario 5 (vedi
    // fondo SPEC-020, sezione deviazioni): la SPEC afferma che anche
    // merkle_hash(OrderService) (la Class) debba cambiare "per la
    // proprietà di propagazione Merkle". Strutturalmente questo non è
    // possibile con la formula di §2: il nodo Tree-sitter "in mano" per
    // una Class è il suo `type_spec` (solo i campi dello struct), che in
    // Go NON contiene i Method dichiarati altrove a livello top-level
    // (confermato anche da ADD §1.6.1 riga 100, che parla di propagazione
    // "fino alla radice della funzione/file", mai della Class). Qui si
    // verifica quindi che il Class hash resti INVARIATO, coerentemente
    // con la scelta approvata dall'utente in questa sessione. ---
    #[test]
    fn scenario5_changed_comparison_operator_propagates_to_validate_and_file_not_class_or_process() {
        let original = read_fixture("order_service.go");
        let changed = original.replace(
            "if len(prices) < s.MinItems {",
            "if len(prices) <= s.MinItems {",
        );
        assert_ne!(original, changed, "il replace deve aver avuto effetto");

        let tree_a = parse(&original);
        let tree_b = parse(&changed);
        let root_a = tree_a.root_node();
        let root_b = tree_b.root_node();

        assert_ne!(
            merkle_hash(find_callable(root_a, &original, "Validate"), original.as_bytes()),
            merkle_hash(find_callable(root_b, &changed, "Validate"), changed.as_bytes()),
            "Method(Validate) hash deve cambiare"
        );
        assert_ne!(
            merkle_hash(root_a, original.as_bytes()),
            merkle_hash(root_b, changed.as_bytes()),
            "File hash deve cambiare (contiene Validate)"
        );
        assert_eq!(
            merkle_hash(find_callable(root_a, &original, "Process"), original.as_bytes()),
            merkle_hash(find_callable(root_b, &changed, "Process"), changed.as_bytes()),
            "Method(Process) hash NON deve cambiare (non toccato)"
        );
        assert_eq!(
            merkle_hash(
                find_type_spec(root_a, &original, "OrderService"),
                original.as_bytes()
            ),
            merkle_hash(
                find_type_spec(root_b, &changed, "OrderService"),
                changed.as_bytes()
            ),
            "Class(OrderService) hash NON deve cambiare: il type_spec non contiene i Method (deviazione da §3 scenario 5, vedi fondo SPEC-020)"
        );
    }

    // --- SPEC-020 §3 scenario 6: scambiare i nomi di due variabili locali
    // tra loro cambia l'hash SOLO se cambia anche l'ordine di prima
    // dichiarazione (normalizzazione posizionale, non "ignora i nomi"). ---
    #[test]
    fn scenario6_swapping_two_local_var_names_same_declaration_order_keeps_hash() {
        let original = r#"package main

func Compute() int {
	a := 1
	b := 2
	return a + b
}
"#;
        // Nomi scambiati tra loro, ma l'ordine di PRIMA DICHIARAZIONE
        // resta lo stesso (prima variabile dichiarata -> LOCAL_0, seconda
        // -> LOCAL_1, indipendentemente da quale nome letterale porta).
        let swapped = r#"package main

func Compute() int {
	b := 1
	a := 2
	return b + a
}
"#;
        let tree_a = parse(original);
        let tree_b = parse(swapped);
        let hash_a = merkle_hash(
            find_callable(tree_a.root_node(), original, "Compute"),
            original.as_bytes(),
        );
        let hash_b = merkle_hash(
            find_callable(tree_b.root_node(), swapped, "Compute"),
            swapped.as_bytes(),
        );
        assert_eq!(
            hash_a, hash_b,
            "scambiare i nomi mantenendo l'ordine di prima dichiarazione deve dare hash identico"
        );

        // Controprova: se invece cambio anche l'ORDINE di dichiarazione
        // (non solo i nomi), l'hash DEVE cambiare — dimostra che la
        // normalizzazione è posizionale, non un semplice "ignora i nomi".
        let reordered = r#"package main

func Compute() int {
	b := 2
	a := 1
	return b + a
}
"#;
        let tree_c = parse(reordered);
        let hash_c = merkle_hash(
            find_callable(tree_c.root_node(), reordered, "Compute"),
            reordered.as_bytes(),
        );
        assert_ne!(
            hash_a, hash_c,
            "cambiare l'ordine di prima dichiarazione (non solo i nomi) deve cambiare l'hash"
        );
    }

    // --- SPEC-020 §4 edge case: variabile locale con lo stesso nome di
    // un identificatore identity-preserving altrove (parametro chiamato
    // come un metodo) — nessuna ambiguità, classificazione per posizione
    // nell'albero (dentro il corpo dove dichiarata con :=/var). ---
    #[test]
    fn edge_case_local_var_name_shared_with_unrelated_identity_preserving_identifier() {
        // "Validate" è sia il nome di un METODO altrove nel file sia,
        // qui, il nome di una variabile locale dichiarata con := dentro
        // Compute: nessuna interferenza, restano due entità indipendenti.
        let source = r#"package main

func Compute() int {
	Validate := 5
	return Validate
}

func Validate() int {
	return 1
}
"#;
        let renamed_local = r#"package main

func Compute() int {
	other := 5
	return other
}

func Validate() int {
	return 1
}
"#;
        let tree_a = parse(source);
        let tree_b = parse(renamed_local);

        let compute_a = merkle_hash(
            find_callable(tree_a.root_node(), source, "Compute"),
            source.as_bytes(),
        );
        let compute_b = merkle_hash(
            find_callable(tree_b.root_node(), renamed_local, "Compute"),
            renamed_local.as_bytes(),
        );
        assert_eq!(
            compute_a, compute_b,
            "la variabile locale Validate (dentro Compute) è alpha-renamed: rinominarla non cambia l'hash di Compute"
        );

        let validate_a = merkle_hash(
            find_callable(tree_a.root_node(), source, "Validate"),
            source.as_bytes(),
        );
        let validate_b = merkle_hash(
            find_callable(tree_b.root_node(), renamed_local, "Validate"),
            renamed_local.as_bytes(),
        );
        assert_eq!(
            validate_a, validate_b,
            "la funzione Validate top-level è indipendente dalla variabile locale omonima in Compute"
        );
    }

    // --- SPEC-020 §4 edge case: corpo senza variabili locali dichiarate
    // con :=/var — nessun placeholder LOCAL_N generato, comportamento
    // normale (non un caso speciale: verificato indirettamente mostrando
    // che l'hash di un corpo così è deterministico e stabile). ---
    #[test]
    fn edge_case_body_without_local_declarations_is_not_special_cased() {
        let source = read_fixture("order_service.go");
        let tree = parse(&source);
        let hash_1 = merkle_hash(
            find_callable(tree.root_node(), &source, "Validate"),
            source.as_bytes(),
        );
        let hash_2 = merkle_hash(
            find_callable(tree.root_node(), &source, "Validate"),
            source.as_bytes(),
        );
        assert_eq!(hash_1, hash_2, "hash deterministico anche senza variabili locali (Validate non ne dichiara nessuna)");
        assert_eq!(hash_1.len(), 64, "SHA-256 esadecimale: 64 caratteri");
    }

    // --- SPEC-020 §7: property test, esplicitamente richiesto e non
    // opzionale ("la classe di test che la SPEC nomina esplicitamente nel
    // titolo del task"). Genera varianti casuali di whitespace (spazi/tab
    // extra a fine riga, non newline: non deve rompere la sintassi Go) e
    // di nomi di variabili locali su un corpo di funzione fisso con due
    // variabili dichiarate con `:=`, e verifica l'invariante "l'hash non
    // cambia" rispetto alla forma canonica. Dipendenza nuova: `proptest`
    // 1.11.0 (vedi Cargo.toml — versione esatta annotata nelle deviazioni
    // della SPEC). 512 casi generati e verificati per run (vedi
    // `ProptestConfig::with_cases` sotto), riportato esplicitamente nel
    // report finale della SPEC, non solo dichiarato presente.
    mod proptest_suite {
        use super::*;
        use proptest::prelude::*;

        const GO_KEYWORDS: &[&str] = &[
            "func", "int", "return", "package", "var", "if", "for", "else", "range", "case",
        ];

        fn arb_identifier() -> impl Strategy<Value = String> {
            "[a-z][a-z0-9_]{0,7}"
                .prop_filter("non deve essere una keyword Go", |s| {
                    !GO_KEYWORDS.contains(&s.as_str())
                })
        }

        fn arb_trailing_whitespace() -> impl Strategy<Value = String> {
            proptest::collection::vec(prop_oneof![Just(' '), Just('\t')], 0..6)
                .prop_map(|chars| chars.into_iter().collect())
        }

        /// Corpo fisso: due variabili locali dichiarate con `:=` e usate
        /// nella `return`. `extra_ws[i]` viene appeso a fine riga in 4
        /// punti distinti (mai una newline: la struttura del programma
        /// resta fissa, cambia solo whitespace superfluo).
        fn build_source(var1: &str, var2: &str, extra_ws: &[String; 4]) -> String {
            format!(
                "package main\n\nfunc Compute() int {{{w0}\n\t{v1} := 1{w1}\n\t{v2} := 2{w2}\n\treturn {v1} + {v2}\n}}{w3}\n",
                w0 = extra_ws[0],
                w1 = extra_ws[1],
                w2 = extra_ws[2],
                w3 = extra_ws[3],
                v1 = var1,
                v2 = var2,
            )
        }

        fn compute_hash(source: &str) -> String {
            let tree = parse(source);
            merkle_hash(
                find_callable(tree.root_node(), source, "Compute"),
                source.as_bytes(),
            )
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            #[test]
            fn merkle_hash_invariant_under_whitespace_and_local_var_renaming(
                var1 in arb_identifier(),
                var2 in arb_identifier(),
                ws0 in arb_trailing_whitespace(),
                ws1 in arb_trailing_whitespace(),
                ws2 in arb_trailing_whitespace(),
                ws3 in arb_trailing_whitespace(),
            ) {
                prop_assume!(var1 != var2);

                let no_ws = [String::new(), String::new(), String::new(), String::new()];
                let canonical = build_source("a", "b", &no_ws);
                let variant = build_source(&var1, &var2, &[ws0, ws1, ws2, ws3]);

                prop_assert_eq!(
                    compute_hash(&canonical),
                    compute_hash(&variant),
                    "l'hash deve restare invariato su whitespace/nomi di variabili locali generati casualmente"
                );
            }
        }
    }

    // --- SPEC-021 §3 scenario 1: doc_hash su un'entità con un commento
    // Go-doc immediatamente precedente reale nel fixture. Notify
    // (notifier.go) ha 3 righe di commento immediatamente prima —
    // verificato leggendo il fixture, non assunto. ---
    #[test]
    fn spec021_scenario1_doc_hash_some_for_entity_with_immediately_preceding_comment() {
        let source = read_fixture("notifier.go");
        let tree = parse(&source);
        let notify = find_callable(tree.root_node(), &source, "Notify");

        let hash = doc_hash(notify, source.as_bytes());
        assert!(
            hash.is_some(),
            "Notify ha un commento Go-doc immediatamente precedente nel fixture, doc_hash deve essere Some"
        );
        assert_eq!(hash.unwrap().len(), 64, "SHA-256 esadecimale: 64 caratteri");
    }

    // --- SPEC-021 §3 scenario 2: doc_hash None per un'entità senza
    // commento immediatamente precedente. DEVIAZIONE dal testo della
    // SPEC: l'esempio suggerito ("Process in order_service.go, se non ne
    // ha uno") in realtà HA un commento Go-doc immediatamente precedente
    // nel fixture reale (verificato leggendo order_service.go) — ogni
    // entità top-level del fixture di SPEC-009 è documentata. Caso
    // dedicato inline, esplicitamente dichiarato come tale (stesso
    // principio ammesso da SPEC-020 §7 e da questa SPEC §7). ---
    #[test]
    fn spec021_scenario2_doc_hash_none_for_entity_without_preceding_comment() {
        let source = "package main\n\nfunc NoDoc() int {\n\treturn 1\n}\n";
        let tree = parse(source);
        let no_doc = find_callable(tree.root_node(), source, "NoDoc");

        assert_eq!(
            doc_hash(no_doc, source.as_bytes()),
            None,
            "nessun commento immediatamente precedente: doc_hash deve essere None"
        );
    }

    // Variante dedicata: un commento SEPARATO da una riga vuota non conta
    // come doc comment (SPEC-021 §2: "senza riga vuota tra l'ultimo
    // commento e la dichiarazione").
    #[test]
    fn edge_case_doc_hash_none_when_blank_line_separates_comment_from_declaration() {
        let source = "package main\n\n// commento non immediatamente precedente\n\nfunc NoDoc() int {\n\treturn 1\n}\n";
        let tree = parse(source);
        let no_doc = find_callable(tree.root_node(), source, "NoDoc");

        assert_eq!(
            doc_hash(no_doc, source.as_bytes()),
            None,
            "una riga vuota tra il commento e la dichiarazione invalida il doc comment"
        );
    }

    // --- SPEC-021 §3 scenario 3: modificare SOLO il testo del commento
    // di documentazione cambia doc_hash ma NON merkle_hash (T2.1) —
    // verifica diretta dell'indipendenza dei due fingerprint. Usa
    // Validate (order_service.go), che nel fixture reale ha un commento
    // Go-doc di due righe immediatamente precedente. ---
    #[test]
    fn spec021_scenario3_changing_only_doc_comment_changes_doc_hash_not_merkle_hash() {
        let original = read_fixture("order_service.go");
        let doc_changed = original.replace(
            "// Validate è un nodo Method (receiver *OrderService), nessuna chiamata in\n// uscita.",
            "// Validate controlla che il carrello abbia abbastanza articoli.",
        );
        assert_ne!(original, doc_changed, "il replace deve aver avuto effetto");

        let tree_a = parse(&original);
        let tree_b = parse(&doc_changed);
        let validate_a = find_callable(tree_a.root_node(), &original, "Validate");
        let validate_b = find_callable(tree_b.root_node(), &doc_changed, "Validate");

        let doc_hash_a = doc_hash(validate_a, original.as_bytes());
        let doc_hash_b = doc_hash(validate_b, doc_changed.as_bytes());
        assert!(doc_hash_a.is_some() && doc_hash_b.is_some());
        assert_ne!(doc_hash_a, doc_hash_b, "doc_hash deve cambiare: il testo del commento è cambiato");

        let merkle_a = merkle_hash(validate_a, original.as_bytes());
        let merkle_b = merkle_hash(validate_b, doc_changed.as_bytes());
        assert_eq!(
            merkle_a, merkle_b,
            "merkle_hash NON deve cambiare: il codice (esclusi i commenti) è identico"
        );
    }

    // --- SPEC-021 §3 scenario 1 variante: due commenti consecutivi su
    // righe distinte contribuiscono ENTRAMBI (concatenazione nell'ordine
    // di apparizione) — non solo l'ultimo prima della dichiarazione.
    // Dimostrato confrontando con una variante che ha SOLO la seconda
    // riga di commento: hash diverso, prova che la prima riga contribuisce
    // davvero al fingerprint.
    #[test]
    fn edge_case_doc_hash_includes_all_consecutive_comment_lines_not_only_the_last() {
        let two_lines = "package main\n\n// prima riga\n// seconda riga\nfunc F() int {\n\treturn 1\n}\n";
        let one_line = "package main\n\n// seconda riga\nfunc F() int {\n\treturn 1\n}\n";

        let tree_a = parse(two_lines);
        let tree_b = parse(one_line);
        let f_a = find_callable(tree_a.root_node(), two_lines, "F");
        let f_b = find_callable(tree_b.root_node(), one_line, "F");

        let hash_a = doc_hash(f_a, two_lines.as_bytes());
        let hash_b = doc_hash(f_b, one_line.as_bytes());
        assert!(hash_a.is_some() && hash_b.is_some());
        assert_ne!(
            hash_a, hash_b,
            "la prima riga di commento deve contribuire al fingerprint, non solo l'ultima"
        );
    }
}
