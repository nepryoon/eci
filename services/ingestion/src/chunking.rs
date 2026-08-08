//! Chunking strutturale cAST (SPEC-021, T2.2), ADD Modulo 1 §1.1: split
//! ricorsivo di sottoalberi troppo grandi, fusione greedy di fratelli
//! piccoli, budget in caratteri non-whitespace configurabile per
//! linguaggio. Testo dei chunk grezzo (non normalizzato come per
//! `ast_hash`, SPEC-020/T2.1) — payload per un futuro step di embedding.

use tree_sitter::Node;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeChunk {
    /// Id del CodeNode padre (stesso schema di T1.1). `chunk_entity` non
    /// riceve un id come parametro (fedele alla firma esatta di SPEC-021
    /// §2): resta stringa vuota qui, da assegnare da chi assembla i
    /// chunk finali insieme al CodeNode (T2.4+, fuori scope §5).
    pub entity_id: String,
    pub chunk_index: u32,
    pub text: String,
    pub char_count: usize,
}

/// Split ricorsivo + fusione greedy dei fratelli, SPEC-021 §2. Se il
/// conteggio caratteri non-whitespace dell'intero `node` è entro
/// `budget_chars`, l'intera entità è UN chunk. Altrimenti ricorre sui
/// figli diretti che superano ancora il budget, e fonde i fratelli
/// consecutivi che restano piccoli in un unico chunk.
pub fn chunk_entity(node: Node, source: &[u8], budget_chars: usize) -> Vec<CodeChunk> {
    let mut chunks = Vec::new();
    chunk_node_rec(node, source, budget_chars, &mut chunks);
    for (index, chunk) in chunks.iter_mut().enumerate() {
        chunk.chunk_index = index as u32;
    }
    chunks
}

fn chunk_node_rec(node: Node, source: &[u8], budget_chars: usize, chunks: &mut Vec<CodeChunk>) {
    let full_text = node_text(node, source);
    let full_count = non_ws_char_count(&full_text);

    // Intera entità come UN chunk se entro budget, oppure nodo foglia
    // atomico che non può essere ulteriormente diviso anche se supera il
    // budget (token singolo, SPEC-021 §4).
    if full_count <= budget_chars || node.child_count() == 0 {
        chunks.push(CodeChunk {
            entity_id: String::new(),
            chunk_index: 0,
            text: full_text,
            char_count: full_count,
        });
        return;
    }

    // Split ricorsivo sui figli diretti con fusione greedy dei fratelli
    // piccoli (SPEC-021 §2): accumula figli consecutivi in un chunk
    // corrente finché il prossimo non farebbe superare il budget.
    let mut run: Option<(Node, Node, usize)> = None; // (primo, ultimo, char_count accumulato)

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        let child_text = node_text(child, source);
        let child_count = non_ws_char_count(&child_text);

        if child_count > budget_chars {
            flush_run(run.take(), source, chunks);
            chunk_node_rec(child, source, budget_chars, chunks);
            continue;
        }

        run = match run {
            Some((first, _last, acc)) if acc + child_count <= budget_chars => {
                Some((first, child, acc + child_count))
            }
            Some(_) => {
                flush_run(run.take(), source, chunks);
                Some((child, child, child_count))
            }
            None => Some((child, child, child_count)),
        };
    }

    flush_run(run.take(), source, chunks);
}

fn flush_run(run: Option<(Node, Node, usize)>, source: &[u8], chunks: &mut Vec<CodeChunk>) {
    if let Some((first, last, char_count)) = run {
        let text = std::str::from_utf8(&source[first.start_byte()..last.end_byte()])
            .unwrap_or("")
            .to_string();
        chunks.push(CodeChunk {
            entity_id: String::new(),
            chunk_index: 0,
            text,
            char_count,
        });
    }
}

fn node_text(node: Node, source: &[u8]) -> String {
    node.utf8_text(source).unwrap_or("").to_string()
}

fn non_ws_char_count(text: &str) -> usize {
    text.chars().filter(|c| !c.is_whitespace()).count()
}

/// Legge `CHUNK_BUDGET_CHARS_GO` (default `1500`, SPEC-021 §2) e lo
/// interpreta come budget in caratteri non-whitespace. Fail-fast (panic
/// esplicito) su valore non parsabile o su `0` — mai un fallback silenzioso
/// diverso dal default dichiarato (SPEC-021 §4).
pub fn chunk_budget_chars_go() -> usize {
    parse_budget_chars("CHUNK_BUDGET_CHARS_GO", "1500")
}

fn parse_budget_chars(key: &str, default: &str) -> usize {
    let raw = eci_common::config::env_or_default(key, default);
    let value: usize = raw
        .parse()
        .unwrap_or_else(|e| panic!("{key} non è un intero valido ({raw:?}): {e}"));
    if value == 0 {
        panic!("{key} deve essere > 0, ottenuto 0");
    }
    value
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

    /// Cerca in profondità (pre-order) il primo nodo del `kind` dato.
    fn find_first_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
        if node.kind() == kind {
            return Some(node);
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if let Some(found) = find_first_of_kind(child, kind) {
                return Some(found);
            }
        }
        None
    }

    // --- SPEC-021 §3 scenario 5: budget generoso -> UN solo CodeChunk
    // che copre l'intera entità. ---
    #[test]
    fn spec021_scenario5_generous_budget_yields_single_chunk_covering_whole_entity() {
        let source = read_fixture("order_service.go");
        let tree = parse(&source);
        let validate = find_callable(tree.root_node(), &source, "Validate");
        let full_text = node_text(validate, source.as_bytes());

        let chunks = chunk_entity(validate, source.as_bytes(), 10_000);

        assert_eq!(chunks.len(), 1, "budget generoso: un solo chunk atteso, ottenuti {chunks:?}");
        assert_eq!(chunks[0].text, full_text, "il chunk deve coprire l'intera entità");
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].char_count, non_ws_char_count(&full_text));
    }

    // --- SPEC-021 §3 scenario 4: budget artificialmente basso (20) su
    // Validate -> più di un CodeChunk, ciascuno con char_count <= 20
    // (Validate non ha token atomici che superano 20 caratteri
    // non-whitespace da soli, verificato qui esplicitamente). ---
    #[test]
    fn spec021_scenario4_low_budget_splits_validate_into_multiple_chunks_within_budget() {
        let source = read_fixture("order_service.go");
        let tree = parse(&source);
        let validate = find_callable(tree.root_node(), &source, "Validate");

        let budget = 20;
        let chunks = chunk_entity(validate, source.as_bytes(), budget);

        assert!(
            chunks.len() > 1,
            "budget basso: attesi più chunk, ottenuto {} chunk", chunks.len()
        );
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i as u32, "chunk_index deve essere sequenziale da 0");
            assert!(
                chunk.char_count <= budget,
                "chunk {i} supera il budget ({} > {budget}): {chunk:?}",
                chunk.char_count
            );
        }
        // Ricostruzione: char_count dichiarato deve combaciare col
        // conteggio reale del testo del chunk.
        for chunk in &chunks {
            assert_eq!(chunk.char_count, non_ws_char_count(&chunk.text));
        }
    }

    // --- SPEC-021 §3 scenario 6: tre dichiarazioni fratelli piccole
    // consecutive con budget che le contiene tutte e tre ma non una
    // quarta -> le prime tre nello STESSO chunk, la quarta in un chunk
    // separato. Caso dedicato inline (nessun corpo del fixture ha una
    // sequenza di 4 var locali comparabile). ---
    #[test]
    fn spec021_scenario6_three_small_siblings_merge_into_one_chunk_fourth_does_not() {
        let source = "package main\n\nfunc Compute() int {\n\ta := 1\n\tb := 2\n\tc := 3\n\td := 4\n\treturn a + b + c + d\n}\n";
        let tree = parse(source);
        let compute = find_callable(tree.root_node(), source, "Compute");
        let body = compute.child_by_field_name("body").expect("body");
        let statement_list = find_first_of_kind(body, "statement_list").expect("statement_list");

        // Ogni "x := N" conta 4 caratteri non-whitespace: 12 contiene le
        // prime tre dichiarazioni ma non la quarta (16).
        let budget = 12;
        let chunks = chunk_entity(statement_list, source.as_bytes(), budget);

        assert!(
            chunks.len() >= 2,
            "attesi almeno 2 chunk (le prime 3 dichiarazioni fuse + almeno la quarta separata), ottenuti: {chunks:?}"
        );
        let first_chunk = &chunks[0];
        assert!(
            first_chunk.text.contains("a := 1")
                && first_chunk.text.contains("b := 2")
                && first_chunk.text.contains("c := 3"),
            "le prime tre dichiarazioni devono essere fuse nello stesso (primo) chunk: {first_chunk:?}"
        );
        assert!(
            !first_chunk.text.contains("d := 4"),
            "la quarta dichiarazione NON deve finire nel primo chunk (supererebbe il budget): {first_chunk:?}"
        );
    }

    // --- SPEC-021 §4 edge case: un singolo nodo foglia (stringa
    // letterale) il cui conteggio supera da solo il budget diventa
    // comunque un chunk a sé, anche se nominalmente sopra budget. ---
    #[test]
    fn edge_case_single_leaf_token_bigger_than_budget_becomes_its_own_chunk() {
        let source = r#"package main

func F() string {
	return "questa è una stringa letterale decisamente più lunga del budget"
}
"#;
        let tree = parse(source);
        let root = tree.root_node();
        // Non `interpreted_string_literal` (che in tree-sitter-go ha 3
        // figli: virgolette di apertura/chiusura + contenuto) ma il nodo
        // FOGLIA vero e proprio, il suo contenuto (child_count() == 0,
        // verificato sotto) — quello che l'edge case §4 descrive come "un
        // singolo nodo foglia".
        let literal = find_first_of_kind(root, "interpreted_string_literal_content")
            .expect("contenuto della stringa letterale");
        assert_eq!(literal.child_count(), 0, "precondizione test: dev'essere un nodo foglia");
        let literal_text = node_text(literal, source.as_bytes());
        let literal_count = non_ws_char_count(&literal_text);

        let budget = 5;
        assert!(literal_count > budget, "precondizione test: il literal deve superare il budget");

        let chunks = chunk_entity(literal, source.as_bytes(), budget);
        assert_eq!(chunks.len(), 1, "un token atomico non può essere ulteriormente diviso");
        assert_eq!(chunks[0].text, literal_text);
        assert_eq!(chunks[0].char_count, literal_count);
        assert!(
            chunks[0].char_count > budget,
            "il chunk supera nominalmente il budget: comportamento atteso per un token atomico"
        );
    }

    // --- SPEC-021 §4 edge case: entità con zero figli e conteggio zero
    // (es. un corpo vuoto) -> un chunk con text vuoto e char_count 0, non
    // un caso speciale/panico. Un source Go completamente vuoto produce
    // una radice (source_file) senza figli. ---
    #[test]
    fn edge_case_zero_children_zero_count_entity_produces_single_empty_chunk() {
        let source = "";
        let tree = parse(source);
        let root = tree.root_node();
        assert_eq!(root.child_count(), 0, "precondizione test: radice senza figli su sorgente vuoto");

        let chunks = chunk_entity(root, source.as_bytes(), 1500);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "");
        assert_eq!(chunks[0].char_count, 0);
    }

    // --- SPEC-021 §4 edge case: budget di configurazione a 0 o non
    // parsabile -> errore esplicito fail-fast, non un fallback silenzioso.
    // Un solo test (non parallelizzato in test distinti) per usare chiavi
    // env uniche in sequenza dentro lo stesso thread, stesso principio già
    // adottato in libs/rust/eci-common/src/config.rs. ---
    #[test]
    fn edge_case_budget_config_fails_fast_on_invalid_or_zero_values() {
        let zero_key = "ECI_TEST_CHUNK_BUDGET_ZERO_SPEC021";
        std::env::set_var(zero_key, "0");
        let result = std::panic::catch_unwind(|| parse_budget_chars(zero_key, "1500"));
        std::env::remove_var(zero_key);
        assert!(result.is_err(), "budget 0 deve fallire fail-fast, non ritornare un default silenzioso");

        let invalid_key = "ECI_TEST_CHUNK_BUDGET_INVALID_SPEC021";
        std::env::set_var(invalid_key, "not-a-number");
        let result = std::panic::catch_unwind(|| parse_budget_chars(invalid_key, "1500"));
        std::env::remove_var(invalid_key);
        assert!(result.is_err(), "valore non parsabile deve fallire fail-fast");

        let unset_key = "ECI_TEST_CHUNK_BUDGET_UNSET_SPEC021";
        std::env::remove_var(unset_key);
        assert_eq!(
            parse_budget_chars(unset_key, "1500"),
            1500,
            "senza override, deve valere il default dichiarato (1500)"
        );
    }
}
