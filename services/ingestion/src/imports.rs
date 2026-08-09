//! Estrazione dei binding di `import` JavaScript (SPEC-025 §2, T2.5 parte
//! 2/3): named import (`import { nome } from '...'`), con o senza alias
//! (`import { nome as alias } from '...'`). Default import (`import X from
//! '...'`) e namespace import (`import * as X from '...'`) sono Non-goal
//! dichiarati (§2/§5): semplicemente non producono nessun `ImportBinding`,
//! non un errore.

use tree_sitter::{Node, Tree};

/// `local_name` è il nome usato nel corpo del file che importa (==
/// `imported_name` se nessun alias). `module_specifier` è la stringa
/// letterale dopo `from`, senza virgolette (es. `"./util.js"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportBinding {
    pub local_name: String,
    pub imported_name: String,
    pub module_specifier: String,
}

/// Estrae tutti i binding di named import a livello top-level del file.
/// Kind Tree-sitter verificati empiricamente prima di scrivere questa
/// funzione (SPEC-025 §10, stesso principio di SPEC-024): `import_statement`
/// (campo `source`: nodo `string` contenente un `string_fragment`, il
/// testo senza virgolette) → figlio `import_clause` → figlio
/// `named_imports` (assente per default/namespace import: quei due casi
/// non producono NESSUN `ImportBinding`, comportamento naturale, non un
/// filtro esplicito) → uno o più `import_specifier`, campo `name`
/// (obbligatorio) + campo `alias` (opzionale, presente solo con `as`).
pub fn extract_imports(tree: &Tree, source: &[u8]) -> Vec<ImportBinding> {
    let mut bindings = Vec::new();
    let root = tree.root_node();

    let mut cursor = root.walk();
    for statement in root
        .children(&mut cursor)
        .filter(|c| c.kind() == "import_statement")
    {
        let Some(source_node) = statement.child_by_field_name("source") else {
            continue;
        };
        // `string` -> `string_fragment` (contenuto senza virgolette). Una
        // stringa letterale vuota (`''`) non ha `string_fragment`: nessun
        // panic, semplicemente uno specifier vuoto.
        let module_specifier = source_node
            .named_child(0)
            .map(|n| node_text(n, source))
            .unwrap_or_default();

        let mut stmt_cursor = statement.walk();
        let Some(import_clause) = statement
            .children(&mut stmt_cursor)
            .find(|c| c.kind() == "import_clause")
        else {
            continue;
        };

        // `named_imports` assente = default import (identifier nudo) o
        // namespace import (`namespace_import`) — Non-goal §2/§5, nessun
        // ImportBinding prodotto, non un errore.
        let mut clause_cursor = import_clause.walk();
        let Some(named_imports) = import_clause
            .children(&mut clause_cursor)
            .find(|c| c.kind() == "named_imports")
        else {
            continue;
        };

        let mut specs_cursor = named_imports.walk();
        for spec in named_imports
            .children(&mut specs_cursor)
            .filter(|c| c.kind() == "import_specifier")
        {
            let Some(name_node) = spec.child_by_field_name("name") else {
                continue;
            };
            let imported_name = node_text(name_node, source);
            let local_name = spec
                .child_by_field_name("alias")
                .map(|a| node_text(a, source))
                .unwrap_or_else(|| imported_name.clone());

            bindings.push(ImportBinding {
                local_name,
                imported_name,
                module_specifier: module_specifier.clone(),
            });
        }
    }

    bindings
}

fn node_text(node: Node, source: &[u8]) -> String {
    node.utf8_text(source).unwrap_or("").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> Tree {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_javascript::LANGUAGE.into())
            .expect("grammatica tree-sitter-javascript valida");
        parser.parse(source, None).expect("tree-sitter parse")
    }

    #[test]
    fn named_import_without_alias() {
        let source = "import { computeTotal } from './util.js';\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());

        assert_eq!(bindings.len(), 1, "atteso 1 ImportBinding: {bindings:?}");
        assert_eq!(bindings[0].local_name, "computeTotal");
        assert_eq!(bindings[0].imported_name, "computeTotal");
        assert_eq!(bindings[0].module_specifier, "./util.js");
    }

    #[test]
    fn named_import_with_alias() {
        let source = "import { computeTotal as total } from './util.js';\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());

        assert_eq!(bindings.len(), 1, "atteso 1 ImportBinding: {bindings:?}");
        assert_eq!(bindings[0].local_name, "total", "local_name deve essere l'alias");
        assert_eq!(
            bindings[0].imported_name, "computeTotal",
            "imported_name deve restare il nome reale nel modulo sorgente"
        );
        assert_eq!(bindings[0].module_specifier, "./util.js");
    }

    #[test]
    fn multiple_specifiers_mixed_alias() {
        let source = "import { a, b as c } from './multi.js';\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());

        assert_eq!(bindings.len(), 2, "attesi 2 ImportBinding: {bindings:?}");
        let a = bindings.iter().find(|b| b.imported_name == "a").expect("binding 'a'");
        assert_eq!(a.local_name, "a");
        let b = bindings.iter().find(|b| b.imported_name == "b").expect("binding 'b'");
        assert_eq!(b.local_name, "c");
    }

    // --- Non-goal §2/§5: default import, nessun ImportBinding prodotto. ---
    #[test]
    fn default_import_produces_no_binding() {
        let source = "import Foo from './qualcosa.js';\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());
        assert!(bindings.is_empty(), "default import non deve produrre ImportBinding: {bindings:?}");
    }

    // --- Non-goal §2/§5: namespace import, nessun ImportBinding prodotto. ---
    #[test]
    fn namespace_import_produces_no_binding() {
        let source = "import * as NS from './qualcosa.js';\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());
        assert!(bindings.is_empty(), "namespace import non deve produrre ImportBinding: {bindings:?}");
    }

    #[test]
    fn no_import_statement_produces_empty_vec() {
        let source = "function f() {}\n";
        let tree = parse(source);
        let bindings = extract_imports(&tree, source.as_bytes());
        assert!(bindings.is_empty());
    }
}
