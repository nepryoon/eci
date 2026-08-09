//! Risoluzione cross-file JavaScript via import (SPEC-025 §2, T2.5 parte
//! 2/3) — resolver su misura, non stack-graphs (ADR-0006). Passaggio a
//! livello di INSIEME di file, non di singolo file (§1): la prima
//! operazione del progetto che richiede vedere più file estratti insieme.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::imports::ImportBinding;
use crate::CodeRelation;

/// Chiamata rimasta irrisolta a livello intra-file (SPEC-025 §2): stesso
/// meccanismo di attraversamento già usato da `collect_calls_js`
/// (SPEC-013/SPEC-024) per le chiamate a un nome non dichiarato
/// localmente — qui riusato per esporre anche il nome tentato, non solo
/// scartarlo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedCall {
    pub caller_id: String,
    pub callee_name: String,
    pub weight: u32,
}

pub type UnresolvedCalls = Vec<UnresolvedCall>;

/// Risolve le chiamate cross-file per l'insieme di file dato (SPEC-025
/// §2): per ciascuna `UnresolvedCall`, se `callee_name` compare tra gli
/// `ImportBinding` del file chiamante, prova a risolvere il
/// `module_specifier` a uno dei file dell'insieme e cerca al suo interno
/// un'entità di primo livello (`Function`/`Class`) con
/// `name == imported_name`. Nessuna verifica di `export` (§2 — semplificazione
/// dichiarata). Un nome non importato, o importato ma non risolvibile,
/// resta semplicemente non risolto — mai un panic, mai una relazione
/// fabbricata verso un file inesistente (§4).
pub fn resolve_cross_file_calls(
    files: &[(PathBuf, Vec<crate::CodeNode>, Vec<ImportBinding>, UnresolvedCalls)],
) -> Vec<CodeRelation> {
    let mut relations = Vec::new();

    for (importer_path, _nodes, imports, unresolved) in files {
        // Mappa local_name -> ImportBinding: un local_name duplicato
        // (import ripetuto/rinominato due volte) vede l'ultimo vincere
        // per costruzione di un inserimento in mappa (§4 edge case), non
        // un caso speciale gestito esplicitamente.
        let mut import_map: HashMap<&str, &ImportBinding> = HashMap::new();
        for binding in imports {
            import_map.insert(binding.local_name.as_str(), binding);
        }

        for call in unresolved {
            let Some(binding) = import_map.get(call.callee_name.as_str()) else {
                continue;
            };
            let Some(resolved_path) = resolve_module_path(importer_path, &binding.module_specifier, files)
            else {
                continue;
            };
            let Some((_, target_nodes, ..)) = files.iter().find(|(p, ..)| *p == resolved_path) else {
                continue;
            };
            let Some(target_node) = target_nodes
                .iter()
                .find(|n| (n.node_type == "Function" || n.node_type == "Class") && n.name == binding.imported_name)
            else {
                continue;
            };

            relations.push(CodeRelation {
                domain: "code".to_string(),
                rel_type: "CALLS".to_string(),
                from_id: call.caller_id.clone(),
                to_id: target_node.id.clone(),
                weight: Some(call.weight),
            });
        }
    }

    relations
}

/// Risolve `module_specifier` (relativo, `./`/`../`) rispetto alla
/// directory di `importer_path` a uno dei percorsi presenti in `files`
/// (SPEC-025 §2). Prova prima il percorso letterale, poi con `.js`
/// appeso se non termina già con un'estensione riconosciuta. Un
/// percorso che non combacia con NESSUN file dell'insieme fornito torna
/// `None` — stesso trattamento di un modulo esterno (§4 edge case): il
/// resolver non legge mai file non forniti esplicitamente.
fn resolve_module_path(
    importer_path: &Path,
    module_specifier: &str,
    files: &[(PathBuf, Vec<crate::CodeNode>, Vec<ImportBinding>, UnresolvedCalls)],
) -> Option<PathBuf> {
    let importer_dir = importer_path.parent().unwrap_or_else(|| Path::new(""));
    let joined = importer_dir.join(module_specifier);
    let candidate = normalize_relative_path(&joined);

    if files.iter().any(|(p, ..)| *p == candidate) {
        return Some(candidate);
    }
    if candidate.extension().is_none() {
        // SPEC-025 §2 diceva solo ".js" (unico linguaggio con questo
        // resolver all'epoca). SPEC-026 §10: verificato EMPIRICAMENTE che
        // il fixture TypeScript dichiarato in SPEC-026 §2 usa uno
        // specifier senza estensione (`from './util'`, non `'./util.ts'`
        // né `'./util.js'`) — con la sola prova ".js", uno scenario del
        // genere contro un file .ts resterebbe non risolto per un
        // dettaglio di implementazione del resolver, non per il limite
        // dichiarato dello scope. Estesa la lista dei tentativi a ".ts"
        // oltre a ".js": unica modifica a questo file per SPEC-026,
        // deviazione dal "nessun codice nuovo" atteso da §2, motivata da
        // questo fallimento empirico e non da un'estensione preventiva.
        for ext in ["js", "ts"] {
            let with_ext = candidate.with_extension(ext);
            if files.iter().any(|(p, ..)| *p == with_ext) {
                return Some(with_ext);
            }
        }
    }
    None
}

/// Collassa i componenti `.`/`..` di un path relativo senza toccare il
/// filesystem (`Path::canonicalize` richiederebbe che il file esista
/// davvero — qui i "file" sono voci logiche di `files`, non
/// necessariamente presenti su disco nei test).
fn normalize_relative_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::extract_imports;
    use crate::{parse_js_file_full, CodeNode};

    fn parse_tree(source: &str) -> tree_sitter::Tree {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_javascript::LANGUAGE.into())
            .expect("grammatica tree-sitter-javascript valida");
        parser.parse(source, None).expect("tree-sitter parse")
    }

    /// Costruisce l'ingresso di `files` per un file dato: stesso
    /// procedimento che un futuro chiamante reale (T2.6+) seguirebbe —
    /// `extract_imports` sul proprio albero, `parse_js_file_full` per
    /// nodi + chiamate irrisolte.
    fn build_file_entry(
        path: &str,
        source: &str,
    ) -> (PathBuf, Vec<CodeNode>, Vec<ImportBinding>, UnresolvedCalls) {
        let tree = parse_tree(source);
        let imports = extract_imports(&tree, source.as_bytes());
        let (nodes, _relations, unresolved) = parse_js_file_full(path, source);
        (PathBuf::from(path), nodes, imports, unresolved)
    }

    fn read_js_fixture(name: &str) -> String {
        let path = format!(
            "{}/../../tests/fixtures/sample-repo-js/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("lettura fixture {path}: {e}"))
    }

    fn find_id<'a>(nodes: &'a [CodeNode], node_type: &str, name: &str) -> &'a str {
        nodes
            .iter()
            .find(|n| n.node_type == node_type && n.name == name)
            .unwrap_or_else(|| panic!("nodo {node_type} {name:?} non trovato in {nodes:?}"))
            .id
            .as_str()
    }

    // --- SPEC-025 §3 scenario 1. ---
    #[test]
    fn scenario1_process_calls_compute_total_cross_file_via_import() {
        let order_service_src = read_js_fixture("order_service.js");
        let util_src = read_js_fixture("util.js");

        let order_service = build_file_entry("order_service.js", &order_service_src);
        let util = build_file_entry("util.js", &util_src);
        let order_service_nodes = order_service.1.clone();
        let util_nodes = util.1.clone();

        let relations = resolve_cross_file_calls(&[order_service, util]);

        let process_id = find_id(&order_service_nodes, "Method", "process").to_string();
        let compute_total_id = find_id(&util_nodes, "Function", "computeTotal").to_string();

        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(
            calls.len(),
            1,
            "atteso esattamente 1 arco CALLS cross-file (process->computeTotal), ottenuti: {calls:?}"
        );
        assert_eq!(calls[0].from_id, process_id);
        assert_eq!(
            calls[0].to_id, compute_total_id,
            "l'id target deve combaciare ESATTAMENTE con quello prodotto dall'estrazione di util.js"
        );
    }

    // --- SPEC-025 §3 scenario 2: alias. ---
    #[test]
    fn scenario2_aliased_import_resolves_to_real_target_name() {
        let order_service_src = r#"import { computeTotal as total } from './util.js';

class OrderService {
  process(prices) {
    return total(prices);
  }
}
"#;
        let util_src = "export function computeTotal(prices) { return 0; }\n";

        let order_service = build_file_entry("order_service.js", order_service_src);
        let util = build_file_entry("util.js", util_src);
        let order_service_nodes = order_service.1.clone();
        let util_nodes = util.1.clone();

        let relations = resolve_cross_file_calls(&[order_service, util]);

        let process_id = find_id(&order_service_nodes, "Method", "process").to_string();
        let compute_total_id = find_id(&util_nodes, "Function", "computeTotal").to_string();

        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(calls.len(), 1, "ottenuti: {calls:?}");
        assert_eq!(calls[0].from_id, process_id);
        assert_eq!(
            calls[0].to_id, compute_total_id,
            "l'arco deve puntare al nodo REALE computeTotal, non a un nodo inesistente 'total'"
        );
        assert!(
            !relations.iter().any(|r| r.to_id == "total"),
            "nessun nodo/arco deve referenziare l'alias come se fosse un id reale"
        );
    }

    // --- SPEC-025 §3 scenario 3: specifier che non risolve a nessun file
    // del set (libreria esterna). ---
    #[test]
    fn scenario3_import_from_external_library_stays_unresolved() {
        let source = r#"import { qualcosa } from 'libreria-esterna';

function caller() {
  qualcosa();
}
"#;
        let entry = build_file_entry("caller.js", source);
        let relations = resolve_cross_file_calls(&[entry]);
        assert!(
            relations.is_empty(),
            "un import da uno specifier non risolvibile non deve produrre nessuna relazione: {relations:?}"
        );
    }

    // --- SPEC-025 §3 scenario 4: nome né locale né importato (globale
    // browser/Node). ---
    #[test]
    fn scenario4_unimported_global_name_stays_unresolved() {
        let source = r#"function caller() {
  setTimeout(() => {}, 100);
}
"#;
        let entry = build_file_entry("caller.js", source);
        let relations = resolve_cross_file_calls(&[entry]);
        assert!(
            relations.is_empty(),
            "setTimeout non è né locale né importato: nessuna relazione attesa: {relations:?}"
        );
    }

    // --- SPEC-025 §3 scenario 5: default import, Non-goal gestito
    // esplicitamente come "non risolto", non un crash. ---
    #[test]
    fn scenario5_default_import_call_stays_unresolved_not_error() {
        let caller_src = r#"import Foo from './qualcosa.js';

function caller() {
  Foo();
}
"#;
        let target_src = "export default function Foo() {}\n";

        let caller = build_file_entry("caller.js", caller_src);
        let target = build_file_entry("qualcosa.js", target_src);

        let relations = resolve_cross_file_calls(&[caller, target]);
        assert!(
            relations.is_empty(),
            "default import è un Non-goal dichiarato: nessuna relazione, ma nessun panic: {relations:?}"
        );
    }

    // --- SPEC-025 §4 edge case: module_specifier risolve a un percorso
    // valido ma quel file non è tra quelli passati. ---
    #[test]
    fn edge_case_resolved_path_not_in_provided_file_set_stays_unresolved() {
        let source = r#"import { computeTotal } from './util.js';

function caller() {
  computeTotal();
}
"#;
        // Solo il file chiamante è passato: util.js non è nel set.
        let entry = build_file_entry("caller.js", source);
        let relations = resolve_cross_file_calls(&[entry]);
        assert!(
            relations.is_empty(),
            "util.js non è tra i file forniti: nessuna relazione, non un tentativo di lettura da disco: {relations:?}"
        );
    }

    // --- SPEC-025 §4 edge case: due ImportBinding con lo stesso
    // local_name -> l'ultimo vince. ---
    #[test]
    fn edge_case_duplicate_local_name_last_import_wins() {
        let caller_src = r#"import { a as x } from './first.js';
import { b as x } from './second.js';

function caller() {
  x();
}
"#;
        let first_src = "export function a() {}\n";
        let second_src = "export function b() {}\n";

        let caller = build_file_entry("caller.js", caller_src);
        let first = build_file_entry("first.js", first_src);
        let second = build_file_entry("second.js", second_src);
        let second_nodes = second.1.clone();

        let relations = resolve_cross_file_calls(&[caller, first, second]);

        let b_id = find_id(&second_nodes, "Function", "b").to_string();
        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(calls.len(), 1, "ottenuti: {calls:?}");
        assert_eq!(
            calls[0].to_id, b_id,
            "il secondo import (last-wins) deve vincere: la chiamata deve risolvere a 'b', non ad 'a'"
        );
    }

    // --- SPEC-025 §4 edge case: ciclo di import — non richiede
    // attraversare il grafo, nessun rischio di loop infinito. ---
    #[test]
    fn edge_case_import_cycle_does_not_infinite_loop() {
        let a_src = r#"import { b } from './b.js';

function callB() {
  b();
}

export function a() {}
"#;
        let b_src = r#"import { a } from './a.js';

function callA() {
  a();
}

export function b() {}
"#;
        let a = build_file_entry("a.js", a_src);
        let b = build_file_entry("b.js", b_src);
        let a_nodes = a.1.clone();
        let b_nodes = b.1.clone();

        // Nessun timeout/panic: la sola asserzione di interesse è che la
        // chiamata ritorni (il test stesso non si blocca).
        let relations = resolve_cross_file_calls(&[a, b]);

        let a_id = find_id(&a_nodes, "Function", "a").to_string();
        let b_id = find_id(&b_nodes, "Function", "b").to_string();
        let calls: Vec<_> = relations.iter().filter(|r| r.rel_type == "CALLS").collect();
        assert_eq!(calls.len(), 2, "ottenuti: {calls:?}");
        assert!(calls.iter().any(|r| r.to_id == a_id));
        assert!(calls.iter().any(|r| r.to_id == b_id));
    }
}
