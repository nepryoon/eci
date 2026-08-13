//! Rilevamento rename/move (`git diff -M`) e riconciliazione di lineage
//! tra vecchio e nuovo `id` via `ast_hash` condiviso (SPEC-027, T2.6
//! parte 1/2). Vedi SPEC-027 §0 per la motivazione architetturale: il
//! nostro `id` (T1.1) include il `file_path`, quindi non è puramente
//! content-addressed come promette l'ADD §1.6.5 — questa tabella di
//! lineage recupera la proprietà "cache hit su rename" a un livello
//! logico sopra l'`id`, senza toccare la formula esistente (romperebbe
//! T1.2-T1.5, già in produzione).

use std::path::Path;

use crate::CodeNode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenamedFile {
    pub old_path: String,
    pub new_path: String,
    pub similarity_percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageLink {
    pub old_node_id: String,
    pub new_node_id: String,
    pub ast_hash: String,
}

/// Errore di `detect_renames` (SPEC-027 §4): refs git invalidi devono
/// propagare un errore esplicito, mai un elenco vuoto silenzioso.
#[derive(Debug)]
pub enum LineageError {
    Spawn(std::io::Error),
    GitDiffFailed {
        old_ref: String,
        new_ref: String,
        stderr: String,
    },
}

impl std::fmt::Display for LineageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LineageError::Spawn(e) => write!(f, "detect_renames: spawn di 'git' fallito: {e}"),
            LineageError::GitDiffFailed {
                old_ref,
                new_ref,
                stderr,
            } => write!(
                f,
                "detect_renames: 'git diff -M --name-status {old_ref} {new_ref}' fallito: {stderr}"
            ),
        }
    }
}

impl std::error::Error for LineageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LineageError::Spawn(e) => Some(e),
            LineageError::GitDiffFailed { .. } => None,
        }
    }
}

/// Rileva i file rinominati/spostati tra `old_ref` e `new_ref` (SPEC-027
/// §2) shellando a `git diff -M --name-status` — non una libreria Rust
/// per git, l'algoritmo di rename detection di git stesso è già maturo e
/// testato, reimplementarlo non aggiunge valore. Righe
/// `R{score}\t{old_path}\t{new_path}` diventano un `RenamedFile`; ogni
/// altro prefisso (`M`, `A`, `D`, ...) è ignorato — questa funzione si
/// occupa solo di rename (SPEC-027 §2/§4).
pub fn detect_renames(
    old_ref: &str,
    new_ref: &str,
    repo_dir: &Path,
) -> Result<Vec<RenamedFile>, LineageError> {
    let _span = tracing::info_span!("detect_renames", old_ref, new_ref).entered();

    let output = std::process::Command::new("git")
        .args(["diff", "-M", "--name-status", old_ref, new_ref])
        .current_dir(repo_dir)
        .output()
        .map_err(LineageError::Spawn)?;

    if !output.status.success() {
        return Err(LineageError::GitDiffFailed {
            old_ref: old_ref.to_string(),
            new_ref: new_ref.to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut renamed = Vec::new();
    for line in stdout.lines() {
        let mut fields = line.split('\t');
        let Some(status) = fields.next() else {
            continue;
        };
        let Some(score) = status.strip_prefix('R') else {
            continue;
        };
        let (Some(old_path), Some(new_path)) = (fields.next(), fields.next()) else {
            continue;
        };
        renamed.push(RenamedFile {
            old_path: old_path.to_string(),
            new_path: new_path.to_string(),
            similarity_percent: score.parse().unwrap_or(0),
        });
    }
    Ok(renamed)
}

/// Riconcilia un rename rilevato: per ogni entità NUOVA il cui `ast_hash`
/// combacia con un'entità VECCHIA non ancora rivendicata produce un
/// `LineageLink` (match greedy per `ast_hash`, mai per nome/posizione —
/// SPEC-027 §2). Ambiguità dichiarata (§2/§4): se più coppie condividono
/// lo stesso hash, l'accoppiamento è arbitrario tra i candidati non
/// ancora rivendicati; nessuna entità vecchia viene mai rivendicata due
/// volte. `renamed` non entra nel matching (che è puramente per
/// `ast_hash`) — resta nella firma per identificare a quale coppia di
/// path si riferisce questa riconciliazione (SPEC-027 §2).
pub fn reconcile_renamed_file(
    renamed: &RenamedFile,
    old_entities: &[(String, String)],
    new_entities: &[CodeNode],
) -> Vec<LineageLink> {
    let _span = tracing::info_span!(
        "reconcile_renamed_file",
        old_path = renamed.old_path,
        new_path = renamed.new_path
    )
    .entered();

    let mut claimed = vec![false; old_entities.len()];
    let mut links = Vec::new();

    for new_entity in new_entities {
        let candidate = old_entities
            .iter()
            .enumerate()
            .find(|(idx, (_, ast_hash))| !claimed[*idx] && *ast_hash == new_entity.ast_hash);
        if let Some((idx, (old_node_id, ast_hash))) = candidate {
            claimed[idx] = true;
            links.push(LineageLink {
                old_node_id: old_node_id.clone(),
                new_node_id: new_entity.id.clone(),
                ast_hash: ast_hash.clone(),
            });
        }
    }

    links
}

#[derive(Debug)]
pub struct PersistLineageError(postgres::Error);

impl std::fmt::Display for PersistLineageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "persist_lineage: {}", self.0)
    }
}

impl std::error::Error for PersistLineageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<postgres::Error> for PersistLineageError {
    fn from(err: postgres::Error) -> Self {
        PersistLineageError(err)
    }
}

/// Persiste `links` (output di `reconcile_renamed_file` per UN rename)
/// dentro un'unica transazione (SPEC-027 §2 "Persistenza" — stesso
/// principio transazionale di `persist_parsed_file`, T1.2): o tutte le
/// righe di questo rename sono scritte, o nessuna — qualunque errore
/// intermedio propaga via `?` e la transazione va in rollback
/// automaticamente al `Drop` (`tx.commit()` mai raggiunto).
pub fn persist_lineage(
    client: &mut postgres::Client,
    links: &[LineageLink],
) -> Result<usize, PersistLineageError> {
    let _span = tracing::info_span!("persist_lineage", links = links.len()).entered();

    let mut tx = client.transaction()?;
    let mut written = 0usize;
    for link in links {
        tx.execute(
            "INSERT INTO lineage (old_node_id, new_node_id, ast_hash) VALUES ($1, $2, $3)",
            &[&link.old_node_id, &link.new_node_id, &link.ast_hash],
        )?;
        written += 1;
    }
    tx.commit()?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper: repo git reale in una directory temporanea (SPEC-027
    // §7 — "non solo parsing di stringhe git diff -M scritte a mano").
    // Nessuna dipendenza da `tempfile` nel Cargo.toml di questo crate:
    // directory unica via nanosecondi da UNIX_EPOCH, ripulita a fine
    // test con `remove_dir_all` (best-effort, non un'asserzione).

    fn temp_repo_dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock di sistema valido")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("eci-lineage-test-{label}-{nanos}"));
        std::fs::create_dir_all(&dir)
            .unwrap_or_else(|e| panic!("crea temp repo dir {dir:?}: {e}"));
        dir
    }

    fn git(repo_dir: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo_dir)
            .output()
            .unwrap_or_else(|e| panic!("spawn 'git {args:?}': {e}"));
        assert!(
            output.status.success(),
            "'git {args:?}' fallito: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn init_repo(dir: &Path) {
        git(dir, &["init", "-q"]);
        git(dir, &["config", "user.email", "eci-test@example.com"]);
        git(dir, &["config", "user.name", "eci-test"]);
    }

    fn commit_all(dir: &Path, message: &str) -> String {
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-q", "-m", message]);
        git(dir, &["rev-parse", "HEAD"])
    }

    // --- SPEC-027 §3 scenario 1: rename puro, nessuna modifica di
    // contenuto -> un RenamedFile con similarity_percent: 100.
    #[test]
    fn scenario1_pure_rename_no_content_change_yields_similarity_100() {
        let repo = temp_repo_dir("scenario1");
        init_repo(&repo);
        std::fs::write(repo.join("old.go"), "package main\n\nfunc Foo() {}\n")
            .expect("scrittura old.go");
        let old_sha = commit_all(&repo, "initial");

        std::fs::rename(repo.join("old.go"), repo.join("new.go")).expect("rename su disco");
        let new_sha = commit_all(&repo, "rename old.go -> new.go");

        let renames =
            detect_renames(&old_sha, &new_sha, &repo).expect("detect_renames scenario 1");

        assert_eq!(renames.len(), 1, "un solo RenamedFile atteso: {renames:?}");
        assert_eq!(renames[0].old_path, "old.go");
        assert_eq!(renames[0].new_path, "new.go");
        assert_eq!(
            renames[0].similarity_percent, 100,
            "contenuto identico -> similarity 100"
        );

        std::fs::remove_dir_all(&repo).ok();
    }

    // --- SPEC-027 §4 edge case: file modificato SUL POSTO (stesso path,
    // `git diff -M` lo riporta come M) -> NON deve comparire come rename.
    #[test]
    fn edge_file_modified_in_place_is_not_reported_as_rename() {
        let repo = temp_repo_dir("edge-inplace");
        init_repo(&repo);
        std::fs::write(repo.join("same.go"), "package main\n\nfunc Foo() {}\n")
            .expect("scrittura same.go");
        let old_sha = commit_all(&repo, "initial");

        std::fs::write(
            repo.join("same.go"),
            "package main\n\nfunc Foo() { println(\"changed\") }\n",
        )
        .expect("riscrittura same.go");
        let new_sha = commit_all(&repo, "modifica in-place");

        let renames =
            detect_renames(&old_sha, &new_sha, &repo).expect("detect_renames edge in-place");

        assert!(
            renames.is_empty(),
            "un file modificato sul posto non deve comparire come rename: {renames:?}"
        );

        std::fs::remove_dir_all(&repo).ok();
    }

    // --- SPEC-027 §4 edge case: ref invalidi -> errore esplicito, non un
    // elenco vuoto silenzioso.
    #[test]
    fn edge_invalid_refs_return_explicit_error_not_empty_list() {
        let repo = temp_repo_dir("edge-badref");
        init_repo(&repo);
        std::fs::write(repo.join("f.go"), "package main\n").expect("scrittura f.go");
        commit_all(&repo, "initial");

        let result = detect_renames(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "HEAD",
            &repo,
        );

        assert!(
            result.is_err(),
            "old_ref inesistente deve propagare un Err, non Ok(vec![])"
        );

        std::fs::remove_dir_all(&repo).ok();
    }

    // --- reconcile_renamed_file: test unitari puri, liste di entità
    // costruite a mano (SPEC-027 §7 — nessun bisogno di git reale qui).

    fn cn(id: &str, ast_hash: &str) -> CodeNode {
        CodeNode {
            id: id.to_string(),
            domain: "code".to_string(),
            node_type: "Function".to_string(),
            name: "whatever".to_string(),
            ast_hash: ast_hash.to_string(),
            file_path: "new.go".to_string(),
        }
    }

    fn renamed_old_new() -> RenamedFile {
        RenamedFile {
            old_path: "old.go".to_string(),
            new_path: "new.go".to_string(),
            similarity_percent: 90,
        }
    }

    // --- SPEC-027 §3 scenario 2: nessun contenuto cambiato -> un
    // LineageLink per OGNI entità.
    #[test]
    fn scenario2_unchanged_content_links_every_entity() {
        let renamed = renamed_old_new();
        let old_entities = vec![
            ("old-file-id".to_string(), "hash-file".to_string()),
            ("old-a-id".to_string(), "hash-a".to_string()),
            ("old-b-id".to_string(), "hash-b".to_string()),
        ];
        let new_entities = vec![
            cn("new-file-id", "hash-file"),
            cn("new-a-id", "hash-a"),
            cn("new-b-id", "hash-b"),
        ];

        let mut links = reconcile_renamed_file(&renamed, &old_entities, &new_entities);
        links.sort_by(|a, b| a.old_node_id.cmp(&b.old_node_id));

        assert_eq!(
            links,
            vec![
                LineageLink {
                    old_node_id: "old-a-id".to_string(),
                    new_node_id: "new-a-id".to_string(),
                    ast_hash: "hash-a".to_string(),
                },
                LineageLink {
                    old_node_id: "old-b-id".to_string(),
                    new_node_id: "new-b-id".to_string(),
                    ast_hash: "hash-b".to_string(),
                },
                LineageLink {
                    old_node_id: "old-file-id".to_string(),
                    new_node_id: "new-file-id".to_string(),
                    ast_hash: "hash-file".to_string(),
                },
            ]
        );
    }

    // --- SPEC-027 §3 scenario 3: una piccola modifica di contenuto ->
    // link per le entità INVARIATE, nessuno per quella modificata (né
    // per il File, il cui ast_hash copre l'intero sorgente e quindi
    // cambia SEMPRE quando cambia qualunque parte del file).
    #[test]
    fn scenario3_changed_entity_gets_no_link_unchanged_ones_do() {
        let renamed = renamed_old_new();
        let old_entities = vec![
            ("old-file-id".to_string(), "hash-file-v1".to_string()),
            ("old-a-id".to_string(), "hash-a".to_string()),
            ("old-b-id".to_string(), "hash-b-v1".to_string()),
        ];
        // File e B cambiano (v1 -> v2), A resta identica.
        let new_entities = vec![
            cn("new-file-id", "hash-file-v2"),
            cn("new-a-id", "hash-a"),
            cn("new-b-id", "hash-b-v2"),
        ];

        let links = reconcile_renamed_file(&renamed, &old_entities, &new_entities);

        assert_eq!(
            links,
            vec![LineageLink {
                old_node_id: "old-a-id".to_string(),
                new_node_id: "new-a-id".to_string(),
                ast_hash: "hash-a".to_string(),
            }],
            "solo l'entità invariata (A) deve produrre un link"
        );
    }

    // --- SPEC-027 §3 scenario 5 / §4 ambiguità: hash duplicati sia lato
    // vecchio che nuovo -> accoppiamento arbitrario ma valido, MAI un
    // panic, MAI la stessa entità vecchia rivendicata due volte.
    #[test]
    fn scenario5_duplicate_hash_pairs_arbitrarily_without_reusing_old_entities() {
        let renamed = renamed_old_new();
        let old_entities = vec![
            ("old-x1".to_string(), "dup-hash".to_string()),
            ("old-x2".to_string(), "dup-hash".to_string()),
        ];
        let new_entities = vec![cn("new-x1", "dup-hash"), cn("new-x2", "dup-hash")];

        let links = reconcile_renamed_file(&renamed, &old_entities, &new_entities);

        assert_eq!(
            links.len(),
            2,
            "due coppie valide attese, nessun collegamento perso: {links:?}"
        );
        let old_ids_used: std::collections::HashSet<&str> =
            links.iter().map(|l| l.old_node_id.as_str()).collect();
        assert_eq!(
            old_ids_used.len(),
            2,
            "ciascuna entità vecchia rivendicata al più una volta: {links:?}"
        );
        let new_ids_used: std::collections::HashSet<&str> =
            links.iter().map(|l| l.new_node_id.as_str()).collect();
        assert_eq!(
            new_ids_used,
            std::collections::HashSet::from(["new-x1", "new-x2"]),
            "entrambe le entità nuove devono comparire in un link: {links:?}"
        );
    }

    // --- SPEC-027 §4: più candidati nuovi che vecchi con lo stesso hash
    // -> gli extra restano semplicemente senza link (nessun panic, nessun
    // errore) invece di riusare un'entità vecchia già rivendicata.
    #[test]
    fn edge_more_new_duplicates_than_old_never_reuses_an_old_entity() {
        let renamed = renamed_old_new();
        let old_entities = vec![("old-y1".to_string(), "dup-hash".to_string())];
        let new_entities = vec![
            cn("new-y1", "dup-hash"),
            cn("new-y2", "dup-hash"),
            cn("new-y3", "dup-hash"),
        ];

        let links = reconcile_renamed_file(&renamed, &old_entities, &new_entities);

        assert_eq!(
            links.len(),
            1,
            "solo una coppia possibile, un solo old_entity disponibile: {links:?}"
        );
        assert_eq!(links[0].old_node_id, "old-y1");
    }

    // --- SPEC-027 §4: un'entità nuova il cui ast_hash non combacia con
    // NESSUNA entità vecchia -> nessun LineageLink per lei, non un errore.
    #[test]
    fn edge_genuinely_new_entity_produces_no_link() {
        let renamed = renamed_old_new();
        let old_entities = vec![("old-a-id".to_string(), "hash-a".to_string())];
        let new_entities = vec![cn("new-a-id", "hash-a"), cn("new-brand-new", "hash-never-seen")];

        let links = reconcile_renamed_file(&renamed, &old_entities, &new_entities);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].new_node_id, "new-a-id");
    }
}
