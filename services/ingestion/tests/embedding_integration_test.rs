//! SPEC-023 §3 scenario 4/§7 — test di integrazione: `embedder-fake`
//! avviato come vero sottoprocesso `uvicorn` dal suo stesso venv
//! (bootstrap automatico se assente, stesso principio già usato da
//! `services/orchestrator/orchestrator/conftest.py` per `vllm-fake`,
//! SPEC-018), non un mock HTTP — il client Rust gli parla davvero via
//! rete. Gated con `#[ignore]` per lo stesso motivo dichiarato in
//! `persist_integration_test.rs` (SPEC-014 §10): Cargo non ha un analogo
//! diretto del build tag Go `-tags=integration`, `#[ignore]` è l'idioma
//! usato per escludere l'ESECUZIONE di default da `cargo test`/`task test`
//! senza impedire la compilazione.
//!
//! Esecuzione manuale (richiede python3 sul PATH, nessun Docker):
//! `cargo test --test embedding_integration_test -- --ignored`

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ingestion::embedding::{embed, EmbeddingClient};

fn repo_root() -> PathBuf {
    // services/ingestion -> repo root due livelli sopra.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind porta libera");
    listener.local_addr().expect("local_addr").port()
}

/// Crea il venv se assente: `python3 -m venv` per primo, `python3 -m
/// virtualenv` come fallback (stesso identico principio di
/// `scripts/task-build.sh:ensure_venv()` e del suo equivalente Python in
/// `conftest.py`, replicato qui in Rust per lo stesso motivo — questo
/// binario di test dipende da un `embedder-fake` realmente in esecuzione).
fn ensure_embedder_fake_venv(fakes_dir: &Path) -> PathBuf {
    let venv_dir = fakes_dir.join(".venv");
    let uvicorn_bin = venv_dir.join("bin").join("uvicorn");
    if uvicorn_bin.exists() {
        return uvicorn_bin;
    }

    let venv_ok = Command::new("python3")
        .args(["-m", "venv"])
        .arg(&venv_dir)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !venv_ok {
        let fallback_ok = Command::new("python3")
            .args(["-m", "virtualenv", "-q"])
            .arg(&venv_dir)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(fallback_ok, "impossibile creare il venv in {venv_dir:?} (venv e virtualenv entrambi falliti)");
    }

    let pip = venv_dir.join("bin").join("pip");
    let status = Command::new(&pip)
        .args(["install", "-q", "-e"])
        .arg(repo_root().join("libs").join("py"))
        .arg("-e")
        .arg(fakes_dir)
        .status()
        .unwrap_or_else(|e| panic!("esecuzione di {pip:?}: {e}"));
    assert!(status.success(), "pip install -e (libs/py, embedder-fake) fallito");

    uvicorn_bin
}

struct EmbedderFakeProcess {
    child: Child,
    pub base_url: String,
}

impl Drop for EmbedderFakeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_embedder_fake() -> EmbedderFakeProcess {
    let fakes_dir = repo_root().join("fakes").join("embedder-fake");
    let uvicorn_bin = ensure_embedder_fake_venv(&fakes_dir);
    let port = free_port();

    let mut child = Command::new(&uvicorn_bin)
        .args(["embedder_fake.main:app", "--port"])
        .arg(port.to_string())
        .current_dir(&fakes_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("avvio di {uvicorn_bin:?}: {e}"));

    // Drena stdout/stderr su un thread dedicato: senza, il buffer del
    // pipe si riempie e il processo figlio si blocca in scrittura non
    // appena uvicorn logga qualcosa (readiness probe compresa).
    if let Some(stdout) = child.stdout.take() {
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                eprintln!("[embedder-fake stdout] {line}");
            }
        });
    }
    if let Some(stderr) = child.stderr.take() {
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                eprintln!("[embedder-fake stderr] {line}");
            }
        });
    }

    let base_url = format!("http://127.0.0.1:{port}");
    wait_for_ready(&base_url, Duration::from_secs(30));

    EmbedderFakeProcess { child, base_url }
}

fn wait_for_ready(base_url: &str, timeout: Duration) {
    let client = EmbeddingClient::new(base_url.to_string());
    let deadline = Instant::now() + timeout;
    let mut last_err = None;
    while Instant::now() < deadline {
        match embed(&client, "readiness probe") {
            Ok(_) => return,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    }
    panic!("embedder-fake non pronto su {base_url} entro {timeout:?}: {last_err:?}");
}

/// Chiamata HTTP grezza allo stesso endpoint, indipendente dal client
/// sotto test — SPEC-023 §3 scenario 4: "verificabile chiamando
/// direttamente l'endpoint HTTP in parallelo nello stesso test e
/// confrontando", non un valore atteso hard-coded che potrebbe divergere
/// silenziosamente dall'implementazione del fake.
fn raw_http_embed(base_url: &str, text: &str) -> Vec<f32> {
    let url = format!("{base_url}/embed");
    let mut response = ureq::post(&url)
        .send_json(serde_json::json!({ "inputs": text }))
        .unwrap_or_else(|e| panic!("chiamata HTTP grezza a {url}: {e}"));
    let vectors: Vec<Vec<f32>> = response
        .body_mut()
        .read_json()
        .unwrap_or_else(|e| panic!("deserializzazione risposta grezza da {url}: {e}"));
    vectors.into_iter().next().expect("almeno un vettore nella risposta")
}

// SPEC-023 §3 scenario 4.
#[test]
#[ignore = "richiede python3 sul PATH (bootstrap venv di fakes/embedder-fake); esclusa da cargo test di default"]
fn scenario4_client_matches_raw_http_call_against_real_embedder_fake() {
    let fake = start_embedder_fake();
    let client = EmbeddingClient::new(fake.base_url.clone());

    let text = "func Validate(prices []float64) error { return nil }";

    let via_client = embed(&client, text).expect("embed via client");
    let via_raw_http = raw_http_embed(&fake.base_url, text);

    assert_eq!(via_client.len(), 1536, "atteso vettore a 1536 dimensioni");
    assert_eq!(
        via_client, via_raw_http,
        "il vettore ottenuto dal client deve corrispondere ESATTAMENTE a quello ottenuto \
         chiamando l'endpoint HTTP direttamente, in parallelo, nello stesso test"
    );

    // Determinismo osservato end-to-end attraverso il client stesso (§3
    // scenari 1/2, già verificati a livello Python — qui solo una
    // controprova economica che il client non introduca non-determinismo
    // proprio, es. per via di un ordine di serializzazione instabile).
    let again = embed(&client, text).expect("embed via client, seconda chiamata");
    assert_eq!(via_client, again, "stesso testo -> stesso vettore anche attraverso il client");

    let different = embed(&client, "func Process() {}").expect("embed via client, testo diverso");
    assert_ne!(via_client, different, "testo diverso -> vettore diverso anche attraverso il client");
}
