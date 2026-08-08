//! Client Rust verso l'API nativa di TEI (Text Embeddings Inference,
//! SPEC-023 T2.4): stesso contratto HTTP contro `embedder-fake` o il vero
//! `jina-code-embeddings-1.5b`, cambia solo `base_url` configurato —
//! nessun failover automatico runtime (SPEC-023 §2, stessa filosofia già
//! stabilita per `vllm-fake` in SPEC-018).

pub struct EmbeddingClient {
    base_url: String,
}

impl EmbeddingClient {
    pub fn new(base_url: String) -> Self {
        Self { base_url }
    }
}

/// Errore del client: messaggio già pronto per il log/propagazione,
/// include sempre il contesto (URL + causa) — mai un dettaglio silente
/// perso (SPEC-023 §4: nessun panic, nessun vettore vuoto silenzioso).
#[derive(Debug)]
pub struct EmbeddingError(String);

impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for EmbeddingError {}

/// Chiama `POST {base_url}/embed` con `{"inputs": text}` e ritorna il
/// primo (unico, per un solo testo in input) vettore della risposta.
/// Stesso contratto HTTP contro `embedder-fake` o il vero TEI (SPEC-023
/// §2) — nessuna differenza di codice, solo di `base_url` configurato.
pub fn embed(client: &EmbeddingClient, text: &str) -> Result<Vec<f32>, EmbeddingError> {
    let url = format!("{}/embed", client.base_url);

    let mut response = ureq::post(&url)
        .send_json(serde_json::json!({ "inputs": text }))
        .map_err(|e| EmbeddingError(format!("richiesta POST {url}: {e}")))?;

    let vectors: Vec<Vec<f32>> = response
        .body_mut()
        .read_json()
        .map_err(|e| EmbeddingError(format!("deserializzazione risposta da {url}: {e}")))?;

    vectors.into_iter().next().ok_or_else(|| {
        EmbeddingError(format!("{url}: risposta vuota (nessun vettore per l'input)"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// Avvia un server HTTP minimale (un solo accept, poi chiude) che
    /// risponde SEMPRE con `raw_response` grezzo, indipendentemente dalla
    /// richiesta ricevuta — sufficiente per esercitare i percorsi di
    /// errore HTTP del client (status non-200, corpo malformato) senza
    /// bisogno del vero `embedder-fake` (che risponde correttamente per
    /// costruzione, non può produrre queste risposte). Non è un mock
    /// del client: è un vero server TCP/HTTP, il client gli parla
    /// davvero via rete.
    fn stub_server(raw_response: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(raw_response.as_bytes());
                let _ = stream.flush();
            }
        });
        format!("http://{addr}")
    }

    // --- SPEC-023 §3 scenario 5: client puntato su un indirizzo che non
    // risponde -> Err esplicito, non un panic né un vettore vuoto
    // silenzioso. 127.0.0.1:1 (porta privilegiata, nessun listener
    // possibile senza root): connessione rifiutata, non un timeout lento
    // — stesso indirizzo già usato per lo stesso scopo in
    // retrieval-engine/semantic-cache (SPEC-016/SPEC-022). ---
    #[test]
    fn scenario5_unreachable_address_returns_err_not_panic_not_empty_vec() {
        let client = EmbeddingClient::new("http://127.0.0.1:1".to_string());
        let result = embed(&client, "qualunque testo");
        assert!(result.is_err(), "atteso Err su indirizzo irraggiungibile, ottenuto {result:?}");
    }

    // --- SPEC-023 §4 edge case: risposta HTTP con status diverso da 200
    // -> Err esplicito col codice di stato incluso nel messaggio, non un
    // tentativo di parsare il body di errore come un vettore. ---
    #[test]
    fn edge_case_non_200_status_returns_err_with_status_code_in_message() {
        let base_url = stub_server(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"boom\"}",
        );
        let client = EmbeddingClient::new(base_url);
        let result = embed(&client, "testo qualunque");
        let err = result.expect_err("atteso Err su status 500");
        assert!(
            err.to_string().contains("500"),
            "il messaggio d'errore deve includere il codice di stato: {err}"
        );
    }

    // --- SPEC-023 §4 edge case: risposta 200 ma JSON malformato/non un
    // array di float -> Err esplicito di deserializzazione, non un panic. ---
    #[test]
    fn edge_case_200_with_malformed_json_returns_deserialization_err_not_panic() {
        let base_url = stub_server(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"not\":\"a vector\"}",
        );
        let client = EmbeddingClient::new(base_url);
        let result = embed(&client, "testo qualunque");
        assert!(
            result.is_err(),
            "atteso Err su corpo 200 non-vettore, ottenuto {result:?}"
        );
    }

    // --- SPEC-023 §4 edge case: risposta 200 con status ok ma corpo non
    // JSON valido affatto (non solo di forma sbagliata) -> stesso
    // trattamento, Err di deserializzazione, non panic. ---
    #[test]
    fn edge_case_200_with_invalid_json_syntax_returns_err_not_panic() {
        let base_url = stub_server(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\nnon e' json affatto",
        );
        let client = EmbeddingClient::new(base_url);
        let result = embed(&client, "testo qualunque");
        assert!(result.is_err(), "atteso Err su corpo 200 non-JSON, ottenuto {result:?}");
    }
}
