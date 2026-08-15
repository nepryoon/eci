// Package embedclient implementa un piccolo client Go verso l'API nativa di
// TEI (Text Embeddings Inference, SPEC-030 §2) — non un riuso del client
// Rust di T2.4 (SPEC-023, services/ingestion/src/embedding.rs), che vive in
// un crate diverso e non è chiamabile da Go: stesso identico contratto HTTP
// replicato in questo linguaggio, nessuna logica nuova.
//
// POST {base_url}/embed {"inputs": text} -> [[float, ...], ...]; il client
// ritorna il primo (unico, per un solo testo in input) vettore della
// risposta. Stesso URL configurabile per fake/reale (embedder-fake in
// sviluppo/test, vero TEI quando disponibile) — nessuna differenza di
// codice, solo di base URL.
package embedclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client è il client HTTP verso l'endpoint /embed nativo TEI.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New costruisce un Client con l'http.Client di default della libreria
// standard (nessun timeout custom in questa SPEC — §5 Non-goals, stesso
// principio già stabilito per il client Rust di T2.4).
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTPClient: http.DefaultClient}
}

// Embed chiama POST {base_url}/embed con {"inputs": text} e ritorna il primo
// vettore della risposta. Un errore ritornato include sempre il contesto
// (URL + causa) — mai un dettaglio silente perso, mai un vettore vuoto
// silenzioso su errore (stesso principio di SPEC-023 §4, replicato qui).
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	url := c.BaseURL + "/embed"

	reqBody, err := json.Marshal(map[string]any{"inputs": text})
	if err != nil {
		return nil, fmt.Errorf("embedclient: serializzazione richiesta per %s: %w", url, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("embedclient: costruzione richiesta POST %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedclient: richiesta POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedclient: POST %s: status %d: %s", url, resp.StatusCode, string(body))
	}

	var vectors [][]float32
	if err := json.NewDecoder(resp.Body).Decode(&vectors); err != nil {
		return nil, fmt.Errorf("embedclient: deserializzazione risposta da %s: %w", url, err)
	}
	if len(vectors) == 0 {
		return nil, fmt.Errorf("embedclient: %s: risposta vuota (nessun vettore per l'input)", url)
	}
	return vectors[0], nil
}
