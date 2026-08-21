// Package embedclient implementa un piccolo client Go verso l'API nativa di
// TEI (Text Embeddings Inference) — stessa API replicata da
// services/embedding-worker/internal/embedclient (SPEC-030), non
// importabile qui perché vive in un modulo Go separato (T3.4, stesso
// vincolo incontrato ripetutamente): stesso identico contratto HTTP
// replicato in questo servizio, nessuna logica nuova (SPEC-041 §2).
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
// standard (nessun timeout custom in questa SPEC, stesso principio già
// stabilito per il client replicato di riferimento).
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTPClient: http.DefaultClient}
}

// Embed chiama POST {base_url}/embed con {"inputs": text} e ritorna il primo
// vettore della risposta. Un errore ritornato include sempre il contesto
// (URL + causa) — mai un dettaglio silente perso, mai un vettore vuoto
// silenzioso su errore.
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
