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
	"errors"
	"fmt"
	"io"
	"net/http"
)

type unavailableError struct {
	err error
}

func (e *unavailableError) Error() string { return e.err.Error() }
func (e *unavailableError) Unwrap() error { return e.err }

// IsUnavailable identifies failures for which moving a valid chunk through
// retry topics and eventually to the DLQ would be data loss: transport
// failures, throttling, and server-side model outages. Client/input failures
// remain ordinary processing errors and retain the bounded retry/DLQ policy.
func IsUnavailable(err error) bool {
	var unavailable *unavailableError
	return errors.As(err, &unavailable)
}

func dependencyUnavailable(err error) error {
	return &unavailableError{err: err}
}

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

// Health calls TEI's native model health endpoint without generating an
// embedding. It is safe for startup and readiness checks because it does not
// enqueue inference work or expose the upstream response body.
func (c *Client) Health(ctx context.Context) error {
	url := c.BaseURL + "/health"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("embedclient: costruzione health GET %s: %w", url, err)
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return dependencyUnavailable(fmt.Errorf("embedclient: health GET %s: %w", url, err))
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK {
		return dependencyUnavailable(fmt.Errorf("embedclient: health GET %s: status %d", url, response.StatusCode))
	}
	return nil
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
		return nil, dependencyUnavailable(fmt.Errorf("embedclient: richiesta POST %s: %w", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		err := fmt.Errorf("embedclient: POST %s: status %d: %s", url, resp.StatusCode, string(body))
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
			return nil, dependencyUnavailable(err)
		}
		return nil, err
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
