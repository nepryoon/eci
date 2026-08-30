// Package rerankclient implementa un piccolo client Go verso l'API nativa
// di TEI per un cross-encoder di reranking (`bge-reranker-v2-m3`, SPEC-044
// §2) — endpoint `/rerank`, distinto da `/embed` (già usato per gli
// embedding, T2.4/T4.1). Stesso principio "stesso contratto HTTP contro
// fake o vero" già stabilito per internal/embedclient (T4.1): URL
// configurabile, nessun failover automatico.
//
// POST {base_url}/rerank {"query": "...", "texts": ["...", ...]}
// -> [{"index": int, "score": float}, ...].
package rerankclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Client è il client HTTP verso l'endpoint /rerank nativo TEI.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New costruisce un Client con l'http.Client di default della libreria
// standard (nessun timeout custom, stesso principio di embedclient).
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTPClient: http.DefaultClient}
}

// Health calls TEI's native model health endpoint without running reranking.
// Readiness therefore does not create recurring GPU inference load.
func (c *Client) Health(ctx context.Context) error {
	url := c.BaseURL + "/health"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("rerankclient: costruzione health GET %s: %w", url, err)
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("rerankclient: health GET %s: %w", url, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("rerankclient: health GET %s: status %d", url, response.StatusCode)
	}
	return nil
}

// Result è UN elemento della risposta /rerank: l'indice del testo in
// ingresso (posizione nella lista `texts` della richiesta) e il suo
// punteggio.
type Result struct {
	Index int     `json:"index"`
	Score float64 `json:"score"`
}

// Rerank chiama POST {base_url}/rerank con {"query": query, "texts": texts}
// e ritorna i risultati così come li restituisce il servizio (ordine non
// garantito == ordine di `texts`: il chiamante deve usare Result.Index per
// riassociare, non la posizione nello slice ritornato). Un errore
// ritornato include sempre il contesto (URL + causa).
func (c *Client) Rerank(ctx context.Context, query string, texts []string) ([]Result, error) {
	url := c.BaseURL + "/rerank"

	reqBody, err := json.Marshal(map[string]any{"query": query, "texts": texts})
	if err != nil {
		return nil, fmt.Errorf("rerankclient: serializzazione richiesta per %s: %w", url, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("rerankclient: costruzione richiesta POST %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rerankclient: richiesta POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("rerankclient: POST %s: status %d: %s", url, resp.StatusCode, string(body))
	}

	var results []Result
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("rerankclient: deserializzazione risposta da %s: %w", url, err)
	}
	return results, nil
}
