// Package models — SPEC-003 §2. Struct Go scritti a mano (non generati) per
// D2 (contracts/jsonschema/hybrid-graph.json): Go non ha discriminated union
// native, quindi CodeNode porta `Ext` come json.RawMessage e ParseExt()
// dispaccia a runtime sul tipo concreto in base a Domain, validando i campi
// che nel JSON Schema sono vincolati da if/then/const + enum.
package models

import (
	"encoding/json"
	"fmt"
)

// Provenance rispecchia #/definitions/provenance di hybrid-graph.json.
type Provenance struct {
	Repo         string `json:"repo"`
	CommitSHA    string `json:"commit_sha"`
	Path         string `json:"path"`
	StartLine    *int   `json:"start_line,omitempty"`
	EndLine      *int   `json:"end_line,omitempty"`
	IngestedAt   string `json:"ingested_at"`
	SourceSystem string `json:"source_system,omitempty"`
}

// Versioning rispecchia #/definitions/versioning di hybrid-graph.json.
type Versioning struct {
	Version    int     `json:"version"`
	IsCurrent  bool    `json:"is_current"`
	ValidFrom  string  `json:"valid_from,omitempty"`
	ValidTo    *string `json:"valid_to,omitempty"`
	Supersedes *string `json:"supersedes,omitempty"`
}

// EmbeddingRef rispecchia #/definitions/embeddingRef di hybrid-graph.json.
type EmbeddingRef struct {
	VectorID         string `json:"vector_id"`
	ModelID          string `json:"model_id"`
	Dim              int    `json:"dim"`
	LogicFingerprint string `json:"logic_fingerprint,omitempty"`
}

// CodeNode rispecchia #/definitions/CodeNode. Ext resta non tipizzato fino a
// ParseExt(): il domain discrimina quale estensione concreta contiene.
type CodeNode struct {
	ID          string          `json:"id"`
	Domain      string          `json:"domain"`
	Name        string          `json:"name"`
	AstHash     string          `json:"ast_hash"`
	ContentHash string          `json:"content_hash,omitempty"`
	Embedding   *EmbeddingRef   `json:"embedding,omitempty"`
	Provenance  Provenance      `json:"provenance"`
	Versioning  Versioning      `json:"versioning"`
	Ext         json.RawMessage `json:"ext,omitempty"`
}

// CodeExtension rispecchia #/definitions/codeExtension.
type CodeExtension struct {
	NodeType   string `json:"node_type"`
	Language   string `json:"language,omitempty"`
	Signature  string `json:"signature,omitempty"`
	Visibility string `json:"visibility,omitempty"`
	SymbolID   string `json:"symbol_id,omitempty"`
	DocHash    string `json:"doc_hash,omitempty"`
}

// DocExtension rispecchia #/definitions/docExtension.
type DocExtension struct {
	NodeType string `json:"node_type"`
	Title    string `json:"title,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
}

// LegalExtension rispecchia #/definitions/legalExtension.
type LegalExtension struct {
	NodeType        string `json:"node_type"`
	Jurisdiction    string `json:"jurisdiction,omitempty"`
	EffectiveDate   string `json:"effective_date,omitempty"`
	ObligationLevel string `json:"obligation_level,omitempty"`
}

var codeNodeTypes = map[string]bool{
	"File": true, "Module": true, "Package": true, "Class": true,
	"Interface": true, "Method": true, "Function": true, "Parameter": true,
	"Field": true, "CallSite": true,
}

var docNodeTypes = map[string]bool{
	"Document": true, "Section": true, "Paragraph": true,
}

var legalNodeTypes = map[string]bool{
	"Contract": true, "Clause": true, "Policy": true, "Regulation": true,
}

// ParseExt dispaccia Ext sul tipo concreto in base a Domain, replicando a
// runtime il vincolo if/then/const di hybrid-graph.json (Go non ha
// discriminated union native). Ritorna errore se Ext è assente, se Domain
// non è uno dei tre domini con estensione tipizzata, o se node_type non è
// nell'enum atteso per quel dominio.
func (n *CodeNode) ParseExt() (any, error) {
	if len(n.Ext) == 0 {
		return nil, fmt.Errorf("ext is required for domain %q", n.Domain)
	}

	switch n.Domain {
	case "code":
		var ext CodeExtension
		if err := json.Unmarshal(n.Ext, &ext); err != nil {
			return nil, fmt.Errorf("unmarshal CodeExtension: %w", err)
		}
		if !codeNodeTypes[ext.NodeType] {
			return nil, fmt.Errorf("invalid node_type %q for domain=code", ext.NodeType)
		}
		return ext, nil
	case "doc":
		var ext DocExtension
		if err := json.Unmarshal(n.Ext, &ext); err != nil {
			return nil, fmt.Errorf("unmarshal DocExtension: %w", err)
		}
		if !docNodeTypes[ext.NodeType] {
			return nil, fmt.Errorf("invalid node_type %q for domain=doc", ext.NodeType)
		}
		return ext, nil
	case "legal":
		var ext LegalExtension
		if err := json.Unmarshal(n.Ext, &ext); err != nil {
			return nil, fmt.Errorf("unmarshal LegalExtension: %w", err)
		}
		if !legalNodeTypes[ext.NodeType] {
			return nil, fmt.Errorf("invalid node_type %q for domain=legal", ext.NodeType)
		}
		return ext, nil
	default:
		return nil, fmt.Errorf("unsupported domain: %q", n.Domain)
	}
}
