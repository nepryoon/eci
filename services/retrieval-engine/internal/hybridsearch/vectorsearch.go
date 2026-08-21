package hybridsearch

import (
	"context"
	"fmt"

	"github.com/qdrant/go-client/qdrant"
)

// VectorSearch — port 1:1 di D5 `_vector_search`: query Qdrant per
// similarità (non per id noto) con filtri di payload domain/repo. Il
// node_id è letto dal *payload* di ciascun risultato, esattamente come D5
// `p.payload.get("node_id", p.id)` (SPEC-041 §2) — nessuna derivazione di
// id necessaria qui, a differenza di sink-vector (T3.1), dove il point id
// Qdrant è derivato via UUIDv5 e il node_id reale vive SOLO nel payload.
//
// Provenance popolata SOLO se il payload ha tutte e cinque le chiavi piatte
// repo/path/start_line/end_line/commit (stessa condizione di D5) — porta
// fedele dell'algoritmo di riferimento, non della forma payload reale
// scritta da sink-vector (SPEC-033), che nidifica la provenance sotto la
// chiave "provenance": deviazione dichiarata a fondo SPEC-041.
func VectorSearch(ctx context.Context, client *qdrant.Client, collection string, queryVector []float32, domain, repo *string, limit int) ([]RetrievedNode, error) {
	var must []*qdrant.Condition
	if domain != nil && *domain != "" {
		must = append(must, qdrant.NewMatch("domain", *domain))
	}
	if repo != nil && *repo != "" {
		must = append(must, qdrant.NewMatch("repo", *repo))
	}
	var filter *qdrant.Filter
	if len(must) > 0 {
		filter = &qdrant.Filter{Must: must}
	}

	points, err := client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQuery(queryVector...),
		Filter:         filter,
		Limit:          qdrant.PtrOf(uint64(limit)),
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, newHybridSearchError(fmt.Sprintf("Qdrant query fallita (collection=%s)", collection), err)
	}

	out := make([]RetrievedNode, 0, len(points))
	for rank, p := range points {
		payload := p.GetPayload()

		nodeID := stringPayload(payload, "node_id")
		if nodeID == "" {
			nodeID = p.GetId().String()
		}
		domainVal := stringPayload(payload, "domain")
		if domainVal == "" {
			domainVal = "code"
		}

		var prov *Provenance
		if hasProvenanceKeys(payload) {
			prov = &Provenance{
				Repo:      stringPayload(payload, "repo"),
				Path:      stringPayload(payload, "path"),
				StartLine: intPayload(payload, "start_line"),
				EndLine:   intPayload(payload, "end_line"),
				Commit:    stringPayload(payload, "commit"),
			}
		}

		score := float64(p.GetScore())
		r := rank + 1
		out = append(out, RetrievedNode{
			NodeID:      nodeID,
			Domain:      domainVal,
			Source:      "vector",
			VectorScore: &score,
			VectorRank:  &r,
			Provenance:  prov,
			Payload:     payloadToMap(payload),
		})
	}
	return out, nil
}

func hasProvenanceKeys(payload map[string]*qdrant.Value) bool {
	for _, k := range []string{"repo", "path", "start_line", "end_line", "commit"} {
		if _, ok := payload[k]; !ok {
			return false
		}
	}
	return true
}

func stringPayload(payload map[string]*qdrant.Value, key string) string {
	v, ok := payload[key]
	if !ok {
		return ""
	}
	return v.GetStringValue()
}

func intPayload(payload map[string]*qdrant.Value, key string) int {
	v, ok := payload[key]
	if !ok {
		return 0
	}
	return int(v.GetIntegerValue())
}

func payloadToMap(payload map[string]*qdrant.Value) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = valueToAny(v)
	}
	return out
}

// valueToAny converte un *qdrant.Value (rappresentazione JSON-con-interi del
// payload Qdrant) nel corrispondente valore Go — inverso di qdrant.NewValue,
// che il client ufficiale non espone. Type switch sul campo Kind (oneof
// generato da protoc), non sui valori di default: un GetStringValue()=="" o
// GetIntegerValue()==0 legittimo (valore realmente vuoto/zero) non va
// confuso con "campo assente".
func valueToAny(v *qdrant.Value) any {
	if v == nil {
		return nil
	}
	switch v.GetKind().(type) {
	case *qdrant.Value_NullValue:
		return nil
	case *qdrant.Value_BoolValue:
		return v.GetBoolValue()
	case *qdrant.Value_IntegerValue:
		return v.GetIntegerValue()
	case *qdrant.Value_DoubleValue:
		return v.GetDoubleValue()
	case *qdrant.Value_StringValue:
		return v.GetStringValue()
	case *qdrant.Value_StructValue:
		fields := v.GetStructValue().GetFields()
		out := make(map[string]any, len(fields))
		for k, fv := range fields {
			out[k] = valueToAny(fv)
		}
		return out
	case *qdrant.Value_ListValue:
		values := v.GetListValue().GetValues()
		out := make([]any, len(values))
		for i, lv := range values {
			out[i] = valueToAny(lv)
		}
		return out
	default:
		return nil
	}
}
