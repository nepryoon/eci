package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

const requiredChunkCursorSchema = 1

// requireChunkCursorMigration is the reader-side half of the in-place
// OpenSearch migration. sink-search writes the mapping marker only after every
// historical document has a concrete chunk_id and event_sequence. Requiring
// it before the gRPC listener starts prevents a rolling deployment from
// enabling cursor/dedup logic over legacy documents that could otherwise be
// skipped at a search_after tie.
func requireChunkCursorMigration(ctx context.Context, client *opensearchapi.Client) error {
	response, err := client.Indices.Mapping.Get(ctx, &opensearchapi.MappingGetReq{
		Indices: []string{retrievalIndexName},
	})
	if err != nil {
		return fmt.Errorf("OpenSearch chunk cursor migration unavailable: %w", err)
	}
	if response == nil {
		return fmt.Errorf("OpenSearch chunk cursor migration unavailable")
	}
	inspect := response.Inspect()
	if inspect.Response == nil || inspect.Response.StatusCode != http.StatusOK {
		return fmt.Errorf("OpenSearch chunk cursor migration unavailable")
	}
	index, ok := response.GetIndices()[retrievalIndexName]
	if !ok {
		return fmt.Errorf("OpenSearch chunk cursor migration unavailable")
	}
	var mapping struct {
		Meta struct {
			ChunkCursorSchema int `json:"eci_chunk_cursor_schema"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(index.Mappings, &mapping); err != nil || mapping.Meta.ChunkCursorSchema != requiredChunkCursorSchema {
		return fmt.Errorf("OpenSearch chunk cursor migration incomplete")
	}
	return nil
}
