// Package outboxmeta validates the canonical metadata promoted by the
// Debezium outbox router. Payload fields are never authority sources.
package outboxmeta

import (
	"errors"
	"regexp"
	"strconv"
	"unicode/utf8"

	kafka "github.com/segmentio/kafka-go"
)

const (
	eventIDHeaderKey   = "event_id"
	eventTypeHeaderKey = "event_type"
	eventSequenceKey   = "event_sequence"
)

// Operation is the closed canonical mutation operation.
type Operation string

const (
	OperationUpsert Operation = "UPSERT"
	OperationDelete Operation = "DELETE"
)

// Metadata contains only authority promoted from canonical outbox columns.
type Metadata struct {
	EventID   string
	Operation Operation
	Sequence  int64
}

// ErrInvalidMetadata deliberately carries no attacker-controlled value.
var ErrInvalidMetadata = errors.New("invalid outbox metadata")

var canonicalUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var canonicalSequence = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

// Parse requires exactly one canonical event ID, operation and sequence
// header. Unknown headers are ignored and never become authority.
func Parse(headers []kafka.Header) (Metadata, error) {
	var metadata Metadata
	var eventIDCount, operationCount, sequenceCount int
	for _, header := range headers {
		switch header.Key {
		case eventIDHeaderKey:
			eventIDCount++
			if !utf8.Valid(header.Value) {
				return Metadata{}, ErrInvalidMetadata
			}
			metadata.EventID = string(header.Value)
		case eventTypeHeaderKey:
			operationCount++
			if !utf8.Valid(header.Value) {
				return Metadata{}, ErrInvalidMetadata
			}
			metadata.Operation = Operation(header.Value)
		case eventSequenceKey:
			sequenceCount++
			if !utf8.Valid(header.Value) || !canonicalSequence.Match(header.Value) {
				return Metadata{}, ErrInvalidMetadata
			}
			sequence, err := strconv.ParseInt(string(header.Value), 10, 64)
			if err != nil {
				return Metadata{}, ErrInvalidMetadata
			}
			metadata.Sequence = sequence
		}
	}

	if eventIDCount != 1 || operationCount != 1 || sequenceCount != 1 ||
		!canonicalUUID.MatchString(metadata.EventID) {
		return Metadata{}, ErrInvalidMetadata
	}
	if metadata.Operation != OperationUpsert && metadata.Operation != OperationDelete {
		return Metadata{}, ErrInvalidMetadata
	}
	return metadata, nil
}
