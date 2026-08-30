package outboxmeta

import (
	"testing"

	kafka "github.com/segmentio/kafka-go"
)

const testEventID = "11111111-1111-1111-1111-111111111111"

func header(key, value string) kafka.Header {
	return kafka.Header{Key: key, Value: []byte(value)}
}

func TestParseValidMetadata(t *testing.T) {
	for _, operation := range []Operation{OperationUpsert, OperationDelete} {
		t.Run(string(operation), func(t *testing.T) {
			metadata, err := Parse([]kafka.Header{
				header("trace_id", "ignored"),
				header("event_type", string(operation)),
				header("event_id", testEventID),
			})
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if metadata.EventID != testEventID || metadata.Operation != operation {
				t.Fatalf("metadata = %+v", metadata)
			}
		})
	}
}

func TestParseRejectsAmbiguousOrMalformedAuthority(t *testing.T) {
	cases := map[string][]kafka.Header{
		"nil":                    nil,
		"missing event id":       {header("event_type", "UPSERT")},
		"missing operation":      {header("event_id", testEventID)},
		"empty event id":         {header("event_id", ""), header("event_type", "UPSERT")},
		"empty operation":        {header("event_id", testEventID), header("event_type", "")},
		"duplicate event id":     {header("event_id", testEventID), header("event_id", testEventID), header("event_type", "UPSERT")},
		"duplicate operation":    {header("event_id", testEventID), header("event_type", "UPSERT"), header("event_type", "UPSERT")},
		"conflicting operation":  {header("event_id", testEventID), header("event_type", "UPSERT"), header("event_type", "DELETE")},
		"unknown operation":      {header("event_id", testEventID), header("event_type", "PATCH")},
		"lowercase operation":    {header("event_id", testEventID), header("event_type", "delete")},
		"noncanonical event id":  {header("event_id", "11111111111111111111111111111111"), header("event_type", "DELETE")},
		"uppercase event id":     {header("event_id", "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"), header("event_type", "DELETE")},
		"surrounding whitespace": {header("event_id", testEventID), header("event_type", " DELETE ")},
		"invalid utf8":           {{Key: "event_id", Value: []byte(testEventID)}, {Key: "event_type", Value: []byte{0xff}}},
	}
	for name, headers := range cases {
		t.Run(name, func(t *testing.T) {
			if metadata, err := Parse(headers); err == nil {
				t.Fatalf("Parse = %+v, want closed error", metadata)
			} else if err.Error() != "invalid outbox metadata" {
				t.Fatalf("error leaks input or unstable detail: %q", err)
			}
		})
	}
}

func FuzzParseNeverPanics(f *testing.F) {
	f.Add("event_id", []byte(testEventID), "event_type", []byte("UPSERT"))
	f.Add("event_type", []byte{0xff}, "event_id", []byte("not-an-id"))
	f.Fuzz(func(t *testing.T, keyA string, valueA []byte, keyB string, valueB []byte) {
		_, _ = Parse([]kafka.Header{{Key: keyA, Value: valueA}, {Key: keyB, Value: valueB}})
	})
}
