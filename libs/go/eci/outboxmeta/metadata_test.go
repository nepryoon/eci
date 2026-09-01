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
				header("event_sequence", "42"),
			})
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if metadata.EventID != testEventID || metadata.Operation != operation || metadata.Sequence != 42 {
				t.Fatalf("metadata = %+v", metadata)
			}
		})
	}
}

func TestParseRejectsInvalidCanonicalSequence(t *testing.T) {
	for name, sequenceHeaders := range map[string][]kafka.Header{
		"missing":   nil,
		"zero":      {header("event_sequence", "0")},
		"negative":  {header("event_sequence", "-1")},
		"leading 0": {header("event_sequence", "01")},
		"plus sign": {header("event_sequence", "+1")},
		"overflow":  {header("event_sequence", "18446744073709551616")},
		"duplicate": {header("event_sequence", "1"), header("event_sequence", "2")},
		"non utf8":  {{Key: "event_sequence", Value: []byte{0xff}}},
	} {
		t.Run(name, func(t *testing.T) {
			headers := []kafka.Header{header("event_id", testEventID), header("event_type", "UPSERT")}
			headers = append(headers, sequenceHeaders...)
			if _, err := Parse(headers); err == nil {
				t.Fatal("invalid event_sequence was accepted")
			}
		})
	}
}

func TestParseRejectsAmbiguousOrMalformedAuthority(t *testing.T) {
	cases := map[string][]kafka.Header{
		"nil":                    nil,
		"missing event id":       {header("event_type", "UPSERT"), header("event_sequence", "42")},
		"missing operation":      {header("event_id", testEventID), header("event_sequence", "42")},
		"empty event id":         {header("event_id", ""), header("event_type", "UPSERT"), header("event_sequence", "42")},
		"empty operation":        {header("event_id", testEventID), header("event_type", ""), header("event_sequence", "42")},
		"duplicate event id":     {header("event_id", testEventID), header("event_id", testEventID), header("event_type", "UPSERT"), header("event_sequence", "42")},
		"duplicate operation":    {header("event_id", testEventID), header("event_type", "UPSERT"), header("event_type", "UPSERT"), header("event_sequence", "42")},
		"conflicting operation":  {header("event_id", testEventID), header("event_type", "UPSERT"), header("event_type", "DELETE"), header("event_sequence", "42")},
		"unknown operation":      {header("event_id", testEventID), header("event_type", "PATCH"), header("event_sequence", "42")},
		"lowercase operation":    {header("event_id", testEventID), header("event_type", "delete"), header("event_sequence", "42")},
		"noncanonical event id":  {header("event_id", "11111111111111111111111111111111"), header("event_type", "DELETE"), header("event_sequence", "42")},
		"uppercase event id":     {header("event_id", "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"), header("event_type", "DELETE"), header("event_sequence", "42")},
		"surrounding whitespace": {header("event_id", testEventID), header("event_type", " DELETE "), header("event_sequence", "42")},
		"invalid utf8":           {{Key: "event_id", Value: []byte(testEventID)}, {Key: "event_type", Value: []byte{0xff}}, header("event_sequence", "42")},
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
