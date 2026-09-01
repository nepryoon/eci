package eventorder

import "testing"

func TestOrderingKeyIsLengthDelimited(t *testing.T) {
	if orderingKey("ab", "c") == orderingKey("a", "bc") {
		t.Fatal("ordering key is ambiguous")
	}
}
