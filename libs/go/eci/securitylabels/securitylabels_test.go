package securitylabels

import "testing"

func TestValidFailsClosed(t *testing.T) {
	if !Valid("tenant", "repo", "group") {
		t.Fatal("valid labels rejected")
	}
	for _, value := range []string{"", " blank", "x\r\ny"} {
		if Valid("tenant", "repo", value) {
			t.Fatalf("invalid label accepted: %q", value)
		}
	}
}
