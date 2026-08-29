// Package accessscope turns an authenticated SecurityContext into the
// immutable, bounded scope used by datastore queries. It never accepts RPC
// request fields, prompts, model output, or caller-provided fallbacks.
package accessscope

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/eci-project/eci/libs/go/eci/secctx"
)

const fingerprintVersion = "eci-acl-scope-v1"

const (
	maxValues      = 128
	maxValueLength = 256
)

var ErrInvalidScope = errors.New("authenticated security scope is missing or invalid")

type Scope struct {
	TenantID     string
	UserID       string
	AllowedRepos []string
	ACLGroups    []string
}

// FromContext derives a defensive, deterministic copy from metadata that the
// secctx interceptor already authenticated and attached to ctx. Any missing,
// blank, oversized, malformed, or control-character-bearing value is denied.
func FromContext(ctx context.Context) (Scope, error) {
	sc, ok := secctx.FromContext(ctx)
	if !ok || sc == nil || !validValue(sc.GetTenantId()) || !validValue(sc.GetUserId()) {
		return Scope{}, ErrInvalidScope
	}
	repos, ok := normalize(sc.GetAllowedRepos())
	if !ok {
		return Scope{}, ErrInvalidScope
	}
	groups, ok := normalize(sc.GetAclGroups())
	if !ok {
		return Scope{}, ErrInvalidScope
	}
	return Scope{
		TenantID:     sc.GetTenantId(),
		UserID:       sc.GetUserId(),
		AllowedRepos: repos,
		ACLGroups:    groups,
	}, nil
}

func normalize(values []string) ([]string, bool) {
	if len(values) == 0 || len(values) > maxValues {
		return nil, false
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validValue(value) {
			return nil, false
		}
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, true
}

func validValue(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) || len(value) > maxValueLength {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Fingerprint returns the canonical ACL partition identifier from a validated
// authorization scope. User identity is deliberately excluded: users with the
// same tenant/repository/group permissions may safely share cache entries.
// Invalid directly-constructed scopes return an empty fingerprint so callers
// cannot accidentally create a default partition.
func Fingerprint(scope Scope) string {
	if !validValue(scope.TenantID) {
		return ""
	}
	repos, ok := normalize(scope.AllowedRepos)
	if !ok {
		return ""
	}
	groups, ok := normalize(scope.ACLGroups)
	if !ok {
		return ""
	}

	var encoded bytes.Buffer
	encoded.WriteString(fingerprintVersion)
	writeLengthPrefixed(&encoded, scope.TenantID)
	writeList(&encoded, repos)
	writeList(&encoded, groups)
	digest := sha256.Sum256(encoded.Bytes())
	return hex.EncodeToString(digest[:])
}

func writeList(dst *bytes.Buffer, values []string) {
	_ = binary.Write(dst, binary.BigEndian, uint32(len(values)))
	for _, value := range values {
		writeLengthPrefixed(dst, value)
	}
}

func writeLengthPrefixed(dst *bytes.Buffer, value string) {
	_ = binary.Write(dst, binary.BigEndian, uint32(len([]byte(value))))
	dst.WriteString(value)
}
