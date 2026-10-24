package protocol

import (
	"reflect"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

func tokenRequest(owners int, nameLen int) *kmsg.DescribeDelegationTokenRequest {
	q := kmsg.NewPtrDescribeDelegationTokenRequest()
	for i := 0; i < owners; i++ {
		o := kmsg.NewDescribeDelegationTokenRequestOwner()
		o.PrincipalType, o.PrincipalName = "User", strings.Repeat(string(rune('a'+i)), nameLen)
		q.Owners = append(q.Owners, o)
	}
	return q
}

func writtenAuth(v any, items, depth int) string {
	var b strings.Builder
	writeAuthValue(&b, reflect.ValueOf(v), items, depth)
	return b.String()
}

// What may be shown is shown as far as the depth and the item count allow, and no further.
func TestAuthValuesAreBoundedByItemsAndDepth(t *testing.T) {
	q := tokenRequest(3, 1)
	owner := func(n string) string { return `{"PrincipalType": "User", "PrincipalName": "` + n + `"}` }
	for _, c := range []struct {
		name         string
		items, depth int
		want         string
	}{
		{"all", 8, 8, `{"Owners": [` + owner("a") + `, ` + owner("b") + `, ` + owner("c") + `]}`},
		{"two of three", 2, 8, `{"Owners": [` + owner("a") + `, ` + owner("b") + `, "… 1 more"]}`},
		{"none of three", 0, 8, `{"Owners": ["… 3 more"]}`},
		{"the list at the limit of depth", 8, 1, `{"Owners": "(hidden)"}`},
		{"the list shown, its items at the limit", 8, 2, `{"Owners": ["(hidden)", "(hidden)", "(hidden)"]}`},
		{"the items shown", 8, 3, `{"Owners": [` + owner("a") + `, ` + owner("b") + `, ` + owner("c") + `]}`},
		{"nothing of depth", 8, 0, `"(hidden)"`},
	} {
		if got := writtenAuth(q, c.items, c.depth); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, c.want)
		}
	}
}

// The text is held under a kilobyte; one of exactly that is not narrowed.
func TestAuthDecodedFieldsOfExactlyTheLimitAreNotNarrowed(t *testing.T) {
	found := false
	for full := 1; full <= 7; full++ {
		for last := 1; last <= 128; last++ {
			q := tokenRequest(full, 128)
			o := kmsg.NewDescribeDelegationTokenRequestOwner()
			o.PrincipalType, o.PrincipalName = "User", strings.Repeat("z", last)
			q.Owners = append(q.Owners, o)
			var b strings.Builder
			writeAuthValue(&b, reflect.ValueOf(q), 8, 8)
			if b.Len() != maxDecodedBytes {
				continue
			}
			found = true
			if got := authDecodedFields(q); got != b.String() {
				t.Errorf("a text of exactly %d bytes was narrowed to %d", maxDecodedBytes, len(got))
			}
		}
	}
	if !found {
		t.Fatal("no request makes a text of exactly the limit")
	}
}

func TestSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"TransactionalId":  "transactional_id",
		"TimeoutMs":        "timeout_ms",
		"V3AndBelowTopics": "v3_and_below_topics",
		"A":                "a",
		"Z":                "z",
		"aB":               "a_b",
		"zB":               "z_b",
		"0B":               "0_b",
		"9B":               "9_b",
		"ABc":              "a_bc",
		"ZBc":              "z_bc",
		"ABa":              "a_ba",
		"ABz":              "a_bz",
		"ABC":              "abc",
		"AB`":              "ab`",
		"AB{":              "ab{",
		"`B":               "`b",
		"{B":               "{b",
		"/B":               "/b",
		":B":               ":b",
		"@B":               "@b",
		"[B":               "[b",
		"@Bc":              "@bc",
		"[Bc":              "[bc",
		"@":                "@",
		"[":                "[",
		"":                 "",
		"B":                "b",
		"ab":               "ab",
	} {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}
