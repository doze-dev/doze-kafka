package schemaregistry

import (
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"testing"
)

// Two readers of text from outside that no target reached until the marker check
// (docs/fuzz_test.go) asked which one did: the JSON document a person types into
// the console's compose panel, which Codec.Encode turns into a record value under
// a schema, and the version element of a registry path.

// Encode is given any text. It is an error, or a value in the registry's wire
// format under the id asked for that the same codec reads back, and what it reads
// back encodes again: nothing is written that cannot be read.
func FuzzEncode(f *testing.F) {
	for _, doc := range []string{
		`{"a":{"string":"x"},"b":[1,2]}`, `{"a":null,"b":[]}`, `{"a":"x","b":[1]}`,
		`{"x":1,"s":["a","b"]}`, `{"x":"1"}`, `{}`, `{"age":3}`, `null`, `[]`, `""`, `0`,
		`{`, `[1,2`, `{"a":null,"b":[]} trailing`, `{"a":null,"b":[9223372036854775808]}`,
		`{"a":null,"b":[1.5]}`, `{"a":null,"b":[1e400]}`, `{"a":null,"b":["1"]}`, `{"a":null,"b":null}`,
		`{"a":{"int":1},"b":[]}`, `{"a":{"string":1},"b":[]}`, `{"a":{},"b":[]}`, `{"a":null}`,
		`{"x":2147483648}`, `{"x":-2147483649}`, `{"x":1.0}`, `{"x":"NaN"}`, `{"s":[null]}`, `{"s":"a"}`,
		`{"x":1,"x":2}`, `{"unknown":1}`, `{"a":null,"b":[],"c":1}`, "\xff", `{"a":"\ud800","b":[]}`,
		`[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]`,
	} {
		f.Add(doc)
	}
	f.Fuzz(func(t *testing.T, doc string) {
		if len(doc) > 1<<14 {
			t.Skip()
		}
		for _, c := range fuzzCodecs {
			out, err := c.Encode(7, doc)
			if err != nil {
				if out != nil {
					t.Fatalf("%s: an error (%v) with a value %q", c.Type(), err, out)
				}
				continue
			}
			if id, ok := WireID(out); !ok || id != 7 {
				t.Fatalf("%s: %q encoded to %q, which is not wire format under id 7", c.Type(), doc, out)
			}
			back, err := c.Decode(out)
			if err != nil {
				t.Fatalf("%s: %q encoded to %q, which does not decode: %v", c.Type(), doc, out, err)
			}
			if !json.Valid([]byte(back)) {
				t.Fatalf("%s: %q reads back as %q, which is not JSON", c.Type(), doc, back)
			}
			if _, err := c.Encode(7, back); err != nil {
				t.Fatalf("%s: %q reads back as %q, which does not encode: %v", c.Type(), doc, back, err)
			}
		}
	})
}

// A version in a path is "latest", -1, or a whole number from 1 to 2^31-1; anything
// else is refused, in the registry's own error and never a 500.
func FuzzVersionPath(f *testing.F) {
	for _, s := range []string{"latest", "-1", "1", "2", "0", "-2", "2147483647", "2147483648", "+1", "01", " 1", "1 ",
		"1e3", "0x1", "٣", "", "LATEST", "latest ", "1/schema", "..", "%2e%2e", "99999999999999999999", "1\x00"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 1<<10 {
			t.Skip()
		}
		n, latest, e := parseVersion(s)
		switch {
		case e != nil:
			if n != 0 || latest {
				t.Fatalf("parseVersion(%q) refused it and still gave %d, latest=%v", s, n, latest)
			}
		case latest:
			if (s != "latest" && s != "-1") || n != 0 {
				t.Fatalf("parseVersion(%q) says latest (n=%d)", s, n)
			}
		default:
			if want, err := strconv.Atoi(s); err != nil || want != n || n < 1 || n > math.MaxInt32 {
				t.Fatalf("parseVersion(%q) = %d, which is not that number, or is outside 1 to 2^31-1", s, n)
			}
		}
		// And where it arrives from: a path, over a registry that holds version 1 of s.
		a := newFuzzAPI(t)
		if _, e := a.reg.Register("s", SchemaInput{Schema: `"string"`}, false); e != nil {
			t.Fatalf("seed schema: %v", e)
		}
		for _, route := range []struct{ method, tail string }{{"GET", ""}, {"GET", "/schema"}, {"GET", "/referencedby"}, {"DELETE", ""}} {
			rec := serve(t, a, route.method, "/subjects/s/versions/"+url.PathEscape(s)+route.tail, "", "")
			if rec == nil {
				continue
			}
			found := e == nil && (latest || n == 1)
			if route.tail == "" && route.method == "GET" && (rec.Code == 200) != found {
				t.Fatalf("GET version %q answered %d; parseVersion says n=%d latest=%v err=%v", s, rec.Code, n, latest, e)
			}
		}
	})
}
