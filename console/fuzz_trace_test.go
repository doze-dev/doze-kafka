package console

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// Record headers are written by any producer, so the trace-header reader
// (traceRefs: W3C traceparent, B3 single and multi, the configured header) reads
// untrusted bytes. What it returns is put in a URL and on a page, and compared
// with what the person asked for, so beyond "no panic" it must only ever return
// an id of the canonical shape that really is in the header.

var (
	fuzzHex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
	fuzzHex16 = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// fuzzTraceHeaders builds a record's headers from the fuzzer's values. Bit i of
// present says whether header i is there at all (a Kafka header may also be null);
// bit 5 puts a decoy with the same name, differently cased, in front, since only
// the first header of a name counts; bit 6 upper-cases the names.
func fuzzTraceHeaders(present uint8, traceparent, b3, b3trace, b3span, b3parent, customName string, customValue []byte) []MessageHeader {
	names := []string{"traceparent", "b3", "x-b3-traceid", "x-b3-spanid", "x-b3-parentspanid"}
	values := []string{traceparent, b3, b3trace, b3span, b3parent}
	var h []MessageHeader
	if present&(1<<5) != 0 {
		h = append(h, MessageHeader{Key: "TraceParent", Value: nil}, MessageHeader{Key: "other", Value: []byte("00-" + traceA + "-" + spanA + "-01")})
	}
	for i, n := range names {
		if present&(1<<i) == 0 {
			continue
		}
		if present&(1<<6) != 0 {
			n = strings.ToUpper(n)
		}
		h = append(h, MessageHeader{Key: n, Value: []byte(values[i])})
	}
	if customName != "" && present&(1<<7) != 0 {
		h = append(h, MessageHeader{Key: customName, Value: customValue})
	}
	return h
}

func FuzzTraceHeaders(f *testing.F) {
	const all = 0x9f
	add := func(present uint8, traceparent, b3, b3trace, b3span, b3parent, customName, customValue string) {
		f.Add(present, traceparent, b3, b3trace, b3span, b3parent, customName, []byte(customValue))
	}
	// What tracing libraries write.
	add(all, "00-"+traceA+"-"+spanA+"-01", traceA+"-"+spanA+"-1-"+parentA, traceA, spanA, parentA, "x-request-id", "req-7781")
	add(0x01, "00-"+strings.ToUpper(traceA)+"-"+strings.ToUpper(spanA)+"-00", "", "", "", "", "", "")
	add(0x02, "", "463ac35c9f6413ad-"+spanA, "", "", "", "", "")
	add(0x02, "", "463ac35c9f6413ad-"+spanA+"-d", "", "", "", "", "")
	add(0x02, "", "0", "", "", "", "", "")
	add(0x1c, "", "", "463ac35c9f6413ad", spanA, parentA, "", "")
	add(0x80, "", "", "", "", "", "X-Correlation-Id", "  7f3c2a\t")
	add(0xe1, " 00-"+traceA+"-"+spanA+"-01\n", "", "", "", "", "TRACEPARENT", "00-"+traceA+"-"+spanA+"-01")
	// What a hostile or broken producer writes.
	add(0x01, "00-00000000000000000000000000000000-"+spanA+"-01", "", "", "", "", "", "")
	add(0x01, "00-"+traceA+"-0000000000000000-01", "", "", "", "", "", "")
	add(0x01, "00-"+traceA+"-"+spanA, "", "", "", "", "", "")
	add(0x01, "00-"+traceA+"-"+spanA+"-01-and-a-great-deal-more"+strings.Repeat("x", 4096), "", "", "", "", "", "")
	add(0x01, "ff-"+traceA+"-"+spanA+"-01", "", "", "", "", "", "")
	add(0x01, "00-"+traceA+"0-"+spanA+"-01", "", "", "", "", "", "")
	add(0x01, "00_"+traceA+"_"+spanA+"_01", "", "", "", "", "", "")
	add(0x01, "\x0000-"+traceA+"-"+spanA+"-01", "", "", "", "", "", "")
	add(0x01, "00-"+strings.Repeat("K", 32)+"-"+spanA+"-01", "", "", "", "", "", "") // Kelvin signs lower-case to k.
	add(0x01, "00-4bf92f3577b34da6a3ce929d0e0e473\xff-"+spanA+"-01", "", "", "", "", "", "")
	add(0x02, "", traceA+"-<script>alert(1)</script>", "", "", "", "", "")
	add(0x02, "", traceA+"-"+strings.Repeat("a", 1<<16), "", "", "", "", "")
	add(0x02, "", traceA+"-"+spanA+"-1-"+strings.Repeat("f", 17), "", "", "", "", "")
	add(0x02, "", "0000000000000000-"+spanA, "", "", "", "", "")
	add(0x02, "", "-", "", "", "", "", "")
	add(0x02, "", traceA+"-", "", "", "", "", "")
	add(0x02, "", strings.Repeat("-", 1000), "", "", "", "", "")
	add(0x1c, "", "", traceA, "not a span\r\nX-Injected: 1", strings.Repeat("9", 300), "", "")
	add(0x1c, "", "", "00000000000000000000000000000000", spanA, parentA, "", "")
	add(0x04, "", "", traceA+traceA, "", "", "", "")
	add(0x80, "", "", "", "", "", "id", strings.Repeat("a", 129))
	add(0x80, "", "", "", "", "", "id", strings.Repeat("a", 128))
	add(0x80, "", "", "", "", "", "id", "   ")
	add(0x80, "", "", "", "", "", "id", "\xff\xfe")
	add(0x80, "", "", "", "", "", "b3", traceA+"-"+spanA)
	add(0x80, "", "", "", "", "", "\xff", "x")

	f.Fuzz(func(t *testing.T, present uint8, traceparent, b3, b3trace, b3span, b3parent, customName string, customValue []byte) {
		h := fuzzTraceHeaders(present, traceparent, b3, b3trace, b3span, b3parent, customName, customValue)
		refs := traceRefs(h, customName)
		// The text each convention reads: the first header of that name that is text
		// (a configured header may share a name with one of the others).
		tpText, tpOK := headerValue(h, "traceparent")
		b3Text, b3OK := headerValue(h, "b3")
		b3TraceText, b3TraceOK := headerValue(h, "x-b3-traceid")
		customText, customOK := "", false
		if customName != "" {
			customText, customOK = headerValue(h, customName)
		}

		if again := traceRefs(h, customName); !reflect.DeepEqual(refs, again) {
			t.Fatalf("two readings of the same headers differ: %+v and %+v", refs, again)
		}
		// One reference per convention at most, most specific first.
		if len(refs) > 4 {
			t.Fatalf("%d references from four conventions: %+v", len(refs), refs)
		}
		rank := func(kind string) int {
			switch kind {
			case "W3C traceparent":
				return 0
			case "B3":
				return 1
			case "header " + customName:
				return 2
			}
			t.Fatalf("a reference of an unknown kind %q", kind)
			return -1
		}
		w3c, b3n, custom := 0, 0, 0
		for i, r := range refs {
			if i > 0 && rank(r.Kind) < rank(refs[i-1].Kind) {
				t.Fatalf("%q comes after %q: most specific first", r.Kind, refs[i-1].Kind)
			}
			switch rank(r.Kind) {
			case 0:
				w3c++
				if !fuzzHex32.MatchString(r.ID) || isZeros(r.ID) {
					t.Fatalf("a traceparent id that is not 32 lower-case hex digits, or is all zeros: %q", r.ID)
				}
				if !fuzzHex16.MatchString(r.Span) || r.Parent != "" {
					t.Fatalf("a traceparent span %q (16 hex digits) with parent %q (traceparent has none)", r.Span, r.Parent)
				}
				// The id is what the header says, where the format puts it: nothing
				// was skipped, repaired or cut out of the middle to find it.
				v := strings.ToLower(tpText)
				if !tpOK || len(v) < 55 || v[2] != '-' || v[3:35] != r.ID || v[35] != '-' || v[36:52] != r.Span || v[52] != '-' {
					t.Fatalf("traceparent %q does not hold id %s and span %s in their places", clip(tpText), r.ID, r.Span)
				}
			case 1:
				b3n++
				if !b3IDShape(r.ID) || isZeros(r.ID) {
					t.Fatalf("a B3 id that is not 16 or 32 lower-case hex digits, or is all zeros: %q", r.ID)
				}
				if r.Span != "" && !fuzzHex16.MatchString(r.Span) {
					t.Fatalf("a B3 span id that is not 16 hex digits: %q", clip(r.Span))
				}
				if r.Parent != "" && !fuzzHex16.MatchString(r.Parent) {
					t.Fatalf("a B3 parent span id that is not 16 hex digits: %q", clip(r.Parent))
				}
				if !(b3OK && strings.HasPrefix(strings.ToLower(b3Text), r.ID+"-")) && !(b3TraceOK && strings.ToLower(b3TraceText) == r.ID) {
					t.Fatalf("B3 id %s is in neither b3 %q nor X-B3-TraceId %q", r.ID, clip(b3Text), clip(b3TraceText))
				}
			case 2:
				custom++
				if !customOK || r.ID == "" || len(r.ID) > 128 || !utf8.ValidString(r.ID) || r.ID != customText || r.ID != strings.TrimSpace(r.ID) || r.Span != "" || r.Parent != "" {
					t.Fatalf("the configured header's value %q gave %+v: it is the whole trimmed text, 1 to 128 bytes, or nothing", clip(customText), r)
				}
			}
		}
		if w3c > 1 || b3n > 2 || custom > 1 {
			t.Fatalf("%d traceparent, %d B3, %d configured references: %+v", w3c, b3n, custom, refs)
		}
		// What cannot be an id is not found as one.
		if w3c > 0 && (!tpOK || len(tpText) < 55) {
			t.Fatalf("a traceparent too short, or not text, gave an id: %q", tpText)
		}
		if custom > 0 && len(customText) > 128 {
			t.Fatalf("an oversized value was taken as an id: %q", clip(customText))
		}
		if len(h) == 1 && !utf8.Valid(h[0].Value) && len(refs) > 0 {
			t.Fatalf("a header that is not text gave %+v", refs)
		}

		// traceOf is the first, and every reference is found again by its own id.
		first := traceOf(h, customName)
		if (first == nil) != (len(refs) == 0) || (first != nil && *first != refs[0]) {
			t.Fatalf("traceOf = %+v, the references are %+v", first, refs)
		}
		for _, r := range refs {
			if _, ok := carries(h, customName, r.ID); !ok {
				t.Fatalf("the record carries %+v and is not found by that id", r)
			}
			if !sameTrace(r.ID, r.ID) {
				t.Fatalf("%q is not the same trace as itself", r.ID)
			}
			// Its canonical rendering reads back as itself.
			var back []traceRef
			switch rank(r.Kind) {
			case 0:
				back = traceRefs([]MessageHeader{hdr("traceparent", "00-"+r.ID+"-"+r.Span+"-01")}, "")
			case 1:
				if r.Span == "" {
					// Only the multi-header form can say a parent and no span.
					multi := []MessageHeader{hdr("X-B3-TraceId", r.ID)}
					if r.Parent != "" {
						multi = append(multi, hdr("X-B3-ParentSpanId", r.Parent))
					}
					back = traceRefs(multi, "")
				} else if r.Parent == "" {
					back = traceRefs([]MessageHeader{hdr("b3", r.ID+"-"+r.Span)}, "")
				} else {
					back = traceRefs([]MessageHeader{hdr("b3", r.ID+"-"+r.Span+"-1-"+r.Parent)}, "")
				}
			case 2:
				// (Under its own name: a configured header called "b3" is also read as B3.)
				back = traceRefs([]MessageHeader{{Key: customName, Value: []byte(r.ID)}}, customName)
				if len(back) > 1 {
					back = back[len(back)-1:]
				}
			}
			if len(back) != 1 || back[0] != r {
				t.Fatalf("%+v written out canonically reads back as %+v", r, back)
			}
		}
	})
}

func b3IDShape(s string) bool {
	return fuzzHex16.MatchString(s) || fuzzHex32.MatchString(s)
}

func clip(s string) string {
	if len(s) > 80 {
		return s[:80] + "..."
	}
	return s
}
