package console

// Message lineage: "where did this message go?"
//
// Apps that propagate a trace id in their record headers leave a trail across topics:
// a request arrives, an event is written, a consumer reads it and writes another. The
// broker indexes none of that, and should not; the console finds the trail by reading.
// Given one record's trace id it scans the records of every topic written around the
// same time, with an ordinary Kafka consumer (so decompression and batch parsing are
// the ones every client uses), and lists the ones that carry the same id, in time
// order, with which consumer groups have read past each.
//
// The scan has a budget (records and seconds) and says plainly when it stopped at it,
// instead of implying it found everything. And it only sees what apps put in headers:
// a message with no trace header is invisible to it, and the page says so.

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// traceRef is a trace id found in a record's headers.
type traceRef struct {
	ID string
	// Kind says which convention it came from: "W3C traceparent", "B3", or the name
	// of the configured header.
	Kind string
	// Span is the span id the header carries, and Parent the parent span id where the
	// convention has one (B3 does; traceparent does not).
	Span, Parent string
}

var (
	traceparentRE = regexp.MustCompile(`^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}`)
	b3IDRE        = regexp.MustCompile(`^(?:[0-9a-f]{16}|[0-9a-f]{32})$`)
	spanIDRE      = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

func isZeros(s string) bool { return strings.Trim(s, "0") == "" }

// headerValue is the text of the first header called name (compared without regard to
// case: Kafka header names are not normalised, and tracing libraries disagree on
// "traceparent", "Traceparent" and "X-B3-TraceId").
func headerValue(h []MessageHeader, name string) (string, bool) {
	for _, x := range h {
		if strings.EqualFold(x.Key, name) && x.Value != nil && utf8.Valid(x.Value) {
			return strings.TrimSpace(string(x.Value)), true
		}
	}
	return "", false
}

// traceRefs reads every trace id a record carries, most specific first. custom is the
// name of an extra header whose whole value is the id (a request id, a correlation id).
// A record may carry several conventions; all of them find it.
func traceRefs(h []MessageHeader, custom string) []traceRef {
	var out []traceRef
	if v, ok := headerValue(h, "traceparent"); ok {
		if m := traceparentRE.FindStringSubmatch(strings.ToLower(v)); m != nil && !isZeros(m[1]) {
			out = append(out, traceRef{ID: m[1], Kind: "W3C traceparent", Span: m[2]})
		}
	}
	if v, ok := headerValue(h, "b3"); ok {
		// Single header: {trace}-{span}[-{sampled}[-{parent}]]. A lone "0", "1" or "d"
		// is only a sampling decision.
		parts := strings.Split(strings.ToLower(v), "-")
		if len(parts) >= 2 && b3IDRE.MatchString(parts[0]) && !isZeros(parts[0]) {
			ref := traceRef{ID: parts[0], Kind: "B3", Span: parts[1]}
			if len(parts) >= 4 && spanIDRE.MatchString(parts[3]) {
				ref.Parent = parts[3]
			}
			out = append(out, ref)
		}
	}
	if v, ok := headerValue(h, "x-b3-traceid"); ok {
		if id := strings.ToLower(v); b3IDRE.MatchString(id) && !isZeros(id) {
			ref := traceRef{ID: id, Kind: "B3"}
			if s, ok := headerValue(h, "x-b3-spanid"); ok {
				ref.Span = strings.ToLower(s)
			}
			if p, ok := headerValue(h, "x-b3-parentspanid"); ok {
				ref.Parent = strings.ToLower(p)
			}
			out = append(out, ref)
		}
	}
	if custom != "" {
		if v, ok := headerValue(h, custom); ok && v != "" && len(v) <= 128 {
			out = append(out, traceRef{ID: v, Kind: "header " + custom})
		}
	}
	return out
}

// traceOf is the id a record is known by, or nil if it carries none.
func traceOf(h []MessageHeader, custom string) *traceRef {
	if refs := traceRefs(h, custom); len(refs) > 0 {
		return &refs[0]
	}
	return nil
}

// carries reports whether a record's headers hold this trace id in any convention.
func carries(h []MessageHeader, custom, id string) (traceRef, bool) {
	for _, ref := range traceRefs(h, custom) {
		if sameTrace(ref.ID, id) {
			return ref, true
		}
	}
	return traceRef{}, false
}

// sameTrace compares two trace ids. B3 may carry a 64-bit id where W3C carries 128
// bits, the short one standing for the long one with leading zeros.
func sameTrace(a, b string) bool {
	if a == b {
		return true
	}
	pad := func(s string) string {
		if len(s) == 16 && b3IDRE.MatchString(s) {
			return strings.Repeat("0", 16) + s
		}
		return s
	}
	return pad(a) == pad(b)
}
