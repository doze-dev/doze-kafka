package console

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// How the console shows the bytes of a key, a value or a header value.
//
// Kafka bytes are opaque: Avro, Protobuf, a compressed blob and a UTF-8 string all
// arrive the same way. Rendering them all as string(b) turned a binary payload
// into a wall of U+FFFD, hid whether a key was null or merely empty (a tombstone
// looked like an empty string), and offered no way to see or copy the real bytes.

const (
	displayLimit = 256 << 10 // most text/hex shown for one value in the drawer.
	prettyLimit  = 512 << 10 // most that re-indented JSON may grow to; past it the text is shown as sent.
	hexLimit     = 16 << 10  // most bytes shown as a hex dump.
	resendLimit  = 64 << 10  // largest value "Send again" will carry in the page.
	previewRunes = 220       // the table's one-line preview; CSS truncates to the column.
)

// bytesView is one key/value/header value, prepared for display.
type bytesView struct {
	Null      bool   // no bytes at all: a null key, or a tombstone (null value).
	Empty     bool   // present, zero length.
	Size      int    // bytes.
	SizeText  string // "2.1 KB".
	Binary    bool   // not text; Text is empty and the hex/base64 views are the way to read it.
	Text      string // what to show for text (JSON is re-indented); capped at displayLimit.
	Raw       string // the text exactly as stored, when it is text and small enough to edit.
	Pretty    bool   // Text is re-indented JSON.
	Hex       string // hex dump of the first hexLimit bytes.
	B64       string // base64 of the whole value, only when small enough to resend.
	Truncated bool   // Text shows less than the whole value (or, for binary, the hex does).
	HexCut    bool   // the hex dump shows less than the whole value.
	Preview   string // one line, for the table.

	// Schema is set when the bytes were written by a registry-aware serializer and
	// Text/Preview are the JSON they decode to. Raw stays empty and B64 stays the
	// stored bytes, so "Send again" and "Edit a copy" still carry the record exactly.
	Schema *schemaTag
}

// schemaTag says which registered schema a value was decoded with.
type schemaTag struct {
	ID        int
	Type      string // AVRO, JSON or PROTOBUF.
	TypeLabel string
	Subject   string
	Version   int
	URL       string
}

func newBytesView(b []byte, null bool) bytesView {
	v := bytesView{Size: len(b), SizeText: humanBytes(len(b))}
	switch {
	case null || b == nil:
		v.Null = true
		v.Preview = "∅ null"
		return v
	case len(b) == 0:
		v.Empty = true
		v.Preview = `""`
		return v
	}

	if len(b) <= resendLimit {
		v.B64 = base64.StdEncoding.EncodeToString(b)
	}
	v.Hex, v.HexCut = hexDump(b, hexLimit)

	if looksBinary(b) {
		v.Binary = true
		v.Truncated = v.HexCut // for a binary value the hex dump is what is read.
		v.Preview = binaryPreview(b)
		return v
	}

	shown := b
	if len(shown) > displayLimit {
		shown = trimToRuneBoundary(shown[:displayLimit])
		v.Truncated = true
	}
	v.Text = string(shown)
	if len(b) <= resendLimit {
		v.Raw = string(b)
	}
	if !v.Truncated {
		if pretty, ok := indentJSON(b); ok {
			v.Text, v.Pretty = pretty, true
		}
	}
	v.Preview = oneLine(string(b), previewRunes)
	return v
}

// looksBinary reports whether bytes are not text a person would read as text:
// invalid UTF-8, or containing NUL or a notable share of control characters.
// A false "text" shows garbage; a false "binary" just shows hex, which is always
// there to read — so when unsure, call it binary.
func looksBinary(b []byte) bool {
	if !utf8.Valid(b) {
		return true
	}
	return mostlyControl(b)
}

// binarySample is how much of a value the table reads to decide text or binary.
const binarySample = 4096

// looksBinarySample is looksBinary for the first bytes of a longer value. The cut
// may fall inside a multi-byte character, which says nothing about the value, so
// a partial character at the end is ignored: 4,095 letters followed by an "é"
// were shown as binary in the table and as text in the drawer.
func looksBinarySample(b []byte) bool {
	sample := b
	if len(b) > binarySample {
		sample = trimToRuneBoundary(b[:binarySample])
	}
	if !utf8.Valid(sample) {
		return true
	}
	return mostlyControl(sample)
}

// mostlyControl reports whether valid UTF-8 contains a NUL or more than 5%
// control characters.
func mostlyControl(b []byte) bool {
	var ctrl int
	sample := b
	if len(sample) > 4096 {
		sample = sample[:4096]
	}
	for _, c := range sample {
		if c == 0 {
			return true
		}
		if c < 0x20 && c != '\n' && c != '\r' && c != '\t' || c == 0x7f {
			ctrl++
		}
	}
	return ctrl*20 > len(sample) // more than 5% control characters.
}

func binaryPreview(b []byte) string {
	n := min(len(b), 12)
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("%02x", b[i])
	}
	more := ""
	if len(b) > n {
		more = " …"
	}
	return fmt.Sprintf("⟨binary · %s⟩ %s%s", humanBytes(len(b)), strings.Join(parts, " "), more)
}

// oneLine collapses a value to a single bounded line without splitting a rune.
func oneLine(s string, maxRunes int) string {
	var out strings.Builder
	n := 0
	for _, r := range s {
		if n >= maxRunes {
			out.WriteString("…")
			break
		}
		if r == '\n' || r == '\r' || r == '\t' {
			r = ' '
		}
		out.WriteRune(r)
		n++
	}
	return out.String()
}

// trimToRuneBoundary drops an incomplete UTF-8 sequence at the end of b, which is
// what cutting text at a byte offset can leave. It removes nothing else: bytes
// that are simply not UTF-8 are left as they are, so binary data stays
// recognisable as binary. (An earlier version peeled off up to three bytes
// "until valid", which turned a three-byte binary value into an empty, and
// therefore "valid", string.)
func trimToRuneBoundary(b []byte) []byte {
	for i := 1; i <= utf8.UTFMax && i <= len(b); i++ {
		if utf8.RuneStart(b[len(b)-i]) {
			if !utf8.FullRune(b[len(b)-i:]) {
				return b[:len(b)-i]
			}
			return b
		}
	}
	return b
}

// hexDump formats up to limit bytes in the conventional layout: an offset column,
// sixteen hex bytes, and an ASCII gutter. truncated reports whether bytes remain.
func hexDump(b []byte, limit int) (dump string, truncated bool) {
	if len(b) > limit {
		b, truncated = b[:limit], true
	}
	var out strings.Builder
	for off := 0; off < len(b); off += 16 {
		end := min(off+16, len(b))
		fmt.Fprintf(&out, "%08x  ", off)
		for i := off; i < off+16; i++ {
			if i < end {
				fmt.Fprintf(&out, "%02x ", b[i])
			} else {
				out.WriteString("   ")
			}
			if i == off+7 {
				out.WriteByte(' ')
			}
		}
		out.WriteString(" |")
		for i := off; i < end; i++ {
			if b[i] >= 0x20 && b[i] < 0x7f {
				out.WriteByte(b[i])
			} else {
				out.WriteByte('.')
			}
		}
		out.WriteString("|\n")
	}
	return out.String(), truncated
}

// headerView is one header of a record, prepared for display and for resending.
type headerView struct {
	Key string
	V   bytesView
}

func headerViews(hs []MessageHeader) []headerView {
	out := make([]headerView, len(hs))
	for i, h := range hs {
		out[i] = headerView{Key: h.Key, V: newBytesView(h.Value, false)}
	}
	return out
}

// resendHeaders is the headers as the composer's JSON rows. A value that is not
// text travels as base64 and is flagged, so resending reproduces the exact bytes.
func resendHeaders(hs []headerView) string {
	type row struct {
		K   string `json:"k"`
		V   string `json:"v"`
		B64 bool   `json:"b64,omitempty"`
	}
	rows := make([]row, 0, len(hs))
	for _, h := range hs {
		switch {
		case h.V.Binary:
			rows = append(rows, row{h.Key, h.V.B64, true})
		default:
			rows = append(rows, row{h.Key, h.V.Raw, false}) // as stored, not as displayed.
		}
	}
	b, _ := json.Marshal(rows)
	return string(b)
}

// utf8Valid is utf8.Valid for a sample cut from a longer value: a cut may leave a
// partial rune at the end, which says nothing about the value, so it is ignored.

// indentJSON re-indents b if it is JSON and the result stays a readable size.
//
// Indentation adds two spaces per level of nesting to every line, so its output
// grows with the square of the depth: a 6 KB value of nested arrays became 18 MB
// of text, and a page listing twenty such records allocated gigabytes. The output
// is therefore written through a writer that gives up at prettyLimit; a value
// that deep is shown as it was sent.
func indentJSON(b []byte) (string, bool) {
	if len(b) == 0 || (b[0] != '{' && b[0] != '[') || !json.Valid(b) {
		return "", false
	}
	var buf bytes.Buffer
	buf.Grow(min(len(b)*2, prettyLimit))
	if err := indentTo(&limitedWriter{w: &buf, left: prettyLimit}, b); err != nil {
		return "", false
	}
	return buf.String(), true
}

var errTooLong = fmt.Errorf("output too long")

type limitedWriter struct {
	w    *bytes.Buffer
	left int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > l.left {
		return 0, errTooLong
	}
	l.left -= len(p)
	return l.w.Write(p)
}

// indentTo writes src re-indented to w, stopping at the first write w refuses.
// json.Indent writes into a bytes.Buffer with no way to stop early, so this walks
// the tokens itself; src is already known to be valid JSON.
func indentTo(w *limitedWriter, src []byte) error {
	depth := 0
	inString, escaped := false, false
	newline := func() error {
		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
		for i := 0; i < depth; i++ {
			if _, err := w.Write([]byte("  ")); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < len(src); i++ {
		c := src[i]
		if inString {
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '"':
			inString = true
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
		case '{', '[':
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
			// An empty object or array stays on one line.
			j := i + 1
			for j < len(src) && (src[j] == ' ' || src[j] == '\t' || src[j] == '\n' || src[j] == '\r') {
				j++
			}
			if j < len(src) && (src[j] == '}' || src[j] == ']') {
				if _, err := w.Write(src[j : j+1]); err != nil {
					return err
				}
				i = j
				continue
			}
			depth++
			if err := newline(); err != nil {
				return err
			}
		case '}', ']':
			depth--
			if err := newline(); err != nil {
				return err
			}
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
		case ',':
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
			if err := newline(); err != nil {
				return err
			}
		case ':':
			if _, err := w.Write([]byte(": ")); err != nil {
				return err
			}
		default:
			if _, err := w.Write(src[i : i+1]); err != nil {
				return err
			}
		}
	}
	return nil
}
