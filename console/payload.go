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
	Pretty    bool   // Text is re-indented JSON.
	Hex       string // hex dump of the first hexLimit bytes.
	B64       string // base64 of the whole value, only when small enough to resend.
	Truncated bool   // Text or Hex shows less than the whole value.
	Preview   string // one line, for the table.
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
	v.Hex, v.Truncated = hexDump(b, hexLimit)

	if looksBinary(b) {
		v.Binary = true
		v.Preview = binaryPreview(b)
		return v
	}

	shown := b
	if len(shown) > displayLimit {
		shown = trimToRuneBoundary(shown[:displayLimit])
		v.Truncated = true
	}
	v.Text = string(shown)
	if !v.Truncated && json.Valid(b) {
		var buf bytes.Buffer
		if json.Indent(&buf, b, "", "  ") == nil {
			v.Text, v.Pretty = buf.String(), true
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

// trimToRuneBoundary drops a trailing partial UTF-8 sequence left by a byte cut.
func trimToRuneBoundary(b []byte) []byte {
	for i := 0; i < 3 && len(b) > 0; i++ {
		if utf8.Valid(b) {
			return b
		}
		b = b[:len(b)-1]
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
			rows = append(rows, row{h.Key, h.V.Text, false})
		}
	}
	b, _ := json.Marshal(rows)
	return string(b)
}

// utf8Valid is utf8.Valid for a sample cut from a longer value: a cut may leave a
// partial rune at the end, which says nothing about the value, so it is ignored.
func utf8Valid(b []byte) bool { return utf8.Valid(trimToRuneBoundary(b)) }
