package console

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestBytesViewDistinguishesNullEmptyTextAndBinary(t *testing.T) {
	if v := newBytesView(nil, true); !v.Null || v.Empty || v.Preview != "∅ null" {
		t.Errorf("null: %+v", v)
	}
	if v := newBytesView([]byte{}, false); v.Null || !v.Empty || v.Preview != `""` {
		t.Errorf("empty must not look like null: %+v", v)
	}
	text := newBytesView([]byte("hello, wörld ✓"), false)
	if text.Binary || text.Text != "hello, wörld ✓" || text.Size != 17 {
		t.Errorf("text: %+v", text)
	}
	if v := newBytesView([]byte{0x6a, 0x8f, 0xc4, 0x00, 0xff}, false); !v.Binary || v.Text != "" || !strings.HasPrefix(v.Preview, "⟨binary · 5 B⟩ 6a 8f c4 00 ff") {
		t.Errorf("binary: %+v", v)
	}
	// Valid UTF-8 containing a NUL is still not text.
	if v := newBytesView([]byte("ab\x00cd"), false); !v.Binary {
		t.Errorf("a NUL byte should make it binary: %+v", v)
	}
	// Protobuf-ish: valid UTF-8 prefix then control bytes.
	if v := newBytesView([]byte{0x08, 0x96, 0x01, 0x12, 0x07, 't', 'e', 's', 't', 'i', 'n', 'g'}, false); !v.Binary {
		t.Errorf("protobuf-like bytes should be binary: %+v", v)
	}
	// Ordinary text with newlines and tabs is text.
	if v := newBytesView([]byte("line one\nline\ttwo\r\n"), false); v.Binary {
		t.Errorf("newlines and tabs are text: %+v", v)
	}
}

func TestBytesViewPrettyPrintsJSONAndRoundTripsBase64(t *testing.T) {
	v := newBytesView([]byte(`{"a":1,"b":[true,null]}`), false)
	if !v.Pretty || !strings.Contains(v.Text, "\n  \"a\": 1") {
		t.Errorf("JSON should be re-indented: %+v", v)
	}
	raw := []byte{0, 1, 2, 250, 251, 252}
	got, err := base64.StdEncoding.DecodeString(newBytesView(raw, false).B64)
	if err != nil || string(got) != string(raw) {
		t.Errorf("base64 must reproduce the exact bytes: %v %v", got, err)
	}
}

func TestBytesViewBoundsWhatItShows(t *testing.T) {
	big := []byte(strings.Repeat("é", displayLimit)) // 2 bytes per rune: a cut can land mid-rune.
	v := newBytesView(big, false)
	if !v.Truncated || len(v.Text) > displayLimit {
		t.Fatalf("a large value must be truncated for display (text %d bytes, truncated=%v)", len(v.Text), v.Truncated)
	}
	if strings.ContainsRune(v.Text, '\uFFFD') {
		t.Error("truncation split a multi-byte character")
	}
	if v.B64 != "" {
		t.Error("a value past the resend limit must not be embedded in the page")
	}
	if v.Size != len(big) {
		t.Errorf("Size must be the real size, got %d", v.Size)
	}
}

func TestOneLineIsRuneSafeAndBounded(t *testing.T) {
	if got := oneLine("a\nb\tc", 10); got != "a b c" {
		t.Errorf("whitespace should collapse: %q", got)
	}
	got := oneLine(strings.Repeat("日本語", 100), 50)
	if r := []rune(got); len(r) != 51 || r[50] != '…' {
		t.Errorf("want 50 runes then an ellipsis, got %d runes", len(r))
	}
	if strings.ContainsRune(got, '\uFFFD') {
		t.Error("the preview split a rune")
	}
}

func TestHexDump(t *testing.T) {
	d, trunc := hexDump([]byte("Hello, hex dump!XYZ"), 64)
	if trunc {
		t.Error("nothing was cut")
	}
	lines := strings.Split(strings.TrimRight(d, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("19 bytes should be 2 rows, got %d:\n%s", len(lines), d)
	}
	if !strings.HasPrefix(lines[0], "00000000  48 65 6c 6c 6f 2c 20 68  65 78 20 64 75 6d 70 21") || !strings.HasSuffix(lines[0], "|Hello, hex dump!|") {
		t.Errorf("first row: %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "00000010  58 59 5a") || !strings.HasSuffix(lines[1], "|XYZ|") {
		t.Errorf("second row: %q", lines[1])
	}
	if _, trunc := hexDump(make([]byte, 100), 32); !trunc {
		t.Error("a cut dump must say so")
	}
}

func TestResendHeadersCarryBinaryAsBase64(t *testing.T) {
	hs := headerViews([]MessageHeader{
		{Key: "trace-id", Value: []byte("abc-1")},
		{Key: "blob", Value: []byte{0xff, 0xfe, 0x00}},
	})
	got := resendHeaders(hs)
	if !strings.Contains(got, `{"k":"trace-id","v":"abc-1"}`) {
		t.Errorf("text header: %s", got)
	}
	if !strings.Contains(got, `{"k":"blob","v":"//4A","b64":true}`) {
		t.Errorf("binary header should travel as flagged base64: %s", got)
	}
}
