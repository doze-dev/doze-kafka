package console

import (
	"bytes"
	"strings"
	"testing"
)

// The inspector carries a value for "Send again" up to resendLimit and shows it up
// to displayLimit: the byte at the limit is inside, the next one is not.
func TestBytesViewLimitsAreInclusive(t *testing.T) {
	at := newBytesView(bytes.Repeat([]byte("x"), resendLimit), false)
	if at.B64 == "" || at.Raw == "" {
		t.Errorf("a value of exactly resendLimit bytes is not carried for resend (b64 %d, raw %d)", len(at.B64), len(at.Raw))
	}
	over := newBytesView(bytes.Repeat([]byte("x"), resendLimit+1), false)
	if over.B64 != "" || over.Raw != "" {
		t.Errorf("a value over resendLimit is carried for resend")
	}
	full := newBytesView(bytes.Repeat([]byte("x"), displayLimit), false)
	if full.Truncated || len(full.Text) != displayLimit {
		t.Errorf("a value of exactly displayLimit bytes: truncated=%v, shown %d", full.Truncated, len(full.Text))
	}
	cut := newBytesView(bytes.Repeat([]byte("x"), displayLimit+1), false)
	if !cut.Truncated || len(cut.Text) != displayLimit {
		t.Errorf("a value of displayLimit+1 bytes: truncated=%v, shown %d", cut.Truncated, len(cut.Text))
	}
}

// A binary value's preview shows twelve bytes and an ellipsis only when there is more.
func TestBinaryPreviewEllipsis(t *testing.T) {
	b := func(n int) []byte { return bytes.Repeat([]byte{0}, n) }
	if p := binaryPreview(b(5)); strings.Contains(p, "…") {
		t.Errorf("5 bytes: %q", p)
	}
	if p := binaryPreview(b(12)); strings.Contains(p, "…") || strings.Count(p, "00") != 12 {
		t.Errorf("12 bytes: %q", p)
	}
	if p := binaryPreview(b(13)); !strings.HasSuffix(p, " …") || strings.Count(p, "00") != 12 {
		t.Errorf("13 bytes: %q", p)
	}
}

// More than one byte in twenty being a control character makes a value binary;
// exactly one in twenty does not.
func TestControlCharacterShare(t *testing.T) {
	if !looksBinary(bytes.Repeat([]byte{1}, 100)) {
		t.Error("a value of control characters reads as text")
	}
	if !looksBinary([]byte("ab\x01\x01cd")) {
		t.Error("2 control characters in 6 read as text")
	}
	if looksBinary([]byte(strings.Repeat("a", 19) + "\x01")) {
		t.Error("exactly 5% control characters reads as binary")
	}
	if !looksBinary([]byte(strings.Repeat("a", 18) + "\x01\x01")) {
		t.Error("10% control characters reads as text")
	}
	if looksBinary([]byte("a\tb\r\nc")) {
		t.Error("tabs and newlines count as control characters")
	}
	if !looksBinary([]byte("ab\x7fcd")) {
		t.Error("DEL reads as text")
	}
}

// A sample cut inside a character is judged without the partial character; a
// value that merely is 4096 bytes long and ends in one is not valid text.
func TestLooksBinarySampleCut(t *testing.T) {
	long := append([]byte(strings.Repeat("a", binarySample)), 'b')
	if looksBinarySample(long) {
		t.Error("letters read as binary")
	}
	cut := append([]byte(strings.Repeat("a", binarySample-1)), 0xc3, 0xa9) // 4095 letters then é: the cut splits the é.
	if looksBinarySample(cut) {
		t.Error("a cut through a final é makes the value binary")
	}
	exact := append([]byte(strings.Repeat("a", binarySample-1)), 0xc3) // 4096 bytes ending in half a character.
	if !looksBinarySample(exact) {
		t.Error("a whole value of 4096 bytes ending mid-character reads as text")
	}
}

func TestTrimToRuneBoundary(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"ab\xc3", "ab"},
		{"\xc3", ""},
		{"ab\xf0\x9f\x98", "ab"},
		{"ab\xf0", "ab"},
		{"ab\xc3\xa9", "ab\xc3\xa9"},
		{"ab", "ab"},
		{"ab\xff", "ab\xff"},
	} {
		if got := string(trimToRuneBoundary([]byte(tc.in))); got != tc.want {
			t.Errorf("trimToRuneBoundary(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestHexDumpLayout(t *testing.T) {
	d, cut := hexDump([]byte("ABCDEFGHIJKLMNOP"), 16)
	if cut {
		t.Error("a dump of exactly the limit is cut")
	}
	want := "00000000  41 42 43 44 45 46 47 48  49 4a 4b 4c 4d 4e 4f 50  |ABCDEFGHIJKLMNOP|\n"
	if d != want {
		t.Errorf("16 bytes:\n got %q\nwant %q", d, want)
	}
	d, _ = hexDump([]byte("AB"), 16)
	want = "00000000  41 42 " + strings.Repeat("   ", 14) + " " + " |AB|\n"
	if d != want {
		t.Errorf("2 bytes:\n got %q\nwant %q", d, want)
	}
	d, cut = hexDump([]byte("ABCDEFGHIJKLMNOPQ"), 16)
	if !cut || strings.Count(d, "\n") != 1 {
		t.Errorf("17 bytes with limit 16: cut=%v, %d rows", cut, strings.Count(d, "\n"))
	}
	d, _ = hexDump([]byte{0x1f, 0x20, 0x7e, 0x7f}, 16)
	if !strings.HasSuffix(d, "|. ~.|\n") {
		t.Errorf("gutter: %q", d)
	}
}

func TestIndentJSONEmptyContainersAndLimit(t *testing.T) {
	got, ok := indentJSON([]byte(`{"a": { }, "b": [
 ]}`))
	if !ok || got != "{\n  \"a\": {},\n  \"b\": []\n}" {
		t.Errorf("empty containers with whitespace: %v %q", ok, got)
	}
	// A value cut off inside an open container does not run off the end.
	var buf bytes.Buffer
	if err := indentTo(&limitedWriter{w: &buf, left: 100}, []byte("[  ")); err != nil || !strings.HasPrefix(buf.String(), "[") {
		t.Errorf("cut value: %q %v", buf.String(), err)
	}
	// The writer takes exactly what it has left, and refuses the byte after.
	lw := &limitedWriter{w: &bytes.Buffer{}, left: 3}
	if n, err := lw.Write([]byte("abc")); n != 3 || err != nil {
		t.Errorf("a write of exactly what is left: %d %v", n, err)
	}
	if _, err := lw.Write([]byte("d")); err == nil {
		t.Error("a write past the limit is accepted")
	}
}
