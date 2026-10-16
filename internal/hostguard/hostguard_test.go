package hostguard

import "testing"

// The decision both listeners share. The console's own tests hold the same tables
// through its wrapper (console/hostguard_test.go); these hold them where the code is.
func TestAllowed(t *testing.T) {
	for _, h := range []string{
		"127.0.0.1:8081", "127.0.0.1", "[::1]:8081", "[::1]", "192.168.1.20:8081", "10.0.0.5",
		"localhost:8081", "LOCALHOST", "localhost.", "app.localhost:8081",
		"kafka.doze", "schema.shop.doze", "SCHEMA.Shop.Doze.:8081", "events.demo.doze:9092", "doze",
		"localhost:", "[::1]:0", "127.0.0.1:65535",
	} {
		if !Allowed(h, nil) {
			t.Errorf("Allowed(%q) = false, want true", h)
		}
	}
	// The shapes a rebinding page, or a lookalike, would present.
	for _, h := range []string{
		"", "evil.example", "evil.example:8081", "127.0.0.1.evil.example", "localhost.evil.example",
		"schema.shop.doze.evil.example", "kafka.doze.evil.example:8081", "notdoze", "evil-doze", "xlocalhost", "[evil]:80",
		"localhost:pw@evil.example", "evil.example:8081@localhost", "localhost:8081@evil.example", "[::1]:80@evil.example",
		"localhost:evil.example", "127.0.0.1:80 evil.example", "kafka.doze:x", "localhost:80a", "localhost: 80", "localhost:-1",
		"host.docker.internal:8081", "EVIL.EXAMPLE.", ":", "[]", ".",
	} {
		if Allowed(h, nil) {
			t.Errorf("Allowed(%q) = true, want false", h)
		}
	}
}

func TestTheCallersListAddsExactNames(t *testing.T) {
	extra := []string{"host.docker.internal", " Dev.Example.com. "}
	for _, h := range []string{"host.docker.internal:8081", "HOST.DOCKER.INTERNAL", "host.docker.internal.", "dev.example.com:9093", "127.0.0.1:1"} {
		if !Allowed(h, extra) {
			t.Errorf("Allowed(%q, %q) = false, want true", h, extra)
		}
	}
	for _, h := range []string{
		"other.example.com", "x.host.docker.internal", "host.docker.internal.evil.example", "host.docker.internal:8081@evil.example",
		"dev.example.com.evil.example", "", "*",
	} {
		if Allowed(h, extra) {
			t.Errorf("Allowed(%q, %q) = true, want false", h, extra)
		}
	}
	// "*" is the caller turning the guard off: every name, never the empty header.
	if !Allowed("anything.example:8081", []string{"a.example", "*"}) {
		t.Error(`"*" in the list should allow any name`)
	}
	if Allowed("", []string{"*"}) {
		t.Error(`an empty Host header is refused even with "*"`)
	}
	// A wildcard pattern is not a feature: only the whole "*" means anything.
	if Allowed("a.example.com", []string{"*.example.com"}) {
		t.Error(`"*.example.com" is an exact name, not a pattern`)
	}
}

func TestNameAndQuote(t *testing.T) {
	for in, want := range map[string]string{
		"evil.example:8081": "evil.example", "evil.example": "evil.example", "[::1]:80": "::1",
		"a:b:c": "a:b:c", "": "",
	} {
		if got := Name(in); got != want {
			t.Errorf("Name(%q) = %q, want %q", in, got, want)
		}
	}
	if got := Quote("a\"b\r\nc"); got != `"abc"` {
		t.Errorf("Quote = %s, want the quote and the line break gone", got)
	}
}

// Found by FuzzHostGuard (Host "]:" with ":" on the caller's list): every bracket
// at either end was dropped, wherever it stood, so a header that is not a name at
// all was read as one. Brackets mean something only as a pair around the whole
// host, which is how an IPv6 literal is written; a stray one is part of the text,
// and the text is then no name anybody listed.
func TestBracketsAreOnlyAPairAroundTheWholeHost(t *testing.T) {
	extra := []string{"dev.example.com", ":", "a:b"}
	for _, h := range []string{
		"]:", "[:", "]dev.example.com", "dev.example.com]", "[dev.example.com", "]dev.example.com[", "]]dev.example.com",
		"localhost]", "[localhost", "]localhost[", "[[::1]]", "]::1[", "::1]", "[127.0.0.1", "kafka.doze]", "[[localhost]]:80",
	} {
		if Allowed(h, extra) {
			t.Errorf("Allowed(%q, %q) = true, want false", h, extra)
		}
	}
	// The pair, with or without a port, is still the host inside it.
	for _, h := range []string{"[::1]", "[::1]:80", "[localhost]", "[localhost]:80", "[dev.example.com]:1", "[127.0.0.1]", "[kafka.doze]:"} {
		if !Allowed(h, extra) {
			t.Errorf("Allowed(%q, %q) = false, want true", h, extra)
		}
	}
}

// Found by FuzzHostGuard's next run (Host "[" with "[" on the list): an entry of the
// caller's list that holds a colon or a bracket is no host name, and no header is
// let in for matching one.
func TestAListedEntryThatIsNoHostNameAdmitsNothing(t *testing.T) {
	for _, h := range []string{"[", "]", ":", "a:b", "[a:b]", "x]y", "[x", "a:b:c"} {
		if Allowed(h, []string{h, "[", "]", ":", "a:b", "x]y", "[x", "a:b:c"}) {
			t.Errorf("Allowed(%q) = true through a listed entry that is not a host name", h)
		}
	}
	// "*" is still everything.
	if !Allowed("x]y", []string{"*"}) {
		t.Error(`"*" should allow any text`)
	}
}
