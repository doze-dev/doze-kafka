package hostguard

import (
	"net/netip"
	"strings"
	"testing"
)

// The Host header is the one thing between a DNS-rebinding page and a console
// that acts with the broker's credentials, or a schema registry (hostguard.go). Allowed reads it,
// so it reads bytes a hostile page's browser, or anything else that can reach the
// port, chose. Beyond "no panic":
//
//  1. Whatever is allowed is, once the port, the brackets and the root dot are
//     off, an IP literal, localhost, a name under .localhost or .doze, or a name
//     the operator listed. Nothing else gets in.
//  2. A foreign DNS name is refused whatever is put around it: a port, another
//     case, the root dot, brackets, userinfo, white space, a trusted name in front.
//  3. A port, the root dot, and upper case do not change the answer.

// fuzzForeignTLDs are endings no trusted name has. Each holds a letter that is not
// a hex digit, so nothing ending in one is an IP literal either.
var fuzzForeignTLDs = []string{"example", "com", "net", "evil", "org", "io", "xyz", "test"}

// fuzzForeignName makes a DNS name the guard has no reason to trust out of any text.
func fuzzForeignName(raw string, pick uint16) string {
	var b strings.Builder
	for _, c := range []byte(strings.ToLower(raw)) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			b.WriteByte(c)
		case c == '.':
			if s := b.String(); s != "" && !strings.HasSuffix(s, ".") {
				b.WriteByte(c)
			}
		}
	}
	name := b.String()
	if name != "" && !strings.HasSuffix(name, ".") {
		name += "."
	}
	return name + fuzzForeignTLDs[int(pick)%len(fuzzForeignTLDs)]
}

// fuzzHostShape is property 1's own reading of a Host header, written without the
// guard's helpers: a port is a colon and digits after a name with no other colon,
// or after a closing bracket.
func fuzzHostShape(hostport string, extra []string) bool {
	h := hostport
	if i := strings.LastIndexByte(h, ':'); i >= 0 && strings.Trim(h[i+1:], "0123456789") == "" {
		if name := h[:i]; !strings.Contains(name, ":") || (strings.HasPrefix(name, "[") && strings.HasSuffix(name, "]")) {
			h = name
		}
	}
	h = strings.TrimSuffix(strings.ToLower(strings.Trim(h, "[]")), ".")
	if h == "" {
		return false
	}
	if a, err := netip.ParseAddr(h); err == nil && a.Zone() == "" {
		return true
	}
	if h == "localhost" || strings.HasSuffix(h, ".localhost") || h == "doze" || strings.HasSuffix(h, ".doze") {
		return true
	}
	for _, e := range extra {
		if strings.TrimSuffix(strings.ToLower(strings.TrimSpace(e)), ".") == h {
			return true
		}
	}
	return false
}

func fuzzASCIIUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

func FuzzHostGuard(f *testing.F) {
	// host: a whole Host header. prefix, name, deco: the pieces of a foreign name in
	// disguise. extra: the operator's --console-allow-host list, comma separated.
	for _, s := range []struct {
		host, prefix, name, extra string
		deco                      uint16
	}{
		// What a browser sends to a console it should reach.
		{"127.0.0.1:9093", "", "evil", "", 0},
		{"localhost:9093", "", "attacker", "", 1},
		{"[::1]:9093", "", "rebind.attacker", "", 2},
		{"kafka.doze", "", "kafka.doze", "", 3},
		{"events.demo.doze:9092", "", "localhost", "dev.example.com", 4},
		{"192.168.1.20:9093", "", "127.0.0.1", "dev.example.com, Staging.Internal.", 5},
		{"dev.example.com:9093", "", "dev.example", "dev.example.com", 6},
		{"LOCALHOST.", "", "LOCALHOST", "", 7},
		{"app.localhost:9093", "", "app.localhost", "", 0x10},
		{"::1", "", "a", "", 0x20},
		{"[::ffff:127.0.0.1]:80", "", "ffff", "", 0x40},
		// What a rebinding page, or a lookalike, presents.
		{"evil.example:9093", "localhost", "evil", "", 0x0100},
		{"127.0.0.1.evil.example", "127.0.0.1", "evil", "", 0x0200},
		{"localhost.evil.example", "localhost", "evil", "", 0x0300},
		{"kafka.doze.evil.example", "kafka.doze", "x", "", 0x0200},
		{"localhost@evil.example", "localhost", "evil", "", 0x0100},
		{"localhost:9093@evil.example", "localhost:9093", "evil", "", 0x0100},
		{"localhost:pw@evil.example", "localhost:pw", "evil", "", 0x0100},
		{"[::1]:80@evil.example", "[::1]:80", "evil", "", 0x0100},
		{"[::1]:evil.example", "[::1]", "evil", "", 0x0300},
		{"localhost:evil.example", "localhost", "evil", "", 0x0300},
		{"127.0.0.1:80 evil.example", "127.0.0.1:80", "evil", "", 0x0600},
		{" localhost", "localhost", "evil", "", 0x0608},
		{"localhost\t", "localhost\t", "evil", "", 0x0010},
		{"localhost\r\nHost: evil.example", "localhost\r\nHost: ", "evil", "", 0},
		{"[evil.example]:80", "", "evil", "", 0x0006},
		{"[localhost]evil.example:80", "[localhost]", "evil", "", 0x0004},
		{"evil.example.", "", "evil", "", 0x0002},
		{"EVIL.EXAMPLE:443", "", "evil", "", 0x0005},
		{"localhost..", "localhost.", "", "", 0x0002},
		{"xlocalhost", "x", "localhost", "", 0},
		{"notdoze", "", "notdoze", "", 0},
		{"evil-doze", "", "evil-doze", "", 0},
		{"doze.", "", "doze", "", 0x0002},
		{"*", "", "star", "*.example.com", 0},
		{"anything.example", "", "anything", "*", 0},
		{"fe80::1%eth0", "fe80::1%", "eth0", "", 0},
		{"0x7f.1", "0x7f.", "1", "", 0},
		{"Kafka.doze", "", "kafka", "Kafka.example", 0},
		{"", "", "", "", 0},
		{":", ":", "", "", 0},
		{"[]", "[]", "", ",", 0},
		{"[", "]", "", "[::1]", 0x0004},
		{":80", "", "", "", 0x0004},
		{"localhost:", "localhost:", "", "", 0},
		{"localhost:99999999999999999999", "", "", "", 0},
		{"a:b:c", "a:b:", "c", "a:b:c", 0},
	} {
		f.Add(s.host, s.prefix, s.name, s.extra, s.deco)
	}

	f.Fuzz(func(t *testing.T, host, prefix, name, extraList string, deco uint16) {
		var extra []string
		if extraList != "" {
			extra = strings.Split(extraList, ",")
		}
		star := false
		for _, e := range extra {
			if strings.TrimSuffix(strings.ToLower(strings.TrimSpace(e)), ".") == "*" {
				star = true
			}
		}
		got := Allowed(host, extra)

		// 1. Only the expected shapes get in ("*" is the operator turning the guard off).
		if !star && got && !fuzzHostShape(host, extra) {
			t.Fatalf("Host %q is allowed (allow list %q) and is not an IP literal, localhost, a .localhost or .doze name, or a listed name", host, extra)
		}
		if star && host != "" && strings.Trim(strings.ToLower(host), "[].") != "" && !strings.Contains(host, ":") && !got {
			t.Fatalf("Host %q is refused though the allow list %q holds *", host, extra)
		}
		// The operator's list adds names; it never takes a built-in one away.
		if Allowed(host, nil) && !got {
			t.Fatalf("Host %q is allowed with no list and refused with %q", host, extra)
		}

		// 3. For a host with no colon or bracket of its own, a port, the root dot and
		// upper case change nothing; nor do brackets with a port after them.
		if !strings.ContainsAny(host, ":[]") {
			for _, same := range []string{host + ":9093", host + ":", "[" + host + "]", "[" + host + "]:443", fuzzASCIIUpper(host)} {
				if Allowed(same, extra) != got {
					t.Fatalf("Host %q allowed=%v, and %q allowed=%v", host, got, same, !got)
				}
			}
			if host != "" && !strings.HasSuffix(host, ".") {
				if Allowed(host+".", extra) != got {
					t.Fatalf("Host %q allowed=%v, and with the root dot allowed=%v", host, got, !got)
				}
			}
		}

		// 2. A foreign name stays refused whatever surrounds it.
		foreign := fuzzForeignName(name, deco>>12)
		for _, e := range extra {
			if strings.Contains(strings.ToLower(e), foreign) {
				return // the fuzzer put the name on the operator's list.
			}
		}
		if star {
			extra = nil
		}
		seps := []string{"", "@", ".", ":", "/", " ", ":@", "\t", "#", "?", "\\", "%00", "\x00", "]", "[", "::"}
		spaces := []string{"", " ", "\t", "\r\n"}
		dressed := prefix + seps[int(deco>>8)%len(seps)] + foreign
		if deco&0x01 != 0 {
			dressed = fuzzASCIIUpper(dressed)
		}
		if deco&0x02 != 0 {
			dressed += "."
		}
		if deco&0x04 != 0 {
			dressed = "[" + dressed + "]"
		}
		switch deco & 0x60 {
		case 0x20:
			dressed += ":9093"
		case 0x40:
			dressed += ":"
		case 0x60:
			dressed += ":0"
		}
		dressed = spaces[int(deco>>3)%len(spaces)] + dressed + spaces[int(deco>>7)%len(spaces)]
		if Allowed(dressed, extra) {
			t.Fatalf("Host %q is allowed (allow list %q): it names the foreign %s", dressed, extra, foreign)
		}
	})
}
