// Package hostguard is the one decision both HTTP listeners of this binary make
// about a request's Host header: the console's and the schema registry's.
//
// The Host guard is what stops DNS rebinding.
//
// Both listeners are, without SASL, unauthenticated, and bound to loopback. A
// malicious web page cannot call them directly (the browser blocks cross-origin
// reads) — unless the attacker controls a DNS name, answers it first with their
// own server and then with 127.0.0.1. The page's origin never changes, so the
// browser considers every later request same-origin, and the attacker can read the
// console and drive its buttons, or read, register and delete schemas.
// Origin checking does not help: Origin and Host both say "evil.example".
//
// The one thing an attacker cannot do is make the browser send a Host header
// naming something they do not control the DNS for. So a listener answers only
// to hostnames it expects. An IP literal can never be a rebinding name (there is
// no DNS lookup to subvert), which is why those are always allowed and why
// reaching a listener by LAN address keeps working.
package hostguard

import (
	"net"
	"strings"
)

// Allowed reports whether a request's Host header names somewhere this process
// should answer: an IP literal, localhost or a name under it, a name in the shared
// .doze zone, or one of extra. extra holds the additional hostnames the caller
// allows (the operator's --console-allow-host, the advertised host); "*" in it
// disables the check.
//
// Only digits after the last colon are a port. Anything else there is part of
// what the header names ("localhost:pw@evil.example" names evil.example, not
// localhost), so the whole text is judged, and refused.
//
// Brackets are the pair an IPv6 literal is written in ("[::1]", "[::1]:8081"),
// around the whole host. A bracket anywhere else is part of the text, which is
// then nobody's name ("]dev.example.com" is not the listed dev.example.com).
//
//fuzz:parser FuzzHostGuard
func Allowed(hostport string, extra []string) bool {
	host := hostport
	if h, port, err := net.SplitHostPort(hostport); err == nil && strings.Trim(port, "0123456789") == "" {
		host = h // SplitHostPort took the pair of brackets off, and accepts no other.
	} else if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "" {
		return false
	}
	switch {
	case net.ParseIP(host) != nil:
		return true
	case host == "localhost", strings.HasSuffix(host, ".localhost"):
		return true
	case host == "doze", strings.HasSuffix(host, ".doze"):
		return true // the shared .doze zone this family of tools publishes names in.
	}
	// What is left with a colon or a bracket in it is not a host name, so it is not
	// one the caller listed, whatever the list holds.
	named := !strings.ContainsAny(host, ":[]")
	for _, a := range extra {
		a = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(a)), ".")
		if a == "*" || (named && a == host) {
			return true
		}
	}
	return false
}

// Name is the host of a Host header without its port, for a message that tells the
// operator what to allow. A header with no port (or one that is not a port) is
// returned whole.
func Name(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

// Quote is s in double quotes with the characters that could end the quotation, or
// start a new line in a plain-text answer, taken out: a refusal repeats a header the
// other side chose.
func Quote(s string) string {
	return `"` + strings.NewReplacer(`"`, "", "\n", "", "\r", "").Replace(s) + `"`
}
