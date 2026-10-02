package console

import (
	"net"
	"net/http"
	"strings"
)

// The Host guard is what stops DNS rebinding.
//
// The console is an unauthenticated page that acts with the broker's own
// credentials, bound to loopback. A malicious web page cannot call it directly
// (the browser blocks cross-origin reads) — unless the attacker controls a DNS
// name, answers it first with their own server and then with 127.0.0.1. The
// page's origin never changes, so the browser considers every later request
// same-origin, and the attacker can read the console and drive its buttons.
// Origin checking does not help: Origin and Host both say "evil.example".
//
// The one thing an attacker cannot do is make the browser send a Host header
// naming something they do not control the DNS for. So the console answers only
// to hostnames it expects. An IP literal can never be a rebinding name (there is
// no DNS lookup to subvert), which is why those are always allowed and why
// reaching the console by LAN address keeps working.

// hostAllowed reports whether a request's Host header names somewhere this
// console should answer. extra holds additional hostnames the operator allowed
// (--console-allow-host); "*" disables the check.
func hostAllowed(hostport string, extra []string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.ToLower(strings.Trim(host, "[]")), ".")
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
	for _, a := range extra {
		a = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(a)), ".")
		if a == "*" || a == host {
			return true
		}
	}
	return false
}

func refuseHost(w http.ResponseWriter, host string) {
	h := host
	if hh, _, err := net.SplitHostPort(host); err == nil {
		h = hh
	}
	http.Error(w, "this console does not answer to the host name "+quote(host)+".\n"+
		"If that is a name you chose, start doze-kafka with --console-allow-host="+h+".\n", http.StatusForbidden)
}

func quote(s string) string {
	return `"` + strings.NewReplacer(`"`, "", "\n", "", "\r", "").Replace(s) + `"`
}
