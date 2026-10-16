package console

import (
	"net/http"

	"github.com/doze-dev/doze-kafka/internal/hostguard"
)

// The Host guard is what stops DNS rebinding: the console is an unauthenticated
// page that acts with the broker's own credentials, so it answers only to
// hostnames it expects. The decision, and why it is the right one, is
// internal/hostguard's: the schema registry's listener makes the same one.

// hostAllowed reports whether a request's Host header names somewhere this
// console should answer. extra holds additional hostnames the operator allowed
// (--console-allow-host); "*" disables the check.
func hostAllowed(hostport string, extra []string) bool {
	return hostguard.Allowed(hostport, extra)
}

func refuseHost(w http.ResponseWriter, host string) {
	http.Error(w, "this console does not answer to the host name "+hostguard.Quote(host)+".\n"+
		"If that is a name you chose, start doze-kafka with --console-allow-host="+hostguard.Name(host)+".\n", http.StatusForbidden)
}
