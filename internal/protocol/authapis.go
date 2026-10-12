package protocol

import (
	"sync/atomic"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// The APIs whose requests carry a credential or something a credential can be worked
// out from: the SASL exchange, the SCRAM credential APIs and delegation tokens. What
// the broker says about such a request (a log line, a row of the Traffic inspector) is
// held to a list of what may be said, instead of to a list of what may not.

// IsAuthAPI says a request of this API can carry a credential.
func IsAuthAPI(key APIKey) bool {
	switch key {
	case kmsg.SASLHandshake, kmsg.SASLAuthenticate,
		kmsg.CreateDelegationToken, kmsg.RenewDelegationToken, kmsg.ExpireDelegationToken, kmsg.DescribeDelegationToken,
		kmsg.DescribeUserSCRAMCredentials, kmsg.AlterUserSCRAMCredentials:
		return true
	}
	return false
}

// routeHook is the test seam of Conn.route: see HookRouteForTest.
var routeHook atomic.Pointer[func(*Request)]

// HookRouteForTest makes f run on the connection's goroutine before every request
// is handed to its handler, and returns a func that takes it out again. It is for
// tests outside this package that need a handler to fail in a way no request can make
// it fail (a panic): what f does is done where the handler would have done it.
func HookRouteForTest(f func(*Request)) (restore func()) {
	old := routeHook.Swap(&f)
	return func() { routeHook.Store(old) }
}
