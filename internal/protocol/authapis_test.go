package protocol

import (
	"fmt"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// A handler that panics while it holds a SASL token can have put the token in what it
// panicked with. The log line for it names the value's type and not its text; for every
// other API the text is logged, because it is what says what went wrong.
func TestAPanicWhileHoldingACredentialIsLoggedWithoutItsText(t *testing.T) {
	p := &handlerPanic{value: fmt.Errorf("unexpected token %q", "hunter2")}
	for key, shown := range map[APIKey]bool{
		kmsg.Produce: true, kmsg.Fetch: true, kmsg.Metadata: true, kmsg.ApiVersions: true, kmsg.CreateACLs: true,
		kmsg.SASLHandshake: false, kmsg.SASLAuthenticate: false,
		kmsg.DescribeUserSCRAMCredentials: false, kmsg.AlterUserSCRAMCredentials: false,
		kmsg.CreateDelegationToken: false, kmsg.RenewDelegationToken: false, kmsg.ExpireDelegationToken: false, kmsg.DescribeDelegationToken: false,
	} {
		got := fmt.Sprint(p.logValue(key))
		if strings.Contains(got, "hunter2") != shown {
			t.Errorf("%s: the panic is logged as %q; its text shown = %v, want %v", APIName(key), got, !shown, shown)
		}
		if !shown && !strings.Contains(got, "*fmt.wrapError") && !strings.Contains(got, "*errors.errorString") {
			t.Errorf("%s: the panic is logged as %q, which does not name the value's type", APIName(key), got)
		}
	}
}

// The seam is taken out by what putting it in returned, and an earlier one comes back.
func TestTheRouteHookIsRestored(t *testing.T) {
	if routeHook.Load() != nil {
		t.Fatal("a route hook is set before the test")
	}
	calls := ""
	outer := HookRouteForTest(func(*Request) { calls += "outer," })
	inner := HookRouteForTest(func(*Request) { calls += "inner," })
	(*routeHook.Load())(nil)
	inner()
	(*routeHook.Load())(nil)
	outer()
	if routeHook.Load() != nil || calls != "inner,outer," {
		t.Errorf("after restoring: hook set = %v, calls = %q", routeHook.Load() != nil, calls)
	}
}
