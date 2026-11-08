package console

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// logServer answers GET /_doze/requests with two requests, newest first, and the cursor
// given; it counts the asks and keeps the last query.
func logServer(t *testing.T, next uint64) (*registryDock, *atomic.Int32, *atomic.Value) {
	t.Helper()
	var asks atomic.Int32
	var last atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asks.Add(1)
		last.Store(r.URL.Query())
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"requests":[`+
			`{"seq":7,"at":"2026-11-01T10:00:00Z","method":"GET","path":"/subjects","status":200,"tookNanos":1000000,"user":"","from":"10.0.0.1"},`+
			`{"seq":6,"at":"2026-11-01T09:59:00Z","method":"POST","path":"/subjects/x/versions","status":409,"tookNanos":2000000,"user":"alice","from":"10.0.0.2"}],"next":%d}`, next)
	}))
	t.Cleanup(srv.Close)
	return &registryDock{rc: newRegistryClient(srv.URL, "", "", nil)}, &asks, &last
}

// The registry's log is read newest first: the row is the request, by its user or else where
// it came from, and the cursor onward is the log's own.
func TestTheRegistryDockNamesWhoAskedAndPagesOnByTheLogsCursor(t *testing.T) {
	d, _, _ := logServer(t, 5)
	rows, next := d.DockEvents(DockFilter{Registry: true}, 8, "")
	if len(rows) != 2 || next != "5" {
		t.Fatalf("rows %d next %q", len(rows), next)
	}
	if rows[0].Client != "10.0.0.1" || rows[1].Client != "alice" {
		t.Errorf("clients = %q, %q, want the address where there is no user, the user where there is", rows[0].Client, rows[1].Client)
	}
	if rows[0].ID != "7" || rows[0].Request != "GET /subjects" || rows[1].Status != 409 {
		t.Errorf("rows = %+v", rows)
	}

	d, _, _ = logServer(t, 0)
	if _, next := d.DockEvents(DockFilter{Registry: true}, 8, ""); next != "" {
		t.Errorf("the end of the log has a next page: %q", next)
	}
}

// A cursor that is a sequence number of the log is asked of the log, from there on; one that is
// not has nothing after it, and the log is not asked.
func TestTheRegistryDockAsksTheLogOnlyFromACursorOfItsOwn(t *testing.T) {
	d, asks, last := logServer(t, 5)
	if rows, _ := d.DockEvents(DockFilter{Registry: true}, 8, "5"); len(rows) != 2 {
		t.Errorf("a cursor of the log gave %d rows, want 2", len(rows))
	}
	if q, _ := last.Load().(interface{ Get(string) string }); q == nil || q.Get("before") != "5" {
		t.Errorf("the log was asked %v, want before=5", last.Load())
	}
	asked := asks.Load()
	for _, bad := range []string{"0", "abc", "-3"} {
		if rows, next := d.DockEvents(DockFilter{Registry: true}, 8, bad); rows != nil || next != "" {
			t.Errorf("cursor %q: rows %v next %q, want nothing", bad, rows, next)
		}
	}
	if got := asks.Load(); got != asked {
		t.Errorf("the log was asked %d times for cursors that are not its own", got-asked)
	}
}

// A registry row is coloured by its status: success, a refusal, a failure of the registry's own.
func TestARegistryRowsStatusClassSplitsAt300And500(t *testing.T) {
	for status, want := range map[int]string{0: "ok", 200: "ok", 299: "ok", 300: "deny", 409: "deny", 499: "deny", 500: "bad", 503: "bad"} {
		if got := (DockEvent{Status: status}).StatusClass(); got != want {
			t.Errorf("status %d is %q, want %q", status, got, want)
		}
	}
}
