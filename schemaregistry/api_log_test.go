package schemaregistry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingHandler keeps the records a logger was given.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) take() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.records
	h.records = nil
	return out
}

func attrs(r slog.Record) map[string]string {
	out := map[string]string{}
	r.Attrs(func(a slog.Attr) bool { out[a.Key] = a.Value.String(); return true })
	return out
}

func ask(api http.Handler, method, path, body, user, pass string) int {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	return rec.Code
}

// Every request leaves one record: a refused sign-in and an incompatible schema at
// info, a store failure as a warning, the rest at debug, each with the method, the
// path, the status, the user and how long it took, and never the password. (SCH-9)
func TestErrorsAreLogged(t *testing.T) {
	h := &recordingHandler{}
	st := NewMemoryStorage()
	r, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(r, Options{AllowedHosts: testHosts, Logger: slog.New(h),
		Auth: func(u, p string) bool { return u == "alice" && p == "secret" }})

	for _, c := range []struct {
		method, path, body, pass string
		status                   int
		level                    slog.Level
	}{
		{"GET", "/subjects", "", "nope-not-it", 401, slog.LevelInfo},
		{"POST", "/subjects/x/versions", schemaBody(avroUser1), "secret", 200, slog.LevelDebug},
		{"POST", "/subjects/x/versions", schemaBody(avroUserBad), "secret", 409, slog.LevelInfo},
		{"GET", "/subjects/none/versions", "", "secret", 404, slog.LevelDebug},
	} {
		if got := ask(api, c.method, c.path, c.body, "alice", c.pass); got != c.status {
			t.Fatalf("%s %s: %d, want %d", c.method, c.path, got, c.status)
		}
		recs := h.take()
		if len(recs) != 1 {
			t.Fatalf("%s %s: %d records, want 1", c.method, c.path, len(recs))
		}
		a := attrs(recs[0])
		if recs[0].Level != c.level || a["method"] != c.method || a["path"] != c.path || a["status"] != strconv.Itoa(c.status) || a["user"] != "alice" || a["took"] == "" {
			t.Errorf("%s %s: record %v %v", c.method, c.path, recs[0].Level, a)
		}
		for _, v := range a {
			if strings.Contains(v, "nope-not-it") || strings.Contains(v, "secret") {
				t.Errorf("the record carries the password: %v", a)
			}
		}
	}

	// A store that fails: 500, a warning.
	r2, _ := Open(&failingStore{MemoryStorage: NewMemoryStorage(), err: errors.New("disk gone")})
	api2 := NewAPI(r2, Options{AllowedHosts: testHosts, Logger: slog.New(h)})
	if got := ask(api2, "POST", "/subjects/y/versions", schemaBody(avroUser1), "", ""); got != 500 {
		t.Fatalf("register against a failing store: %d, want 500", got)
	}
	if recs := h.take(); len(recs) != 1 || recs[0].Level != slog.LevelWarn {
		t.Errorf("a 500 left %d records, want one warning: %v", len(recs), recs)
	}
}

// Five wrong passwords in a row from one address are answered at once; the sixth and
// later wait, longer each time, and a right password clears the count. (SCH-38)
func TestRepeatedSignInFailuresAreSlowed(t *testing.T) {
	r, _ := openReg(t)
	api := NewAPI(r, Options{AllowedHosts: testHosts, Auth: func(u, p string) bool { return p == "secret" }})
	var waits []time.Duration
	api.sleep = func(d time.Duration) { waits = append(waits, d) }
	for range 5 {
		ask(api, "GET", "/subjects", "", "alice", "wrong")
	}
	if len(waits) != 0 {
		t.Fatalf("the first five failures waited %v", waits)
	}
	ask(api, "GET", "/subjects", "", "alice", "wrong")
	ask(api, "GET", "/subjects", "", "alice", "wrong")
	if len(waits) != 2 || waits[0] != 100*time.Millisecond || waits[1] != 200*time.Millisecond {
		t.Fatalf("the sixth and seventh failures waited %v, want 100ms then 200ms", waits)
	}
	for range 40 {
		ask(api, "GET", "/subjects", "", "alice", "wrong")
	}
	if last := waits[len(waits)-1]; last != 2*time.Second {
		t.Errorf("after 47 failures the wait is %s, want the 2 s cap", last)
	}
	if got := ask(api, "GET", "/subjects", "", "alice", "secret"); got != 200 {
		t.Fatalf("the right password: %d", got)
	}
	waits = nil
	ask(api, "GET", "/subjects", "", "alice", "wrong")
	if len(waits) != 0 {
		t.Errorf("after a success the next failure waited %v, want none", waits)
	}
}
