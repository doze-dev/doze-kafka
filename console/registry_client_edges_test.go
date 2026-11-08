package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeRegistry answers the few routes these tests ask, and keeps the request line of
// each (the path with its query, as the client sent it).
type fakeRegistry struct {
	mu       sync.Mutex
	asked    []string
	overview string // "" answers 404, as another registry would.
	subjects string
}

func (f *fakeRegistry) serve(t *testing.T) *registryClient {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.asked = append(f.asked, r.RequestURI)
		f.mu.Unlock()
		switch {
		case r.URL.Path == "/_doze/overview" && f.overview != "":
			w.Write([]byte(f.overview))
		case r.URL.Path == "/subjects" && f.subjects != "":
			w.Write([]byte(f.subjects))
		case strings.HasSuffix(r.URL.Path, "/check"):
			w.Write([]byte(`{"compatible":true}`))
		case strings.Contains(r.URL.Path, "/versions/"):
			w.Write([]byte(`{"version":1}`))
		default:
			w.WriteHeader(404)
			w.Write([]byte(`{"error_code":40401,"message":"not here"}`))
		}
	}))
	t.Cleanup(ts.Close)
	return newRegistryClient(ts.URL, "", "", nil)
}

func (f *fakeRegistry) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.asked[len(f.asked)-1]
}

// A check with no options asks the bare route, and each option rides on the query.
func TestACheckWithNoOptionsAsksTheBareRoute(t *testing.T) {
	f := &fakeRegistry{}
	rc := f.serve(t)
	ctx := context.Background()
	if _, err := rc.CheckAt(ctx, "orders-value", schemaBody{Schema: "{}"}, "", "", false); err != nil {
		t.Fatal(err)
	}
	if got := f.last(); got != "/_doze/subjects/orders-value/check" {
		t.Errorf("no options asked %q", got)
	}
	if _, err := rc.CheckAt(ctx, "orders-value", schemaBody{Schema: "{}"}, "FULL", "2", true); err != nil {
		t.Fatal(err)
	}
	if got := f.last(); got != "/_doze/subjects/orders-value/check?level=FULL&normalize=true&version=2" {
		t.Errorf("all options asked %q", got)
	}
}

// A version of 0 (or less) asks for the latest; a number asks for that version.
func TestAVersionInfoOfZeroAsksForTheLatest(t *testing.T) {
	f := &fakeRegistry{}
	rc := f.serve(t)
	ctx := context.Background()
	for version, want := range map[int]string{0: "latest", -1: "latest", 1: "1", 7: "7"} {
		if _, ok := rc.VersionInfo(ctx, "orders-value", version); !ok {
			t.Errorf("version %d: not answered", version)
		}
		if got := f.last(); got != "/_doze/subjects/orders-value/versions/"+want {
			t.Errorf("version %d asked %q, want .../versions/%s", version, got, want)
		}
	}
}

// A subject named for no topic is a record-named one, in the overview, or in the plain
// list of subjects when the registry has no overview. A soft-deleted one does not count.
func TestRecordNamedSubjectsAreFoundInTheOverviewOrTheSubjectList(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, overview, subjects string
		want                     bool
	}{
		{"overview with only topic subjects", `{"subjects":[{"subject":"a-value"},{"subject":"b-key"}]}`, "", false},
		{"overview with a record-named subject", `{"subjects":[{"subject":"a-value"},{"subject":"com.acme.Order"}]}`, "", true},
		{"overview whose record-named subject is deleted", `{"subjects":[{"subject":"a-value"},{"subject":"com.acme.Order","deleted":true}]}`, "", false},
		{"overview with no subjects", `{"subjects":[]}`, "", false},
		{"no overview, only topic subjects", "", `["a-value","b-key"]`, false},
		{"no overview, a record-named subject", "", `["a-value","com.acme.Order"]`, true},
		{"no overview, no subjects", "", `[]`, false},
	} {
		f := &fakeRegistry{overview: tc.overview, subjects: tc.subjects}
		if got := f.serve(t).hasRecordNamedSubjects(ctx); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
