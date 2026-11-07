package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// SCH-8: the subject list is drawn from doze-kafka's overview, which carries no schema
// text, and not from GET /schemas, which sends every version's text on every page. A
// registry without the overview (Confluent's) still gets a list, through /schemas.
func TestSubjectListIsDrawnWithoutEverySchemasText(t *testing.T) {
	for _, own := range []bool{true, false} {
		reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
		if err != nil {
			t.Fatal(err)
		}
		api := schemaregistry.NewAPI(reg, schemaregistry.Options{})
		var mu sync.Mutex
		var paths []string
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.Method+" "+r.URL.Path)
			mu.Unlock()
			if !own && strings.HasPrefix(r.URL.Path, "/_doze/") {
				http.Error(w, `{"error_code":404,"message":"HTTP 404 Not Found"}`, 404)
				return
			}
			api.ServeHTTP(w, r)
		}))
		t.Cleanup(ts.Close)
		c, _ := newTestConsoleAddr(t, func(o *Options) { o.RegistryURL = ts.URL })
		register(t, c, "orders-value", userV1)
		register(t, c, "orders-value", userV2)
		register(t, c, "audit-value", userV1)
		mu.Lock()
		paths = nil
		mu.Unlock()

		for _, p := range []string{"/_console/schemas", "/_console/schemas/orders-value"} {
			b := readable(req(t, c, "GET", p, nil).Body.String())
			if !strings.Contains(b, "audit-value") || !strings.Contains(b, "orders-value") {
				t.Errorf("own=%v %s: the list lacks a subject", own, p)
			}
		}
		if b := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String()); !strings.Contains(b, "2 subjects, 2 schemas, ids 1 to 2.") {
			t.Errorf("own=%v: the list's foot: %s", own, between(b, "subjects", "."))
		}
		mu.Lock()
		asked := strings.Join(paths, "\n")
		mu.Unlock()
		if got := strings.Contains(asked, "GET /schemas\n") || strings.HasSuffix(asked, "GET /schemas"); got == own {
			t.Errorf("own=%v: GET /schemas asked = %v:\n%s", own, got, asked)
		}
	}
}
