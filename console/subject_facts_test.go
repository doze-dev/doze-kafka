package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/schemaregistry"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newProxiedRegistryConsole is newRegistryConsole with a registry that a test can
// answer for: intercept sees each request first and, returning true, has answered it;
// otherwise the real registry does.
func newProxiedRegistryConsole(t *testing.T, intercept func(w http.ResponseWriter, r *http.Request) bool) (*Console, string) {
	t.Helper()
	reg, err := schemaregistry.Open(schemaregistry.NewMemoryStorage())
	if err != nil {
		t.Fatal(err)
	}
	real := schemaregistry.NewAPI(reg, schemaregistry.Options{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if intercept != nil && intercept(w, r) {
			return
		}
		real.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return newTestConsoleAddr(t, func(o *Options) { o.RegistryURL = ts.URL })
}

// withoutDozeRoutes answers the console's own /_doze/ routes as another registry
// would: not found.
func withoutDozeRoutes(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/_doze/") {
		return false
	}
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(`{"error_code":404,"message":"HTTP 404 Not Found"}`))
	return true
}

func produceFramedTo(t *testing.T, addr, topic string, counts map[int]int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for id, n := range counts {
		for i := 0; i < n; i++ {
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: framedValue(id)}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// What deleting a subject touches is measured: its versions' ids, which of them
// another subject holds too (those survive a permanent delete), the next free id, and
// the messages on the topic it is named for that carry them.
func TestSubjectFactsMeasureTheIdsAndTheMessagesThatCarryThem(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "orders", 1, nil); err != nil {
		t.Fatal(err)
	}
	register(t, c, "orders-value", userV1) // id 1
	register(t, c, "orders-value", userV2) // id 2
	register(t, c, "audit-value", userV1)  // id 1 again: shared
	produceFramedTo(t, addr, "orders", map[int]int{1: 3, 2: 2})

	f := c.subjectFacts(ctx, "orders-value", []int{1, 2}, 2)
	if len(f.IDs) != 2 || f.IDs[0] != 1 || f.IDs[1] != 2 || f.SchemaID != 2 || f.NextID != 3 {
		t.Errorf("ids %v schema id %d next id %d", f.IDs, f.SchemaID, f.NextID)
	}
	if len(f.Shared) != 1 || len(f.Shared[1]) != 1 || f.Shared[1][0] != "audit-value" {
		t.Errorf("shared %v", f.Shared)
	}
	// Id 1 is audit-value's too, so it is not lost with this subject: only id 2's 2
	// messages are counted as the ones the delete strands.
	if !f.Scanned || f.CarryTopic != "orders" || f.Carry != 2 || f.CarryAll != 2 {
		t.Errorf("scanned %v topic %q carry %d carry-all %d", f.Scanned, f.CarryTopic, f.Carry, f.CarryAll)
	}
	if f := c.subjectFacts(ctx, "orders-value", []int{1, 2}, 1); f.SchemaID != 1 || f.Carry != 3 || f.CarryAll != 2 {
		t.Errorf("the shown version is 1: schema id %d carry %d carry-all %d", f.SchemaID, f.Carry, f.CarryAll)
	}

	// A subject named for no topic has nothing to scan; one whose topic is not there
	// cannot be scanned either.
	register(t, c, "com.acme.Thing", userV1)
	if f := c.subjectFacts(ctx, "com.acme.Thing", []int{1}, 1); f.Scanned || f.CarryTopic != "" {
		t.Errorf("a record-named subject was scanned: %+v", f)
	}
	register(t, c, "nowhere-value", userV1)
	if f := c.subjectFacts(ctx, "nowhere-value", []int{1}, 1); f.Scanned {
		t.Errorf("a topic that is not there was scanned: %+v", f)
	}
}

// Against a registry that keeps no version times (no /_doze/ routes), the ids come from
// the ordinary version lookup, and the shown version's is the one the confirm names.
func TestSubjectFactsReadTheIdsFromAnotherRegistryToo(t *testing.T) {
	c, _ := newProxiedRegistryConsole(t, withoutDozeRoutes)
	register(t, c, "orders-value", userV1) // id 1
	register(t, c, "orders-value", userV2) // id 2
	f := c.subjectFacts(context.Background(), "orders-value", []int{1, 2}, 2)
	if len(f.IDs) != 2 || f.IDs[0] != 1 || f.IDs[1] != 2 || f.SchemaID != 2 || f.NextID != 0 {
		t.Errorf("ids %v schema id %d next id %d", f.IDs, f.SchemaID, f.NextID)
	}
	_, got := confirmOf(t, c, "schema.version.delete?subject=orders-value&version=2")
	if txt, _ := got["text"].(string); !strings.Contains(txt, "Schema id 2 still resolves") {
		t.Errorf("the version confirm: %v", got)
	}
}
