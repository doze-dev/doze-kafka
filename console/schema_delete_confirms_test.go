package console

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Every delete confirm names exactly what the request then removes, checked against a
// real registry: a soft version delete, and the permanent delete of a subject (its soft
// delete first) where one id is shared with another subject and one version was
// already soft-deleted by a client.
func TestDeleteConfirmsMatchTheRequests(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	const userV3 = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":["null","string"],"default":null},{"name":"age","type":"int","default":0}]}`
	register(t, c, "orders-value", userV1) // id 1
	register(t, c, "orders-value", userV2) // id 2
	register(t, c, "orders-value", userV3) // id 3
	register(t, c, "audit-value", userV1)  // id 1 again: shared
	resolves := func(id string) bool {
		resp, err := http.Get(ts.URL + "/schemas/ids/" + id)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode == 200
	}
	text := func(path string) string {
		t.Helper()
		_, got := confirmOf(t, c, path)
		s, _ := got["text"].(string)
		return s
	}

	// Soft delete of the latest, v3: latest becomes v2, id 3 still resolves, and the
	// same schema registered again is v4.
	say := text("schema.version.delete?subject=orders-value&version=3")
	for _, want := range []string{"Version 3 stops appearing", "Latest becomes v2", "Schema id 3 still resolves", "new version number (v4)"} {
		if !strings.Contains(say, want) {
			t.Errorf("the version confirm does not say %q:\n%s", want, say)
		}
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/delete", url.Values{"version": {"3"}}); rec.Code != 303 {
		t.Fatalf("soft delete v3: %d %s", rec.Code, rec.Body)
	}
	if vs, _ := c.reg.Versions(t.Context(), "orders-value"); len(vs) != 2 || vs[1] != 2 {
		t.Errorf("versions after the soft delete = %v, want [1 2]", vs)
	}
	if !resolves("3") {
		t.Error("id 3 no longer resolves after a soft delete; the confirm said it would")
	}

	// A client soft-deletes v2 behind the console's back.
	if err := c.reg.SoftDeleteVersion(t.Context(), "orders-value", 2); err != nil {
		t.Fatal(err)
	}

	// The permanent delete: two calls; v1, v2 and v3 all go (the soft-deleted ones are
	// held until now); id 1 is audit-value's too, so it stays and its messages decode;
	// ids 2 and 3 go and are never handed out again.
	say = text("schema.delete?subject=orders-value")
	for _, want := range []string{"Two calls, in this order", "All 3 versions, the 2 soft-deleted ones included", "Ids 2 and 3 are never handed out again", "Id 1 stays: audit-value uses it too"} {
		if !strings.Contains(say, want) {
			t.Errorf("the subject confirm does not say %q:\n%s", want, say)
		}
	}
	if rec := req(t, c, "POST", "/_console/schemas/orders-value/delete", url.Values{"confirm": {"orders-value"}}); rec.Code != 303 {
		t.Fatalf("delete for good: %d %s", rec.Code, rec.Body)
	}
	if !resolves("1") {
		t.Error("id 1 stopped resolving; audit-value still uses it")
	}
	for _, id := range []string{"2", "3"} {
		if resolves(id) {
			t.Errorf("id %s still resolves after the permanent delete", id)
		}
	}
	if d, _ := c.reg.DeletedSubjects(t.Context()); len(d) != 0 {
		t.Errorf("still held as soft-deleted: %v", d)
	}
	register(t, c, "fresh-value", `{"type":"record","name":"Fresh","fields":[{"name":"x","type":"string"}]}`)
	if o, ok := c.reg.Overview(t.Context()); !ok || o.NextID != 5 {
		t.Errorf("next id after a new registration = %+v, want 5 (ids 2 and 3 stay spent)", o)
	}
}
