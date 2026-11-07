package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// FuzzSchemaForms posts what a browser could post to every Schemas form, with the real
// registry beside the console: whatever comes in, the answer is a page or a refusal,
// never a server error or a panic (SCH-28; the forms read through schemaFromForm).
func FuzzSchemaForms(f *testing.F) {
	c, _, _ := newRegistryConsole(f)
	routes := []string{"/_console/new/schema", "/_console/schemas/{s}/check", "/_console/schemas/{s}/register",
		"/_console/schemas/{s}/config", "/_console/schemas/{s}/delete"}
	f.Add(uint8(0), "orders-value", userV1, "", "", "[]")
	f.Add(uint8(1), "orders-value", userV2, "AVRO", "", `[{"name":"x","subject":"y","version":1}]`)
	f.Add(uint8(2), "a/b%2F", `{"type":"object"}`, "JSON", "", "null")
	f.Add(uint8(3), "s", "", "", "FULL", "")
	f.Add(uint8(4), "s", "", "", "", "")
	f.Add(uint8(1), "p", `syntax = "proto3"; message M { int32 a = 1; }`, "PROTOBUF", "", `[{"name":1}]`)
	f.Fuzz(func(t *testing.T, which uint8, subject, schema, typ, level, refs string) {
		if len(schema) > 64<<10 || len(refs) > 4<<10 || len(subject) > 1<<10 {
			return
		}
		path := strings.ReplaceAll(routes[int(which)%len(routes)], "{s}", groupSegment(subject))
		form := url.Values{"subject": {subject}, "schema": {schema}, "type": {typ}, "level": {level}, "references": {refs}, "version": {level}}
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Host = "127.0.0.1"
		rec := httptest.NewRecorder()
		c.ServeHTTP(rec, req)
		if rec.Code >= 500 && rec.Code != http.StatusBadGateway {
			t.Fatalf("POST %s %v: %d\n%s", path, form, rec.Code, rec.Body)
		}
	})
}
