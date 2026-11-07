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
// never a server error or a panic (SCH-28; the forms read through schemaFromForm and
// draftFromForm: the register page's reference rows, a fix's path, a typed confirm).
func FuzzSchemaForms(f *testing.F) {
	c, _, _ := newRegistryConsole(f)
	routes := []string{"/_console/new/schema", "/_console/new/schema/check", "/_console/new/schema/fix",
		"/_console/schemas/{s}/config", "/_console/schemas/{s}/delete", "/_console/schemas/{s}/mode",
		"/_console/schemas/{s}/level-check", "/_console/schemas/{s}/try"}
	f.Add(uint8(0), "orders-value", userV1, "", "", "[]")
	f.Add(uint8(1), "orders-value", userV2, "AVRO", "", `[{"name":"x","subject":"y","version":1}]`)
	f.Add(uint8(2), "a/b%2F", `{"type":"object"}`, "JSON", "", "null")
	f.Add(uint8(3), "s", "", "", "FULL", "")
	f.Add(uint8(4), "s", "", "", "", "")
	f.Add(uint8(1), "p", `syntax = "proto3"; message M { int32 a = 1; }`, "PROTOBUF", "", `[{"name":1}]`)
	f.Add(uint8(2), "orders-value", `{"type":"record","name":"U","fields":[{"name":"a","type":"string"}]}`, "AVRO", "/fields/0", "")
	f.Add(uint8(1), "r", userV1, "AVRO", "", "h.K,orders-value,1;,,")
	f.Add(uint8(6), "orders-value", "", "", "FULL_TRANSITIVE", "")
	f.Add(uint8(7), "orders-value", "", "", "1", `{"id": 1}`)
	f.Fuzz(func(t *testing.T, which uint8, subject, schema, typ, level, refs string) {
		if len(schema) > 64<<10 || len(refs) > 4<<10 || len(subject) > 1<<10 {
			return
		}
		path := strings.ReplaceAll(routes[int(which)%len(routes)], "{s}", groupSegment(subject))
		form := url.Values{"subject": {subject}, "schema": {schema}, "type": {typ}, "level": {level}, "references": {refs}, "version": {level},
			"path": {level}, "mode": {level}, "doc": {refs}, "confirm": {subject}, "scope": {typ}}
		// The register page's reference rows: name, subject and version, a row each.
		for _, row := range strings.Split(refs, ";") {
			parts := append(strings.SplitN(row, ",", 3), "", "", "")
			form["ref_name"] = append(form["ref_name"], parts[0])
			form["ref_subject"] = append(form["ref_subject"], parts[1])
			form["ref_version"] = append(form["ref_version"], parts[2])
		}
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
