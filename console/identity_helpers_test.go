package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// serveRaw serves one request exactly as written (no prefix rewriting: the identity
// test names its own prefix) and returns the status.
func serveRaw(t *testing.T, c *Console, method, path string, form map[string][]string) int {
	t.Helper()
	return serveRawRec(t, c, method, path, form).Code
}

func serveRawBody(t *testing.T, c *Console, method, path string) string {
	t.Helper()
	return serveRawRec(t, c, method, path, nil).Body.String()
}

func serveRawRec(t *testing.T, c *Console, method, path string, form map[string][]string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if form != nil {
		r = httptest.NewRequest(method, path, strings.NewReader(url.Values(form).Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Host = "127.0.0.1:9093"
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, r)
	return rec
}
