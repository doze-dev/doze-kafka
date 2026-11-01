package console

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegistryClientConstruction(t *testing.T) {
	if newRegistryClient("", "u", "p", nil) != nil {
		t.Error("a client for no registry")
	}
	rc := newRegistryClient("http://reg:8081/", "u", "p", nil)
	if rc.base != "http://reg:8081" || rc.hc.Timeout != 10*time.Second || rc.hc.Transport != nil || rc.user != "u" || rc.pass != "p" {
		t.Errorf("%+v timeout %v", rc, rc.hc.Timeout)
	}
	cfg := &tls.Config{ServerName: "reg"}
	rc = newRegistryClient("https://reg", "", "", cfg)
	if tr, ok := rc.hc.Transport.(*http.Transport); !ok || tr.TLSClientConfig != cfg {
		t.Errorf("the registry's TLS config is not used: %+v", rc.hc.Transport)
	}
}

func TestRegistryClientRequestsAndRefusals(t *testing.T) {
	var gotUser, gotPass string
	var hadAuth bool
	var gotType, gotAccept, gotMethod, gotPath, gotBody string
	status := 200
	reply := `{"mode":"READWRITE"}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, hadAuth = r.BasicAuth()
		gotType, gotAccept, gotMethod, gotPath = r.Header.Get("Content-Type"), r.Header.Get("Accept"), r.Method, r.URL.Path
		b := make([]byte, 200)
		n, _ := r.Body.Read(b)
		gotBody = string(b[:n])
		w.WriteHeader(status)
		w.Write([]byte(reply))
	}))
	defer ts.Close()
	ctx := context.Background()

	rc := newRegistryClient(ts.URL, "alice", "secret", nil)
	if m, err := rc.Mode(ctx); err != nil || m != "READWRITE" {
		t.Fatalf("%q %v", m, err)
	}
	if !hadAuth || gotUser != "alice" || gotPass != "secret" || gotMethod != "GET" || gotPath != "/mode" ||
		gotType != "application/vnd.schemaregistry.v1+json" || !strings.HasPrefix(gotAccept, "application/vnd.schemaregistry.v1+json") {
		t.Errorf("request: auth %v %s/%s, %s %s, %s, %s", hadAuth, gotUser, gotPass, gotMethod, gotPath, gotType, gotAccept)
	}
	open := newRegistryClient(ts.URL, "", "", nil)
	if _, err := open.Mode(ctx); err != nil || hadAuth {
		t.Errorf("a registry with no login was sent one (%v, %v)", hadAuth, err)
	}
	if err := open.SetCompat(ctx, "a/b", "FULL"); err != nil || gotMethod != "PUT" || gotPath != "/config/a/b" && gotPath != "/config/a%2Fb" || !strings.Contains(gotBody, `"compatibility":"FULL"`) {
		t.Errorf("SetCompat: %v %s %s %s", err, gotMethod, gotPath, gotBody)
	}
	// 299 is still a success; 300 is a refusal.
	status = 299
	if _, err := open.Mode(ctx); err != nil {
		t.Errorf("status 299: %v", err)
	}
	status, reply = 300, "plain words\n"
	_, err := open.Mode(ctx)
	var re *regError
	if !errors.As(err, &re) || re.Status != 300 || re.Message != "plain words" || re.Code != 0 {
		t.Errorf("status 300: %v", err)
	}
	status, reply = 422, `{"error_code":42201,"message":"Invalid schema"}`
	_, err = open.Mode(ctx)
	if !errors.As(err, &re) || re.Status != 422 || re.Code != 42201 || re.Message != "Invalid schema" || !errors.Is(err, errBadInput) {
		t.Errorf("a refusal with a code: %+v %v", re, err)
	}
	status, reply = 500, `{"error_code":50001}`
	if _, err = open.Mode(ctx); !errors.As(err, &re) || re.Message != `{"error_code":50001}` {
		t.Errorf("a refusal with no message keeps what it said: %+v", re)
	}
	ts.Close()
	if _, err = open.Mode(ctx); err == nil || !strings.Contains(err.Error(), "the schema registry is not reachable") {
		t.Errorf("a registry that is gone: %v", err)
	}
}

func TestRegistryCatalogueAndCompatFallback(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/schemas", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[
		 {"subject":"zeta","version":1,"id":5,"schema":"{}"},
		 {"subject":"alpha","version":2,"id":9,"schemaType":"JSON","schema":"{}"},
		 {"subject":"alpha","version":1,"id":3,"schema":"{}"},
		 {"subject":"alpha","version":3,"id":7,"schemaType":"PROTOBUF","schema":"{}"}]`))
	})
	mux.HandleFunc("/config/own", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"compatibilityLevel":"FULL"}`)) })
	mux.HandleFunc("/config/inherits", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte(`{"error_code":40408,"message":"Subject 'inherits' does not have subject-level compatibility configured"}`))
	})
	mux.HandleFunc("/config/broken", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte(`{"error_code":40401,"message":"nope"}`))
	})
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"compatibilityLevel":"BACKWARD"}`)) })
	ts := httptest.NewServer(mux)
	defer ts.Close()
	rc := newRegistryClient(ts.URL, "", "", nil)
	ctx := context.Background()

	rows, ids, err := rc.Catalogue(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("%+v %v", rows, err)
	}
	if rows[0].Name != "alpha" || rows[0].Versions != 3 || rows[0].Latest != 3 || rows[0].ID != 7 || rows[0].Type != "PROTOBUF" || rows[0].TypeName() != "PROTOBUF" || rows[0].TypeShort() != "PROTO" {
		t.Errorf("alpha: %+v", rows[0])
	}
	if rows[1].Name != "zeta" || rows[1].Versions != 1 || rows[1].Latest != 1 || rows[1].TypeName() != "AVRO" || rows[1].TypeShort() != "AVRO" {
		t.Errorf("zeta: %+v", rows[1])
	}
	if len(ids) != 4 || ids[0] != 3 || ids[1] != 5 || ids[2] != 7 || ids[3] != 9 {
		t.Errorf("ids %v", ids)
	}
	if (SubjectRow{Type: "JSON"}).TypeName() != "JSON" || (SubjectRow{Type: "JSON"}).TypeShort() != "JSON" {
		t.Error("JSON names")
	}

	if l, own, err := rc.Compat(ctx, "own"); err != nil || l != "FULL" || !own {
		t.Errorf("own: %q %v %v", l, own, err)
	}
	if l, own, err := rc.Compat(ctx, "inherits"); err != nil || l != "BACKWARD" || own {
		t.Errorf("inherits: %q %v %v", l, own, err)
	}
	if l, own, err := rc.Compat(ctx, "broken"); err == nil || l != "" || own {
		t.Errorf("broken: %q %v %v", l, own, err)
	}
	if l, err := rc.GlobalCompat(ctx); err != nil || l != "BACKWARD" {
		t.Errorf("global: %q %v", l, err)
	}
	for v, want := range map[string]string{"JSON": "JSON Schema", "PROTOBUF": "Protobuf", "AVRO": "Avro", "": "Avro"} {
		if got := typeLabel(v); got != want {
			t.Errorf("typeLabel(%q) = %q", v, got)
		}
	}
}
