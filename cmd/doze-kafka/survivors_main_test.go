package main

import (
	"bytes"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func TestURLForWildcardsAndBadAddresses(t *testing.T) {
	for _, c := range []struct{ scheme, addr, want string }{
		{"http", ":8081", "http://localhost:8081"},
		{"http", "0.0.0.0:8081", "http://localhost:8081"},
		{"https", "[::]:8081", "https://localhost:8081"},
		{"http", "10.0.0.7:8081", "http://10.0.0.7:8081"},
		{"http", "[fe80::1]:8081", "http://[fe80::1]:8081"},
		{"https", "kafka.orders.doze:8081", "https://kafka.orders.doze:8081"},
		{"http", "no-port", "http://no-port"},
	} {
		if got := urlFor(c.scheme, c.addr); got != c.want {
			t.Errorf("urlFor(%q, %q) = %q, want %q", c.scheme, c.addr, got, c.want)
		}
	}
	if got := httpURL(":80"); got != "http://localhost:80" {
		t.Errorf("httpURL = %q", got)
	}
}

type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

func TestDialableNamesTheAdvertisedHostForAWildcard(t *testing.T) {
	for _, c := range []struct{ bound, want string }{
		{"0.0.0.0:9092", "kafka.orders.doze:9092   (listening on every interface)"},
		{"[::]:9092", "kafka.orders.doze:9092   (listening on every interface)"},
		{":9092", "kafka.orders.doze:9092   (listening on every interface)"},
		{"127.0.0.1:9092", "127.0.0.1:9092"},
		{"kafka.orders.doze:9092", "kafka.orders.doze:9092"},
		{"not-an-address", "not-an-address"},
	} {
		if got := dialable(fakeAddr(c.bound), "kafka.orders.doze"); got != c.want {
			t.Errorf("dialable(%q) = %q, want %q", c.bound, got, c.want)
		}
	}
}

func TestAbsPathMakesAPathAbsolute(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if got := absPath(filepath.Join("a", "b.pem")); got != filepath.Join(wd, "a", "b.pem") {
		t.Errorf("absPath = %q", got)
	}
	abs := filepath.Join(wd, "c.pem")
	if got := absPath(abs); got != abs {
		t.Errorf("absPath of an absolute path = %q", got)
	}
}

// The console trusts the one certificate its broker was started with and no other; with
// none to trust it trusts nothing, and does not fail to build.
func TestConsoleClientTLSTrustsOnlyTheBrokersOwnCertificate(t *testing.T) {
	own, other := []byte("own certificate"), []byte("another certificate")
	for _, c := range []struct {
		name   string
		server *tls.Config
		raw    []byte
		trust  bool
	}{
		{"the same", &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{own}}}}, own, true},
		{"another", &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{own}}}}, other, false},
		{"none held", &tls.Config{}, own, false},
		{"an empty chain", &tls.Config{Certificates: []tls.Certificate{{}}}, own, false},
	} {
		cfg := consoleClientTLS(c.server)
		err := cfg.VerifyPeerCertificate([][]byte{c.raw}, nil)
		if (err == nil) != c.trust {
			t.Errorf("%s: %v, trusted %v", c.name, err, c.trust)
		}
		if err := cfg.VerifyPeerCertificate(nil, nil); err == nil {
			t.Errorf("%s: a peer with no certificate was trusted", c.name)
		}
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// The command that points a shell at the broker is on the line with its explanation up
// to 24 characters, and on its own line past that.
func TestAnnounceAlignsOnlyAShortEvalCommand(t *testing.T) {
	cfg := config.Default()
	for _, c := range []struct {
		env    string
		inline bool
	}{
		{strings.Repeat("e", 14), true},  // eval "$(...)" is 24 characters.
		{strings.Repeat("e", 15), false}, // 25.
		{"", true},                       // the default, doze-kafka env, is 24.
	} {
		var out bytes.Buffer
		announce(&out, discardLogger(), cfg, readyInfo{bootstrap: "b", dataDir: "d", kafka: 4, auth: "no auth", envCommand: c.env})
		env := c.env
		if env == "" {
			env = "doze-kafka env"
		}
		eval := `eval "$(` + env + `)"`
		want := "    point this shell at it:\n      " + eval + "\n"
		if c.inline {
			want = "    " + eval + " point this shell at it\n"
		}
		if !strings.Contains(out.String(), want) {
			t.Errorf("env %q: no %q in:\n%s", c.env, want, out.String())
		}
	}
	var out bytes.Buffer
	announce(&out, discardLogger(), cfg, readyInfo{bootstrap: "b", dataDir: "d", kafka: 4, auth: "no auth", configFile: "/etc/doze-kafka.toml"})
	if !strings.Contains(out.String(), "    config    /etc/doze-kafka.toml\n") {
		t.Errorf("no config line:\n%s", out.String())
	}
	out.Reset()
	announce(&out, discardLogger(), cfg, readyInfo{bootstrap: "b", dataDir: "d", kafka: 4, auth: "no auth"})
	if strings.Contains(out.String(), "config ") {
		t.Errorf("a config line without a file:\n%s", out.String())
	}
}

var _ net.Addr = fakeAddr("")
