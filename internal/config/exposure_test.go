package config

import (
	"bytes"
	"strings"
	"testing"
)

// Which listen addresses another machine can reach. A name is not resolved: one
// that is not localhost is taken at its word.
func TestBeyondLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9093": false, "127.5.6.7:9093": false, "[::1]:9093": false, "localhost:9093": false, "LOCALHOST:0": false,
		":9093": true, "0.0.0.0:9093": true, "[::]:9093": true, "192.168.1.20:9093": true, "10.0.0.1:0": true,
		"devbox.lan:9093": true, "[fe80::1]:9093": true,
		"not an address": false, // refused elsewhere, as not host:port.
	} {
		if got := BeyondLoopback(addr); got != want {
			t.Errorf("BeyondLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}

// The registry serves TLS exactly when it is on, the broker has a certificate and
// nobody turned it off.
func TestRegistryTLSFollowsTheBrokersCertificate(t *testing.T) {
	c := Default()
	if c.RegistryTLS() {
		t.Error("TLS on the registry with no certificate")
	}
	c.TLSCertFile, c.TLSKeyFile = "cert.pem", "key.pem"
	if !c.RegistryTLS() {
		t.Error("a broker with a certificate serves its registry in the clear by default")
	}
	c.SchemaRegistryTLS = false
	if c.RegistryTLS() {
		t.Error("schema-registry-tls = false did not turn it off")
	}
	c.SchemaRegistryTLS, c.SchemaRegistry = true, false
	if c.RegistryTLS() {
		t.Error("a registry that is off serves nothing")
	}
}

// The two settings are file keys, are printed with their defaults, and survive
// being printed and read back.
func TestExposureKeysAreFileKeysAndArePrinted(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteTOML(&buf, Default()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"schema-registry-tls = true\n", "console-unprotected = false\n"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("the printed defaults are missing %q:\n%s", want, buf.String())
		}
	}
	cfg := Default()
	if err := LoadFile(writeFile(t, "schema-registry-tls = false\nconsole-unprotected = true\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaRegistryTLS || !cfg.ConsoleUnprotected {
		t.Errorf("the file did not apply: tls=%v unprotected=%v", cfg.SchemaRegistryTLS, cfg.ConsoleUnprotected)
	}
	buf.Reset()
	if err := WriteTOML(&buf, cfg); err != nil {
		t.Fatal(err)
	}
	back := Default()
	if err := LoadFile(writeFile(t, buf.String()), &back); err != nil {
		t.Fatal(err)
	}
	if back.SchemaRegistryTLS || !back.ConsoleUnprotected {
		t.Errorf("printed and read back: tls=%v unprotected=%v", back.SchemaRegistryTLS, back.ConsoleUnprotected)
	}
}

// A plain registry beyond loopback beside a TLS broker is warned about, and the
// warning says whether passwords are among what crosses in the clear.
func TestWarningsNameWhatIsLeftOpen(t *testing.T) {
	c := Default()
	c.ListenAddr, c.ConsoleAddr, c.SchemaRegistryAddr = "127.0.0.1:9092", "127.0.0.1:9093", "0.0.0.0:8081"
	if w := c.Warnings(); len(w) != 0 {
		t.Errorf("no TLS, nothing to warn about: %q", w)
	}
	c.TLSCertFile, c.TLSKeyFile = "cert.pem", "key.pem"
	if w := c.Warnings(); len(w) != 0 {
		t.Errorf("a TLS registry is what was asked for: %q", w)
	}
	c.SchemaRegistryTLS = false
	w := c.Warnings()
	if len(w) != 1 || !strings.Contains(w[0], "0.0.0.0:8081 over plain HTTP") || strings.Contains(w[0], "password") {
		t.Errorf("warnings = %q, want one about the registry and no mention of passwords", w)
	}
	c.SASLMechanisms = []string{SASLPlain}
	if w := c.Warnings(); len(w) != 1 || !strings.Contains(w[0], "basic-auth passwords") {
		t.Errorf("with SASL the warning names the passwords: %q", w)
	}
	c.SchemaRegistryAddr = "127.0.0.1:8081"
	if w := c.Warnings(); len(w) != 0 {
		t.Errorf("a plain registry on loopback: %q", w)
	}
}
