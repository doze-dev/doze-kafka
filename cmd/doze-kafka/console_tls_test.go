package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// selfSigned writes a self-signed certificate and key, as a developer would make
// for local TLS, and returns their paths.
func selfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600)
	return certFile, keyFile
}

// With TLS on, the console was handed the broker's server configuration to dial
// with, and every page failed while the summary still advertised the console.
func TestConsoleReachesTheBrokerOverTLS(t *testing.T) {
	cert, key := selfSigned(t)
	out, _ := start(t, "--tls-cert", cert, "--tls-key", key)
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "ready") })
	base := regexp.MustCompile(`http://127\.0\.0\.1:\d+`).FindString(out.String())
	resp, err := http.Get(base + "/_console/cluster")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || strings.Contains(string(body), "Can't reach the broker") {
		t.Fatalf("the console's cluster page with TLS on: %d %.300s", resp.StatusCode, body)
	}
}

// The console's client trusts exactly the certificate its broker was started
// with, and nothing else.
func TestConsoleClientTLSAcceptsOnlyTheBrokersCertificate(t *testing.T) {
	server := func() *tls.Config {
		pair, err := tls.LoadX509KeyPair(selfSigned(t))
		if err != nil {
			t.Fatal(err)
		}
		return &tls.Config{Certificates: []tls.Certificate{pair}}
	}
	a, b := server(), server()
	own := a.Certificates[0].Certificate[0]
	if err := consoleClientTLS(a).VerifyPeerCertificate([][]byte{own}, nil); err != nil {
		t.Errorf("the broker's own certificate was refused: %v", err)
	}
	if err := consoleClientTLS(b).VerifyPeerCertificate([][]byte{own}, nil); err == nil {
		t.Error("a certificate that is not the broker's was accepted")
	}
	if err := consoleClientTLS(a).VerifyPeerCertificate(nil, nil); err == nil {
		t.Error("no certificate at all was accepted")
	}
}

// "__console" is the console's own login. A user of that name in the
// configuration used to have its password replaced without a word.
func TestTheConsoleUserNameIsReserved(t *testing.T) {
	t.Chdir(t.TempDir())
	st, err := loadConfig([]string{"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--zone=false", "--data-dir", t.TempDir(),
		"--sasl-mechanisms", "PLAIN", "--sasl-user", "__console:mine"})
	if err == nil {
		out := &safeBuf{}
		err = run(t.Context(), st, newLogger(st.cfg, out), out)
	}
	if err == nil || !strings.Contains(err.Error(), "reserved for the web console") {
		t.Fatalf("a configured __console user: %v, want an error saying the name is reserved", err)
	}
}
