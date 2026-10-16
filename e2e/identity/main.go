// Command identity runs a command against a broker started with the SECOND
// identity (foundation B11): the listener on 127.0.0.1:0, a console prefix, a data
// directory with a space in its name, SASL authentication on and TLS on. The suite
// that passes with the defaults must pass with this too; a path that was only ever
// typed as "/_console", a directory that was never quoted or a client that never
// authenticated shows up here.
//
//	go run ./e2e/identity [-bin path/to/doze-kafka] -- bunx playwright test
//
// It builds nothing: -bin names the binary (default bin/doze-kafka). The command
// runs with E2E_EXTERNAL=1, E2E_KAFKA_ADDR, E2E_CONSOLE_URL (with the prefix),
// E2E_REGISTRY_URL, E2E_SASL_USER, E2E_SASL_PASS and E2E_TLS=1 in its environment,
// and the broker is stopped and its directory removed when it returns.
package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	// A prefix of two segments that is not the default, so nothing can have assumed
	// "/_console".
	consolePrefix = "/ops/kafka-ui"
	user          = "e2e-admin"
	pass          = "e2e pass/word+1"
)

func main() {
	bin := flag.String("bin", "bin/doze-kafka", "the doze-kafka binary")
	flag.Parse()
	cmdline := flag.Args()
	if len(cmdline) == 0 {
		fmt.Fprintln(os.Stderr, "usage: identity [-bin path] -- command ...")
		os.Exit(2)
	}
	code, err := run(*bin, cmdline)
	if err != nil {
		fmt.Fprintln(os.Stderr, "identity:", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func run(bin string, cmdline []string) (int, error) {
	root, err := os.MkdirTemp("", "doze identity ") // the space is the point.
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(root)
	dataDir := filepath.Join(root, "data dir")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return 0, err
	}
	cert, key, err := selfSigned(root)
	if err != nil {
		return 0, err
	}
	absBin, err := filepath.Abs(bin)
	if err != nil {
		return 0, err
	}

	broker := exec.Command(absBin,
		"--listen", "127.0.0.1:0", "--console-addr", "127.0.0.1:0", "--schema-registry-addr", "127.0.0.1:0",
		"--console-prefix", consolePrefix, "--data-dir", dataDir,
		"--sasl-mechanisms", "SCRAM-SHA-512", "--sasl-user", user+":"+pass,
		"--tls-cert", cert, "--tls-key", key, "--zone=false", "--log-format", "json")
	stdout, err := broker.StderrPipe() // the structured log is on stderr.
	if err != nil {
		return 0, err
	}
	broker.Stdout = nil
	if err := broker.Start(); err != nil {
		return 0, err
	}
	defer func() {
		broker.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { broker.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			broker.Process.Kill()
		}
	}()

	type ready struct {
		Msg       string `json:"msg"`
		Bootstrap string `json:"bootstrap"`
		Console   string `json:"console"`
		Registry  string `json:"schema_registry"`
	}
	found := make(chan ready, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			fmt.Fprintln(os.Stderr, sc.Text())
			var r ready
			if json.Unmarshal([]byte(sc.Text()), &r) == nil && r.Msg == "ready" {
				found <- r
				break
			}
		}
		for sc.Scan() { // keep the pipe drained, and the log visible.
			fmt.Fprintln(os.Stderr, sc.Text())
		}
	}()
	var r ready
	select {
	case r = <-found:
	case <-time.After(60 * time.Second):
		return 0, fmt.Errorf("the broker did not report ready")
	}
	if !strings.HasPrefix(r.Console, "http://") {
		return 0, fmt.Errorf("the broker's ready record names no console: %+v", r)
	}

	child := exec.Command(cmdline[0], cmdline[1:]...)
	child.Stdout, child.Stderr, child.Stdin = os.Stdout, os.Stderr, os.Stdin
	child.Env = append(os.Environ(),
		"E2E_EXTERNAL=1", "E2E_KAFKA_ADDR="+r.Bootstrap, "E2E_CONSOLE_URL="+strings.TrimSuffix(r.Console, "/")+consolePrefix+"/",
		"E2E_REGISTRY_URL="+r.Registry, "E2E_SASL_USER="+user, "E2E_SASL_PASS="+pass, "E2E_TLS=1", "E2E_DATA_DIR="+dataDir)
	if err := child.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode(), nil
		}
		return 0, err
	}
	return 0, nil
}

// selfSigned writes a throwaway certificate for 127.0.0.1 and its key.
func selfSigned(dir string) (certFile, keyFile string, err error) {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		return "", "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		return "", "", err
	}
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", err
	}
	return certFile, keyFile, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600)
}
