package broker_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// startAuthBroker boots a broker with the given SASL mechanisms/credentials (and
// optional TLS keypair) and returns its address.
func startAuthBroker(t *testing.T, mechs []string, creds map[string]string, certFile, keyFile string) string {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.MetaDB = filepath.Join(dir, "meta.db")
	cfg.SASLMechanisms = mechs
	cfg.Credentials = creds
	cfg.TLSCertFile = certFile
	cfg.TLSKeyFile = keyFile
	addr, stop := boot(t, cfg)
	t.Cleanup(stop)
	return addr
}

// authRoundTrip proves a SASL/TLS-configured client can do real work: create a
// topic, produce, and consume it back. base carries the SASL and (optional) TLS
// dial options.
func authRoundTrip(t *testing.T, addr, topic string, base ...kgo.Opt) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seed := append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.DialTimeout(3 * time.Second)}, base...)

	producer, err := kgo.NewClient(seed...)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer producer.Close()
	mustCreateTopic(t, ctx, producer, topic, 1)

	const n = 5
	for i := range n {
		rec := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "m%d", i)}
		if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce m%d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(append(seed,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)...)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consumer.Close()

	var got int
	for got < n {
		fs := consumer.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume: %v", errs[0].Err)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
}

func TestAuth_PlainRoundTrip(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, "", "")
	mech := plain.Auth{User: "alice", Pass: "s3cret"}.AsMechanism()
	authRoundTrip(t, addr, "plain-topic", kgo.SASL(mech))
}

func TestAuth_ScramSHA256RoundTrip(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLScramSHA256}, map[string]string{"bob": "hunter2"}, "", "")
	mech := scram.Auth{User: "bob", Pass: "hunter2"}.AsSha256Mechanism()
	authRoundTrip(t, addr, "scram256-topic", kgo.SASL(mech))
}

func TestAuth_ScramSHA512RoundTrip(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLScramSHA512}, map[string]string{"carol": "correcthorse"}, "", "")
	mech := scram.Auth{User: "carol", Pass: "correcthorse"}.AsSha512Mechanism()
	authRoundTrip(t, addr, "scram512-topic", kgo.SASL(mech))
}

// requireRejected asserts the broker actively refused the client: the request
// failed AND it failed before the test's deadline. "Any error" is too weak — a
// broker that hung or crashed would also satisfy it — so a timeout is a failure
// here, the refusal has to be prompt.
func requireRejected(t *testing.T, ctx context.Context, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the broker to reject the request, got success")
	}
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request timed out instead of being rejected: %v", err)
	}
}

func TestAuth_WrongPasswordRejected(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLScramSHA256}, map[string]string{"bob": "hunter2"}, "", "")
	mech := scram.Auth{User: "bob", Pass: "wrong"}.AsSha256Mechanism()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second), kgo.SASL(mech))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	t.Logf("wrong-password error: %v", err)
	requireRejected(t, ctx, err)
	if !errors.Is(err, kerr.SaslAuthenticationFailed) {
		t.Fatalf("wrong password: err = %v, want SASL_AUTHENTICATION_FAILED", err)
	}
}

func TestAuth_PlainWrongPasswordRejected(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, "", "")
	mech := plain.Auth{User: "alice", Pass: "nope"}.AsMechanism()
	assertSASLRefused(t, addr, mech)
}

func TestAuth_UnknownUserRejected(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLScramSHA256}, map[string]string{"bob": "hunter2"}, "", "")
	mech := scram.Auth{User: "mallory", Pass: "hunter2"}.AsSha256Mechanism()
	assertSASLRefused(t, addr, mech)
}

func TestAuth_UnsupportedMechanismRejected(t *testing.T) {
	// Only PLAIN is enabled; a client offering SCRAM must be refused at the
	// handshake, not allowed to proceed or hung.
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, "", "")
	mech := scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism()
	assertSASLRefused(t, addr, mech)
}

// assertSASLRefused connects with the given mechanism and requires a prompt
// refusal of the very first request.
func assertSASLRefused(t *testing.T, addr string, mech sasl.Mechanism) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second), kgo.SASL(mech))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	t.Logf("refusal: %v", err)
	requireRejected(t, ctx, err)
}

func TestAuth_UnauthenticatedRejected(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, "", "")

	// A client with no SASL configured — the gate must block its first non-SASL
	// request (Metadata) and drop the connection.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	t.Logf("unauthenticated error: %v", err)
	requireRejected(t, ctx, err)
}

func TestAuth_TLSWithPlain(t *testing.T) {
	certFile, keyFile, pool := selfSignedCert(t)
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, certFile, keyFile)

	mech := plain.Auth{User: "alice", Pass: "s3cret"}.AsMechanism()
	tlsCfg := &tls.Config{RootCAs: pool, ServerName: "localhost"}
	authRoundTrip(t, addr, "tls-topic", kgo.SASL(mech), kgo.DialTLSConfig(tlsCfg))
}

func TestAuth_TLSRejectsPlaintextClient(t *testing.T) {
	certFile, keyFile, _ := selfSignedCert(t)
	addr := startAuthBroker(t, []string{config.SASLPlain}, map[string]string{"alice": "s3cret"}, certFile, keyFile)

	// A plaintext client against a TLS listener cannot complete the handshake.
	mech := plain.Auth{User: "alice", Pass: "s3cret"}.AsMechanism()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second), kgo.SASL(mech))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	t.Logf("plaintext-to-TLS error: %v", err)
	requireRejected(t, ctx, err)
}

func TestAuth_DisabledDoesNotAdvertiseSASL(t *testing.T) {
	addr := startBroker(t, 4) // no auth.
	cl := newClient(t, addr)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("api versions: %v", err)
	}
	for _, k := range resp.ApiKeys {
		if k.ApiKey == 17 || k.ApiKey == 36 {
			t.Fatalf("SASL API %d advertised while auth is disabled", k.ApiKey)
		}
	}
}

// selfSignedCert writes a fresh self-signed certificate (valid for 127.0.0.1 and
// localhost) to temp files and returns their paths plus a pool trusting it.
func selfSignedCert(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "doze-kafka-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	certOut, _ := os.Create(certFile)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	certOut.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyOut, _ := os.Create(keyFile)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	keyOut.Close()

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(cert)
	return certFile, keyFile, pool
}
