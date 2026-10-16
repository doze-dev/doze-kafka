package main

import (
	"crypto/tls"
	"net/http"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// The seed reaches the broker the way the suite's identity says (foundation B11):
// by default plaintext and anonymous, and with E2E_SASL_USER / E2E_SASL_PASS and
// E2E_TLS=1 it signs in and verifies the broker's own certificate (the second
// identity's is self-signed, so any certificate is accepted: the seed is not what
// is being tested).

// connOpts are the options every Kafka client the seed makes starts with.
func connOpts() []kgo.Opt {
	var opts []kgo.Opt
	if user := os.Getenv("E2E_SASL_USER"); user != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: os.Getenv("E2E_SASL_PASS")}.AsSha512Mechanism()))
	}
	if os.Getenv("E2E_TLS") == "1" {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true})) //nolint:gosec // a throwaway certificate on loopback.
	}
	return opts
}

// newKgo is kgo.NewClient with the identity's connection options in front.
func newKgo(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append(connOpts(), opts...)...)
}

// doRegistry sends a request to the schema registry, signed in when the identity is.
func doRegistry(req *http.Request) (*http.Response, error) {
	if user := os.Getenv("E2E_SASL_USER"); user != "" {
		req.SetBasicAuth(user, os.Getenv("E2E_SASL_PASS"))
	}
	return http.DefaultClient.Do(req)
}
