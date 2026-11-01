// Package connopt is how the suite's small helper programs (put, hold) reach the
// broker the way the run's identity says: plaintext and anonymous by default, and
// signed in over TLS when E2E_SASL_USER / E2E_SASL_PASS and E2E_TLS=1 are set
// (foundation B11). A throwaway certificate on loopback is accepted.
package connopt

import (
	"crypto/tls"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Opts are the connection options every helper client starts with.
func Opts() []kgo.Opt {
	var opts []kgo.Opt
	if user := os.Getenv("E2E_SASL_USER"); user != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: os.Getenv("E2E_SASL_PASS")}.AsSha512Mechanism()))
	}
	if os.Getenv("E2E_TLS") == "1" {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true})) //nolint:gosec // a throwaway certificate on loopback.
	}
	return opts
}
