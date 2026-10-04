package broker

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// authenticator holds the enabled SASL mechanisms and the credential verifiers.
// PLAIN keeps plaintext passwords for a constant-time compare; SCRAM keeps only
// derived per-user verifiers (salt + StoredKey + ServerKey), never the password.
// It is nil when auth is disabled.
type authenticator struct {
	order   []string                               // advertised order (config order).
	enabled map[string]bool                        // mechanism -> enabled.
	plain   map[string]string                      // user -> password (PLAIN).
	scram   map[string]map[string]*scramCredential // mechanism -> user -> verifier.
}

// newAuthenticator builds the authenticator from config, or returns nil when SASL
// is disabled. SCRAM verifiers are derived once here (a random salt per user), so
// authentication does no PBKDF2 work per connection beyond what the client drives.
func newAuthenticator(cfg config.Config) (*authenticator, error) {
	if !cfg.AuthEnabled() {
		return nil, nil
	}
	a := &authenticator{
		enabled: make(map[string]bool),
		plain:   make(map[string]string),
		scram:   make(map[string]map[string]*scramCredential),
	}
	for _, m := range cfg.SASLMechanisms {
		if !a.enabled[m] {
			a.order = append(a.order, m)
		}
		a.enabled[m] = true
	}
	if a.enabled[config.SASLPlain] {
		for u, p := range cfg.Credentials {
			a.plain[u] = p
		}
	}
	for _, m := range []string{config.SASLScramSHA256, config.SASLScramSHA512} {
		if !a.enabled[m] {
			continue
		}
		a.scram[m] = make(map[string]*scramCredential)
		for u, p := range cfg.Credentials {
			cred, err := newSCRAMCredential(scramHashFor(m), p)
			if err != nil {
				return nil, fmt.Errorf("auth: derive %s verifier for %q: %w", m, u, err)
			}
			a.scram[m][u] = cred
		}
	}
	return a, nil
}

// mechanisms returns the enabled mechanisms in advertised order.
func (a *authenticator) mechanisms() []string { return a.order }

// step advances the SASL exchange for a connection's negotiated mechanism. It
// returns the response token, whether authentication is now complete, and any
// error (which fails the exchange). PLAIN completes in one step; SCRAM takes two.
func (a *authenticator) step(conn *protocol.Conn, mechanism string, in []byte) (out []byte, done bool, err error) {
	switch mechanism {
	case config.SASLPlain:
		return a.plainStep(in)

	case config.SASLScramSHA256, config.SASLScramSHA512:
		if conn.SASLState() == nil {
			resp, conv, err := a.scramFirst(mechanism, in)
			if err != nil {
				return nil, false, err
			}
			conn.SetSASLState(conv)
			return resp, false, nil // server-first sent; await client-final.
		}
		conv, ok := conn.SASLState().(*scramConversation)
		if !ok {
			return nil, false, fmt.Errorf("scram: corrupt conversation state")
		}
		resp, err := scramFinal(conv, in)
		if err != nil {
			return nil, false, err
		}
		return resp, true, nil

	default:
		return nil, false, fmt.Errorf("no SASL mechanism negotiated")
	}
}

// plainStep verifies a SASL/PLAIN response: [authzid] NUL authcid NUL password.
func (a *authenticator) plainStep(in []byte) ([]byte, bool, error) {
	parts := bytes.Split(in, []byte{0})
	if len(parts) != 3 {
		return nil, false, fmt.Errorf("plain: malformed response")
	}
	user, pass := string(parts[1]), parts[2]
	want, ok := a.plain[user]
	if !ok || subtle.ConstantTimeCompare(pass, []byte(want)) != 1 {
		return nil, false, fmt.Errorf("plain: invalid credentials")
	}
	return nil, true, nil
}

// handleSASLHandshake negotiates the mechanism. It always reports the full set of
// enabled mechanisms; if the requested one is enabled it is recorded on the
// connection and the client proceeds to SaslAuthenticate, otherwise the client
// gets UNSUPPORTED_SASL_MECHANISM and picks from the advertised list.
func (b *Broker) handleSASLHandshake(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	hr := kreq.(*kmsg.SASLHandshakeRequest)

	resp := kmsg.NewPtrSASLHandshakeResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.SupportedMechanisms = b.auth.mechanisms()

	if b.auth.enabled[hr.Mechanism] {
		req.Conn.SetSASLMechanism(hr.Mechanism)
		req.Conn.SetSASLState(nil)
	} else {
		resp.ErrorCode = int16(protocol.ErrUnsupportedSASLMechanism)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleSASLAuthenticate carries one SASL token exchange. A failed attempt returns
// SASL_AUTHENTICATION_FAILED with a message and leaves the connection
// unauthenticated (so the gate still blocks it); success on the final step marks
// the connection authenticated.
func (b *Broker) handleSASLAuthenticate(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	ar := kreq.(*kmsg.SASLAuthenticateRequest)

	resp := kmsg.NewPtrSASLAuthenticateResponse()
	resp.SetVersion(req.Header.APIVersion)

	mechanism := req.Conn.SASLMechanism()
	if mechanism == "" {
		resp.ErrorCode = int16(protocol.ErrIllegalSASLState)
		msg := "SaslAuthenticate before a successful SaslHandshake"
		resp.ErrorMessage = &msg
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	out, done, err := b.auth.step(req.Conn, mechanism, ar.SASLAuthBytes)
	if err != nil {
		resp.ErrorCode = int16(protocol.ErrSASLAuthenticationFailed)
		msg := err.Error()
		resp.ErrorMessage = &msg
		req.Conn.SetSASLState(nil)
		b.log.Warn("SASL authentication failed", "mechanism", mechanism, "err", err)
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	resp.SASLAuthBytes = out
	if done {
		req.Conn.SetAuthenticated(true)
		b.log.Info("SASL authentication succeeded", "mechanism", mechanism)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}
