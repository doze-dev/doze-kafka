package broker

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
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

// What Apache Kafka says when it refuses a login, word for word (recorded:
// conformance/recordings/sasl-plain.json and sasl-scram.json; claims: auth A2 to
// A6, A11 to A13). A client shows this text to whoever is debugging the login, and
// the same text for a user that does not exist as for a wrong password is what
// keeps the answer from saying which users there are.
const (
	msgPlainBadCredentials = "Authentication failed: Invalid username or password"
	msgPlainNoUser         = "Authentication failed: username not specified"
	msgPlainNoPassword     = "Authentication failed: password not specified"
	msgAuthzidDiffers      = "Authentication failed: Client requested an authorization id that is different from username"
	msgAfterSuccess        = "SaslAuthenticate request received after successful authentication"
)

func msgPlainTokens(n int) string {
	return fmt.Sprintf("Invalid SASL/PLAIN response: expected 3 tokens, got %d", n)
}

func msgScramRefused(mechanism string) string {
	return "Authentication failed during authentication due to invalid credentials with SASL mechanism " + mechanism
}

// saslRefusal is a login refused: what the client is told (Kafka's words) and what
// the log says (why, in ours; never anything the client sent).
type saslRefusal struct {
	wire string
	why  string
	// silent closes the connection with no answer at all, where Kafka does.
	silent bool
}

func (r *saslRefusal) Error() string { return r.why }

// step advances the SASL exchange for a connection's negotiated mechanism. It
// returns the response token, whether authentication is now complete, and a
// refusal (which fails the exchange). PLAIN completes in one step; SCRAM takes two.
func (a *authenticator) step(conn *protocol.Conn, mechanism string, in []byte) (out []byte, done bool, err *saslRefusal) {
	switch mechanism {
	case config.SASLPlain:
		user, err := a.plainStep(in)
		if err != nil {
			return nil, false, err
		}
		conn.SetPrincipal("User:" + user)
		return nil, true, nil

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
			return nil, false, &saslRefusal{wire: msgScramRefused(mechanism), why: "scram: corrupt conversation state"}
		}
		resp, err := scramFinal(conv, in)
		if err != nil {
			return nil, false, err
		}
		conn.SetPrincipal("User:" + conv.user)
		return resp, true, nil

	default:
		return nil, false, &saslRefusal{wire: "no SASL mechanism negotiated", why: "no SASL mechanism negotiated"}
	}
}

// plainStep verifies a SASL/PLAIN response: [authzid] NUL authcid NUL password
// (RFC 4616), in the order Kafka checks it: the number of parts, an empty user, an
// empty password, the password, and only then the authorization id, which must be
// empty or the user's own name. This broker has no notion of one user acting as
// another, and neither has Kafka: accepting "bob NUL alice NUL alice's password"
// and going on as alice told a client that asked to be bob that it was.
func (a *authenticator) plainStep(in []byte) (user string, err *saslRefusal) {
	parts := bytes.Split(in, []byte{0})
	if len(parts) != 3 {
		return "", &saslRefusal{wire: msgPlainTokens(len(parts)), why: "plain: malformed response"}
	}
	authzid, user, pass := string(parts[0]), string(parts[1]), parts[2]
	if user == "" {
		return "", &saslRefusal{wire: msgPlainNoUser, why: "plain: no username"}
	}
	if len(pass) == 0 {
		return "", &saslRefusal{wire: msgPlainNoPassword, why: "plain: no password"}
	}
	want, ok := a.plain[user]
	// The comparison runs for a user that does not exist too, so the time taken
	// does not say which it was.
	if subtle.ConstantTimeCompare(pass, []byte(want)) != 1 || !ok {
		return "", &saslRefusal{wire: msgPlainBadCredentials, why: "plain: invalid credentials"}
	}
	if authzid != "" && authzid != user {
		return "", &saslRefusal{wire: msgAuthzidDiffers, why: "plain: the authorization id is not the user"}
	}
	return user, nil
}

// handleSASLHandshake negotiates the mechanism, once per connection, as Kafka
// holds it (claims: auth A7 to A9):
//
//   - on a connection that has logged in it is ILLEGAL_SASL_STATE with no mechanism
//     list, and nothing changes: the connection stays who it is;
//   - a mechanism that is not offered is UNSUPPORTED_SASL_MECHANISM with the list,
//     and the connection is closed after the answer;
//   - a second handshake before the login is over never gets here (protocol.Conn
//     answers it ILLEGAL_SASL_STATE and closes).
func (b *Broker) handleSASLHandshake(ctx context.Context, req *protocol.Request, hr *kmsg.SASLHandshakeRequest, resp *kmsg.SASLHandshakeResponse) error {
	if req.Conn.Authenticated() {
		resp.ErrorCode = kerr.IllegalSaslState.Code
		return nil
	}
	resp.SupportedMechanisms = b.auth.mechanisms()

	if b.auth.enabled[hr.Mechanism] {
		req.Conn.SetSASLMechanism(hr.Mechanism)
		req.Conn.SetSASLState(nil)
	} else {
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		req.Conn.CloseAfterResponse()
	}
	return nil
}

// handleSASLAuthenticate carries one SASL token exchange. A refused login is
// SASL_AUTHENTICATION_FAILED with Kafka's message and the connection is closed
// after the answer (claim: auth A2); success on the final step marks the
// connection authenticated, for good: a further SaslAuthenticate is
// ILLEGAL_SASL_STATE and changes nothing (claim: auth A7).
func (b *Broker) handleSASLAuthenticate(ctx context.Context, req *protocol.Request, ar *kmsg.SASLAuthenticateRequest, resp *kmsg.SASLAuthenticateResponse) error {
	if req.Conn.Authenticated() {
		// Before this a second token replaced the principal: a connection logged in
		// as alice became bob with bob's password, and a wrong password logged it out.
		resp.ErrorCode = kerr.IllegalSaslState.Code
		msg := msgAfterSuccess
		resp.ErrorMessage = &msg
		return nil
	}
	mechanism := req.Conn.SASLMechanism()
	if mechanism == "" {
		// No handshake: protocol.Conn closes such a connection before it gets here.
		return &protocol.CloseError{Reason: "SaslAuthenticate came before a SaslHandshake"}
	}

	out, done, refusal := b.auth.step(req.Conn, mechanism, ar.SASLAuthBytes)
	if refusal != nil {
		req.Conn.SetSASLState(nil)
		b.log.Warn("SASL authentication failed", "mechanism", mechanism, "err", refusal.why)
		if refusal.silent {
			return &protocol.CloseError{Reason: "authentication failed (" + refusal.why + ")"}
		}
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		msg := refusal.wire
		resp.ErrorMessage = &msg
		// The connection ends with this answer (claim: auth A2): thousands of
		// passwords could otherwise be tried over one connection.
		req.Conn.CloseAfterResponse()
		return nil
	}

	// Kafka sends an empty message, not a null one, with every accepted token.
	empty := ""
	resp.ErrorMessage = &empty
	resp.SASLAuthBytes = out
	if done {
		req.Conn.SetSASLState(nil)
		req.Conn.SetAuthenticated(true)
		b.log.Info("SASL authentication succeeded", "mechanism", mechanism)
	}
	return nil
}
