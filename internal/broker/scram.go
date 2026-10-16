package broker

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"hash"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// scramIterations is the PBKDF2 work factor advertised to clients. 4096 is the
// SCRAM spec's floor and Kafka's own default — ample for a local-dev fixture.
const scramIterations = 4096

// scramCredential is a user's derived SCRAM verifier: everything the server needs
// to check a proof without storing the plaintext password (RFC 5802). Salt and
// iterations are sent to the client in the server-first message; StoredKey and
// ServerKey stay server-side.
type scramCredential struct {
	salt       []byte
	iterations int
	storedKey  []byte
	serverKey  []byte
}

// scramHashFor returns the hash constructor for a SCRAM mechanism name.
func scramHashFor(mechanism string) func() hash.Hash {
	switch mechanism {
	case config.SASLScramSHA512:
		return sha512.New
	default:
		return sha256.New
	}
}

// deriveSCRAM computes a user's verifier from a password, salt, and iteration
// count, per RFC 5802:
//
//	SaltedPassword = PBKDF2(password, salt, i)
//	ClientKey      = HMAC(SaltedPassword, "Client Key")
//	StoredKey      = H(ClientKey)
//	ServerKey      = HMAC(SaltedPassword, "Server Key")
func deriveSCRAM(newHash func() hash.Hash, password string, salt []byte, iterations int) (*scramCredential, error) {
	salted, err := pbkdf2.Key(newHash, password, salt, iterations, newHash().Size())
	if err != nil {
		return nil, err
	}
	clientKey := hmacSum(newHash, salted, []byte("Client Key"))
	h := newHash()
	h.Write(clientKey)
	return &scramCredential{
		salt:       salt,
		iterations: iterations,
		storedKey:  h.Sum(nil),
		serverKey:  hmacSum(newHash, salted, []byte("Server Key")),
	}, nil
}

func newSCRAMCredential(newHash func() hash.Hash, password string) (*scramCredential, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	return deriveSCRAM(newHash, password, salt, scramIterations)
}

// scramConversation is the server-side state carried between the two
// SaslAuthenticate steps of one SCRAM exchange (stored on the connection).
type scramConversation struct {
	newHash         func() hash.Hash
	cred            *scramCredential
	clientFirstBare string
	serverFirst     string
	combinedNonce   string
	gs2Header       string
	user            string // who is authenticating, for the principal on success.
}

// scramFirst handles the client-first message and produces the server-first
// message (r=<nonce>,s=<salt>,i=<iters>), returning the conversation state to
// carry into the final step.
//
//fuzz:parser class=sasl FuzzSCRAMClientFirst
func (a *authenticator) scramFirst(mechanism string, in []byte) ([]byte, *scramConversation, error) {
	// This message is kept on the connection until the client's second: a user name and
	// a nonce the client made up. A megabyte of nonce, on every connection that has
	// not logged in yet, is not a login (bounds.go).
	if len(in) > maxSCRAMFirstBytes {
		return nil, nil, fmt.Errorf("scram: client-first of %d bytes, more than %d", len(in), maxSCRAMFirstBytes)
	}
	newHash := scramHashFor(mechanism)
	s := string(in)

	// gs2-header = cbind-flag "," [authzid] "," — the two commas before the bare.
	i1 := strings.IndexByte(s, ',')
	if i1 < 0 {
		return nil, nil, fmt.Errorf("scram: malformed client-first (no gs2 header)")
	}
	rest := s[i1+1:]
	i2 := strings.IndexByte(rest, ',')
	if i2 < 0 {
		return nil, nil, fmt.Errorf("scram: malformed client-first (truncated gs2 header)")
	}
	gs2Header := s[:i1+1+i2+1]
	bare := s[i1+1+i2+1:]

	if flag := s[:i1]; flag != "n" && flag != "y" {
		// "p" means the client demands channel binding, which we do not offer.
		return nil, nil, fmt.Errorf("scram: unsupported channel-binding flag %q", flag)
	}

	attrs := scramAttrs(bare)
	user := scramUnescape(attrs['n'])
	clientNonce := attrs['r']
	if user == "" || clientNonce == "" {
		return nil, nil, fmt.Errorf("scram: client-first missing username or nonce")
	}

	cred := a.scram[mechanism][user]
	if cred == nil {
		return nil, nil, fmt.Errorf("scram: invalid credentials") // the same answer as a wrong password: do not confirm which users exist.
	}

	serverNonce, err := randomNonce()
	if err != nil {
		return nil, nil, err
	}
	combined := clientNonce + serverNonce
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", combined, base64.StdEncoding.EncodeToString(cred.salt), cred.iterations)

	return []byte(serverFirst), &scramConversation{
		newHash:         newHash,
		cred:            cred,
		clientFirstBare: bare,
		serverFirst:     serverFirst,
		combinedNonce:   combined,
		gs2Header:       gs2Header,
		user:            user,
	}, nil
}

// scramFinal verifies the client-final message's proof and produces the
// server-final message (v=<server-signature>).
//
//fuzz:parser class=sasl FuzzSCRAMClientFinal
func scramFinal(conv *scramConversation, in []byte) ([]byte, error) {
	attrs := scramAttrs(string(in))
	channel, nonce, proofB64 := attrs['c'], attrs['r'], attrs['p']

	if channel != base64.StdEncoding.EncodeToString([]byte(conv.gs2Header)) {
		return nil, fmt.Errorf("scram: channel binding mismatch")
	}
	if subtle.ConstantTimeCompare([]byte(nonce), []byte(conv.combinedNonce)) != 1 {
		return nil, fmt.Errorf("scram: nonce mismatch")
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return nil, fmt.Errorf("scram: bad proof encoding")
	}

	clientFinalWithoutProof := "c=" + channel + ",r=" + nonce
	authMessage := conv.clientFirstBare + "," + conv.serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSum(conv.newHash, conv.cred.storedKey, []byte(authMessage))
	if len(proof) != len(clientSignature) {
		return nil, fmt.Errorf("scram: proof length mismatch")
	}
	clientKey := make([]byte, len(proof))
	for i := range clientKey {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	h := conv.newHash()
	h.Write(clientKey)
	if subtle.ConstantTimeCompare(h.Sum(nil), conv.cred.storedKey) != 1 {
		return nil, fmt.Errorf("scram: invalid credentials")
	}

	serverSignature := hmacSum(conv.newHash, conv.cred.serverKey, []byte(authMessage))
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature)), nil
}

func hmacSum(newHash func() hash.Hash, key, msg []byte) []byte {
	m := hmac.New(newHash, key)
	m.Write(msg)
	return m.Sum(nil)
}

// randomNonce returns a printable, comma-free nonce (base64 has no comma).
func randomNonce() (string, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buf), nil
}

// scramAttrs parses a comma-separated SCRAM message into single-letter attributes
// (a=…,b=…). A malformed field is skipped.
//
//fuzz:parser class=sasl FuzzSCRAMUsernameEscaping, FuzzSCRAMClientFirst, FuzzSCRAMClientFinal
func scramAttrs(s string) map[byte]string {
	out := make(map[byte]string)
	for _, part := range strings.Split(s, ",") {
		if len(part) >= 2 && part[1] == '=' {
			out[part[0]] = part[2:]
		}
	}
	return out
}

// scramUnescape reverses SCRAM username escaping (=2C for ',' and =3D for '=').
//
//fuzz:parser class=sasl FuzzSCRAMUsernameEscaping, FuzzSCRAMClientFirst
func scramUnescape(s string) string {
	s = strings.ReplaceAll(s, "=2C", ",")
	s = strings.ReplaceAll(s, "=3D", "=")
	return s
}
