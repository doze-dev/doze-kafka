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
	"unicode/utf8"

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
	mechanism       string
	newHash         func() hash.Hash
	cred            *scramCredential
	clientFirstBare string
	serverFirst     string
	combinedNonce   string
	gs2Header       string
	user            string // who is authenticating, for the principal on success.
}

// scramClientFirst is a client-first message taken apart.
type scramClientFirst struct {
	gs2Header string // "n,," or "n,a=<authzid>,".
	authzid   string // unescaped; empty when none was sent.
	user      string // unescaped.
	nonce     string
	// bare is client-first-message-bare as it goes into the signature: the user as
	// sent, the nonce and the extensions after it. A reserved "m=" attribute is not
	// part of it, which is what Kafka signs (claim: auth A15): a client that sends
	// one is told to go on and then fails the proof.
	bare string
}

// parseScramClientFirst reads a client-first message (RFC 5802 section 7):
//
//	"n," [ "a=" saslname ] "," [ "m=" value "," ] "n=" saslname ",r=" nonce *( "," ext )
//
// Only "n" is taken for the channel-binding flag: "p=..." asks for a binding this
// broker has none of, and "y" says the client believes the server has none, which
// Kafka refuses as well (claim: auth A15). Anything else that does not fit is
// refused: a missing or empty user or nonce, the two in the other order, a bad
// "=XX" escape in a name, an empty attribute, bytes that are not UTF-8.
func parseScramClientFirst(msg []byte) (scramClientFirst, bool) {
	var f scramClientFirst
	if !utf8.Valid(msg) {
		return f, false
	}
	s := string(msg)
	rest, ok := strings.CutPrefix(s, "n,")
	if !ok {
		return f, false
	}
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return f, false
	}
	if a := rest[:comma]; a != "" {
		name, ok := strings.CutPrefix(a, "a=")
		if !ok {
			return f, false
		}
		if f.authzid, ok = scramName(name); !ok {
			return f, false
		}
	}
	f.gs2Header = s[:2+comma+1]
	attrs := strings.Split(rest[comma+1:], ",")
	if strings.HasPrefix(attrs[0], "m=") {
		attrs = attrs[1:]
	}
	if len(attrs) < 2 {
		return f, false
	}
	rawUser, ok := strings.CutPrefix(attrs[0], "n=")
	if !ok {
		return f, false
	}
	if f.user, ok = scramName(rawUser); !ok {
		return f, false
	}
	if f.nonce, ok = strings.CutPrefix(attrs[1], "r="); !ok || !scramPrintable(f.nonce) {
		return f, false
	}
	for _, ext := range attrs[2:] {
		// An extension is a letter, "=", and a value that is not empty.
		if len(ext) < 3 || ext[1] != '=' || !isASCIILetter(ext[0]) {
			return f, false
		}
	}
	f.bare = strings.Join(attrs, ",")
	return f, true
}

// scramName unescapes a saslname: any characters but "," and "=", which are
// written "=2C" and "=3D". It is refused when empty or when a "=" is anything else.
func scramName(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == ',':
			return "", false
		case s[i] != '=':
			b.WriteByte(s[i])
		case strings.HasPrefix(s[i:], "=2C"):
			b.WriteByte(',')
			i += 2
		case strings.HasPrefix(s[i:], "=3D"):
			b.WriteByte('=')
			i += 2
		default:
			return "", false
		}
	}
	return b.String(), true
}

// scramPrintable is a nonce: one or more printable ASCII characters, no comma.
func scramPrintable(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e || s[i] == ',' {
			return false
		}
	}
	return true
}

func isASCIILetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// scramFirst handles the client-first message and produces the server-first
// message (r=<nonce>,s=<salt>,i=<iters>), returning the conversation state to
// carry into the final step. Every refusal is the same answer but one (claims:
// auth A12, A13, A15): a message that is not a client-first message and a user
// that does not exist are "invalid credentials", and an authorization id that is
// not the user's own name is refused in so many words, after the user was looked
// up. As with PLAIN, nobody here acts as anybody else.
func (a *authenticator) scramFirst(mechanism string, in []byte) ([]byte, *scramConversation, *saslRefusal) {
	refuse := func(why string) ([]byte, *scramConversation, *saslRefusal) {
		return nil, nil, &saslRefusal{wire: msgScramRefused(mechanism), why: why}
	}
	first, ok := parseScramClientFirst(in)
	if !ok {
		return refuse("scram: malformed client-first message")
	}
	cred := a.scram[mechanism][first.user]
	if cred == nil {
		return refuse("scram: invalid credentials")
	}
	if first.authzid != "" && first.authzid != first.user {
		return nil, nil, &saslRefusal{wire: msgAuthzidDiffers, why: "scram: the authorization id is not the user"}
	}

	serverNonce, err := randomNonce()
	if err != nil {
		return refuse("scram: no random nonce: " + err.Error())
	}
	combined := first.nonce + serverNonce
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", combined, base64.StdEncoding.EncodeToString(cred.salt), cred.iterations)

	return []byte(serverFirst), &scramConversation{
		mechanism:       mechanism,
		newHash:         scramHashFor(mechanism),
		cred:            cred,
		clientFirstBare: first.bare,
		serverFirst:     serverFirst,
		combinedNonce:   combined,
		gs2Header:       first.gs2Header,
		user:            first.user,
	}, nil
}

// scramFinal verifies the client-final message's proof and produces the
// server-final message (v=<server-signature>). The message is exactly
// "c=<binding>,r=<nonce>,p=<proof>": the binding must be the GS2 header the
// exchange opened with, the nonce the one the server sent, and nothing may come
// between or after (Kafka signs what it expects, so an extension fails the proof
// there too; claim: auth A16). Kafka does not compare the binding with the header
// (claim: auth A17); this broker does, as RFC 5802 section 7 has a server do, and
// is the stricter of the two.
//
// A proof that is base64 but not the length of the hash ends the connection with
// no answer, as on Kafka (claim: auth A14); every other refusal is answered.
func scramFinal(conv *scramConversation, in []byte) ([]byte, *saslRefusal) {
	refuse := func(why string) ([]byte, *saslRefusal) {
		return nil, &saslRefusal{wire: msgScramRefused(conv.mechanism), why: why}
	}
	attrs := strings.Split(string(in), ",")
	if len(attrs) != 3 {
		return refuse("scram: malformed client-final message")
	}
	channel, okC := strings.CutPrefix(attrs[0], "c=")
	nonce, okR := strings.CutPrefix(attrs[1], "r=")
	proofB64, okP := strings.CutPrefix(attrs[2], "p=")
	if !okC || !okR || !okP {
		return refuse("scram: malformed client-final message")
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return refuse("scram: bad proof encoding")
	}
	if len(proof) != conv.newHash().Size() {
		return nil, &saslRefusal{why: "scram: proof length mismatch", silent: true}
	}
	if channel != base64.StdEncoding.EncodeToString([]byte(conv.gs2Header)) {
		return refuse("scram: channel binding mismatch")
	}
	if subtle.ConstantTimeCompare([]byte(nonce), []byte(conv.combinedNonce)) != 1 {
		return refuse("scram: nonce mismatch")
	}

	clientFinalWithoutProof := "c=" + channel + ",r=" + nonce
	authMessage := conv.clientFirstBare + "," + conv.serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSum(conv.newHash, conv.cred.storedKey, []byte(authMessage))
	clientKey := make([]byte, len(proof))
	for i := range clientKey {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}
	h := conv.newHash()
	h.Write(clientKey)
	if subtle.ConstantTimeCompare(h.Sum(nil), conv.cred.storedKey) != 1 {
		return refuse("scram: invalid credentials")
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
