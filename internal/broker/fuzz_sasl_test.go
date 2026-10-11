package broker

import (
	"bytes"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Foundation B12 (and access finding ACC-16): the SASL messages are bytes a client
// sends before it has authenticated, so each parser has a fuzz target. The
// property is stronger than "never panics": a step reports success only for the
// one input that is a correct proof for the configured password.

const fuzzUser, fuzzPassword = "alice", "s3cret"

func fuzzAuthenticator(t testing.TB) *authenticator {
	cfg := config.Default()
	cfg.SASLMechanisms = []string{config.SASLPlain, config.SASLScramSHA256}
	cfg.Credentials = map[string]string{fuzzUser: fuzzPassword}
	a, err := newAuthenticator(cfg)
	if err != nil || a == nil {
		t.Fatalf("authenticator: %v", err)
	}
	return a
}

func FuzzSASLPlain(f *testing.F) {
	a := fuzzAuthenticator(f)
	f.Add([]byte("\x00alice\x00s3cret"))
	f.Add([]byte("authz\x00alice\x00s3cret"))
	f.Add([]byte("\x00alice\x00wrong"))
	f.Add([]byte("\x00\x00"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, in []byte) {
		out, done, user, err := a.plainStep(in)
		if err != nil {
			if done || user != "" || out != nil {
				t.Fatalf("an error with done=%v user=%q out=%q", done, user, out)
			}
			return
		}
		parts := bytes.Split(in, []byte{0})
		if !done || user != fuzzUser || len(parts) != 3 || string(parts[1]) != fuzzUser || string(parts[2]) != fuzzPassword {
			t.Fatalf("PLAIN accepted %q as user %q (done=%v)", in, user, done)
		}
	})
}

func FuzzSCRAMClientFirst(f *testing.F) {
	a := fuzzAuthenticator(f)
	f.Add([]byte("n,,n=alice,r=rOprNGfwEbeRWgbNEkqO"))
	f.Add([]byte("y,a=other,n=alice,r=abc"))
	f.Add([]byte("p=tls-unique,,n=alice,r=abc"))
	f.Add([]byte("n,,n=mallory,r=abc"))
	f.Add([]byte("n,,n=al=3Dice,r=abc"))
	f.Add([]byte(",,"))
	f.Fuzz(func(t *testing.T, in []byte) {
		out, conv, err := a.scramFirst(config.SASLScramSHA256, in)
		if err != nil {
			if out != nil || conv != nil {
				t.Fatalf("an error with a conversation: out=%q", out)
			}
			return
		}
		if conv == nil || conv.user != fuzzUser {
			t.Fatalf("a SCRAM conversation for %q from %q", conv.user, in)
		}
		attrs := scramAttrs(string(out))
		client := scramAttrs(conv.clientFirstBare)
		if !bytes.HasPrefix([]byte(attrs['r']), []byte(client['r'])) || len(attrs['r']) <= len(client['r']) {
			t.Fatalf("server nonce %q does not extend the client's %q", attrs['r'], client['r'])
		}
	})
}

// referenceProof is the client proof for a conversation, computed with none of the
// server's code: SaltedPassword, ClientKey, StoredKey, ClientSignature (RFC 5802).
func referenceProof(t testing.TB, conv *scramConversation, channel, nonce string) []byte {
	salted, err := pbkdf2.Key(sha256.New, fuzzPassword, conv.cred.salt, conv.cred.iterations, sha256.Size)
	if err != nil {
		t.Fatal(err)
	}
	mac := func(key []byte, msg string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(msg))
		return h.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	authMessage := conv.clientFirstBare + "," + conv.serverFirst + ",c=" + channel + ",r=" + nonce
	sig := mac(stored[:], authMessage)
	proof := make([]byte, len(sig))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	return proof
}

func FuzzSCRAMClientFinal(f *testing.F) {
	a := fuzzAuthenticator(f)
	_, conv, err := a.scramFirst(config.SASLScramSHA256, []byte("n,,n=alice,r=clientnonce"))
	if err != nil {
		f.Fatal(err)
	}
	channel := base64.StdEncoding.EncodeToString([]byte(conv.gs2Header))
	good := "c=" + channel + ",r=" + conv.combinedNonce + ",p=" + base64.StdEncoding.EncodeToString(referenceProof(f, conv, channel, conv.combinedNonce))
	f.Add([]byte(good))
	f.Add([]byte("c=" + channel + ",r=" + conv.combinedNonce + ",p="))
	f.Add([]byte("c=" + channel + ",r=other,p=AAAA"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, in []byte) {
		out, err := scramFinal(conv, in)
		if err != nil {
			if out != nil {
				t.Fatalf("an error with output %q", out)
			}
			return
		}
		attrs := scramAttrs(string(in))
		want := base64.StdEncoding.EncodeToString(referenceProof(t, conv, attrs['c'], attrs['r']))
		if attrs['c'] != channel || attrs['r'] != conv.combinedNonce || attrs['p'] != want {
			t.Fatalf("SCRAM final accepted %q, which is not the proof for the password", in)
		}
		if !bytes.HasPrefix(out, []byte("v=")) {
			t.Fatalf("server-final %q", out)
		}
	})
}

// scramUnescape and scramAttrs take the same bytes; between them a username is
// recovered. Whatever it is given they must not panic, and escaping then
// unescaping a name returns it.
func FuzzSCRAMUsernameEscaping(f *testing.F) {
	f.Add("al=2Cice")
	f.Add("a=3Db,c")
	f.Add("=")
	f.Fuzz(func(t *testing.T, name string) {
		escaped := bytes.ReplaceAll(bytes.ReplaceAll([]byte(name), []byte("="), []byte("=3D")), []byte(","), []byte("=2C"))
		if got := scramUnescape(string(escaped)); got != name {
			t.Fatalf("%q escaped to %q came back as %q", name, escaped, got)
		}
		scramAttrs(name)
	})
}
