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
		user, err := a.plainStep(in)
		if err != nil {
			if user != "" || err.wire == "" || err.silent {
				t.Fatalf("a refusal with user=%q, wire text %q, silent=%v", user, err.wire, err.silent)
			}
			return
		}
		// The only tokens that log in: the user and the password, with no
		// authorization id or the user's own name as one.
		parts := bytes.Split(in, []byte{0})
		if user != fuzzUser || len(parts) != 3 || string(parts[1]) != fuzzUser || string(parts[2]) != fuzzPassword ||
			(len(parts[0]) != 0 && string(parts[0]) != fuzzUser) {
			t.Fatalf("PLAIN accepted %q as user %q", in, user)
		}
	})
}

func FuzzSCRAMClientFirst(f *testing.F) {
	a := fuzzAuthenticator(f)
	f.Add([]byte("n,,n=alice,r=rOprNGfwEbeRWgbNEkqO"))
	f.Add([]byte("y,a=other,n=alice,r=abc"))
	f.Add([]byte("n,a=other,n=alice,r=abc"))
	f.Add([]byte("n,a=alice,n=alice,r=abc"))
	f.Add([]byte("n,,m=x,n=alice,r=abc,e=xt"))
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
		// What was accepted is a client-first message with no channel binding, for
		// the one user, as nobody else: the header is "n,," or "n,a=alice,".
		if conv.gs2Header != "n,," && conv.gs2Header != "n,a="+fuzzUser+"," {
			t.Fatalf("a SCRAM conversation opened by the header %q (from %q)", conv.gs2Header, in)
		}
		if !bytes.HasPrefix(in, []byte(conv.gs2Header)) || !bytes.HasSuffix(in, []byte(conv.clientFirstBare)) {
			t.Fatalf("header %q and bare %q are not the ends of %q", conv.gs2Header, conv.clientFirstBare, in)
		}
		attrs := fuzzAttrs(string(out))
		client := fuzzAttrs(conv.clientFirstBare)
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
		attrs := fuzzAttrs(string(in))
		want := base64.StdEncoding.EncodeToString(referenceProof(t, conv, attrs['c'], attrs['r']))
		if attrs['c'] != channel || attrs['r'] != conv.combinedNonce || attrs['p'] != want {
			t.Fatalf("SCRAM final accepted %q, which is not the proof for the password", in)
		}
		if !bytes.HasPrefix(out, []byte("v=")) {
			t.Fatalf("server-final %q", out)
		}
	})
}

// fuzzAttrs reads a comma-separated SCRAM message into its single-letter
// attributes, with none of the server's code.
func fuzzAttrs(s string) map[byte]string {
	out := make(map[byte]string)
	for _, part := range bytes.Split([]byte(s), []byte(",")) {
		if len(part) >= 2 && part[1] == '=' {
			out[part[0]] = string(part[2:])
		}
	}
	return out
}

// A username is escaped on the wire ("," as "=2C", "=" as "=3D"). Whatever
// scramName is given it must not panic; escaping then unescaping a name returns
// it; and a name it accepts holds no bare comma and no "=" that is not one of the
// two escapes.
func FuzzSCRAMUsernameEscaping(f *testing.F) {
	f.Add("al=2Cice")
	f.Add("a=3Db,c")
	f.Add("=")
	f.Fuzz(func(t *testing.T, name string) {
		escaped := bytes.ReplaceAll(bytes.ReplaceAll([]byte(name), []byte("="), []byte("=3D")), []byte(","), []byte("=2C"))
		got, ok := scramName(string(escaped))
		if name == "" {
			if ok {
				t.Fatal("an empty name was accepted")
			}
		} else if !ok || got != name {
			t.Fatalf("%q escaped to %q came back as %q (ok=%v)", name, escaped, got, ok)
		}
		if raw, ok := scramName(name); ok {
			again := bytes.ReplaceAll(bytes.ReplaceAll([]byte(raw), []byte("="), []byte("=3D")), []byte(","), []byte("=2C"))
			if string(again) != name {
				t.Fatalf("scramName accepted %q as %q, which escapes to %q", name, raw, again)
			}
		}
	})
}
