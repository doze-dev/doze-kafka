package broker

import (
	"encoding/base64"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The recordings (conformance/recordings/sasl-scram.json) pin what a client is
// answered. These hold the parts of RFC 5802's grammar a recording cannot tell
// apart from "that user does not exist": the cases below are refused when the
// user they would name is a user.

func scramAuthenticator(t *testing.T, users map[string]string) *authenticator {
	t.Helper()
	cfg := config.Default()
	cfg.SASLMechanisms = []string{config.SASLScramSHA256}
	cfg.Credentials = users
	a, err := newAuthenticator(cfg)
	if err != nil || a == nil {
		t.Fatalf("authenticator: %v", err)
	}
	return a
}

func TestSCRAMClientFirstIsHeldToTheGrammar(t *testing.T) {
	// Each user here is what a lenient reading of some message below would name.
	a := scramAuthenticator(t, map[string]string{"alice": "pw", "al=ice": "pw", "al,ice": "pw", "al=3": "pw", "a": "pw"})
	for _, c := range []struct {
		msg  string
		user string // "" = refused.
		bare string
	}{
		{"n,,n=alice,r=abc", "alice", "n=alice,r=abc"},
		{"n,a=alice,n=alice,r=abc", "alice", "n=alice,r=abc"},
		{"n,,n=al=2Cice,r=abc", "al,ice", "n=al=2Cice,r=abc"},
		{"n,,n=al=3Dice,r=abc", "al=ice", "n=al=3Dice,r=abc"},
		{"n,,n=alice,r=abc,x=ext", "alice", "n=alice,r=abc,x=ext"},
		// A reserved m= attribute is not part of what is signed (recorded: a client
		// that sends one fails its proof on Kafka).
		{"n,,m=ext,n=alice,r=abc", "alice", "n=alice,r=abc"},

		// RFC 5802 5.1: "=" not followed by 2C or 3D fails the authentication.
		{"n,,n=al=ice,r=abc", "", ""},
		{"n,,n=al=3,r=abc", "", ""},
		{"n,,n=al=2cice,r=abc", "", ""},
		// A comma cannot be in a name unescaped: this is user "a" and then garbage.
		{"n,,n=a,l,r=abc", "", ""},
		// The authorization id is held to the same grammar, and to the user.
		{"n,a=al=ice,n=al=3Dice,r=abc", "", ""},
		{"n,a=al=3Dice,n=al=3Dice,r=abc", "al=ice", "n=al=3Dice,r=abc"},
		{"n,a=,n=alice,r=abc", "", ""},
		{"n,b=alice,n=alice,r=abc", "", ""},
		{"n,a=a,n=alice,r=abc", "", ""},
		// The flag is "n" and nothing else.
		{"y,,n=alice,r=abc", "", ""},
		{"p=tls-unique,,n=alice,r=abc", "", ""},
		{"N,,n=alice,r=abc", "", ""},
		{"n=alice,r=abc", "", ""},
		// The user, then the nonce, each present and not empty.
		{"n,,r=abc,n=alice", "", ""},
		{"n,,n=alice", "", ""},
		{"n,,n=alice,r=", "", ""},
		{"n,,n=,r=abc", "", ""},
		{"n,,n=alice,r=a b", "", ""},
		{"n,,n=alice,r=abc\x7f", "", ""},
		// An extension is a letter, "=", a value.
		{"n,,n=alice,r=abc,", "", ""},
		{"n,,n=alice,r=abc,x", "", ""},
		{"n,,n=alice,r=abc,x=", "", ""},
		{"n,,n=alice,r=abc,1=x", "", ""},
		{"n,,n=alice,r=abc\xff", "", ""},
		{"", "", ""},
	} {
		out, conv, refusal := a.scramFirst(config.SASLScramSHA256, []byte(c.msg))
		if c.user == "" {
			if refusal == nil {
				t.Errorf("%q was accepted, as user %q", c.msg, conv.user)
			} else if out != nil || conv != nil || refusal.silent || refusal.wire == "" {
				t.Errorf("%q: a refusal with out=%q conv=%v silent=%v wire=%q", c.msg, out, conv, refusal.silent, refusal.wire)
			}
			continue
		}
		if refusal != nil {
			t.Errorf("%q was refused: %v", c.msg, refusal)
			continue
		}
		if conv.user != c.user || conv.clientFirstBare != c.bare {
			t.Errorf("%q: user %q and bare %q, want %q and %q", c.msg, conv.user, conv.clientFirstBare, c.user, c.bare)
		}
	}
}

// An authorization id that is not the user is refused in Kafka's words, and only
// once the user is known to exist (recorded: authzid-other-user-unknown-user-256).
func TestSCRAMAuthorizationIDMustBeTheUser(t *testing.T) {
	a := scramAuthenticator(t, map[string]string{"alice": "pw", "bob": "pw"})
	_, _, refusal := a.scramFirst(config.SASLScramSHA256, []byte("n,a=bob,n=alice,r=abc"))
	if refusal == nil || refusal.wire != msgAuthzidDiffers {
		t.Fatalf("alice as bob: %+v, want the refusal %q", refusal, msgAuthzidDiffers)
	}
	_, _, refusal = a.scramFirst(config.SASLScramSHA256, []byte("n,a=bob,n=mallory,r=abc"))
	if refusal == nil || refusal.wire != msgScramRefused(config.SASLScramSHA256) {
		t.Fatalf("an unknown user as bob: %+v, want the refusal %q", refusal, msgScramRefused(config.SASLScramSHA256))
	}
}

// The client-final message is exactly c=, r=, p=: the binding the exchange opened
// with, the nonce the server sent, a proof of the hash's length.
func TestSCRAMClientFinalIsHeldToTheGrammar(t *testing.T) {
	a := scramAuthenticator(t, map[string]string{fuzzUser: fuzzPassword})
	open := func() (*scramConversation, string, string) {
		_, conv, refusal := a.scramFirst(config.SASLScramSHA256, []byte("n,,n=alice,r=clientnonce"))
		if refusal != nil {
			t.Fatal(refusal)
		}
		channel := base64.StdEncoding.EncodeToString([]byte(conv.gs2Header))
		return conv, channel, base64.StdEncoding.EncodeToString(referenceProof(t, conv, channel, conv.combinedNonce))
	}
	conv, channel, proof := open()
	if out, refusal := scramFinal(conv, []byte("c="+channel+",r="+conv.combinedNonce+",p="+proof)); refusal != nil || len(out) < 3 {
		t.Fatalf("the right proof was refused: %v", refusal)
	}
	yBinding := base64.StdEncoding.EncodeToString([]byte("y,,"))
	yProof := base64.StdEncoding.EncodeToString(referenceProof(t, conv, yBinding, conv.combinedNonce))
	for name, c := range map[string]struct {
		msg    string
		silent bool
	}{
		"an extension before the proof":  {"c=" + channel + ",r=" + conv.combinedNonce + ",x=ext,p=" + proof, false},
		"an attribute after the proof":   {"c=" + channel + ",r=" + conv.combinedNonce + ",p=" + proof + ",x=ext", false},
		"no nonce":                       {"c=" + channel + ",p=" + proof, false},
		"no binding":                     {"r=" + conv.combinedNonce + ",p=" + proof, false},
		"the client's half of the nonce": {"c=" + channel + ",r=clientnonce,p=" + proof, false},
		"a longer nonce":                 {"c=" + channel + ",r=" + conv.combinedNonce + "x,p=" + proof, false},
		// Kafka accepts this one (recorded: final-wrong-channel-binding); RFC 5802 7
		// has the server check it, and so does this broker.
		"another header's binding, proven": {"c=" + yBinding + ",r=" + conv.combinedNonce + ",p=" + yProof, false},
		"a proof that is not base64":       {"c=" + channel + ",r=" + conv.combinedNonce + ",p=!!!", false},
		"a short proof":                    {"c=" + channel + ",r=" + conv.combinedNonce + ",p=AAAA", true},
		"an empty proof":                   {"c=" + channel + ",r=" + conv.combinedNonce + ",p=", true},
		"a proof of the other hash":        {"c=" + channel + ",r=" + conv.combinedNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 64)), true},
		"a wrong proof":                    {"c=" + channel + ",r=" + conv.combinedNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, 32)), false},
	} {
		out, refusal := scramFinal(conv, []byte(c.msg))
		switch {
		case refusal == nil:
			t.Errorf("%s: accepted", name)
		case out != nil:
			t.Errorf("%s: a refusal with output %q", name, out)
		case refusal.silent != c.silent:
			t.Errorf("%s: silent=%v, want %v", name, refusal.silent, c.silent)
		case !c.silent && refusal.wire != msgScramRefused(config.SASLScramSHA256):
			t.Errorf("%s: the client is told %q", name, refusal.wire)
		}
	}
}
