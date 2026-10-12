package conformance

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios of who a connection is (conformance/claims/auth.md): what a real
// Apache Kafka answers to every SASL/PLAIN and SASL/SCRAM exchange a client can
// send, right or wrong, in order or out of it, and who the authorizer then takes
// the connection to be.
//
// Both brokers run an authorizer that refuses whatever no ACL allows, and two
// topics, each of which one user may describe. A Metadata request for both is
// therefore a question "who am I": the answer names the topic the connection's
// principal may see (0) and refuses the other (TOPIC_AUTHORIZATION_FAILED, 29).
// It is asked after every exchange that leaves a connection open, so an exchange
// that ends as another user than the one whose password was proven shows.

func init() {
	sasl := func(name string, run func(*Env), mechanisms []string, known []Known) Scenario {
		return Scenario{Name: name, Run: run, Users: saslUsers, Admin: "admin", Mechanisms: mechanisms, Known: known,
			KafkaEnv: []string{
				"KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
				"KAFKA_SUPER_USERS=User:admin;User:ANONYMOUS",
			},
			Doze: func(c *config.Config) { c.ACLDefault, c.ACLSuperUsers = "deny", []string{"User:admin"} }}
	}
	Scenarios = append(Scenarios,
		sasl("sasl-plain", saslPlain, nil, knownSASLPlain),
		sasl("sasl-scram", saslScram, []string{"PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"}, knownSASLScram),
	)
}

var saslUsers = map[string]string{"admin": "admin-secret", "alice": "alice-secret", "bob": "bob-secret"}

// What differs from Apache Kafka in the two scenarios, each the stricter answer
// (docs/limitations.md, "SASL details"), and the list of APIs in an ApiVersions
// answer, which is the API table's business and held to Kafka's elsewhere
// (internal/protocol: the profile test).
const (
	limSASLMidLogin = "A request sent in the middle of a login closes the connection without an answer."
	limSASLBinding  = "A SCRAM client-final message whose channel binding is not the header the exchange opened with is refused."
	limAPIList      = "ApiVersions lists the APIs and versions this broker serves, not Kafka's."
)

var (
	knownSASLPlain = []Known{
		{Step: "request-before-token-who", Limitation: limSASLMidLogin},
		{Step: "versions-first-versions", Path: "ApiKeys", Limitation: limAPIList},
		{Step: "versions-first-versions-after-login", Path: "ApiKeys", Limitation: limAPIList},
		{Step: "versions-twice-first", Path: "ApiKeys", Limitation: limAPIList},
	}
	knownSASLScram = []Known{
		{Step: "request-before-final-who", Limitation: limSASLMidLogin},
		{Step: "request-before-first-who", Limitation: limSASLMidLogin},
		// Kafka logs the connection in; this broker refuses the message and closes.
		{Step: "final-wrong-channel-binding-client-final", Limitation: limSASLBinding},
		{Step: "final-wrong-channel-binding-who", Limitation: limSASLBinding},
	}
)

// saslFixture makes the two topics and the two ACLs, as the admin, and waits until
// the broker lists both ACLs (Kafka applies them from its metadata log a moment
// after it has answered CreateAcls).
func saslFixture(e *Env) {
	e.Open("admin", "admin", saslUsers["admin"])
	e.Use("admin")
	e.Do("fixture-topics", createTopics(false, newTopic("for-alice", 1, 1), newTopic("for-bob", 1, 1)), 7)
	acls := kmsg.NewPtrCreateACLsRequest()
	for _, u := range []string{"alice", "bob"} {
		c := kmsg.NewCreateACLsRequestCreation()
		c.ResourceType, c.ResourceName, c.ResourcePatternType = kmsg.ACLResourceTypeTopic, "for-"+u, kmsg.ACLResourcePatternTypeLiteral
		c.Principal, c.Host = "User:"+u, "*"
		c.Operation, c.PermissionType = kmsg.ACLOperationDescribe, kmsg.ACLPermissionTypeAllow
		acls.Creations = append(acls.Creations, c)
	}
	e.Do("fixture-acls", acls, 3)
	d := kmsg.NewPtrDescribeACLsRequest()
	d.ResourceType, d.ResourcePatternType = kmsg.ACLResourceTypeTopic, kmsg.ACLResourcePatternTypeAny
	d.Operation, d.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	e.Await("both ACLs are listed", d, 3, func(r kmsg.Response) bool {
		return len(r.(*kmsg.DescribeACLsResponse).Resources) == 2
	})
}

// apiVersions is ApiVersions v0, which has no body: the request every client opens
// a connection with, before it logs in.
func apiVersions(e *Env, label string) {
	e.Do(label, kmsg.NewPtrApiVersionsRequest(), 0)
}

// whoAmI asks for both topics: see the top of this file.
func whoAmI(e *Env, label string) {
	e.Do(label, metadata(false, "for-alice", "for-bob"), 9)
}

// fresh opens a connection of its own, not logged in, and makes it the current one.
func fresh(e *Env, name string) {
	e.Open(name, "", "")
	e.Use(name)
}

func handshake(e *Env, label, mechanism string) {
	h := kmsg.NewPtrSASLHandshakeRequest()
	h.Mechanism = mechanism
	e.Do(label, h, 1)
}

func authenticate(e *Env, label string, token []byte, version int16) *kmsg.SASLAuthenticateResponse {
	a := kmsg.NewPtrSASLAuthenticateRequest()
	a.SASLAuthBytes = token
	return e.Do(label, a, version).(*kmsg.SASLAuthenticateResponse)
}

// --- SASL/PLAIN ---

func saslPlain(e *Env) {
	saslFixture(e)

	// One token, then "who am I", each on a connection of its own. A PLAIN token is
	// authzid NUL authcid NUL password (RFC 4616).
	for _, c := range []struct{ name, token string }{
		{"good", "\x00alice\x00alice-secret"},
		{"good-bob", "\x00bob\x00bob-secret"},
		{"authzid-same", "alice\x00alice\x00alice-secret"},
		{"authzid-other-user", "bob\x00alice\x00alice-secret"},
		{"authzid-admin", "admin\x00alice\x00alice-secret"},
		{"authzid-not-a-user", "mallory\x00alice\x00alice-secret"},
		{"authzid-other-user-wrong-password", "bob\x00alice\x00nope"},
		{"authzid-other-user-empty-password", "bob\x00alice\x00"},
		{"wrong-password", "\x00alice\x00nope"},
		{"other-users-password", "\x00alice\x00bob-secret"},
		{"password-prefix", "\x00alice\x00alice-secre"},
		{"password-longer", "\x00alice\x00alice-secret1"},
		{"unknown-user", "\x00mallory\x00alice-secret"},
		{"user-in-capitals", "\x00ALICE\x00alice-secret"},
		{"empty-password", "\x00alice\x00"},
		{"empty-user", "\x00\x00alice-secret"},
		{"empty-user-and-password", "\x00\x00"},
		{"no-nuls", "alice alice-secret"},
		{"two-parts", "alice\x00alice-secret"},
		{"four-parts", "\x00alice\x00alice-secret\x00extra"},
		{"trailing-nul", "\x00alice\x00alice-secret\x00"},
		{"empty-token", ""},
		{"not-utf8", "\x00alice\x00\xff\xfe\xfd"},
		{"token-of-64k", "\x00alice\x00" + strings.Repeat("x", 64<<10)},
	} {
		fresh(e, c.name)
		handshake(e, c.name+"-handshake", "PLAIN")
		authenticate(e, c.name+"-authenticate", []byte(c.token), 2)
		whoAmI(e, c.name+"-who")
	}
	good := []byte("\x00alice\x00alice-secret")

	// A mechanism the listener does not offer, and what may follow the refusal.
	for _, m := range []struct{ name, mechanism string }{
		{"unknown-mechanism", "BOGUS"}, {"lowercase-mechanism", "plain"}, {"empty-mechanism", ""}, {"scram-not-enabled", "SCRAM-SHA-256"},
	} {
		fresh(e, m.name)
		handshake(e, m.name+"-handshake", m.mechanism)
		authenticate(e, m.name+"-authenticate", good, 2)
		whoAmI(e, m.name+"-who")
	}
	fresh(e, "refused-then-right")
	handshake(e, "refused-then-right-handshake", "BOGUS")
	handshake(e, "refused-then-right-handshake-plain", "PLAIN")
	authenticate(e, "refused-then-right-authenticate", good, 2)
	whoAmI(e, "refused-then-right-who")

	// Out of order.
	fresh(e, "no-handshake")
	authenticate(e, "no-handshake-authenticate", good, 2)
	whoAmI(e, "no-handshake-who")
	fresh(e, "no-login")
	whoAmI(e, "no-login-who")
	fresh(e, "request-before-token")
	handshake(e, "request-before-token-handshake", "PLAIN")
	whoAmI(e, "request-before-token-who")
	whoAmI(e, "request-before-token-who-again")
	authenticate(e, "request-before-token-authenticate", good, 2)
	whoAmI(e, "request-before-token-who-at-the-end")

	// ApiVersions, which a client sends first, and where else it may come.
	fresh(e, "versions-first")
	apiVersions(e, "versions-first-versions")
	handshake(e, "versions-first-handshake", "PLAIN")
	authenticate(e, "versions-first-authenticate", good, 2)
	whoAmI(e, "versions-first-who")
	apiVersions(e, "versions-first-versions-after-login")
	whoAmI(e, "versions-first-who-at-the-end")
	fresh(e, "versions-before-token")
	handshake(e, "versions-before-token-handshake", "PLAIN")
	apiVersions(e, "versions-before-token-versions")
	authenticate(e, "versions-before-token-authenticate", good, 2)
	whoAmI(e, "versions-before-token-who")
	fresh(e, "versions-twice")
	apiVersions(e, "versions-twice-first")
	apiVersions(e, "versions-twice-second")
	handshake(e, "versions-twice-handshake", "PLAIN")
	authenticate(e, "versions-twice-authenticate", good, 2)
	whoAmI(e, "versions-twice-who")
	fresh(e, "handshake-twice")
	handshake(e, "handshake-twice-first", "PLAIN")
	handshake(e, "handshake-twice-second", "PLAIN")
	authenticate(e, "handshake-twice-authenticate", good, 2)
	whoAmI(e, "handshake-twice-who")

	// After a login: nothing changes who the connection is.
	fresh(e, "twice")
	handshake(e, "twice-handshake", "PLAIN")
	authenticate(e, "twice-authenticate", good, 2)
	whoAmI(e, "twice-who")
	authenticate(e, "twice-authenticate-again", good, 2)
	whoAmI(e, "twice-who-after-second-token")
	handshake(e, "twice-handshake-again", "PLAIN")
	authenticate(e, "twice-authenticate-as-bob", []byte("\x00bob\x00bob-secret"), 2)
	whoAmI(e, "twice-who-after-bob")
	authenticate(e, "twice-authenticate-wrong-password", []byte("\x00alice\x00nope"), 2)
	whoAmI(e, "twice-who-after-wrong-password")
	handshake(e, "twice-handshake-unknown-mechanism", "BOGUS")
	whoAmI(e, "twice-who-at-the-end")

	// The older versions of SaslAuthenticate.
	for _, v := range []int16{0, 1} {
		name := "authenticate-v" + string(rune('0'+v))
		fresh(e, name)
		handshake(e, name+"-handshake", "PLAIN")
		authenticate(e, name+"-good", good, v)
		whoAmI(e, name+"-who")
		authenticate(e, name+"-again", good, v)
		fresh(e, name+"-wrong")
		handshake(e, name+"-wrong-handshake", "PLAIN")
		authenticate(e, name+"-wrong-authenticate", []byte("\x00alice\x00nope"), v)
		whoAmI(e, name+"-wrong-who")
	}
}

// --- SASL/SCRAM (RFC 5802) ---

// ScramIterations and ScramSalt are the credentials the recorder gives Kafka's
// SCRAM users (targets.go): Kafka's default iteration count, and a salt made from
// the user's name so a recording is the same every time.
const ScramIterations = 4096

func ScramSalt(user string) []byte { return []byte("salt-of-" + user) }

func scramMechanismCode(mechanism string) int8 {
	if mechanism == "SCRAM-SHA-512" {
		return 2
	}
	return 1
}

func scramHash(mechanism string) func() hash.Hash {
	if mechanism == "SCRAM-SHA-512" {
		return sha512.New
	}
	return sha256.New
}

func scramSalted(mechanism, password string, salt []byte, iterations int) []byte {
	h := scramHash(mechanism)
	k, err := pbkdf2.Key(h, password, salt, iterations, h().Size())
	if err != nil {
		panic(err)
	}
	return k
}

func scramHMAC(h func() hash.Hash, key []byte, msg string) []byte {
	m := hmac.New(h, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// scramExchange is one client's side of an exchange on the current connection.
type scramExchange struct {
	e         *Env
	name      string // the connection, and the prefix of its step labels.
	mechanism string
	gs2       string // the GS2 header the client-first message opened with.
	bare      string // client-first-message-bare.
	nonce     string // the client's nonce.
	// From the server-first message, when the broker sent one.
	serverFirst string
	fullNonce   string
	salt        []byte
	iterations  int
}

// scramOpen opens a connection named name, offers mechanism in a SaslHandshake and
// returns the exchange. The client's nonce is made from the name, so no two are
// alike and none begins another.
func scramOpen(e *Env, name, mechanism string) *scramExchange {
	fresh(e, name)
	x := &scramExchange{e: e, name: name, mechanism: mechanism, nonce: "nonce-of-" + name + "."}
	e.ClientNonce(x.nonce)
	handshake(e, name+"-handshake", mechanism)
	return x
}

// first sends "<gs2><bare>" as the client-first message and keeps what a
// server-first message in the answer says.
func (x *scramExchange) first(label, gs2, bare string) *kmsg.SASLAuthenticateResponse {
	x.gs2, x.bare = gs2, bare
	resp := authenticate(x.e, x.name+"-"+label, []byte(gs2+bare), 2)
	x.readServerFirst(resp)
	return resp
}

// firstAs is the ordinary client-first message for user: no channel binding, no
// authorization id.
func (x *scramExchange) firstAs(user string) *kmsg.SASLAuthenticateResponse {
	return x.first("client-first", "n,,", "n="+user+",r="+x.nonce)
}

func (x *scramExchange) readServerFirst(resp *kmsg.SASLAuthenticateResponse) {
	x.serverFirst, x.fullNonce, x.salt, x.iterations = "", "", nil, 0
	if resp == nil || resp.ErrorCode != 0 || len(resp.SASLAuthBytes) == 0 {
		return
	}
	x.serverFirst = string(resp.SASLAuthBytes)
	for _, a := range strings.Split(x.serverFirst, ",") {
		switch {
		case strings.HasPrefix(a, "r="):
			x.fullNonce = a[2:]
		case strings.HasPrefix(a, "s="):
			x.salt, _ = base64.StdEncoding.DecodeString(a[2:])
		case strings.HasPrefix(a, "i="):
			for _, d := range a[2:] {
				x.iterations = x.iterations*10 + int(d-'0')
			}
		}
	}
}

// proof is the client proof for password over withoutProof (the client-final
// message up to its proof), and the server signature a broker that knows the
// password answers with.
func (x *scramExchange) proof(password, withoutProof string) (proof, signature string) {
	h := scramHash(x.mechanism)
	salted := scramSalted(x.mechanism, password, x.salt, x.iterations)
	clientKey := scramHMAC(h, salted, "Client Key")
	sum := h()
	sum.Write(clientKey)
	storedKey := sum.Sum(nil)
	authMessage := x.bare + "," + x.serverFirst + "," + withoutProof
	clientSig := scramHMAC(h, storedKey, authMessage)
	p := make([]byte, len(clientKey))
	for i := range p {
		p[i] = clientKey[i] ^ clientSig[i]
	}
	serverKey := scramHMAC(h, salted, "Server Key")
	return base64.StdEncoding.EncodeToString(p), base64.StdEncoding.EncodeToString(scramHMAC(h, serverKey, authMessage))
}

// finalMessage is the client-final message for password, with the channel binding
// and nonce given, and the proof named in the recording for the password it proves.
// Without a server-first message (the broker refused the first, or closed) there is
// nothing to prove against and the proof is a placeholder of the right shape.
func (x *scramExchange) finalMessage(password, binding, nonce string) (msg, signature string) {
	without := "c=" + binding + ",r=" + nonce
	if x.serverFirst == "" {
		return without + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, scramHash(x.mechanism)().Size())), ""
	}
	proof, signature := x.proof(password, without)
	x.e.Scrub(proof, "<proof of "+password+">")
	return without + ",p=" + proof, signature
}

func (x *scramExchange) binding() string { return base64.StdEncoding.EncodeToString([]byte(x.gs2)) }

// finalWith sends the ordinary client-final message for password, and fails the run
// if a broker that accepted it did not prove it knows the password too.
func (x *scramExchange) finalWith(label, password string) *kmsg.SASLAuthenticateResponse {
	nonce := x.fullNonce
	if nonce == "" {
		nonce = x.nonce
	}
	msg, signature := x.finalMessage(password, x.binding(), nonce)
	resp := authenticate(x.e, x.name+"-"+label, []byte(msg), 2)
	if resp.ErrorCode == 0 && signature != "" && !x.e.lastWasClosed() && string(resp.SASLAuthBytes) != "v="+signature {
		x.e.fail("%s-%s: the server signature is %q, the password gives v=%s", x.name, label, resp.SASLAuthBytes, signature)
	}
	return resp
}

func (x *scramExchange) who() { whoAmI(x.e, x.name+"-who") }

func saslScram(e *Env) {
	saslFixture(e)
	const m256, m512 = "SCRAM-SHA-256", "SCRAM-SHA-512"

	// The exchanges a client makes, on each mechanism.
	for _, m := range []struct{ tag, mechanism string }{{"256", m256}, {"512", m512}} {
		x := scramOpen(e, "good-"+m.tag, m.mechanism)
		x.firstAs("alice")
		x.finalWith("client-final", "alice-secret")
		x.who()

		x = scramOpen(e, "wrong-password-"+m.tag, m.mechanism)
		x.firstAs("alice")
		x.finalWith("client-final", "nope")
		x.who()

		x = scramOpen(e, "other-users-password-"+m.tag, m.mechanism)
		x.firstAs("alice")
		x.finalWith("client-final", "bob-secret")
		x.who()

		x = scramOpen(e, "unknown-user-"+m.tag, m.mechanism)
		x.firstAs("mallory")
		x.finalWith("client-final", "alice-secret")
		x.who()

		// An authorization id (a=) in the GS2 header: the user's own, and another's.
		x = scramOpen(e, "authzid-same-"+m.tag, m.mechanism)
		x.first("client-first", "n,a=alice,", "n=alice,r="+x.nonce)
		x.finalWith("client-final", "alice-secret")
		x.who()

		x = scramOpen(e, "authzid-other-user-"+m.tag, m.mechanism)
		x.first("client-first", "n,a=bob,", "n=alice,r="+x.nonce)
		x.finalWith("client-final", "alice-secret")
		x.who()

		x = scramOpen(e, "authzid-admin-"+m.tag, m.mechanism)
		x.first("client-first", "n,a=admin,", "n=alice,r="+x.nonce)
		x.finalWith("client-final", "alice-secret")
		x.who()

		// Which is looked at first, the user or the authorization id.
		x = scramOpen(e, "authzid-other-user-unknown-user-"+m.tag, m.mechanism)
		x.first("client-first", "n,a=bob,", "n=mallory,r="+x.nonce)
		x.who()
	}

	// A client-first message that is not one.
	for _, c := range []struct{ name, gs2, bare string }{
		{"first-empty", "", ""},
		{"first-garbage", "", "this is not a scram message"},
		{"first-no-gs2-header", "", "n=alice,r=%NONCE%"},
		{"first-no-nonce", "n,,", "n=alice"},
		{"first-empty-nonce", "n,,", "n=alice,r="},
		{"first-no-user", "n,,", "r=%NONCE%"},
		{"first-empty-user", "n,,", "n=,r=%NONCE%"},
		{"first-nonce-before-user", "n,,", "r=%NONCE%,n=alice"},
		{"first-channel-binding-required", "p=tls-unique,,", "n=alice,r=%NONCE%"},
		{"first-channel-binding-y", "y,,", "n=alice,r=%NONCE%"},
		{"first-unknown-gs2-flag", "x,,", "n=alice,r=%NONCE%"},
		{"first-mandatory-extension", "n,,", "m=ext,n=alice,r=%NONCE%"},
		{"first-optional-extension", "n,,", "n=alice,r=%NONCE%,x=ext"},
		{"first-user-bad-escape", "n,,", "n=al=ice,r=%NONCE%"},
		{"first-user-escaped", "n,,", "n=al=2Cice,r=%NONCE%"},
		{"first-user-escaped-equals-sign-as-alice", "n,,", "n=alic=65,r=%NONCE%"},
		{"first-nonce-with-comma", "n,,", "n=alice,r=%NONCE%,,"},
		{"first-not-utf8", "n,,", "n=alice,r=%NONCE%\xff"},
		{"first-plain-token", "", "\x00alice\x00alice-secret"},
	} {
		x := scramOpen(e, c.name, m256)
		x.first("client-first", c.gs2, strings.ReplaceAll(c.bare, "%NONCE%", x.nonce))
		x.finalWith("client-final", "alice-secret")
		x.who()
	}

	// A client-final message that is not one, after a good client-first.
	for _, c := range []struct {
		name string
		make func(x *scramExchange) string
	}{
		{"final-empty", func(x *scramExchange) string { return "" }},
		{"final-garbage", func(x *scramExchange) string { return "this is not a scram message" }},
		{"final-no-proof", func(x *scramExchange) string { return "c=" + x.binding() + ",r=" + x.fullNonce }},
		{"final-proof-not-base64", func(x *scramExchange) string { return "c=" + x.binding() + ",r=" + x.fullNonce + ",p=!!!not-base64!!!" }},
		{"final-proof-too-short", func(x *scramExchange) string { return "c=" + x.binding() + ",r=" + x.fullNonce + ",p=AAAA" }},
		{"final-proof-empty", func(x *scramExchange) string { return "c=" + x.binding() + ",r=" + x.fullNonce + ",p=" }},
		{"final-no-nonce", func(x *scramExchange) string {
			without := "c=" + x.binding()
			p, _ := x.proof("alice-secret", without)
			x.e.Scrub(p, "<proof of alice-secret>")
			return without + ",p=" + p
		}},
		{"final-no-channel-binding", func(x *scramExchange) string {
			without := "r=" + x.fullNonce
			p, _ := x.proof("alice-secret", without)
			x.e.Scrub(p, "<proof of alice-secret>")
			return without + ",p=" + p
		}},
		{"final-wrong-channel-binding", func(x *scramExchange) string {
			// "y,," where the exchange opened with "n,,".
			m, _ := x.finalMessage("alice-secret", base64.StdEncoding.EncodeToString([]byte("y,,")), x.fullNonce)
			return m
		}},
		{"final-channel-binding-not-base64", func(x *scramExchange) string {
			m, _ := x.finalMessage("alice-secret", "!!!", x.fullNonce)
			return m
		}},
		{"final-wrong-nonce", func(x *scramExchange) string {
			m, _ := x.finalMessage("alice-secret", x.binding(), x.nonce+"not-the-servers-half")
			return m
		}},
		{"final-client-nonce-only", func(x *scramExchange) string {
			m, _ := x.finalMessage("alice-secret", x.binding(), x.nonce)
			return m
		}},
		{"final-nonce-with-suffix", func(x *scramExchange) string {
			m, _ := x.finalMessage("alice-secret", x.binding(), x.fullNonce+"x")
			return m
		}},
		{"final-extension-before-proof", func(x *scramExchange) string {
			without := "c=" + x.binding() + ",r=" + x.fullNonce + ",x=ext"
			p, _ := x.proof("alice-secret", without)
			x.e.Scrub(p, "<proof of alice-secret>")
			return without + ",p=" + p
		}},
		{"final-trailing-attribute", func(x *scramExchange) string {
			m, _ := x.finalMessage("alice-secret", x.binding(), x.fullNonce)
			return m + ",x=ext"
		}},
	} {
		x := scramOpen(e, c.name, m256)
		x.firstAs("alice")
		authenticate(e, c.name+"-client-final", []byte(c.make(x)), 2)
		x.who()
	}

	// A final message sent again, on the connection it logged in and on another.
	x := scramOpen(e, "replay", m256)
	x.firstAs("alice")
	replayed, _ := x.finalMessage("alice-secret", x.binding(), x.fullNonce)
	authenticate(e, "replay-client-final", []byte(replayed), 2)
	x.who()
	authenticate(e, "replay-client-final-again", []byte(replayed), 2)
	whoAmI(e, "replay-who-after")
	other := scramOpen(e, "replay-elsewhere", m256)
	other.firstAs("alice")
	authenticate(e, "replay-elsewhere-client-final", []byte(replayed), 2)
	other.who()
	// The whole of another user's exchange after one's own: bob's first and final
	// messages on the connection alice logged in.
	x = scramOpen(e, "reauth", m256)
	x.firstAs("alice")
	x.finalWith("client-final", "alice-secret")
	x.who()
	x.first("client-first-as-bob", "n,,", "n=bob,r="+x.nonce)
	x.finalWith("client-final-as-bob", "bob-secret")
	whoAmI(e, "reauth-who-after-bob")
	handshake(e, "reauth-handshake-again", m256)
	x.first("client-first-after-handshake", "n,,", "n=bob,r="+x.nonce)
	whoAmI(e, "reauth-who-after-handshake")
	handshake(e, "reauth-handshake-plain", "PLAIN")
	authenticate(e, "reauth-plain-as-bob", []byte("\x00bob\x00bob-secret"), 2)
	whoAmI(e, "reauth-who-at-the-end")

	// Another mechanism in the middle of an exchange.
	x = scramOpen(e, "switch-to-plain", m256)
	x.firstAs("alice")
	handshake(e, "switch-to-plain-handshake-plain", "PLAIN")
	authenticate(e, "switch-to-plain-token", []byte("\x00bob\x00bob-secret"), 2)
	x.who()
	x = scramOpen(e, "switch-to-512", m256)
	x.firstAs("alice")
	handshake(e, "switch-to-512-handshake-512", m512)
	x.finalWith("client-final", "alice-secret")
	x.who()
	x = scramOpen(e, "handshake-256-then-512", m256)
	handshake(e, "handshake-256-then-512-second", m512)
	x.mechanism = m512
	x.firstAs("alice")
	x.finalWith("client-final", "alice-secret")
	x.who()
	// The proof of one mechanism under the other's handshake.
	x = scramOpen(e, "proof-of-512-under-256", m256)
	x.firstAs("alice")
	x.mechanism = m512
	x.finalWith("client-final", "alice-secret")
	x.who()
	// A PLAIN token where the client-final message belongs.
	x = scramOpen(e, "plain-token-as-final", m256)
	x.firstAs("alice")
	authenticate(e, "plain-token-as-final-token", []byte("\x00alice\x00alice-secret"), 2)
	x.who()

	// Out of order.
	x = scramOpen(e, "request-before-final", m256)
	x.firstAs("alice")
	x.who()
	whoAmI(e, "request-before-final-who-again")
	x.finalWith("client-final", "alice-secret")
	whoAmI(e, "request-before-final-who-at-the-end")
	x = scramOpen(e, "request-before-first", m256)
	x.who()
	x.firstAs("alice")
	x.finalWith("client-final", "alice-secret")
	whoAmI(e, "request-before-first-who-at-the-end")
	x = scramOpen(e, "versions-before-final", m256)
	x.firstAs("alice")
	apiVersions(e, "versions-before-final-versions")
	x.finalWith("client-final", "alice-secret")
	x.who()
	x = scramOpen(e, "final-without-first", m256)
	x.gs2 = "n,,"
	x.finalWith("client-final", "alice-secret")
	x.who()
	x = scramOpen(e, "first-twice", m256)
	x.firstAs("alice")
	x.first("client-first-again", "n,,", "n=alice,r="+x.nonce)
	x.finalWith("client-final", "alice-secret")
	x.who()
	fresh(e, "scram-no-handshake")
	authenticate(e, "scram-no-handshake-client-first", []byte("n,,n=alice,r=nonce-with-no-handshake"), 2)
	whoAmI(e, "scram-no-handshake-who")
	// A third message after the exchange is over.
	x = scramOpen(e, "third-message", m256)
	x.firstAs("alice")
	x.finalWith("client-final", "alice-secret")
	authenticate(e, "third-message-empty", nil, 2)
	x.who()

	// The older versions of SaslAuthenticate.
	for _, v := range []int16{0, 1} {
		name := "scram-authenticate-v" + string(rune('0'+v))
		x := scramOpen(e, name, m256)
		x.gs2, x.bare = "n,,", "n=alice,r="+x.nonce
		x.readServerFirst(authenticate(e, name+"-client-first", []byte(x.gs2+x.bare), v))
		msg, _ := x.finalMessage("alice-secret", x.binding(), x.fullNonce)
		authenticate(e, name+"-client-final", []byte(msg), v)
		x.who()
		x = scramOpen(e, name+"-wrong", m256)
		x.gs2, x.bare = "n,,", "n=alice,r="+x.nonce
		x.readServerFirst(authenticate(e, name+"-wrong-client-first", []byte(x.gs2+x.bare), v))
		msg, _ = x.finalMessage("nope", x.binding(), x.fullNonce)
		authenticate(e, name+"-wrong-client-final", []byte(msg), v)
		x.who()
	}
}
