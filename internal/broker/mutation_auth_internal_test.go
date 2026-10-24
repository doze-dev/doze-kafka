package broker

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Tests that pin doze's own behaviour where a mutation of the auth units had lived.

// Describe comes with every operation that reads or changes what it describes
// (the comment on implied): Delete and Alter as much as Read and Write.
func TestDescribeComesWithDeleteAndAlter(t *testing.T) {
	for _, stored := range []int8{opRead, opWrite, opDelete, opAlter} {
		if !implied(stored, opDescribe) {
			t.Errorf("operation %d does not grant Describe", stored)
		}
	}
	for _, stored := range []int8{opCreate, opDescribeConfig, opAlterConfigs, opIdempotent} {
		if implied(stored, opDescribe) {
			t.Errorf("operation %d grants Describe", stored)
		}
	}
	if !implied(opAlterConfigs, opDescribeConfig) || implied(opAlter, opDescribeConfig) {
		t.Error("only AlterConfigs comes with DescribeConfigs")
	}
}

// An explicit deny on the cluster is the one named, or ALL, for the principal and
// host it names.
func TestDeniedOnClusterIsTheNamedOperationOrAll(t *testing.T) {
	const alice, bob, host = "User:alice", "User:bob", "10.0.0.5"
	cluster := func(p string, op, perm int8) meta.ACL {
		return acl(resCluster, clusterResourceName, aclLiteral, p, "*", op, perm)
	}
	for _, tc := range []struct {
		name string
		a    *authorizer
		who  string
		want bool
	}{
		{"the operation itself", authz("allow", nil, cluster(alice, opIdempotent, permDeny)), alice, true},
		{"ALL", authz("allow", nil, cluster(alice, opAll, permDeny)), alice, true},
		{"another operation", authz("allow", nil, cluster(alice, opAlter, permDeny)), alice, false},
		{"an allow of the operation", authz("allow", nil, cluster(alice, opIdempotent, permAllow)), alice, false},
		{"an allow of ALL", authz("allow", nil, cluster(alice, opAll, permAllow)), alice, false},
		{"another principal", authz("allow", nil, cluster(alice, opIdempotent, permDeny)), bob, false},
		{"no ACLs", authz("allow", nil), alice, false},
	} {
		if got := tc.a.deniedOnCluster(tc.who, host, opIdempotent); got != tc.want {
			t.Errorf("%s: denied %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The port Metadata advertises: the override, else the port the listener bound,
// else the listen address's, else 9092.
func TestAdvertisedPortOrder(t *testing.T) {
	bind := func(b *Broker, port int) { b.SetBoundAddr(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}) }
	for _, tc := range []struct {
		name  string
		cfg   config.Config
		bound int // 0: SetBoundAddr is not called.
		want  int32
	}{
		{"the override beats the bound port", config.Config{AdvertisedPort: 7000, ListenAddr: "127.0.0.1:9100"}, 8000, 7000},
		{"the bound port beats the listen address", config.Config{ListenAddr: "127.0.0.1:9100"}, 8000, 8000},
		{"the listen address", config.Config{ListenAddr: "127.0.0.1:9100"}, 0, 9100},
		{"listen port 0 is no port", config.Config{ListenAddr: "127.0.0.1:0"}, 0, 9092},
		{"a listen port that is not a number", config.Config{ListenAddr: "127.0.0.1:http"}, 0, 9092},
		{"a listen address with no port", config.Config{ListenAddr: "localhost"}, 0, 9092},
		{"no listen address", config.Config{}, 0, 9092},
	} {
		b := &Broker{cfg: tc.cfg}
		if tc.bound > 0 {
			bind(b, tc.bound)
		}
		if got := b.advertisedPort(); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
	}
	// A listener that bound port 0, or an address that is not TCP, teaches nothing.
	b := &Broker{cfg: config.Config{ListenAddr: "127.0.0.1:9100"}}
	bind(b, 0)
	b.SetBoundAddr(&net.UnixAddr{Name: "/tmp/x", Net: "unix"})
	if got := b.advertisedPort(); got != 9100 {
		t.Errorf("after a bound port 0 and a unix address: %d, want the listen address's 9100", got)
	}
	bind(b, 1)
	if got := b.advertisedPort(); got != 1 {
		t.Errorf("a bound port 1: %d, want 1", got)
	}
}

// aclRig is a broker over a store whose ACL removal can be made to fail.
type aclRig struct {
	meta.Store
	removeErr error
	topicErr  error
}

func (r *aclRig) RemoveACLs(a []meta.ACL) error {
	if r.removeErr != nil {
		return r.removeErr
	}
	return r.Store.RemoveACLs(a)
}

func (r *aclRig) Topic(name string) (meta.Topic, bool, error) {
	if r.topicErr != nil {
		return meta.Topic{}, false, r.topicErr
	}
	return r.Store.Topic(name)
}

func newACLRig(t *testing.T) (*Broker, *aclRig) {
	t.Helper()
	b, _ := batchBroker(t)
	rig := &aclRig{Store: b.meta}
	b.meta = rig
	return b, rig
}

// callAPI sends q through its handler at a version, as the connection would, and
// returns the decoded answer.
func callAPI[Q kmsg.Request, R kmsg.Response](t *testing.T, h func(context.Context, *protocol.Request, Q, R) error, q Q, version int16) R {
	t.Helper()
	q.SetVersion(version)
	body := q.AppendTo(nil)
	skip := 4 // the correlation id.
	if q.IsFlexible() {
		body = q.AppendTo([]byte{0}) // the header's tagged fields come first.
		skip = 5
	}
	out, err := typed(h)(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(q.Key()), APIVersion: version, CorrelationID: 1},
		Body:   body,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := q.ResponseKind()
	resp.SetVersion(version)
	if err := resp.ReadFrom(out[skip:]); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.(R)
}

// DescribeACLs lists resources by type code (topics before groups), then name, then pattern type, however the
// store holds them.
func TestDescribeACLsListsResourcesInOrder(t *testing.T) {
	b, _ := newACLRig(t)
	put := func(rt int8, name string, pt int8) meta.ACL {
		return acl(rt, name, pt, "User:a", "*", opRead, permAllow)
	}
	if err := b.meta.AddACLs([]meta.ACL{
		put(resTopic, "b", aclLiteral),
		put(resGroup, "z", aclLiteral),
		put(resTopic, "b", aclPrefix),
		put(resTopic, "a", aclPrefix),
		put(resTopic, "a", aclLiteral),
		put(resGroup, "a", aclLiteral),
	}); err != nil {
		t.Fatal(err)
	}
	b.refreshAuthorizer()
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType, req.ResourcePatternType = kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny
	req.Operation, req.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	resp := callAPI(t, b.handleDescribeACLs, req, 2)
	var got []string
	for _, r := range resp.Resources {
		got = append(got, strings.Join([]string{r.ResourceType.String(), r.ResourceName, r.ResourcePatternType.String()}, "/"))
	}
	want := []string{"TOPIC/a/LITERAL", "TOPIC/a/PREFIXED", "TOPIC/b/LITERAL", "TOPIC/b/PREFIXED", "GROUP/a/LITERAL", "GROUP/z/LITERAL"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("resources %v, want %v", got, want)
	}
}

// A DeleteACLs whose removal fails answers the storage error for the filters that
// were fine, drops the entries it said it would remove, and leaves a filter that
// was already refused with its own refusal.
func TestDeleteACLsWhoseRemovalFailsSaysSoPerFilter(t *testing.T) {
	b, rig := newACLRig(t)
	if err := b.meta.AddACLs([]meta.ACL{acl(resTopic, "t", aclLiteral, "User:a", "*", opRead, permAllow)}); err != nil {
		t.Fatal(err)
	}
	b.refreshAuthorizer()
	good := kmsg.NewDeleteACLsRequestFilter()
	good.ResourceType, good.ResourcePatternType = kmsg.ACLResourceTypeTopic, kmsg.ACLResourcePatternTypeAny
	good.Operation, good.PermissionType = kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	bad := good
	bad.ResourceType = 99
	req := kmsg.NewPtrDeleteACLsRequest()
	req.Filters = []kmsg.DeleteACLsRequestFilter{good, bad}

	rig.removeErr = &fs.PathError{Op: "write", Path: "meta.db", Err: errors.New("no space left on device")}
	resp := callAPI(t, b.handleDeleteACLs, req, 2)
	if len(resp.Results) != 2 {
		t.Fatalf("%d results", len(resp.Results))
	}
	if r := resp.Results[0]; r.ErrorCode != kerr.KafkaStorageError.Code || len(r.MatchingACLs) != 0 {
		t.Errorf("the good filter: code %d with %d matches, want KAFKA_STORAGE_ERROR and none", r.ErrorCode, len(r.MatchingACLs))
	}
	if r := resp.Results[1]; r.ErrorCode != kerr.InvalidRequest.Code || r.ErrorMessage == nil {
		t.Errorf("the refused filter: code %d message %v, want INVALID_REQUEST kept", r.ErrorCode, r.ErrorMessage)
	}
	if all, _ := b.meta.ListACLs(); len(all) != 1 {
		t.Errorf("%d entries stored, want the 1 that was not removed", len(all))
	}

	// A failure that is not the disk's is the server's.
	rig.removeErr = errors.New("bookkeeping")
	resp = callAPI(t, b.handleDeleteACLs, req, 2)
	if c := resp.Results[0].ErrorCode; c != kerr.UnknownServerError.Code {
		t.Errorf("a failure that is not I/O: code %d, want UNKNOWN_SERVER_ERROR", c)
	}

	// With the store well, the same request deletes and reports the entry.
	rig.removeErr = nil
	resp = callAPI(t, b.handleDeleteACLs, req, 2)
	if r := resp.Results[0]; r.ErrorCode != 0 || len(r.MatchingACLs) != 1 {
		t.Errorf("the good filter: code %d with %d matches, want 0 and 1", r.ErrorCode, len(r.MatchingACLs))
	}
}

// A handler that answers with a *kerr.Error tells a disk fault from the broker's own
// failure: CreatePartitions reading the topic.
func TestCreatePartitionsTellsADiskFaultFromAnotherFailure(t *testing.T) {
	b, rig := newACLRig(t)
	createTopicsCall(t, b, false, "grow")
	for _, tc := range []struct {
		err  error
		want int16
	}{
		{&fs.PathError{Op: "read", Path: "meta.db", Err: errors.New("input/output error")}, kerr.KafkaStorageError.Code},
		{errors.New("bookkeeping"), kerr.UnknownServerError.Code},
	} {
		rig.topicErr = tc.err
		req := kmsg.NewPtrCreatePartitionsRequest()
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic, rt.Count = "grow", 2
		req.Topics = append(req.Topics, rt)
		req.TimeoutMillis = 5000
		resp := callAPI(t, b.handleCreatePartitions, req, 3)
		if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != tc.want {
			t.Errorf("%v: %+v, want code %d", tc.err, resp.Topics, tc.want)
		}
	}
}

// A client-first message is held to maxSCRAMFirstBytes: that many bytes are read,
// one more is refused before it is parsed.
func TestSCRAMClientFirstSizeBoundary(t *testing.T) {
	a := scramAuthenticator(t, map[string]string{"alice": "pw"})
	msg := func(n int) []byte {
		const head = "n,,n=alice,r="
		return []byte(head + strings.Repeat("x", n-len(head)))
	}
	if _, conv, refusal := a.scramFirst(config.SASLScramSHA256, msg(maxSCRAMFirstBytes)); refusal != nil || conv == nil {
		t.Errorf("%d bytes: refused (%v)", maxSCRAMFirstBytes, refusal)
	}
	_, conv, refusal := a.scramFirst(config.SASLScramSHA256, msg(maxSCRAMFirstBytes+1))
	if refusal == nil || conv != nil {
		t.Fatalf("%d bytes: accepted", maxSCRAMFirstBytes+1)
	}
	if !strings.Contains(refusal.why, "more than 4096") {
		t.Errorf("the reason %q does not name the bound", refusal.why)
	}
}

// A PLAIN response is held to the checks in the order the plainStep comment gives, and
// an authorization id is accepted only when empty or the user's own name.
func TestPlainResponseIsCheckedInOrderAndAuthzidIsTheUser(t *testing.T) {
	cfg := config.Default()
	cfg.SASLMechanisms = []string{config.SASLPlain}
	cfg.Credentials = map[string]string{"alice": "pw", "bob": "pw2"}
	a, err := newAuthenticator(cfg)
	if err != nil || a == nil {
		t.Fatalf("authenticator: %v", err)
	}
	for _, tc := range []struct {
		name string
		in   string
		user string // "" = refused.
		wire string
	}{
		{"no authorization id", "\x00alice\x00pw", "alice", ""},
		{"the user as its own authorization id", "alice\x00alice\x00pw", "alice", ""},
		{"another user as the authorization id", "bob\x00alice\x00pw", "", msgAuthzidDiffers},
		{"another authorization id with the wrong password", "bob\x00alice\x00nope", "", msgPlainBadCredentials},
		{"an unknown user", "\x00nobody\x00pw", "", msgPlainBadCredentials},
		{"another user's password", "\x00alice\x00pw2", "", msgPlainBadCredentials},
		{"no user", "\x00\x00pw", "", msgPlainNoUser},
		{"no password", "\x00alice\x00", "", msgPlainNoPassword},
		{"two parts", "alice\x00pw", "", msgPlainTokens(2)},
		{"four parts", "a\x00b\x00c\x00d", "", msgPlainTokens(4)},
		{"nothing at all", "", "", msgPlainTokens(1)},
	} {
		user, refusal := a.plainStep([]byte(tc.in))
		if tc.user == "" {
			if refusal == nil || refusal.wire != tc.wire || user != "" {
				t.Errorf("%s: user %q refusal %+v, want a refusal saying %q", tc.name, user, refusal, tc.wire)
			}
			continue
		}
		if refusal != nil || user != tc.user {
			t.Errorf("%s: user %q refusal %+v, want %q", tc.name, user, refusal, tc.user)
		}
	}
}

// The edges of the client-first grammar (RFC 5802 5.1): a nonce is printable ASCII
// without a comma, from "!" to "~"; an extension is one ASCII letter, "=", and a
// value.
func TestSCRAMClientFirstGrammarEdges(t *testing.T) {
	a := scramAuthenticator(t, map[string]string{"alice": "pw"})
	for _, tc := range []struct {
		name string
		msg  string
		ok   bool
	}{
		{"the first printable character in a nonce", "n,,n=alice,r=!abc", true},
		{"the last printable character in a nonce", "n,,n=alice,r=abc~", true},
		{"a space in a nonce", "n,,n=alice,r=a bc", false},
		{"DEL in a nonce", "n,,n=alice,r=abc\x7f", false},
		{"a control character in a nonce", "n,,n=alice,r=abc\x1f", false},
		{"a comma ends the nonce and leaves a malformed extension", "n,,n=alice,r=a,bc", false},
		{"the shortest extension", "n,,n=alice,r=abc,x=y", true},
		{"an extension with no value", "n,,n=alice,r=abc,x=", false},
		{"an extension of a lower-case a", "n,,n=alice,r=abc,a=1", true},
		{"an extension of a lower-case z", "n,,n=alice,r=abc,z=1", true},
		{"an extension of an upper-case A", "n,,n=alice,r=abc,A=1", true},
		{"an extension of an upper-case Z", "n,,n=alice,r=abc,Z=1", true},
		{"an extension named by the character before a", "n,,n=alice,r=abc,`=1", false},
		{"an extension named by the character after z", "n,,n=alice,r=abc,{=1", false},
		{"an extension named by the character before A", "n,,n=alice,r=abc,@=1", false},
		{"an extension named by the character after Z", "n,,n=alice,r=abc,[=1", false},
		{"an extension named by a digit", "n,,n=alice,r=abc,0=1", false},
	} {
		_, conv, refusal := a.scramFirst(config.SASLScramSHA256, []byte(tc.msg))
		if tc.ok && (refusal != nil || conv == nil || conv.user != "alice") {
			t.Errorf("%s: %q was refused: %+v", tc.name, tc.msg, refusal)
		}
		if !tc.ok && refusal == nil {
			t.Errorf("%s: %q was accepted", tc.name, tc.msg)
		}
	}
}

// A SCRAM proof of the wrong length ends the connection with no answer at all, where
// every other refusal is answered and the connection closes after it (claim: auth
// A14, A2).
func TestASilentSCRAMRefusalClosesTheConnectionWithNoAnswer(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	b.auth = scramAuthenticator(t, map[string]string{"alice": "pw"})
	step := func(conn *protocol.Conn, msg string) (*kmsg.SASLAuthenticateResponse, error) {
		resp := kmsg.NewPtrSASLAuthenticateResponse()
		ar := kmsg.NewPtrSASLAuthenticateRequest()
		ar.SASLAuthBytes = []byte(msg)
		return resp, b.handleSASLAuthenticate(context.Background(), &protocol.Request{Conn: conn}, ar, resp)
	}
	open := func() *protocol.Conn {
		conn := &protocol.Conn{}
		conn.SetSASLMechanism(config.SASLScramSHA256)
		resp, err := step(conn, "n,,n=alice,r=clientnonce")
		if err != nil || resp.ErrorCode != 0 || len(resp.SASLAuthBytes) == 0 {
			t.Fatalf("client-first: %v %+v", err, resp)
		}
		return conn
	}

	// A proof of the wrong length: no answer, the connection is closed with the reason.
	_, err := step(open(), "c=biws,r=clientnonce,p=AAAA")
	var closing *protocol.CloseError
	if !errors.As(err, &closing) || !strings.Contains(closing.Reason, "authentication failed (scram: proof length mismatch)") || closing.Class != protocol.ClassDenied {
		t.Errorf("a short proof: %v, want the connection closed as denied, naming the proof's length", err)
	}
	// Any other refusal is an answer.
	resp, err := step(open(), "c=biws,r=clientnonce,p=!!!")
	if err != nil || resp.ErrorCode != kerr.SaslAuthenticationFailed.Code || resp.ErrorMessage == nil || *resp.ErrorMessage != msgScramRefused(config.SASLScramSHA256) {
		t.Errorf("a proof that is not base64: %v %+v, want SASL_AUTHENTICATION_FAILED with Kafka's words", err, resp)
	}
}
