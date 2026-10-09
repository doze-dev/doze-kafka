package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// authzUsers: admin is a super user; the rest are ordinary.
var authzUsers = map[string]string{"admin": "pw-admin", "alice": "pw-alice", "bob": "pw-bob", "mallory": "pw-mallory"}

func startAuthzBroker(t *testing.T, aclDefault string) string {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg.DataDir, cfg.MetaDB = filepath.Join(dir, "data"), filepath.Join(dir, "meta.db")
	cfg.SASLMechanisms = []string{"PLAIN"}
	cfg.Credentials = authzUsers
	cfg.ACLDefault = aclDefault
	cfg.ACLSuperUsers = []string{"User:admin"}
	t.Cleanup(runBroker(t, cfg))
	return cfg.ListenAddr
}

// as is a client logged in as user.
func as(t *testing.T, addr, user string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.DialTimeout(3*time.Second),
		kgo.SASL(plain.Auth{User: user, Pass: authzUsers[user]}.AsMechanism()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// rawAs is a bare connection logged in as user, for requests the client library would
// answer itself or send at another version.
func rawAs(t *testing.T, addr, user string) *rawConn {
	t.Helper()
	c := openRaw(t, addr)
	if code, open := c.plain(user, authzUsers[user]); !open || code != 0 {
		t.Fatalf("login as %s: code %d open %v", user, code, open)
	}
	return c
}

func ask[R kmsg.Response](t *testing.T, c *rawConn, req kmsg.Request, version int16, resp R) R {
	t.Helper()
	req.SetVersion(version)
	resp.SetVersion(version)
	if !c.do(req, resp) {
		t.Fatalf("%T: the connection was closed", req)
	}
	return resp
}

func allow(addr string, rt kmsg.ACLResourceType, name string, pt kmsg.ACLResourcePatternType, user string, op kmsg.ACLOperation, perm kmsg.ACLPermissionType) kmsg.CreateACLsRequestCreation {
	return createACL(rt, name, pt, "User:"+user, "*", op, perm)
}

const (
	topicRes = kmsg.ACLResourceTypeTopic
	groupRes = kmsg.ACLResourceTypeGroup
	literal  = kmsg.ACLResourcePatternTypeLiteral
	permit   = kmsg.ACLPermissionTypeAllow
	forbid   = kmsg.ACLPermissionTypeDeny
)

func produceErr(t *testing.T, ctx context.Context, cl *kgo.Client, topic string) error {
	t.Helper()
	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr()
}

// With no ACLs and the default of allow nothing changes: everyone does everything. ACLs
// created through the API then take effect on the next request, for the principals and
// operations they name, and only on the resources they are about.
func TestCreatedACLsTakeEffect(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "allow")
	admin := as(t, addr, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, admin, "orders", 1)
	mustCreateTopic(t, ctx, admin, "other", 1)

	// Before any ACL: unchanged.
	if err := produceErr(t, ctx, as(t, addr, "mallory"), "orders"); err != nil {
		t.Fatalf("with no ACLs a write was refused: %v", err)
	}

	// alice may write orders, bob may read it.
	for _, r := range createACLs(t, ctx, admin,
		allow(addr, topicRes, "orders", literal, "alice", kmsg.ACLOperationWrite, permit),
		allow(addr, topicRes, "orders", literal, "bob", kmsg.ACLOperationRead, permit),
		allow(addr, groupRes, "g", literal, "bob", kmsg.ACLOperationRead, permit),
	) {
		if r.ErrorCode != 0 {
			t.Fatalf("create: %d %v", r.ErrorCode, r.ErrorMessage)
		}
	}

	alice, bob, mallory := as(t, addr, "alice"), as(t, addr, "bob"), as(t, addr, "mallory")
	if err := produceErr(t, ctx, alice, "orders"); err != nil {
		t.Errorf("alice, who may write orders, could not: %v", err)
	}
	for _, who := range []struct {
		name string
		cl   *kgo.Client
	}{{"bob (may only read)", bob}, {"mallory (no ACL)", mallory}} {
		if err := produceErr(t, ctx, who.cl, "orders"); err == nil || !isCode(err, kerr.TopicAuthorizationFailed) {
			t.Errorf("%s wrote to orders: %v, want TOPIC_AUTHORIZATION_FAILED", who.name, err)
		}
	}
	// A topic no ACL is about follows the default (allow), for everyone.
	for _, cl := range []*kgo.Client{alice, bob, mallory} {
		if err := produceErr(t, ctx, cl, "other"); err != nil {
			t.Errorf("a topic with no ACL was refused: %v", err)
		}
	}

	// bob reads what alice wrote; alice cannot read; the super user can.
	fetchCodeAs := func(user string) int16 {
		resp := ask(t, rawAs(t, addr, user), fetchReq("orders", 0, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}), 11, kmsg.NewPtrFetchResponse())
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	if c := fetchCodeAs("bob"); c != 0 {
		t.Errorf("bob (may read) fetch: code %d", c)
	}
	if c := fetchCodeAs("alice"); c != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("alice (may only write) fetch: code %d, want TOPIC_AUTHORIZATION_FAILED", c)
	}
	if c := fetchCodeAs("admin"); c != 0 {
		t.Errorf("the super user's fetch: code %d", c)
	}

	// Metadata shows a principal what it may describe: Read and Write imply Describe.
	listing := func(user string) map[string]bool {
		resp := ask(t, rawAs(t, addr, user), kmsg.NewPtrMetadataRequest(), 9, kmsg.NewPtrMetadataResponse())
		seen := map[string]bool{}
		for _, tp := range resp.Topics {
			seen[*tp.Topic] = true
		}
		return seen
	}
	if s := listing("alice"); !s["orders"] || !s["other"] {
		t.Errorf("alice's topic listing %v lacks what she may describe", s)
	}
	if s := listing("mallory"); s["orders"] || !s["other"] {
		t.Errorf("mallory's listing %v: orders must be hidden and other shown", s)
	}

	// Deleting the ACL lifts the restriction at once; a DENY outranks an allow.
	deleteACLs(t, ctx, admin, func() aclQuery { q := anyACL(); q.name = sp("orders"); q.principal = sp("User:alice"); return q }())
	createACLs(t, ctx, admin, allow(addr, topicRes, "orders", literal, "bob", kmsg.ACLOperationRead, forbid))
	if c := fetchCodeAs("bob"); c != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("a DENY did not outrank bob's allow: code %d", c)
	}
}

// Kafka's own default: a resource no ACL allows is closed. Only a super user gets in,
// and ACLs open things up one at a time.
func TestDefaultDenyClosesWhatNoACLAllows(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")
	admin := as(t, addr, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, admin, "orders", 1)

	alice := as(t, addr, "alice")
	if err := produceErr(t, ctx, alice, "orders"); !isCode(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("alice, with nothing allowed, wrote: %v", err)
	}
	createACLs(t, ctx, admin, allow(addr, topicRes, "orders", literal, "alice", kmsg.ACLOperationWrite, permit))
	// Producing idempotently came with the right to write.
	if err := produceErr(t, ctx, alice, "orders"); err != nil {
		t.Errorf("alice, allowed to write orders, could not: %v", err)
	}
	if err := produceErr(t, ctx, as(t, addr, "bob"), "orders"); !isCode(err, kerr.TopicAuthorizationFailed) {
		t.Errorf("bob wrote: %v", err)
	}
}

func isCode(err error, want *kerr.Error) bool {
	for ; err != nil; err = unwrap(err) {
		if err == want {
			return true
		}
	}
	return false
}

func unwrap(err error) error {
	u, ok := err.(interface{ Unwrap() error })
	if !ok {
		return nil
	}
	return u.Unwrap()
}

// A transactional id is a resource of its own: Write on it lets a producer take it over
// and run transactions, and the produce of a transactional batch needs it too, besides
// Write on the topic.
func TestTransactionalIDsAreAuthorized(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")
	admin := as(t, addr, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, admin, "t", 1)
	for _, r := range createACLs(t, ctx, admin,
		allow(addr, topicRes, "t", literal, "alice", kmsg.ACLOperationWrite, permit),
		allow(addr, kmsg.ACLResourceTypeTransactionalId, "alice-tx", literal, "alice", kmsg.ACLOperationWrite, permit),
		allow(addr, kmsg.ACLResourceTypeTransactionalId, "alice-tx", literal, "alice", kmsg.ACLOperationDescribe, permit),
	) {
		if r.ErrorCode != 0 {
			t.Fatalf("create: %d %v", r.ErrorCode, r.ErrorMessage)
		}
	}
	init := func(user, id string) int16 {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.TransactionalID, req.TransactionTimeoutMillis = &id, 30000
		return ask(t, rawAs(t, addr, user), req, 4, kmsg.NewPtrInitProducerIDResponse()).ErrorCode
	}
	if c := init("alice", "alice-tx"); c != 0 {
		t.Errorf("alice taking her own transactional id: code %d", c)
	}
	for _, tc := range []struct{ user, id string }{{"alice", "someone-elses"}, {"bob", "alice-tx"}} {
		if c := init(tc.user, tc.id); c != kerr.TransactionalIDAuthorizationFailed.Code {
			t.Errorf("%s taking %q: code %d, want TRANSACTIONAL_ID_AUTHORIZATION_FAILED", tc.user, tc.id, c)
		}
	}
	// FindCoordinator for a transaction needs Describe on the id.
	find := func(user, id string) int16 {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorKeys, req.CoordinatorType = []string{id}, 1
		return ask(t, rawAs(t, addr, user), req, 4, kmsg.NewPtrFindCoordinatorResponse()).Coordinators[0].ErrorCode
	}
	if c := find("alice", "alice-tx"); c != 0 {
		t.Errorf("alice finding the coordinator of her own id: code %d", c)
	}
	if c := find("bob", "alice-tx"); c != kerr.TransactionalIDAuthorizationFailed.Code {
		t.Errorf("bob finding it: code %d", c)
	}
}
