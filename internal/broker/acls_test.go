package broker_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
)

func sp(s string) *string { return &s }

func createACL(rt kmsg.ACLResourceType, name string, pt kmsg.ACLResourcePatternType, principal, host string, op kmsg.ACLOperation, perm kmsg.ACLPermissionType) kmsg.CreateACLsRequestCreation {
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName, c.ResourcePatternType = rt, name, pt
	c.Principal, c.Host, c.Operation, c.PermissionType = principal, host, op, perm
	return c
}

func createACLs(t *testing.T, ctx context.Context, cl *kgo.Client, cs ...kmsg.CreateACLsRequestCreation) []kmsg.CreateACLsResponseResult {
	t.Helper()
	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = cs
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Results) != len(cs) {
		t.Fatalf("CreateAcls: %v %+v", err, resp)
	}
	return resp.Results
}

type aclQuery struct {
	rt        kmsg.ACLResourceType
	name      *string
	pt        kmsg.ACLResourcePatternType
	principal *string
	op        kmsg.ACLOperation
	perm      kmsg.ACLPermissionType
}

func anyACL() aclQuery {
	return aclQuery{rt: kmsg.ACLResourceTypeAny, pt: kmsg.ACLResourcePatternTypeAny, op: kmsg.ACLOperationAny, perm: kmsg.ACLPermissionTypeAny}
}

// describe returns "type:name:pattern:principal:op:perm" for every entry matching q.
func describeACLs(t *testing.T, ctx context.Context, cl *kgo.Client, q aclQuery) (entries []string, code int16) {
	t.Helper()
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType, req.ResourceName, req.ResourcePatternType = q.rt, q.name, q.pt
	req.Principal, req.Operation, req.PermissionType = q.principal, q.op, q.perm
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("DescribeAcls: %v", err)
	}
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			entries = append(entries, strings.Join([]string{strconv.Itoa(int(r.ResourceType)), r.ResourceName, strconv.Itoa(int(r.ResourcePatternType)),
				a.Principal, strconv.Itoa(int(a.Operation)), strconv.Itoa(int(a.PermissionType))}, ":"))
		}
	}
	return entries, resp.ErrorCode
}

func deleteACLs(t *testing.T, ctx context.Context, cl *kgo.Client, q aclQuery) kmsg.DeleteACLsResponseResult {
	t.Helper()
	req := kmsg.NewPtrDeleteACLsRequest()
	f := kmsg.NewDeleteACLsRequestFilter()
	f.ResourceType, f.ResourceName, f.ResourcePatternType = q.rt, q.name, q.pt
	f.Principal, f.Operation, f.PermissionType = q.principal, q.op, q.perm
	req.Filters = append(req.Filters, f)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Results) != 1 {
		t.Fatalf("DeleteAcls: %v %+v", err, resp)
	}
	return resp.Results[0]
}

// An entry must be concrete and say what it is; the answer for each way of getting
// that wrong is INVALID_REQUEST, as Terraform and the Java admin client expect.
func TestCreateACLsRejectsEntriesThatAreNotConcrete(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const lit, pre = kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypePrefixed
	for _, tc := range []struct {
		name string
		c    kmsg.CreateACLsRequestCreation
		ok   bool
	}{
		{"a good entry", createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), true},
		{"prefixed", createACL(kmsg.ACLResourceTypeTopic, "t-", pre, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), true},
		{"any resource type", createACL(kmsg.ACLResourceTypeAny, "t", lit, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), false},
		{"match pattern", createACL(kmsg.ACLResourceTypeTopic, "t", kmsg.ACLResourcePatternTypeMatch, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), false},
		{"any operation", createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "*", kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAllow), false},
		{"any permission", createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAny), false},
		{"a principal with no type", createACL(kmsg.ACLResourceTypeTopic, "t", lit, "alice", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), false},
		{"no host", createACL(kmsg.ACLResourceTypeTopic, "t", lit, "User:a", "", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), false},
		{"no name", createACL(kmsg.ACLResourceTypeTopic, "", lit, "User:a", "*", kmsg.ACLOperationRead, kmsg.ACLPermissionTypeAllow), false},
		{"a cluster by another name", createACL(kmsg.ACLResourceTypeCluster, "mine", lit, "User:a", "*", kmsg.ACLOperationAlter, kmsg.ACLPermissionTypeAllow), false},
	} {
		res := createACLs(t, ctx, cl, tc.c)[0]
		if tc.ok && res.ErrorCode != 0 {
			t.Errorf("%s: refused: %d %v", tc.name, res.ErrorCode, res.ErrorMessage)
		}
		if !tc.ok && (res.ErrorCode != kerr.InvalidRequest.Code || res.ErrorMessage == nil) {
			t.Errorf("%s: code %d, want INVALID_REQUEST with a message", tc.name, res.ErrorCode)
		}
	}
	if all, _ := describeACLs(t, ctx, cl, anyACL()); len(all) != 2 {
		t.Errorf("%d entries stored after the table, want only the 2 good ones: %v", len(all), all)
	}
	// The cluster resource has one name, and that name is accepted. (Last: an ACL on
	// the cluster is enforced like any other, and would refuse this anonymous client.)
	if r := createACLs(t, ctx, cl, createACL(kmsg.ACLResourceTypeCluster, "kafka-cluster", lit, "User:a", "*", kmsg.ACLOperationAlter, kmsg.ACLPermissionTypeAllow))[0]; r.ErrorCode != 0 {
		t.Errorf("the cluster by its own name: %d %v", r.ErrorCode, r.ErrorMessage)
	}
}

// Create, describe with every kind of filter, delete by filter: Kafka's semantics for
// "any", literal, prefixed and match, and the entries come back as they went in.
func TestACLsRoundTripWithFilters(t *testing.T) {
	addr := startBroker(t, 4)
	cl := newClient(t, addr)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const lit, pre = kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypePrefixed
	const allow, deny = kmsg.ACLPermissionTypeAllow, kmsg.ACLPermissionTypeDeny
	for _, r := range createACLs(t, ctx, cl,
		createACL(kmsg.ACLResourceTypeTopic, "orders", lit, "User:alice", "*", kmsg.ACLOperationRead, allow),
		createACL(kmsg.ACLResourceTypeTopic, "orders", lit, "User:bob", "*", kmsg.ACLOperationWrite, deny),
		createACL(kmsg.ACLResourceTypeTopic, "ord", pre, "User:alice", "*", kmsg.ACLOperationDescribe, allow),
		createACL(kmsg.ACLResourceTypeTopic, "*", lit, "User:carol", "*", kmsg.ACLOperationRead, allow),
		createACL(kmsg.ACLResourceTypeGroup, "workers", lit, "User:alice", "*", kmsg.ACLOperationRead, allow),
		createACL(kmsg.ACLResourceTypeTopic, "xyz", pre, "User:dave", "*", kmsg.ACLOperationRead, allow), // a prefix that does not cover "orders".
	) {
		if r.ErrorCode != 0 {
			t.Fatalf("create: %d %v", r.ErrorCode, r.ErrorMessage)
		}
	}
	// Adding one that exists is not an error and not a second entry.
	createACLs(t, ctx, cl, createACL(kmsg.ACLResourceTypeTopic, "orders", lit, "User:alice", "*", kmsg.ACLOperationRead, allow))

	count := func(q aclQuery) int { e, c := describeACLs(t, ctx, cl, q); _ = c; return len(e) }
	topics := func(q aclQuery) aclQuery { q.rt = kmsg.ACLResourceTypeTopic; return q }
	for _, tc := range []struct {
		name string
		q    aclQuery
		want int
	}{
		{"everything", anyACL(), 6},
		{"topics", topics(anyACL()), 5},
		{"groups", func() aclQuery { q := anyACL(); q.rt = kmsg.ACLResourceTypeGroup; return q }(), 1},
		{"the name orders, any pattern", func() aclQuery { q := topics(anyACL()); q.name = sp("orders"); return q }(), 2},
		{"the name orders, literal", func() aclQuery { q := topics(anyACL()); q.name = sp("orders"); q.pt = lit; return q }(), 2},
		{"the name orders, prefixed (none is named that)", func() aclQuery { q := topics(anyACL()); q.name = sp("orders"); q.pt = pre; return q }(), 0},
		{"the name ord, prefixed", func() aclQuery { q := topics(anyACL()); q.name = sp("ord"); q.pt = pre; return q }(), 1},
		// MATCH: every entry that would apply to the topic "orders" — its own, the
		// prefix "ord" covers it, and the "*" wildcard.
		{"what applies to orders", func() aclQuery {
			q := topics(anyACL())
			q.name, q.pt = sp("orders"), kmsg.ACLResourcePatternTypeMatch
			return q
		}(), 4},
		{"alice", func() aclQuery { q := anyACL(); q.principal = sp("User:alice"); return q }(), 3},
		{"reads", func() aclQuery { q := anyACL(); q.op = kmsg.ACLOperationRead; return q }(), 4},
		{"denials", func() aclQuery { q := anyACL(); q.perm = deny; return q }(), 1},
		{"nothing matches", func() aclQuery { q := anyACL(); q.principal = sp("User:nobody"); return q }(), 0},
	} {
		if got := count(tc.q); got != tc.want {
			t.Errorf("%s: %d entries, want %d", tc.name, got, tc.want)
		}
	}

	// What was stored is what is read back.
	e, _ := describeACLs(t, ctx, cl, func() aclQuery { q := topics(anyACL()); q.name = sp("orders"); q.principal = sp("User:bob"); return q }())
	if len(e) != 1 || e[0] != "2:orders:3:User:bob:4:2" {
		t.Errorf("bob's entry = %v", e)
	}

	// A filter that names something unknown is a refusal, not "matches nothing".
	bad := anyACL()
	bad.op = 0
	if _, code := describeACLs(t, ctx, cl, bad); code != kerr.InvalidRequest.Code {
		t.Errorf("an unknown operation in a filter: code %d", code)
	}

	// Delete reports what it removed, and a second delete finds nothing.
	del := func() aclQuery { q := topics(anyACL()); q.principal = sp("User:alice"); return q }()
	if r := deleteACLs(t, ctx, cl, del); r.ErrorCode != 0 || len(r.MatchingACLs) != 2 {
		t.Errorf("delete alice's topic entries: %+v, want the two reported", r)
	}
	if r := deleteACLs(t, ctx, cl, del); r.ErrorCode != 0 || len(r.MatchingACLs) != 0 {
		t.Errorf("deleting again: %+v", r)
	}
	if got := count(anyACL()); got != 4 {
		t.Errorf("%d entries left, want 4", got)
	}
	if r := deleteACLs(t, ctx, cl, func() aclQuery { q := anyACL(); q.op = 0; return q }()); r.ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("a delete filter with an unknown operation: %+v", r)
	}
	if got := count(anyACL()); got != 4 {
		t.Errorf("a refused delete changed the entries: %d", got)
	}
}

// ACLs survive a restart and are in force after it: a denial created before the
// restart still refuses, the broker says at start-up that ACLs are enforced, and it
// stays quiet when there are none to say it about.
func TestACLsPersistAndAreEnforcedAfterARestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir, cfg.MetaDB = filepath.Join(dir, "data"), filepath.Join(dir, "meta.db")

	start := func(logs io.Writer) (stop func()) {
		srv, err := embed.NewFromConfig(cfg, slog.New(slog.NewTextHandler(logs, nil)))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		bound, err := srv.Bind(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ListenAddr = bound.String() // the restart binds the same address.
		done := make(chan struct{})
		go func() { srv.Serve(ctx); close(done) }()
		return func() { cancel(); <-done }
	}

	var first lockedLog
	stop := start(&first)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := newClient(t, cfg.ListenAddr)
	if strings.Contains(first.String(), "ACLs are enforced") {
		t.Error("announced enforcement with no ACLs stored")
	}
	mustCreateTopic(t, ctx, cl, "guarded", 1)
	// Nothing authenticates here, so everyone is User:ANONYMOUS, and an ACL can name it.
	createACLs(t, ctx, cl, createACL(kmsg.ACLResourceTypeTopic, "guarded", kmsg.ACLResourcePatternTypeLiteral, "User:ANONYMOUS", "*", kmsg.ACLOperationWrite, kmsg.ACLPermissionTypeDeny))
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "guarded", Value: []byte("denied")}).FirstErr(); err == nil {
		t.Error("a write an ACL denies succeeded")
	}
	cl.Close()
	stop()

	var second lockedLog
	stop = start(&second)
	defer stop()
	if !strings.Contains(second.String(), "ACLs are enforced") {
		t.Errorf("a restart with ACLs stored did not say they are enforced:\n%s", second.String())
	}
	cl = newClient(t, cfg.ListenAddr)
	defer cl.Close()
	if all, _ := describeACLs(t, ctx, cl, anyACL()); len(all) != 1 {
		t.Errorf("after a restart: %v, want the one entry", all)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "guarded", Value: []byte("still denied")}).FirstErr(); err == nil {
		t.Error("after a restart the denial no longer holds")
	}
}

// lockedLog is a log destination a test can read while the broker is still writing.
type lockedLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedLog) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }
