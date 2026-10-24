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

func callFlexible[Q kmsg.Request, R kmsg.Response](t *testing.T, h func(context.Context, *protocol.Request, Q, R) error, key kmsg.Request, version int16) R {
	t.Helper()
	key.SetVersion(version)
	out, err := typed(h)(context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: protocol.APIKey(key.Key()), APIVersion: version, CorrelationID: 1},
		Body:   key.AppendTo([]byte{0}),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := key.ResponseKind()
	resp.SetVersion(version)
	if err := resp.ReadFrom(out[5:]); err != nil {
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
	resp := callFlexible(t, b.handleDescribeACLs, req, 2)
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
	resp := callFlexible(t, b.handleDeleteACLs, req, 2)
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
	resp = callFlexible(t, b.handleDeleteACLs, req, 2)
	if c := resp.Results[0].ErrorCode; c != kerr.UnknownServerError.Code {
		t.Errorf("a failure that is not I/O: code %d, want UNKNOWN_SERVER_ERROR", c)
	}

	// With the store well, the same request deletes and reports the entry.
	rig.removeErr = nil
	resp = callFlexible(t, b.handleDeleteACLs, req, 2)
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
		resp := callFlexible(t, b.handleCreatePartitions, req, 3)
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
