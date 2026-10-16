package console

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// saslBroker starts a broker with SASL PLAIN users and the given ACL default and
// super users, and a console logged in as the first super user.
func saslBroker(t *testing.T, aclDefault string, supers []string, users map[string]string) (*Console, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("boots a real broker")
	}
	cfg := config.Default()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	cfg.ListenAddr, cfg.AdvertisedHost = "127.0.0.1:0", "127.0.0.1"
	cfg.SASLMechanisms = []string{config.SASLPlain}
	cfg.Credentials = users
	cfg.ACLDefault = aclDefault
	cfg.ACLSuperUsers = supers
	cfg.Console, cfg.Zone, cfg.SchemaRegistry, cfg.ClientTelemetry = false, false, false, false
	anySrv, err := serverhook.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := anySrv.(*server.Server)
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	admin := supers[0][len("User:"):]
	c, err := New(Options{Addr: bound.String(), SASLUser: admin, SASLPass: users[admin], SASLMechanism: config.SASLPlain})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, bound.String()
}

func clientAs(t *testing.T, addr, user, pass string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()),
		kgo.DisableIdempotentWrite(), kgo.RecordRetries(0), kgo.RequestRetries(0), kgo.ProduceRequestTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// What each operation is tried with: a request the broker checks that one right for,
// and whether it was refused for it.
func tryDescribeTopic(ctx context.Context, cl *kgo.Client, topic string) (bool, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = 0, -1
	rt.Partitions = []kmsg.ListOffsetsRequestTopicPartition{rp}
	req.Topics = []kmsg.ListOffsetsRequestTopic{rt}
	resp, err := req.RequestWith(ctx, cl)
	if errors.Is(err, kerr.TopicAuthorizationFailed) {
		return false, nil // the client asked for the topic's metadata first, and was refused.
	}
	if err != nil {
		return false, err
	}
	return resp.Topics[0].Partitions[0].ErrorCode != kerr.TopicAuthorizationFailed.Code, nil
}

func tryWriteTopic(ctx context.Context, cl *kgo.Client, topic string) (bool, error) {
	err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr()
	if err == nil {
		return true, nil
	}
	if kerr.IsRetriable(err) {
		return false, err
	}
	return false, nil
}

func tryReadGroup(ctx context.Context, cl *kgo.Client, group, topic string) (bool, error) {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = group, -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset = 0, 0
	rt.Partitions = []kmsg.OffsetCommitRequestTopicPartition{rp}
	req.Topics = []kmsg.OffsetCommitRequestTopic{rt}
	resp, err := req.RequestWith(ctx, cl)
	if errors.Is(err, kerr.GroupAuthorizationFailed) {
		return false, nil // finding the coordinator was refused.
	}
	if err != nil {
		return false, err
	}
	return resp.Topics[0].Partitions[0].ErrorCode != kerr.GroupAuthorizationFailed.Code, nil
}

func tryDescribeGroup(ctx context.Context, cl *kgo.Client, group string) (bool, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	resp, err := req.RequestWith(ctx, cl)
	if errors.Is(err, kerr.GroupAuthorizationFailed) {
		return false, nil // finding the coordinator was refused.
	}
	if err != nil {
		return false, err
	}
	return resp.ErrorCode != kerr.GroupAuthorizationFailed.Code, nil
}

func createACL(t *testing.T, cl *kgo.Client, principal string, rt kmsg.ACLResourceType, name string, pattern kmsg.ACLResourcePatternType, op kmsg.ACLOperation, perm kmsg.ACLPermissionType, host string) {
	t.Helper()
	req := kmsg.NewPtrCreateACLsRequest()
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName, c.ResourcePatternType = rt, name, pattern
	c.Principal, c.Host, c.Operation, c.PermissionType = principal, host, op, perm
	req.Creations = []kmsg.CreateACLsRequestCreation{c}
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil || resp.Results[0].ErrorCode != 0 {
		t.Fatalf("create ACL: %v %+v", err, resp)
	}
}

// The checker is held to the broker: every question is asked twice, once of the
// checker and once of the broker as a real request by that principal, and the two
// answers must agree. Literal, prefixed and "*" names; "User:*"; ALL; Describe given
// by Read and Write; a DENY over an ALLOW; a host that does not match; a resource no
// ACL is about, under each default; a super user.
func TestTheCheckerAgreesWithTheBroker(t *testing.T) {
	users := map[string]string{"admin": "a", "alice": "a", "bob": "b", "carol": "c", "dave": "d"}
	for _, def := range []string{config.ACLAllow, config.ACLDeny} {
		t.Run("default "+def, func(t *testing.T) {
			c, addr := saslBroker(t, def, []string{"User:admin"}, users)
			ctx := context.Background()
			admin := clientAs(t, addr, "admin", "a")
			for _, topic := range []string{"t-open", "t-alice", "pre-x", "pre-y", "t-host", "t-star", "t-w"} {
				req := kmsg.NewPtrCreateTopicsRequest()
				rt := kmsg.NewCreateTopicsRequestTopic()
				rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 1, 1
				req.Topics = []kmsg.CreateTopicsRequestTopic{rt}
				if _, err := req.RequestWith(ctx, admin); err != nil {
					t.Fatal(err)
				}
			}
			A, D := kmsg.ACLPermissionTypeAllow, kmsg.ACLPermissionTypeDeny
			T, G := kmsg.ACLResourceTypeTopic, kmsg.ACLResourceTypeGroup
			lit, pre := kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypePrefixed
			createACL(t, admin, "User:alice", T, "t-alice", lit, kmsg.ACLOperationRead, A, "*")
			createACL(t, admin, "User:bob", T, "t-alice", lit, kmsg.ACLOperationAll, D, "*")
			createACL(t, admin, "User:bob", T, "t-alice", lit, kmsg.ACLOperationWrite, A, "*")
			createACL(t, admin, "User:*", T, "pre-", pre, kmsg.ACLOperationDescribe, A, "*")
			createACL(t, admin, "User:carol", T, "pre-", pre, kmsg.ACLOperationWrite, D, "*")
			createACL(t, admin, "User:carol", T, "pre-x", lit, kmsg.ACLOperationWrite, A, "*")
			createACL(t, admin, "User:dave", T, "pre-y", lit, kmsg.ACLOperationWrite, A, "*")
			createACL(t, admin, "User:dave", T, "t-w", lit, kmsg.ACLOperationWrite, A, "*") // Write grants Describe.
			createACL(t, admin, "User:bob", T, "t-host", lit, kmsg.ACLOperationWrite, A, "10.0.0.1")
			createACL(t, admin, "User:carol", T, "*", lit, kmsg.ACLOperationDescribe, A, "*")
			createACL(t, admin, "User:alice", G, "g-alice", lit, kmsg.ACLOperationRead, A, "*")
			createACL(t, admin, "User:bob", G, "g-alice", lit, kmsg.ACLOperationDescribe, D, "*")

			rules, err := c.aclRules(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if rules.OpenByDefault != (def == config.ACLAllow) || len(rules.SuperUsers) != 1 || rules.SuperUsers[0] != "User:admin" {
				t.Fatalf("the rules read from the broker: default %v, super users %v", rules.OpenByDefault, rules.SuperUsers)
			}
			asked, allowed := 0, 0
			for _, user := range []string{"admin", "alice", "bob", "carol", "dave"} {
				cl := clientAs(t, addr, user, users[user])
				principal := "User:" + user
				for _, topic := range []string{"t-open", "t-alice", "pre-x", "pre-y", "t-host", "t-star", "t-w"} {
					for op, try := range map[string]func() (bool, error){
						"describe": func() (bool, error) { return tryDescribeTopic(ctx, cl, topic) },
						"write":    func() (bool, error) { return tryWriteTopic(ctx, cl, topic) },
					} {
						did, err := try()
						if err != nil {
							t.Fatalf("%s %s %s: %v", user, op, topic, err)
						}
						ans := rules.check(aclQuestion{Principal: principal, Operation: op, Resource: "topic", Name: topic, Host: "127.0.0.1"})
						if did {
							allowed++
						}
						if ans.Allowed != did {
							t.Errorf("%s %s topic %s: the broker %s, the checker says %v (%+v)", user, op, topic, map[bool]string{true: "allowed it", false: "refused it"}[did], ans.Allowed, ans.Steps)
						}
						asked++
					}
				}
				for _, group := range []string{"g-alice", "g-open"} {
					read, err := tryReadGroup(ctx, cl, group, "t-open")
					if err != nil {
						t.Fatal(err)
					}
					desc, err := tryDescribeGroup(ctx, cl, group)
					if err != nil {
						t.Fatal(err)
					}
					for op, did := range map[string]bool{"read": read, "describe": desc} {
						ans := rules.check(aclQuestion{Principal: principal, Operation: op, Resource: "group", Name: group, Host: "127.0.0.1"})
						if did {
							allowed++
						}
						if ans.Allowed != did {
							t.Errorf("%s %s group %s: the broker %v, the checker %v (%+v)", user, op, group, did, ans.Allowed, ans.Steps)
						}
						asked++
					}
				}
			}
			if asked != 5*(7*2+2*2) {
				t.Fatalf("asked %d questions", asked)
			}
			// Both answers are asked about, many times each.
			if allowed < 10 || asked-allowed < 10 {
				t.Fatalf("%d of %d allowed: the matrix does not ask both ways", allowed, asked)
			}
			t.Logf("%d questions, %d allowed", asked, allowed)
		})
	}
}
