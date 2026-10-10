package broker_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// authzCase asks one API as a user and returns the authorization-relevant error code
// from its answer (0 when it was let through).
type authzCase struct {
	api  kmsg.Key
	want int16 // the code an unauthorized principal gets.
	ask  func(t *testing.T, c *rawConn) int16
}

// authzExempt are the APIs that carry no authorization: they are how a client says who
// it is and what the broker speaks, before it can be anyone.
var authzExempt = map[kmsg.Key]string{
	kmsg.ApiVersions:               "answered before authentication",
	kmsg.SASLHandshake:             "part of authenticating",
	kmsg.SASLAuthenticate:          "part of authenticating",
	kmsg.GetTelemetrySubscriptions: "KIP-714: any authenticated client may say how it is doing; Kafka requires no ACL for it",
	kmsg.PushTelemetry:             "KIP-714: as above",
}

func authzCases() []authzCase {
	const T, G = "t", "g"
	topicPart := func() (string, int32) { return T, 0 }
	_ = topicPart
	return []authzCase{
		{kmsg.Produce, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrProduceRequest()
			req.Acks, req.TimeoutMillis = 1, 1000
			rt := kmsg.NewProduceRequestTopic()
			rt.Topic = T
			rp := kmsg.NewProduceRequestTopicPartition()
			rp.Records = nil
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 9, kmsg.NewPtrProduceResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.Fetch, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			return ask(t, c, fetchReq(T, 0, 1, 1<<20, map[int32][2]int64{0: {0, 1 << 20}}), 11, kmsg.NewPtrFetchResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.ListOffsets, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrListOffsetsRequest()
			req.ReplicaID = -1
			rt := kmsg.NewListOffsetsRequestTopic()
			rt.Topic = T
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Timestamp = -1
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 7, kmsg.NewPtrListOffsetsResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.Metadata, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrMetadataRequest()
			name := T
			mt := kmsg.NewMetadataRequestTopic()
			mt.Topic = &name
			req.Topics = append(req.Topics, mt)
			return ask(t, c, req, 9, kmsg.NewPtrMetadataResponse()).Topics[0].ErrorCode
		}},
		{kmsg.CreateTopics, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrCreateTopicsRequest()
			ct := kmsg.NewCreateTopicsRequestTopic()
			ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "made-by-test", 1, 1
			req.Topics = append(req.Topics, ct)
			return ask(t, c, req, 7, kmsg.NewPtrCreateTopicsResponse()).Topics[0].ErrorCode
		}},
		{kmsg.DeleteTopics, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDeleteTopicsRequest()
			req.TopicNames = []string{"not-there"}
			dt := kmsg.NewDeleteTopicsRequestTopic()
			name := "not-there"
			dt.Topic = &name
			req.Topics = append(req.Topics, dt)
			return ask(t, c, req, 5, kmsg.NewPtrDeleteTopicsResponse()).Topics[0].ErrorCode
		}},
		{kmsg.CreatePartitions, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrCreatePartitionsRequest()
			rt := kmsg.NewCreatePartitionsRequestTopic()
			rt.Topic, rt.Count = T, 5
			req.ValidateOnly = true
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 3, kmsg.NewPtrCreatePartitionsResponse()).Topics[0].ErrorCode
		}},
		{kmsg.DeleteRecords, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDeleteRecordsRequest()
			rt := kmsg.NewDeleteRecordsRequestTopic()
			rt.Topic = T
			rp := kmsg.NewDeleteRecordsRequestTopicPartition()
			rp.Offset = -1
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 2, kmsg.NewPtrDeleteRecordsResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.DescribeConfigs, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDescribeConfigsRequest()
			r := kmsg.NewDescribeConfigsRequestResource()
			r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, T
			req.Resources = append(req.Resources, r)
			return ask(t, c, req, 4, kmsg.NewPtrDescribeConfigsResponse()).Resources[0].ErrorCode
		}},
		{kmsg.AlterConfigs, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrAlterConfigsRequest()
			req.ValidateOnly = true
			r := kmsg.NewAlterConfigsRequestResource()
			r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, T
			req.Resources = append(req.Resources, r)
			return ask(t, c, req, 2, kmsg.NewPtrAlterConfigsResponse()).Resources[0].ErrorCode
		}},
		{kmsg.IncrementalAlterConfigs, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrIncrementalAlterConfigsRequest()
			req.ValidateOnly = true
			r := kmsg.NewIncrementalAlterConfigsRequestResource()
			r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, T
			req.Resources = append(req.Resources, r)
			return ask(t, c, req, 1, kmsg.NewPtrIncrementalAlterConfigsResponse()).Resources[0].ErrorCode
		}},
		{kmsg.DescribeCluster, kerr.ClusterAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			return ask(t, c, kmsg.NewPtrDescribeClusterRequest(), 1, kmsg.NewPtrDescribeClusterResponse()).ErrorCode
		}},
		{kmsg.FindCoordinator, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrFindCoordinatorRequest()
			req.CoordinatorKeys = []string{G}
			return ask(t, c, req, 4, kmsg.NewPtrFindCoordinatorResponse()).Coordinators[0].ErrorCode
		}},
		{kmsg.JoinGroup, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrJoinGroupRequest()
			req.Group, req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = G, 10000, 10000, "consumer"
			req.MemberID = "m"
			return ask(t, c, req, 7, kmsg.NewPtrJoinGroupResponse()).ErrorCode
		}},
		{kmsg.SyncGroup, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrSyncGroupRequest()
			req.Group, req.Generation, req.MemberID = G, 1, "m"
			return ask(t, c, req, 5, kmsg.NewPtrSyncGroupResponse()).ErrorCode
		}},
		{kmsg.Heartbeat, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrHeartbeatRequest()
			req.Group, req.Generation, req.MemberID = G, 1, "m"
			return ask(t, c, req, 4, kmsg.NewPtrHeartbeatResponse()).ErrorCode
		}},
		{kmsg.LeaveGroup, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrLeaveGroupRequest()
			req.Group = G
			m := kmsg.NewLeaveGroupRequestMember()
			m.MemberID = "m"
			req.Members = append(req.Members, m)
			return ask(t, c, req, 4, kmsg.NewPtrLeaveGroupResponse()).ErrorCode
		}},
		{kmsg.OffsetCommit, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrOffsetCommitRequest()
			req.Group, req.Generation = G, -1
			rt := kmsg.NewOffsetCommitRequestTopic()
			rt.Topic = T
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 8, kmsg.NewPtrOffsetCommitResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.OffsetFetch, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrOffsetFetchRequest()
			g := kmsg.NewOffsetFetchRequestGroup()
			g.Group = G
			req.Groups = append(req.Groups, g)
			return ask(t, c, req, 8, kmsg.NewPtrOffsetFetchResponse()).Groups[0].ErrorCode
		}},
		{kmsg.ListGroups, 0, func(t *testing.T, c *rawConn) int16 {
			// A listing shows only what may be described: nothing, to mallory.
			resp := ask(t, c, kmsg.NewPtrListGroupsRequest(), 4, kmsg.NewPtrListGroupsResponse())
			return int16(len(resp.Groups)) // 0 for the unauthorized, 1 for the super user.
		}},
		{kmsg.DescribeGroups, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDescribeGroupsRequest()
			req.Groups = []string{G}
			return ask(t, c, req, 5, kmsg.NewPtrDescribeGroupsResponse()).Groups[0].ErrorCode
		}},
		{kmsg.DeleteGroups, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDeleteGroupsRequest()
			req.Groups = []string{"never-existed"}
			return ask(t, c, req, 2, kmsg.NewPtrDeleteGroupsResponse()).Groups[0].ErrorCode
		}},
		{kmsg.OffsetDelete, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrOffsetDeleteRequest()
			req.Group = G
			return ask(t, c, req, 0, kmsg.NewPtrOffsetDeleteResponse()).ErrorCode
		}},
		{kmsg.ConsumerGroupHeartbeat, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
			req.Group, req.MemberEpoch = "cg", 0
			req.SubscribedTopicNames = []string{T}
			return ask(t, c, req, 0, kmsg.NewPtrConsumerGroupHeartbeatResponse()).ErrorCode
		}},
		{kmsg.ConsumerGroupDescribe, kerr.GroupAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrConsumerGroupDescribeRequest()
			req.Groups = []string{"cg"}
			return ask(t, c, req, 0, kmsg.NewPtrConsumerGroupDescribeResponse()).Groups[0].ErrorCode
		}},
		{kmsg.AddPartitionsToTxn, kerr.TransactionalIDAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrAddPartitionsToTxnRequest()
			req.TransactionalID = "tx1"
			rt := kmsg.NewAddPartitionsToTxnRequestTopic()
			rt.Topic, rt.Partitions = T, []int32{0}
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 3, kmsg.NewPtrAddPartitionsToTxnResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.AddOffsetsToTxn, kerr.TransactionalIDAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrAddOffsetsToTxnRequest()
			req.TransactionalID, req.Group = "tx1", G
			return ask(t, c, req, 3, kmsg.NewPtrAddOffsetsToTxnResponse()).ErrorCode
		}},
		{kmsg.EndTxn, kerr.TransactionalIDAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrEndTxnRequest()
			req.TransactionalID = "tx1"
			return ask(t, c, req, 3, kmsg.NewPtrEndTxnResponse()).ErrorCode
		}},
		{kmsg.TxnOffsetCommit, kerr.TransactionalIDAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrTxnOffsetCommitRequest()
			req.TransactionalID, req.Group, req.Generation = "tx1", G, -1
			rt := kmsg.NewTxnOffsetCommitRequestTopic()
			rt.Topic = T
			rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 3, kmsg.NewPtrTxnOffsetCommitResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.DescribeTransactions, kerr.TransactionalIDAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDescribeTransactionsRequest()
			req.TransactionalIDs = []string{"tx1"}
			return ask(t, c, req, 0, kmsg.NewPtrDescribeTransactionsResponse()).TransactionStates[0].ErrorCode
		}},
		{kmsg.ListTransactions, 0, func(t *testing.T, c *rawConn) int16 {
			// Only the transactional ids that may be described are listed.
			return int16(len(ask(t, c, kmsg.NewPtrListTransactionsRequest(), 0, kmsg.NewPtrListTransactionsResponse()).TransactionStates))
		}},
		{kmsg.DescribeProducers, kerr.TopicAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDescribeProducersRequest()
			rt := kmsg.NewDescribeProducersRequestTopic()
			rt.Topic, rt.Partitions = T, []int32{0}
			req.Topics = append(req.Topics, rt)
			return ask(t, c, req, 0, kmsg.NewPtrDescribeProducersResponse()).Topics[0].Partitions[0].ErrorCode
		}},
		{kmsg.InitProducerID, kerr.ClusterAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			return ask(t, c, kmsg.NewPtrInitProducerIDRequest(), 4, kmsg.NewPtrInitProducerIDResponse()).ErrorCode
		}},
		{kmsg.DescribeACLs, kerr.ClusterAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDescribeACLsRequest()
			req.ResourceType, req.ResourcePatternType, req.Operation, req.PermissionType = 1, 1, 1, 1
			return ask(t, c, req, 3, kmsg.NewPtrDescribeACLsResponse()).ErrorCode
		}},
		{kmsg.CreateACLs, kerr.ClusterAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrCreateACLsRequest()
			req.Creations = append(req.Creations, createACL(topicRes, "zzz", literal, "User:zed", "*", kmsg.ACLOperationRead, permit))
			return ask(t, c, req, 3, kmsg.NewPtrCreateACLsResponse()).Results[0].ErrorCode
		}},
		{kmsg.DeleteACLs, kerr.ClusterAuthorizationFailed.Code, func(t *testing.T, c *rawConn) int16 {
			req := kmsg.NewPtrDeleteACLsRequest()
			f := kmsg.NewDeleteACLsRequestFilter()
			f.ResourceType, f.ResourcePatternType, f.Operation, f.PermissionType = 1, 1, 1, 1
			req.Filters = append(req.Filters, f)
			return ask(t, c, req, 3, kmsg.NewPtrDeleteACLsResponse()).Results[0].ErrorCode
		}},
	}
}

// Under Kafka's default (what no ACL allows is closed) an ordinary principal is
// refused by every API with the code that API uses, and the super user is not. This
// is also the guard that keeps authorization from being forgotten: an API the broker
// serves with no case here, and not named as exempt, fails the test.
func TestEveryServedAPIIsAuthorized(t *testing.T) {
	skipShort(t)
	addr := startAuthzBroker(t, "deny")
	admin := as(t, addr, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, admin, "t", 1)
	commitOffset(t, ctx, admin, "g", "t", 0) // a group exists for the super user to list.
	// A transactional id exists for the super user to list.
	txid := "tx1"
	ireq := kmsg.NewPtrInitProducerIDRequest()
	ireq.TransactionalID, ireq.TransactionTimeoutMillis = &txid, 30000
	if resp, err := ireq.RequestWith(ctx, admin); err != nil || resp.ErrorCode != 0 {
		t.Fatalf("InitProducerId for a transactional id as the super user: %v %+v", err, resp)
	}

	covered := map[kmsg.Key]bool{}
	for _, tc := range authzCases() {
		covered[tc.api] = true
		t.Run(protocol.APIName(protocol.APIKey(tc.api)), func(t *testing.T) {
			got := tc.ask(t, rawAs(t, addr, "mallory"))
			if got != tc.want {
				t.Errorf("an unauthorized principal got %d, want %d (%s)", got, tc.want, kerr.ErrorForCode(tc.want))
			}
			// The super user must not be refused: the check is about the principal.
			if ok := tc.ask(t, rawAs(t, addr, "admin")); tc.api == kmsg.ListGroups || tc.api == kmsg.ListTransactions {
				if ok < 1 {
					t.Errorf("the super user lists %d, want at least the one that exists", ok)
				}
			} else if ok == kerr.TopicAuthorizationFailed.Code || ok == kerr.GroupAuthorizationFailed.Code || ok == kerr.ClusterAuthorizationFailed.Code {
				t.Errorf("the super user was refused: code %d", ok)
			}
		})
	}
	for _, api := range protocol.APIs {
		key := kmsg.Key(api.Key)
		if _, exempt := authzExempt[key]; !covered[key] && !exempt {
			t.Errorf("%s is served but has no authorization case, and is not exempt: decide what it needs and add it to authzCases", protocol.APIName(api.Key))
		}
	}
}
