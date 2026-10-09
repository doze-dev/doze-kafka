package broker

import (
	"context"
	"sort"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The transaction APIs a producer drives: AddPartitionsToTxn and AddOffsetsToTxn say
// what the transaction touches, TxnOffsetCommit stages the consumer offsets that commit
// with it, EndTxn decides it. DescribeTransactions, ListTransactions and
// DescribeProducers are for tooling. The state machine is in txn.go.

const resTxnID int8 = 5 // TransactionalId, as ACLs number it.

func (b *Broker) canTxnID(conn *protocol.Conn, id string, op int8) bool {
	return b.can(conn, resTxnID, id, op)
}

// groupFence is the check on a (generation, member) pair that offsets are committed
// under, in whichever protocol runs the group. The empty member and a negative
// generation are a "simple" commit with no membership to check.
func (b *Broker) groupFence(group, memberID string, instanceID *string, generation int32) int16 {
	if group == "" || meta.ValidateGroupID(group) != nil {
		return kerr.InvalidGroupID.Code
	}
	if b.claims.ownerOf(group) == protoConsumer {
		return b.cg848.validateCommit(group, memberID, generation)
	}
	return b.groups.validateCommit(group, memberID, instanceID, generation)
}

func (b *Broker) handleAddPartitionsToTxn(ctx context.Context, req *protocol.Request, ar *kmsg.AddPartitionsToTxnRequest, resp *kmsg.AddPartitionsToTxnResponse) error {
	fail := func(code int16) {
		for _, t := range ar.Topics {
			rt := kmsg.NewAddPartitionsToTxnResponseTopic()
			rt.Topic = t.Topic
			for _, p := range t.Partitions {
				rp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
				rp.Partition, rp.ErrorCode = p, code
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
	}
	if !b.canTxnID(req.Conn, ar.TransactionalID, opWrite) {
		fail(kerr.TransactionalIDAuthorizationFailed.Code)
		return nil
	}
	e := b.txns.get(ar.TransactionalID)
	if e == nil {
		fail(kerr.InvalidProducerIDMapping.Code)
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.owns(ar.ProducerID, ar.ProducerEpoch); c != 0 {
		fail(c)
		return nil
	}
	if c := e.canAdd(); c != 0 {
		fail(c)
		return nil
	}

	// Every partition is checked before any is added; one that cannot be leaves the
	// rest not attempted, as in Kafka.
	type pending struct {
		topic string
		part  int32
	}
	var add []pending
	bad := false
	for _, t := range ar.Topics {
		rt := kmsg.NewAddPartitionsToTxnResponseTopic()
		rt.Topic = t.Topic
		_, topic, ok := b.resolveTopic(t.Topic, [16]byte{})
		for _, p := range t.Partitions {
			rp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
			rp.Partition = p
			switch {
			case !b.canTopic(req.Conn, t.Topic, opWrite):
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
			case !ok || p < 0 || p >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				add = append(add, pending{t.Topic, p})
			}
			bad = bad || rp.ErrorCode != 0
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	if bad {
		for i := range resp.Topics {
			for j := range resp.Topics[i].Partitions {
				if resp.Topics[i].Partitions[j].ErrorCode == 0 {
					resp.Topics[i].Partitions[j].ErrorCode = kerr.OperationNotAttempted.Code
				}
			}
		}
		return nil
	}

	e.begin()
	for _, a := range add {
		tp := meta.TxnPartition{Topic: a.topic, Partition: a.part}
		if !containsPartition(e.rec.Partitions, tp) {
			e.rec.Partitions = append(e.rec.Partitions, tp)
		}
	}
	if c := b.persistTxn(e); c != 0 {
		for i := range resp.Topics {
			for j := range resp.Topics[i].Partitions {
				resp.Topics[i].Partitions[j].ErrorCode = c
			}
		}
	}
	return nil
}

func containsPartition(list []meta.TxnPartition, tp meta.TxnPartition) bool {
	for _, p := range list {
		if p == tp {
			return true
		}
	}
	return false
}

func (b *Broker) handleAddOffsetsToTxn(ctx context.Context, req *protocol.Request, ar *kmsg.AddOffsetsToTxnRequest, resp *kmsg.AddOffsetsToTxnResponse) error {
	switch {
	case !b.canTxnID(req.Conn, ar.TransactionalID, opWrite):
		resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		return nil
	case !b.canGroup(req.Conn, ar.Group, opRead):
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return nil
	case ar.Group == "" || meta.ValidateGroupID(ar.Group) != nil:
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return nil
	}
	e := b.txns.get(ar.TransactionalID)
	if e == nil {
		resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.owns(ar.ProducerID, ar.ProducerEpoch); c != 0 {
		resp.ErrorCode = c
		return nil
	}
	if c := e.canAdd(); c != 0 {
		resp.ErrorCode = c
		return nil
	}
	e.begin()
	for _, g := range e.rec.Groups {
		if g == ar.Group {
			return nil
		}
	}
	e.rec.Groups = append(e.rec.Groups, ar.Group)
	resp.ErrorCode = b.persistTxn(e)
	return nil
}

func (b *Broker) handleTxnOffsetCommit(ctx context.Context, req *protocol.Request, tr *kmsg.TxnOffsetCommitRequest, resp *kmsg.TxnOffsetCommitResponse) error {
	fail := func(code int16) {
		for _, t := range tr.Topics {
			rt := kmsg.NewTxnOffsetCommitResponseTopic()
			rt.Topic = t.Topic
			for _, p := range t.Partitions {
				rp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
				rp.Partition, rp.ErrorCode = p.Partition, code
				rt.Partitions = append(rt.Partitions, rp)
			}
			resp.Topics = append(resp.Topics, rt)
		}
	}
	switch {
	case !b.canTxnID(req.Conn, tr.TransactionalID, opWrite):
		fail(kerr.TransactionalIDAuthorizationFailed.Code)
		return nil
	case !b.canGroup(req.Conn, tr.Group, opRead):
		fail(kerr.GroupAuthorizationFailed.Code)
		return nil
	}
	e := b.txns.get(tr.TransactionalID)
	if e == nil {
		fail(kerr.InvalidProducerIDMapping.Code)
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.owns(tr.ProducerID, tr.ProducerEpoch); c != 0 {
		fail(c)
		return nil
	}
	// The offsets are for a group the transaction named in AddOffsetsToTxn.
	named := false
	for _, g := range e.rec.Groups {
		named = named || g == tr.Group
	}
	if e.rec.State != txnOngoing || !named {
		fail(kerr.InvalidTxnState.Code)
		return nil
	}
	// A member that names itself must still belong to the group at that generation.
	fence := int16(0)
	if tr.Generation >= 0 && tr.MemberID != "" {
		fence = b.groupFence(tr.Group, tr.MemberID, tr.InstanceID, tr.Generation)
	}

	changed := false
	for _, t := range tr.Topics {
		rt := kmsg.NewTxnOffsetCommitResponseTopic()
		rt.Topic = t.Topic
		_, topic, ok := b.resolveTopic(t.Topic, [16]byte{})
		topicRead := b.canTopic(req.Conn, t.Topic, opRead)
		for _, p := range t.Partitions {
			rp := kmsg.NewTxnOffsetCommitResponseTopicPartition()
			rp.Partition = p.Partition
			switch {
			case fence != 0:
				rp.ErrorCode = fence
			case !topicRead:
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
			case !ok || p.Partition < 0 || p.Partition >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				md := ""
				if p.Metadata != nil {
					md = *p.Metadata
				}
				o := meta.TxnOffset{Group: tr.Group, Topic: t.Topic, Partition: p.Partition, Offset: p.Offset, Metadata: md}
				replaced := false
				for i := range e.rec.Offsets {
					x := e.rec.Offsets[i]
					if x.Group == o.Group && x.Topic == o.Topic && x.Partition == o.Partition {
						e.rec.Offsets[i], replaced = o, true
					}
				}
				if !replaced {
					e.rec.Offsets = append(e.rec.Offsets, o)
				}
				changed = true
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	if changed {
		if c := b.persistTxn(e); c != 0 {
			for i := range resp.Topics {
				for j := range resp.Topics[i].Partitions {
					if resp.Topics[i].Partitions[j].ErrorCode == 0 {
						resp.Topics[i].Partitions[j].ErrorCode = c
					}
				}
			}
		}
	}
	return nil
}

func (b *Broker) handleEndTxn(ctx context.Context, req *protocol.Request, er *kmsg.EndTxnRequest, resp *kmsg.EndTxnResponse) error {
	if !b.canTxnID(req.Conn, er.TransactionalID, opWrite) {
		resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		return nil
	}
	resp.ErrorCode = b.endTxn(er.TransactionalID, er.ProducerID, er.ProducerEpoch, er.Commit)
	return nil
}

func (b *Broker) handleDescribeTransactions(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeTransactionsRequest, resp *kmsg.DescribeTransactionsResponse) error {
	for _, id := range dr.TransactionalIDs {
		rs := kmsg.NewDescribeTransactionsResponseTransactionState()
		rs.TransactionalID = id
		switch e := b.txns.get(id); {
		case !b.canTxnID(req.Conn, id, opDescribe):
			rs.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
		case e == nil:
			rs.ErrorCode = kerr.TransactionalIDNotFound.Code
		default:
			e.mu.RLock()
			rs.State = e.rec.State
			rs.TimeoutMillis = e.rec.TimeoutMs
			rs.StartTimestamp = e.rec.StartedMs
			if e.rec.State != txnOngoing && e.rec.State != txnPrepareCommit && e.rec.State != txnPrepareAbort {
				rs.StartTimestamp = -1
			}
			rs.ProducerID, rs.ProducerEpoch = e.rec.ProducerID, int16(e.rec.Epoch)
			byTopic := map[string][]int32{}
			for _, p := range e.rec.Partitions {
				byTopic[p.Topic] = append(byTopic[p.Topic], p.Partition)
			}
			e.mu.RUnlock()
			topics := make([]string, 0, len(byTopic))
			for t := range byTopic {
				topics = append(topics, t)
			}
			sort.Strings(topics)
			for _, t := range topics {
				rt := kmsg.NewDescribeTransactionsResponseTransactionStateTopic()
				rt.Topic, rt.Partitions = t, byTopic[t]
				rs.Topics = append(rs.Topics, rt)
			}
		}
		resp.TransactionStates = append(resp.TransactionStates, rs)
	}
	return nil
}

func (b *Broker) handleListTransactions(ctx context.Context, req *protocol.Request, lr *kmsg.ListTransactionsRequest, resp *kmsg.ListTransactionsResponse) error {
	states := map[string]bool{}
	for _, s := range lr.StateFilters {
		states[s] = true
	}
	pids := map[int64]bool{}
	for _, p := range lr.ProducerIDFilters {
		pids[p] = true
	}
	var rows []kmsg.ListTransactionsResponseTransactionState
	for _, e := range b.txns.all() {
		e.mu.RLock()
		id, state, pid := e.rec.ID, e.rec.State, e.rec.ProducerID
		e.mu.RUnlock()
		if state == "" || (len(states) > 0 && !states[state]) || (len(pids) > 0 && !pids[pid]) || !b.canTxnID(req.Conn, id, opDescribe) {
			continue
		}
		rs := kmsg.NewListTransactionsResponseTransactionState()
		rs.TransactionalID, rs.ProducerID, rs.TransactionState = id, pid, state
		rows = append(rows, rs)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TransactionalID < rows[j].TransactionalID })
	resp.TransactionStates = rows
	return nil
}

func (b *Broker) handleDescribeProducers(ctx context.Context, req *protocol.Request, dr *kmsg.DescribeProducersRequest, resp *kmsg.DescribeProducersResponse) error {
	for _, t := range dr.Topics {
		rt := kmsg.NewDescribeProducersResponseTopic()
		rt.Topic = t.Topic
		name, topic, ok := b.resolveTopic(t.Topic, [16]byte{})
		authorized := b.canTopic(req.Conn, t.Topic, opRead)
		for _, part := range t.Partitions {
			rp := kmsg.NewDescribeProducersResponseTopicPartition()
			rp.Partition = part
			switch {
			case !authorized:
				rp.ErrorCode = kerr.TopicAuthorizationFailed.Code
			case !ok || part < 0 || part >= topic.Partitions:
				rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			default:
				rp.ActiveProducers = b.activeProducers(name, part)
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return nil
}

// activeProducers is what the partition knows of the producers that have written to it:
// from the sequence tracker, and, for the one with a transaction open, where it began.
func (b *Broker) activeProducers(topic string, partition int32) []kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
	open := map[int64]int64{}
	if plog, err := b.logs.ExistingPartition(topic, partition); err == nil {
		if o, err := plog.OpenTransactions(); err == nil {
			open = o
		}
	}
	var out []kmsg.DescribeProducersResponseTopicPartitionActiveProducer
	if plog, err := b.logs.ExistingPartition(topic, partition); err == nil {
		pp := b.idem.forPartition(topic, partition)
		pp.mu.Lock()
		_ = pp.loadFrom(plog) // a restart forgets nothing the log still holds.
		pp.mu.Unlock()
	}
	for _, p := range b.idem.snapshot(topic, partition) {
		a := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
		a.ProducerID, a.ProducerEpoch, a.LastSequence, a.LastTimestamp = p.pid, int32(p.epoch), p.lastSeq, p.lastUsedMs
		a.CoordinatorEpoch, a.CurrentTxnStartOffset = 0, -1
		if first, ok := open[p.pid]; ok {
			a.CurrentTxnStartOffset = first
		}
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProducerID < out[j].ProducerID })
	return out
}
