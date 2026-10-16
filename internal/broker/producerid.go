package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// producerIDBlock is how many producer ids are reserved from the metadata store at
// a time. Each reservation is a durable write (an fsync, ~7 ms); every idempotent
// producer calls InitProducerId once per session, so a test suite that creates a
// client per test paid that for each. Ids only need to be unique, so a block can
// be handed out from memory; a crash skips the unused remainder.
const producerIDBlock = 1000

// nextProducerID returns a fresh producer id from the in-memory block, reserving
// a new block from the metadata store when it runs out.
func (b *Broker) nextProducerID() (int64, error) {
	b.pidMu.Lock()
	defer b.pidMu.Unlock()
	if b.pidNext >= b.pidEnd {
		first, err := b.meta.ReserveProducerIDs(producerIDBlock)
		if err != nil {
			return 0, err
		}
		b.pidNext, b.pidEnd = first, first+producerIDBlock
	}
	id := b.pidNext
	b.pidNext++
	return id, nil
}

// handleInitProducerID hands out a fresh producer id (epoch 0). It is needed even
// without transactions: producers default to enable.idempotence=true and call it
// before producing anything, so failing it puts the client into a fatal state.
// The id is what the produce path deduplicates on (idempotency.go). A
// transactional.id in the request is not acted on: there are no transactions
// here, and the transaction APIs that would follow are not advertised.
func (b *Broker) handleInitProducerID(ctx context.Context, req *protocol.Request, ir *kmsg.InitProducerIDRequest, resp *kmsg.InitProducerIDResponse) error {
	// A transactional id makes the caller the owner of that id's transactions (txn.go).
	if ir.TransactionalID != nil {
		if !b.canTxnID(req.Conn, *ir.TransactionalID, opWrite) {
			resp.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
			resp.ProducerID, resp.ProducerEpoch = -1, -1
			return nil
		}
		// An empty id is not "no id" (that is null): claim api V4.
		if *ir.TransactionalID == "" {
			resp.ErrorCode, resp.ProducerID, resp.ProducerEpoch = kerr.InvalidRequest.Code, -1, -1
			return nil
		}
		pid, epoch, code := b.initTxnProducer(*ir.TransactionalID, ir.TransactionTimeoutMillis)
		resp.ErrorCode, resp.ProducerID, resp.ProducerEpoch = code, pid, epoch
		return nil
	}
	if !b.canWriteIdempotently(req.Conn) {
		resp.ErrorCode = kerr.ClusterAuthorizationFailed.Code
		resp.ProducerID = -1
		resp.ProducerEpoch = -1
		return nil
	}
	pid, err := b.nextProducerID()
	if err != nil {
		return err
	}
	resp.ProducerID = pid
	resp.ProducerEpoch = 0
	return nil
}
