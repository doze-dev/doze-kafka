package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
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

// handleInitProducerID hands out a fresh monotonic producer id (epoch 0). This is
// mandatory even without transactions: modern producers default to
// enable.idempotence=true and call it before producing anything, so failing it
// puts the client into a fatal state. We accept producer sequence numbers without
// enforcing real dedup (at-least-once is fine for local dev) and never advertise
// transactions.
func (b *Broker) handleInitProducerID(ctx context.Context, req *protocol.Request, _ *kmsg.InitProducerIDRequest, resp *kmsg.InitProducerIDResponse) error {
	pid, err := b.nextProducerID()
	if err != nil {
		return err
	}
	resp.ProducerID = pid
	resp.ProducerEpoch = 0
	return nil
}
