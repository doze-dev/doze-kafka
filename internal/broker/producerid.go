package broker

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// handleInitProducerID hands out a fresh monotonic producer id (epoch 0). This is
// mandatory even without transactions: modern producers default to
// enable.idempotence=true and call it before producing anything, so failing it
// puts the client into a fatal state. We accept producer sequence numbers without
// enforcing real dedup (at-least-once is fine for local dev) and never advertise
// transactions.
func (b *Broker) handleInitProducerID(ctx context.Context, req *protocol.Request) ([]byte, error) {
	pid, err := b.meta.NextProducerID()
	if err != nil {
		return nil, err
	}
	resp := kmsg.NewPtrInitProducerIDResponse()
	resp.SetVersion(req.Header.APIVersion)
	resp.ProducerID = pid
	resp.ProducerEpoch = 0
	return protocol.EncodeResponse(req.Header, resp), nil
}
