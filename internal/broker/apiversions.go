package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleAPIVersions advertises the min/max version supported per API key — the
// single response that defines the entire compatibility envelope. The advertised
// set comes from the active version profile (selected by the declared Kafka
// version). It also handles the pre-negotiation downgrade: a client may send a
// version we don't support, and expects a v0-framed response carrying
// UNSUPPORTED_VERSION with our supported ranges, after which it retries lower.
func (b *Broker) handleAPIVersions(ctx context.Context, req *protocol.Request) ([]byte, error) {
	resp := kmsg.NewPtrApiVersionsResponse()
	_, _ = req.Decode() // kept for the observer: ApiVersions is where a client says what it is. A version we don't know fails to decode, and is answered all the same.
	for _, a := range b.advertised() {
		k := kmsg.NewApiVersionsResponseApiKey()
		k.ApiKey = int16(a.Key)
		k.MinVersion = a.Min
		k.MaxVersion = a.Max
		resp.ApiKeys = append(resp.ApiKeys, k)
	}

	var maxVer int16
	if r, ok := b.profile.RangeFor(kmsg.ApiVersions); ok {
		maxVer = r.Max
	}

	if v := req.Header.APIVersion; v < 0 || v > maxVer {
		// Downgrade dance: reply at v0 so any client can parse it, carrying the
		// error and our ranges. The client retries at a version we support.
		resp.SetVersion(0)
		resp.ErrorCode = kerr.UnsupportedVersion.Code
		return req.Reply(resp), nil
	}

	resp.SetVersion(req.Header.APIVersion)
	resp.ErrorCode = errNone
	return req.Reply(resp), nil
}
