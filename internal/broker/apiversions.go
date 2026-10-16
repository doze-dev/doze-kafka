package broker

import (
	"context"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"

	"github.com/twmb/franz-go/pkg/kerr"
)

// handleAPIVersions advertises the min/max version supported per API key: the
// single response that defines the entire compatibility envelope. The advertised
// set comes from the active version profile (selected by the declared Kafka
// version).
//
// Two answers are not the list, and both are Apache Kafka's, recorded against 4.3.1
// (conformance/recordings/apiversions-*.json):
//
//   - A version of ApiVersions the broker does not serve is answered in the v0 form
//     (the only one a client is sure to read) with UNSUPPORTED_VERSION and one row: the
//     versions of ApiVersions itself. The client asks again at one of those and is
//     given the list then. The body of such a request is not read.
//   - From v3 a client says what it is. A software name or version that is empty, or
//     is not letters and digits with dots and dashes between them, is INVALID_REQUEST
//     with no rows, and the connection lives on.
func (b *Broker) handleAPIVersions(ctx context.Context, req *protocol.Request) ([]byte, error) {
	resp := kmsg.NewPtrApiVersionsResponse()

	var served protocol.VersionRange
	if r, ok := b.profile.RangeFor(kmsg.ApiVersions); ok {
		served = r
	}
	if v := req.Header.APIVersion; v < served.Min || v > served.Max {
		k := kmsg.NewApiVersionsResponseApiKey()
		k.ApiKey, k.MinVersion, k.MaxVersion = int16(kmsg.ApiVersions), served.Min, served.Max
		resp.ApiKeys = append(resp.ApiKeys, k)
		resp.SetVersion(0)
		resp.ErrorCode = kerr.UnsupportedVersion.Code
		return req.Reply(resp), nil
	}
	resp.SetVersion(req.Header.APIVersion)
	req.Conn.AnsweredVersions() // the one ApiVersions a connection that has not logged in may send.

	// A body that is not an ApiVersions request, at a version that is served, is
	// answered the list all the same, as it always was here: what Kafka does with one
	// is not recorded, and until it is the answer stays.
	if q, err := req.Decode(); err == nil {
		if av, ok := q.(*kmsg.ApiVersionsRequest); ok && req.Header.APIVersion >= 3 {
			if !protocol.ValidClientSoftware(av.ClientSoftwareName) || !protocol.ValidClientSoftware(av.ClientSoftwareVersion) {
				resp.ErrorCode = kerr.InvalidRequest.Code
				return req.Reply(resp), nil
			}
			req.Conn.SetClientSoftware(av.ClientSoftwareName, av.ClientSoftwareVersion)
		}
	}

	for _, a := range b.advertised() {
		k := kmsg.NewApiVersionsResponseApiKey()
		k.ApiKey = int16(a.Key)
		k.MinVersion = a.Min
		k.MaxVersion = a.Max
		resp.ApiKeys = append(resp.ApiKeys, k)
	}
	resp.ErrorCode = errNone
	return req.Reply(resp), nil
}
