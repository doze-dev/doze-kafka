package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// DecodeRequest decodes a request body into its kmsg type for the given header,
// skipping the request-header tagged fields on flexible versions. Handlers that
// need the request (e.g. Metadata) call this; ApiVersions does not, since it only
// builds a response.
func DecodeRequest(header RequestHeader, body []byte) (kmsg.Request, error) {
	req := kmsg.RequestForKey(int16(header.APIKey))
	if req == nil {
		return nil, fmt.Errorf("protocol: unknown api key %d", int16(header.APIKey))
	}
	// A version the message type has no definition for is not a request we can
	// decode meaningfully (the fuzzer found a header claiming version 12336).
	// Refuse it before parsing any of the body.
	if header.APIVersion < 0 || header.APIVersion > req.MaxVersion() {
		return nil, fmt.Errorf("protocol: %s v%d is outside the known range 0-%d",
			header.APIKey.Name(), header.APIVersion, req.MaxVersion())
	}
	req.SetVersion(header.APIVersion)
	if req.IsFlexible() {
		body = skipTaggedFields(body)
	}
	if err := req.ReadFrom(body); err != nil {
		return nil, fmt.Errorf("protocol: decode %s v%d: %w", header.APIKey.Name(), header.APIVersion, err)
	}
	return req, nil
}

// EncodeResponse frames a response for the wire: the correlation id, then — for
// flexible responses — the response-header tagged fields, then the response body
// encoded at resp's version. ApiVersions is the one exception: its response header
// is always v0 (no tagged fields), because the client must parse the response
// before it knows the broker's negotiated version.
//
// The caller sets resp's version (via SetVersion) before calling.
func EncodeResponse(header RequestHeader, resp kmsg.Response) []byte {
	return EncodeResponseSized(header, resp, 128)
}

// EncodeResponseSized is EncodeResponse with a capacity hint for the encoded
// response, so a large one (a fetch of megabytes of records) is written into one
// right-sized buffer instead of growing by doubling and copying.
func EncodeResponseSized(header RequestHeader, resp kmsg.Response, sizeHint int) []byte {
	buf := make([]byte, 0, max(sizeHint, 128))
	buf = binary.BigEndian.AppendUint32(buf, uint32(header.CorrelationID))
	if header.APIKey != APIApiVersions && flexibleResponse(header.APIKey, resp.GetVersion()) {
		buf = append(buf, 0) // empty tagged-fields section (unsigned varint 0).
	}
	return resp.AppendTo(buf)
}

// flexibleResponse reports whether the (key, version) uses flexible (compact +
// tagged-field) encoding.
func flexibleResponse(key APIKey, version int16) bool {
	req := kmsg.RequestForKey(int16(key))
	if req == nil {
		return false
	}
	req.SetVersion(version)
	return req.IsFlexible()
}

// skipTaggedFields advances past a tagged-fields section — an unsigned-varint
// count followed by that many {tag, size, bytes} entries — and returns the
// remainder. It is defensive: a malformed section returns whatever is left rather
// than panicking.
func skipTaggedFields(b []byte) []byte {
	n, adv := binary.Uvarint(b)
	if adv <= 0 {
		return b
	}
	b = b[adv:]
	for range n {
		if _, adv = binary.Uvarint(b); adv <= 0 { // tag
			return b
		}
		b = b[adv:]
		size, adv2 := binary.Uvarint(b) // size
		if adv2 <= 0 {
			return b
		}
		b = b[adv2:]
		if uint64(len(b)) < size {
			return nil
		}
		b = b[size:]
	}
	return b
}
