package protocol

// RequestHeader carries the fields common to every Kafka request, decoded far
// enough to route and log.
//
// Request header versions ≥2 append a tagged-fields block after client_id. The
// skeleton does not consume it here — full, version-aware request decoding is the
// handler's job (via kmsg). ParseRequestHeader therefore reports the body offset
// as the position immediately after client_id, and any header tagged fields fall
// at the start of the body slice for the handler to skip.
type RequestHeader struct {
	APIKey        APIKey
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ParseRequestHeader decodes the request header from a frame payload and returns
// the header together with the byte offset at which the request body begins.
func ParseRequestHeader(frame []byte) (RequestHeader, int, error) {
	r := NewReader(frame)
	h := RequestHeader{
		APIKey:        APIKey(r.Int16()),
		APIVersion:    r.Int16(),
		CorrelationID: r.Int32(),
	}
	clientID, _ := r.NullableString()
	h.ClientID = clientID
	if err := r.Err(); err != nil {
		return RequestHeader{}, 0, err
	}
	return h, r.Pos(), nil
}
