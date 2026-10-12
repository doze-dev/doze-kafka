package protocol

// RequestHeader carries the fields common to every Kafka request, decoded far
// enough to route and log.
//
// Request header versions ≥2 append a tagged-fields block after client_id. It is
// not consumed here, because whether a request is that version depends on its API
// and version, which is the codec's knowledge: ParseRequestHeader reports the body
// as starting immediately after client_id, and DecodeRequest skips the header's
// tagged fields at the start of that body for the versions that have them.
type RequestHeader struct {
	APIKey        APIKey
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ParseRequestHeader decodes the request header from a frame payload and returns
// the header together with the byte offset at which the request body begins.
//
//fuzz:parser class=request-header FuzzParseAndDecodeRequest, FuzzDescribe
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
