package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// Foundation B12: after a request is decoded, the traffic recorder describes it
// with the API table's summariser, on whatever a client managed to get decoded
// (and a zero-valued response when the handler failed). A summariser that panics
// takes the connection's goroutine with it, so each is fed fuzzed requests.
func FuzzDescribe(f *testing.F) {
	for _, a := range APIs {
		f.Add(int16(a.Key), int16(0), []byte{})
		f.Add(int16(a.Key), int16(a.Versions[3].Max), []byte{0, 0, 0, 1, 0, 0, 0, 0})
	}
	f.Fuzz(func(t *testing.T, key, version int16, body []byte) {
		hdr := make([]byte, 0, 14+len(body))
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(key))
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(version))
		hdr = binary.BigEndian.AppendUint32(hdr, 7)
		hdr = binary.BigEndian.AppendUint16(hdr, 0xffff) // null client id
		hdr = append(hdr, body...)
		header, off, err := ParseRequestHeader(hdr)
		if err != nil {
			return
		}
		q, err := DecodeRequest(header, hdr[off:])
		if err != nil {
			return
		}
		for _, answered := range []kmsg.Response{nil, kmsg.ResponseForKey(int16(header.APIKey))} {
			r := &Request{Header: header, decoded: q, answered: answered}
			_ = r.Describe()
			_ = DescribeFacts(q, answered)
		}
	})
}
