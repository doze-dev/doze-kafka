package protocol

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// bodyOf is a request of the key at the version as it comes off the wire after its
// header: the request-header tagged fields (an empty section) on a flexible version,
// then the body.
func bodyOf(key APIKey, v int16) []byte {
	req := kmsg.RequestForKey(int16(key))
	req.SetVersion(v)
	b := req.AppendTo(nil)
	if req.IsFlexible() {
		b = append([]byte{0}, b...)
	}
	return b
}

// The first version (0) and the last the type defines decode; the one past each end is
// refused.
func TestDecodeRequestTakesTheFirstAndLastVersion(t *testing.T) {
	for _, key := range []APIKey{kmsg.Metadata, kmsg.ApiVersions, kmsg.Produce} {
		max := kmsg.RequestForKey(int16(key)).MaxVersion()
		for _, v := range []int16{0, max} {
			if _, err := DecodeRequest(RequestHeader{APIKey: key, APIVersion: v}, bodyOf(key, v)); err != nil {
				t.Errorf("%s v%d: %v", APIName(key), v, err)
			}
		}
		if _, err := DecodeRequest(RequestHeader{APIKey: key, APIVersion: max + 1}, bodyOf(key, max)); err == nil {
			t.Errorf("%s v%d was decoded", APIName(key), max+1)
		}
	}
}

// The response header of a flexible version carries an empty tagged-field section,
// which ApiVersions never does and a version before flexibility does not either.
func TestEncodeResponseTagsOnlyFlexibleVersionsOtherThanApiVersions(t *testing.T) {
	h := RequestHeader{CorrelationID: 7}
	meta := func(v int16) []byte {
		resp := kmsg.NewPtrMetadataResponse()
		resp.SetVersion(v)
		return resp.AppendTo(nil)
	}
	for _, c := range []struct {
		v    int16
		flex bool
	}{{8, false}, {9, true}} {
		h.APIKey = kmsg.Metadata
		resp := kmsg.NewPtrMetadataResponse()
		resp.SetVersion(c.v)
		want := append([]byte{0, 0, 0, 7}, meta(c.v)...)
		if c.flex {
			want = append([]byte{0, 0, 0, 7, 0}, meta(c.v)...)
		}
		if got := EncodeResponse(h, resp); string(got) != string(want) {
			t.Errorf("Metadata v%d: % x, want % x", c.v, got, want)
		}
	}
	h.APIKey = kmsg.ApiVersions
	av := kmsg.NewPtrApiVersionsResponse()
	av.SetVersion(3)
	want := append([]byte{0, 0, 0, 7}, av.AppendTo(nil)...)
	if got := EncodeResponse(h, av); string(got) != string(want) {
		t.Errorf("ApiVersions v3: % x, want % x", got, want)
	}
}

func TestFlexibleResponseOfAnUnknownKeyIsFalse(t *testing.T) {
	if flexibleResponse(APIKey(9999), 3) {
		t.Error("an unknown key is flexible")
	}
	if !flexibleResponse(kmsg.Metadata, 9) || flexibleResponse(kmsg.Metadata, 8) {
		t.Error("Metadata flexibility is v9 and later")
	}
}

func TestSkipTaggedFields(t *testing.T) {
	for _, c := range []struct {
		name string
		in   []byte
		want []byte
		nil  bool
	}{
		{"none", []byte{0, 9, 9}, []byte{9, 9}, false},
		{"one", []byte{1, 5, 2, 'a', 'b', 9}, []byte{9}, false},
		{"two", []byte{2, 1, 1, 'a', 2, 0, 9}, []byte{9}, false},
		{"one that ends the body", []byte{1, 5, 2, 'a', 'b'}, []byte{}, false},
		{"one that runs past the body", []byte{1, 5, 3, 'a', 'b'}, nil, true},
		{"a tag with no size", []byte{1, 5}, []byte{}, false},
		{"a count with no entries", []byte{3}, []byte{}, false},
		{"a count that does not end", []byte{0x80}, []byte{0x80}, false},
		{"a tag that does not end", []byte{1, 0x80}, []byte{0x80}, false},
		{"a size that does not end", []byte{1, 5, 0x80}, []byte{0x80}, false},
		{"empty", []byte{}, []byte{}, false},
	} {
		got := skipTaggedFields(c.in)
		if string(got) != string(c.want) || (got == nil) != c.nil {
			t.Errorf("%s: %v (nil %v), want %v (nil %v)", c.name, got, got == nil, c.want, c.nil)
		}
	}
}

func pipeConn(t *testing.T) *Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return newConn(a, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
}

// A peer whose address is not host:port is named as it is written.
func TestRemoteHostAndIPOfAPeerWithoutAPort(t *testing.T) {
	c := pipeConn(t)
	if got := c.RemoteHost(); got != "/pipe" {
		t.Errorf("RemoteHost = %q", got)
	}
	if got := c.RemoteIP(); got != "pipe" {
		t.Errorf("RemoteIP = %q", got)
	}
	var none *Conn
	if got := none.RemoteIP(); got != "" {
		t.Errorf("RemoteIP of no connection = %q", got)
	}
}

func TestClientSoftwareIsKeptWholeAndCutToItsBound(t *testing.T) {
	a := func(n int) string { return strings.Repeat("a", n) }
	for _, c := range []struct{ label, name, version, want string }{
		{"fits", "apache-kafka-java", "3.7.0", "apache-kafka-java 3.7.0"},
		{"name at the bound", a(128), "", a(128)},
		{"version at the bound", "", a(128), a(128)},
		{"name past the bound", a(200), "", a(125) + "…"},
		{"version past the bound", "", a(200), a(125) + "…"},
		{"one past the bound", a(129), "", a(125) + "…"},
		{"huge", a(1 << 20), a(1 << 20), a(125) + "…"},
		{"not characters", strings.Repeat("\x80", 200), "", "…"},
	} {
		if got := ClientSoftware(c.name, c.version); got != c.want {
			t.Errorf("%s: %q, want %q", c.label, got, c.want)
		}
	}
}

func TestValidClientSoftwareAtTheEdgesOfEachRange(t *testing.T) {
	for _, s := range []string{"a", "z", "A", "Z", "0", "9", "a-z", "A.Z", "0-9", "a9Zz"} {
		if !ValidClientSoftware(s) {
			t.Errorf("%q refused", s)
		}
	}
	for _, s := range []string{"", "`", "{", "@", "[", "/", ":", "-a", "a-", ".a", "a b", "a_b", "a/b", "a:b", "a@b", "a[b"} {
		if ValidClientSoftware(s) {
			t.Errorf("%q accepted", s)
		}
	}
}
