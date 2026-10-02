package protocol

import (
	"strings"
	"testing"
	"time"
)

// TestDecodeRequestRejectsOutOfRangeVersions: a version outside what kmsg
// defines for the key must be refused up front, never decoded.
func TestDecodeRequestRejectsOutOfRangeVersions(t *testing.T) {
	for _, v := range []int16{-1, 12336, 32767} {
		_, err := DecodeRequest(RequestHeader{APIKey: APIMetadata, APIVersion: v}, []byte{0, 0, 0, 0})
		if err == nil || !strings.Contains(err.Error(), "outside the known range") {
			t.Errorf("Metadata v%d: err = %v, want an out-of-range refusal", v, err)
		}
	}
	// In range still decodes.
	if _, err := DecodeRequest(RequestHeader{APIKey: APIMetadata, APIVersion: 1}, []byte{0, 0, 0, 0}); err != nil {
		t.Errorf("Metadata v1 should decode: %v", err)
	}
}

// TestTaggedFieldCountCannotSpin is the guard for third_party/kmsg (see its
// PATCHES.md). A flexible Produce request whose tagged-field count (4 billion)
// outruns its bytes makes upstream kmsg iterate until the count runs out —
// well over a minute for ~70 bytes. If the replace directive is dropped or the
// patched loop is lost, this test hangs until `go test` times it out.
func TestTaggedFieldCountCannotSpin(t *testing.T) {
	// Produce v11 (flexible): null transactional id, acks, timeout, an empty
	// topics array, then a tagged-field section claiming 2^32-1 entries.
	body := []byte{
		0,    // request-header tagged fields (skipped by DecodeRequest)
		0,    // transactional id: null
		0, 1, // acks
		0, 0, 0, 100, // timeout
		1,                            // topics: empty compact array
		0xff, 0xff, 0xff, 0xff, 0x0f, // tagged-field count = 2^32-1
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = DecodeRequest(RequestHeader{APIKey: APIProduce, APIVersion: 11}, body)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DecodeRequest is still spinning on an exhausted tagged-field section: third_party/kmsg is not in use")
	}
}

// TestDecodeRequestHostileInputIsCheap pins the cost, not just the outcome: the
// fuzzer's findings were decodes that errored correctly but took far too long.
// Every key, at representative versions, must reject garbage quickly.
func TestDecodeRequestHostileInputIsCheap(t *testing.T) {
	hostile := []byte("9000000\xd80\x000000\x000\xa9\x8b\xdd\\\x00")
	start := time.Now()
	for key := APIKey(0); key < 100; key++ {
		for _, v := range []int16{0, 1, 5, 12336} {
			_, _ = DecodeRequest(RequestHeader{APIKey: key, APIVersion: v}, hostile)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("decoding hostile bytes across all keys took %v; something is walking attacker-controlled counts", d)
	}
}

// Produce below v3 and Fetch below v4 carry the legacy message-set format, which
// the log does not understand; advertising them invited clients to send bytes it
// would have stored as garbage.
func TestProfilesDoNotAdvertiseLegacyMessageSetVersions(t *testing.T) {
	for _, major := range SupportedKafkaMajors() {
		prof, _ := ProfileFor(major)
		if r, ok := prof.RangeFor(APIProduce); !ok || r.Min < 3 {
			t.Errorf("Kafka %d profile advertises Produce from v%d; record batches start at v3", major, r.Min)
		}
		if r, ok := prof.RangeFor(APIFetch); !ok || r.Min < 4 {
			t.Errorf("Kafka %d profile advertises Fetch from v%d; record batches start at v4", major, r.Min)
		}
	}
}
