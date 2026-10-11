package log

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// allocated is the bytes the process allocated while f ran (cumulative, so garbage
// counts: that is the point, a bomb's memory is gone only once it was taken).
func allocated(f func()) uint64 {
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	f()
	runtime.ReadMemStats(&b)
	return b.TotalAlloc - a.TotalAlloc
}

// A 6 KB frame that inflates to 60 MiB was fully inflated before its size was looked
// at, per connection and in parallel: a limit that is checked after the memory is
// taken is not a limit (TRF-2).
func TestDecompressZstdNeverAllocatesPastTheLimit(t *testing.T) {
	const limit = 512 << 10
	for name, window := range map[string]int{"a small window": 64 << 10, "a 2 MiB window": 2 << 20, "an 8 MiB window": 8 << 20, "a 64 MiB window": 64 << 20} {
		t.Run(name, func(t *testing.T) {
			enc, err := zstd.NewWriter(nil, zstd.WithWindowSize(window), zstd.WithEncoderConcurrency(1))
			if err != nil {
				t.Fatal(err)
			}
			bomb := enc.EncodeAll(make([]byte, 60<<20), nil)
			if len(bomb) > 1<<20 {
				t.Fatalf("the test's bomb is %d bytes: it is meant to be small", len(bomb))
			}
			var out []byte
			var derr error
			got := allocated(func() { out, derr = Decompress(codecZstd, bomb, limit) })
			if derr == nil || out != nil {
				t.Fatalf("a frame that inflates to 60 MiB was accepted past a %d byte limit (%d bytes)", limit, len(out))
			}
			if got > 6<<20 {
				t.Errorf("refusing it allocated %d bytes, want a small multiple of the %d limit", got, limit)
			}
		})
	}
	// What is under the limit still reads, with the windows real compressors declare
	// (zstd's default level asks for 2 MiB, whatever the payload is).
	for _, window := range []int{1 << 10, 2 << 20} {
		enc, _ := zstd.NewWriter(nil, zstd.WithWindowSize(window))
		want := bytes.Repeat([]byte("a metric "), 1000)
		out, err := Decompress(codecZstd, enc.EncodeAll(want, nil), limit)
		if err != nil || !bytes.Equal(out, want) {
			t.Errorf("window %d: %d bytes, %v", window, len(out), err)
		}
	}
}
