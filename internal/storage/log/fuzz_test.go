package log

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// Foundation B12: every parser of bytes a client sent has a fuzz target. These
// are the log's: the Produce payload check, the recovery scan over a segment
// file, and the decompressors. (The record reader's is FuzzDecodeRecords.)

// ValidateBatches runs on every Produce payload before anything is written. It
// must never panic, and a payload it accepts must be exactly whole batches: the
// header walk the engine does afterwards lands on the end of the input.
func FuzzValidateBatches(f *testing.F) {
	one := makeBatch(3, []byte("hello"))
	two := append(append([]byte(nil), one...), makeBatch(1, nil)...)
	f.Add(one)
	f.Add(two)
	f.Add(one[:len(one)-1])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if ValidateBatches(b) != nil {
			return
		}
		pos := 0
		for pos < len(b) {
			info, ok := parseHeader(b[pos:])
			if !ok || info.totalBytes <= 0 || pos+int(info.totalBytes) > len(b) {
				t.Fatalf("accepted a payload whose batch at %d does not fit (ok=%v, %+v, len %d)", pos, ok, info, len(b))
			}
			if _, ok := ParseBatch(b[pos : pos+int(info.totalBytes)]); !ok {
				t.Fatalf("accepted a batch at %d that ParseBatch refuses", pos)
			}
			pos += int(info.totalBytes)
		}
		if got := LargestBatch(b); got > int64(len(b)) {
			t.Fatalf("LargestBatch %d is larger than the payload (%d)", got, len(b))
		}
	})
}

// scanLog is what recovery truncates a segment by. Whatever bytes are on disk it
// must not panic, and the valid end it reports is on a batch boundary inside the
// file, so truncating there leaves whole batches only.
func FuzzScanLog(f *testing.F) {
	b0, b1 := makeBatch(3, []byte("hello")), makeBatch(2, []byte("world"))
	seg := append(append([]byte(nil), b0...), b1...)
	f.Add(seg, int64(0), int64(64))
	f.Add(seg[:len(seg)-4], int64(10), int64(4096))
	f.Add(append(seg, 0xff, 0xff), int64(0), int64(1))
	f.Fuzz(func(t *testing.T, data []byte, base, interval int64) {
		read := func(pos int64, n int) ([]byte, error) {
			if pos < 0 || n < 0 || pos+int64(n) > int64(len(data)) {
				t.Fatalf("scanLog read [%d,+%d) of a %d byte segment", pos, n, len(data))
			}
			return data[pos : pos+int64(n)], nil
		}
		validEnd, leo, want, _, err := scanLog(read, int64(len(data)), base, interval)
		if err != nil {
			t.Fatal(err)
		}
		if validEnd < 0 || validEnd > int64(len(data)) {
			t.Fatalf("validEnd %d outside the %d byte segment", validEnd, len(data))
		}
		// validEnd must be reachable by walking batch headers from the start.
		var pos int64
		for pos < validEnd {
			info, ok := parseHeader(data[pos:])
			if !ok {
				t.Fatalf("no batch header at %d, below validEnd %d", pos, validEnd)
			}
			pos += info.totalBytes
		}
		if pos != validEnd {
			t.Fatalf("validEnd %d is not a batch boundary (the walk ends at %d)", validEnd, pos)
		}
		for _, e := range want {
			if e.position < 0 || e.position >= validEnd {
				t.Fatalf("index entry at %d, valid end %d", e.position, validEnd)
			}
		}
		if validEnd == 0 && leo != base {
			t.Fatalf("an empty valid log has leo %d, base %d", leo, base)
		}
	})
}

// Decompress takes the body of a batch or of a telemetry push, from the network.
// For every codec: no panic, and nothing larger than the limit comes back. (Codec
// 0 returns the body as it is; the caller bounds that, as PushTelemetry does.)
func FuzzDecompress(f *testing.F) {
	for codec := 0; codec <= 5; codec++ {
		f.Add(uint8(codec), []byte("hello hello hello hello"), uint16(64))
	}
	good, _ := compress(codecGzip, bytes.Repeat([]byte("a"), 100))
	f.Add(uint8(codecGzip), good, uint16(10))
	f.Add(uint8(codecSnappy), append(append([]byte(nil), xerialMagic...), 0, 0, 0), uint16(100))
	f.Add(uint8(codecSnappy), xerialMagic, uint16(100))
	// A whole xerial stream (the 16 byte header, then length-prefixed snappy blocks, as
	// the Java client writes it), the same cut inside its second block, and one whose
	// block length claims more than is there: what the block reader has to walk. Without
	// these no seed got past the header.
	xerial := append(append([]byte(nil), xerialMagic...), 0, 0, 0, 1, 0, 0, 0, 1)
	for _, part := range []string{"hello hello hello hello", "world world world"} {
		block := snappy.Encode(nil, []byte(part))
		xerial = binary.BigEndian.AppendUint32(xerial, uint32(len(block)))
		xerial = append(xerial, block...)
	}
	f.Add(uint8(codecSnappy), xerial, uint16(100))
	f.Add(uint8(codecSnappy), xerial, uint16(30))
	f.Add(uint8(codecSnappy), xerial[:len(xerial)-5], uint16(100))
	f.Add(uint8(codecSnappy), append(append([]byte(nil), xerial[:16]...), 0x7f, 0xff, 0xff, 0xff, 1), uint16(100))
	// A zstd frame declaring a 64 MiB window over a megabyte of zeros: refused before the
	// window is allocated (the produce path's bomb), and a tiny snappy block that claims
	// the whole limit.
	enc, _ := zstd.NewWriter(nil, zstd.WithWindowSize(64<<20), zstd.WithEncoderConcurrency(1))
	f.Add(uint8(codecZstd), enc.EncodeAll(make([]byte, 1<<20), nil), uint16(65535))
	f.Add(uint8(codecSnappy), []byte{0xff, 0xff, 0xff, 0x7f, 0x00}, uint16(65535))
	f.Fuzz(func(t *testing.T, codec uint8, body []byte, limit uint16) {
		out, err := Decompress(int(codec%6), body, int(limit))
		if err == nil && codec%6 != codecNone && len(out) > int(limit) {
			t.Fatalf("codec %d inflated to %d bytes past the limit %d", codec%6, len(out), limit)
		}
	})
}

// A xerial-framed snappy body is a magic and a 16 byte header; one that stops
// inside the header used to slice past its end and panic the connection's
// goroutine (reachable by Produce with snappy and by PushTelemetry codec 2).
func TestDecompressShortXerialHeaderIsAnError(t *testing.T) {
	for n := len(xerialMagic); n < 16; n++ {
		body := make([]byte, n)
		copy(body, xerialMagic)
		if _, err := Decompress(codecSnappy, body, 1<<20); err == nil {
			t.Errorf("a %d byte xerial header decoded", n)
		}
	}
	// Exactly the header and no blocks is an empty stream, not an error.
	hdr := make([]byte, 16)
	copy(hdr, xerialMagic)
	binary.BigEndian.PutUint32(hdr[8:], 1)
	if out, err := Decompress(codecSnappy, hdr, 1<<20); err != nil || len(out) != 0 {
		t.Errorf("a header with no blocks: %q, %v", out, err)
	}
}
