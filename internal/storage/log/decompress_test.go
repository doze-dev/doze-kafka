package log

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func compressWith(t *testing.T, name string, data []byte) (codec int, out []byte) {
	t.Helper()
	switch name {
	case "none":
		return 0, data
	case "gzip":
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(data)
		w.Close()
		return 1, b.Bytes()
	case "snappy (a plain block, as librdkafka writes it)":
		return 2, snappy.Encode(nil, data)
	case "snappy (xerial framing, as the Java client writes it)":
		block := snappy.Encode(nil, data)
		out := append([]byte{}, xerialMagic...)
		out = binary.BigEndian.AppendUint32(out, 1) // version
		out = binary.BigEndian.AppendUint32(out, 1) // oldest compatible version
		out = binary.BigEndian.AppendUint32(out, uint32(len(block)))
		return 2, append(out, block...)
	case "lz4":
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		w.Write(data)
		w.Close()
		return 3, b.Bytes()
	case "zstd":
		e, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		return 4, e.EncodeAll(data, nil)
	}
	t.Fatalf("no codec %q", name)
	return 0, nil
}

var codecNames = []string{"none", "gzip", "snappy (a plain block, as librdkafka writes it)", "snappy (xerial framing, as the Java client writes it)", "lz4", "zstd"}

// Telemetry payloads use the record batch codecs, and so does any client that pushes
// them: every one is read, including both framings snappy comes in.
func TestDecompressReadsEveryCodec(t *testing.T) {
	data := []byte(strings.Repeat("a metric name and a value, ", 200))
	for _, name := range codecNames {
		codec, packed := compressWith(t, name, data)
		got, err := Decompress(codec, packed, 1<<20)
		if err != nil || !bytes.Equal(got, data) {
			t.Errorf("%s: %v (got %d bytes, want %d)", name, err, len(got), len(data))
		}
	}
	if _, err := Decompress(9, []byte("x"), 100); err == nil {
		t.Error("an unknown codec was accepted")
	}
}

// A payload can claim far more than it weighs. The limit holds for every codec, and a
// snappy block that only claims a size is refused before anything is allocated for it.
func TestDecompressRefusesWhatInflatesPastItsLimit(t *testing.T) {
	big := make([]byte, 1<<20) // compresses to almost nothing.
	for _, name := range codecNames[1:] {
		codec, packed := compressWith(t, name, big)
		if _, err := Decompress(codec, packed, 4096); err == nil {
			t.Errorf("%s: a megabyte inflated under a 4 KiB limit", name)
		}
		if got, err := Decompress(codec, packed, 1<<20); err != nil || len(got) != len(big) {
			t.Errorf("%s: exactly at the limit failed: %v", name, err)
		}
	}
	// A raw snappy header that claims 3 GiB, with no data behind it, is refused without
	// the 3 GiB being allocated for it.
	claim := append(binary.AppendUvarint(nil, 3<<30), 0, 0)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := Decompress(2, claim, 1<<20)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Error("a snappy block claiming 3 GiB was accepted")
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 64<<20 {
		t.Errorf("refusing a snappy block that claims 3 GiB allocated %d bytes", grew)
	}
}

// A zstd frame declares the window the compressor was set to, not how little it
// compressed. This is a real PushTelemetry payload from librdkafka 2.15.1: 480 bytes
// that inflate to a few kilobytes of metrics, in a frame that declares a 2 MiB window
// and no content size. It was refused as "window size exceeded" under a 512 KiB limit,
// and the client stopped sending telemetry for good.
func TestDecompressReadsARealLibrdkafkaTelemetryFrame(t *testing.T) {
	frame, err := os.ReadFile("testdata/librdkafka-telemetry.zst")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decompress(4, frame, 512<<10)
	if err != nil {
		t.Fatalf("a librdkafka frame under a 512 KiB limit: %v", err)
	}
	if len(got) < 1000 || len(got) > 512<<10 {
		t.Errorf("inflated to %d bytes", len(got))
	}
	if !bytes.Contains(got, []byte("org.apache.kafka.producer.")) {
		t.Errorf("what came out is not the client's metrics: %q", got[:min(80, len(got))])
	}
	// The limit still holds against the same frame.
	if _, err := Decompress(4, frame, 100); err == nil {
		t.Error("a limit of 100 bytes let a 3 KiB payload through")
	}
}
