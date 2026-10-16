package broker

// A broker over a disk that can lose power. Every durability test in this
// package that asks "is it on disk?" asks this rig, which answers from what was
// fsynced (internal/powercut), not from a count of calls that were supposed to
// fsync.

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/powercut"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

type rig struct {
	t    *testing.T
	root string
	pc   *powercut.Model // nil for a rig opened on what a cut left.
	seg  int64

	b    *Broker
	logs *log.Manager
	meta meta.Store
	open bool

	// failSync fails every flush before it is tried (the manager's BeforeSync).
	failSync atomic.Bool
}

// newRig starts a broker on an empty directory with a power-cut model over it.
// segBytes is the segment size (small, so that a few records roll).
func newRig(t *testing.T, segBytes int64) *rig {
	t.Helper()
	root := t.TempDir()
	r := &rig{t: t, root: root, seg: segBytes, pc: powercut.New(root)}
	t.Cleanup(log.SetFSHookForTest(func(ev log.FSEvent) error {
		return r.pc.Observe(string(ev.Op), ev.Path, ev.To, ev.Done)
	}))
	t.Cleanup(meta.SetDurabilityHookForTest(func(event, path string) {
		switch event {
		case "commit":
			r.pc.Synced(path)
		case "dirsync":
			r.pc.DirSynced(path)
		}
	}))
	t.Cleanup(r.shutdown)
	r.start()
	return r
}

func (r *rig) data() string { return filepath.Join(r.root, "data") }

// start opens the stores and the broker over the rig's directory, as the server
// does: registry first, then the logs, then what a stop left half done.
func (r *rig) start() {
	r.t.Helper()
	store, err := meta.Open(filepath.Join(r.root, "meta.db"))
	if err != nil {
		r.t.Fatalf("open meta.db: %v", err)
	}
	cfg := config.Default()
	cfg.FsyncInterval = 0
	cfg.SegmentBytes = r.seg
	topics, err := store.ListTopics()
	if err != nil {
		r.t.Fatal(err)
	}
	configs := map[string]map[string]string{}
	for _, tp := range topics {
		configs[tp.Name] = tp.Config
	}
	logs, err := log.OpenManager(r.data(), log.ManagerOptions{
		SegmentBytes: r.seg, IndexIntervalBytes: 64, TopicLimits: InitialTopicLimits(cfg, configs),
		BeforeSync: func() error {
			if r.failSync.Load() {
				return errDiskOnFire
			}
			return nil
		},
	})
	if err != nil {
		r.t.Fatalf("open the data directory: %v", err)
	}
	for _, tp := range topics {
		if err := logs.EnsurePartitionDirs(tp.Name, tp.Partitions); err != nil {
			r.t.Fatal(err)
		}
	}
	b, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), logs, store)
	if err != nil {
		r.t.Fatal(err)
	}
	b.RecoverTransactions()
	r.b, r.logs, r.meta, r.open = b, logs, store, true
}

// kill ends the broker as SIGKILL does: nothing is flushed. The page cache (the
// live directory) keeps everything it wrote.
func (r *rig) kill() {
	if !r.open {
		return
	}
	r.b.Stop()
	r.logs.AbandonForTest()
	r.meta.Close()
	r.open = false
}

// restart is kill -9 and a start on the same directory.
func (r *rig) restart() {
	r.t.Helper()
	r.kill()
	r.start()
}

// stop is a graceful shutdown.
func (r *rig) stop() {
	r.t.Helper()
	if !r.open {
		return
	}
	r.b.Stop()
	r.meta.Close()
	if err := r.logs.Close(); err != nil {
		r.t.Fatalf("graceful stop: %v", err)
	}
	r.open = false
}

func (r *rig) shutdown() {
	if r.open {
		r.b.Stop()
		r.meta.Close()
		r.logs.Close()
		r.open = false
	}
}

// cut is the power going now: a broker started on what the disk would hold.
// The rig it was cut from carries on untouched.
func (r *rig) cut(mode powercut.Mode, seed int64) *rig {
	r.t.Helper()
	return r.cutOf(r.pc, mode, seed)
}

// cutOf is cut for a state of the disk kept earlier (powercut.Model.Kept).
func (r *rig) cutOf(pc *powercut.Model, mode powercut.Mode, seed int64) *rig {
	r.t.Helper()
	dst, err := pc.Cut(r.t.TempDir(), mode, rand.New(rand.NewSource(seed)))
	if err != nil {
		r.t.Fatal(err)
	}
	after := &rig{t: r.t, root: dst, seg: r.seg}
	r.t.Cleanup(after.shutdown)
	after.start()
	return after
}

// call runs one request through its handler, at the version set on it, and
// returns the decoded answer.
func (r *rig) call(req kmsg.Request) kmsg.Response {
	r.t.Helper()
	key := protocol.APIKey(req.Key())
	out, err := r.b.handlers()[key](context.Background(), &protocol.Request{
		Header: protocol.RequestHeader{APIKey: key, APIVersion: req.GetVersion(), CorrelationID: 1},
		Body:   req.AppendTo(nil),
	})
	if err != nil {
		r.t.Fatalf("%s: %v", protocol.APIName(key), err)
	}
	resp := req.ResponseKind()
	if err := resp.ReadFrom(out[4:]); err != nil {
		r.t.Fatalf("%s: decode: %v", protocol.APIName(key), err)
	}
	return resp
}

// createTopic creates a topic through CreateTopics and returns the error code.
func (r *rig) createTopic(name string, partitions int32, configs map[string]string) int16 {
	r.t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.SetVersion(4)
	req.TimeoutMillis = 1000
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, 1
	for k, v := range configs {
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		val := v
		c.Name, c.Value = k, &val
		rt.Configs = append(rt.Configs, c)
	}
	req.Topics = append(req.Topics, rt)
	return r.call(req).(*kmsg.CreateTopicsResponse).Topics[0].ErrorCode
}

func (r *rig) mustCreateTopic(name string, partitions int32, configs map[string]string) {
	r.t.Helper()
	if code := r.createTopic(name, partitions, configs); code != 0 {
		r.t.Fatalf("create topic %s: code %d", name, code)
	}
}

// produce sends one Produce for one partition and returns its answer.
func (r *rig) produce(topic string, partition int32, acks int16, txnID *string, records []byte) kmsg.ProduceResponseTopicPartition {
	r.t.Helper()
	return r.produceMany(topic, acks, txnID, []int32{partition}, map[int32][]byte{partition: records})[0]
}

// produceMany sends one Produce for several partitions of a topic, in order.
func (r *rig) produceMany(topic string, acks int16, txnID *string, order []int32, records map[int32][]byte) []kmsg.ProduceResponseTopicPartition {
	r.t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(8)
	req.Acks, req.TimeoutMillis, req.TransactionID = acks, 1000, txnID
	pt := kmsg.NewProduceRequestTopic()
	pt.Topic = topic
	for _, p := range order {
		pp := kmsg.NewProduceRequestTopicPartition()
		pp.Partition, pp.Records = p, records[p]
		pt.Partitions = append(pt.Partitions, pp)
	}
	req.Topics = append(req.Topics, pt)
	return r.call(req).(*kmsg.ProduceResponse).Topics[0].Partitions
}

// payloadBatch is a one-record batch whose body is payload, opaque to the log:
// what a test compares when it reads the record back. pid -1 is a plain producer;
// attrs 0x10 makes it transactional.
func payloadBatch(pid int64, epoch int16, seq int32, attrs uint16, payload string) []byte {
	b := make([]byte, 61+len(payload))
	binary.BigEndian.PutUint32(b[8:], uint32(len(b)-12))
	b[16] = 2
	binary.BigEndian.PutUint16(b[21:], attrs)
	binary.BigEndian.PutUint64(b[43:], uint64(pid))
	binary.BigEndian.PutUint16(b[51:], uint16(epoch))
	binary.BigEndian.PutUint32(b[53:], uint32(seq))
	binary.BigEndian.PutUint32(b[57:], 1)
	copy(b[61:], payload)
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	return b
}

// stored is one batch as a partition holds it.
type stored struct {
	offset  int64
	payload string
	control bool
}

// read returns every batch a consumer of the partition gets, from the log start
// to the end, or to the last stable offset without the batches of aborted
// transactions when committed is set. Every byte served must pass the checks a
// produce is held to.
func (r *rig) read(topic string, partition int32, committed bool) []stored {
	r.t.Helper()
	part, err := r.logs.ExistingPartition(topic, partition)
	if err != nil {
		r.t.Fatalf("open %s-%d: %v", topic, partition, err)
	}
	var out []stored
	next := part.LogStartOffset()
	for {
		data, lso, aborted, err := part.ReadIsolated(next, 1<<20, committed)
		if err != nil {
			r.t.Fatalf("read %s-%d at %d: %v", topic, partition, next, err)
		}
		if len(data) == 0 {
			end := part.LogEndOffset()
			if committed {
				end = lso
			}
			if next < end {
				r.t.Fatalf("read %s-%d at %d returned nothing before the end %d", topic, partition, next, end)
			}
			return out
		}
		for pos := 0; pos < len(data); {
			h, ok := log.ParseBatch(data[pos:])
			if !ok || h.TotalBytes <= 0 || pos+h.TotalBytes > len(data) {
				r.t.Fatalf("read %s-%d at %d: bytes that are not a batch at %d of %d", topic, partition, next, pos, len(data))
			}
			batch := data[pos : pos+h.TotalBytes]
			base := int64(binary.BigEndian.Uint64(batch))
			if crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)) != binary.BigEndian.Uint32(batch[17:]) {
				r.t.Fatalf("read %s-%d: the batch at offset %d fails its CRC: a consumer is served a corrupt batch", topic, partition, base)
			}
			skip := false
			if committed && h.Transactional && !h.Control {
				for _, a := range aborted {
					if a.ProducerID == h.ProducerID && a.FirstOffset <= base && base <= a.LastOffset {
						skip = true
					}
				}
			}
			if !skip && base >= next {
				out = append(out, stored{offset: base, payload: string(batch[61:]), control: h.Control})
			}
			next = base + int64(h.RecordCount)
			pos += h.TotalBytes
		}
	}
}

// payloadAt is the payload of the data batch at offset, and whether there is one.
func payloadAt(batches []stored, offset int64) (string, bool) {
	for _, s := range batches {
		if s.offset == offset && !s.control {
			return s.payload, true
		}
	}
	return "", false
}
