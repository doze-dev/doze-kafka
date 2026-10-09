package broker_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Soak + chaos harness. It drives the REAL compiled binary under sustained,
// correctness-checked load and — depending on mode — either injects faults
// (SIGKILL/SIGTERM restarts under load) or watches for leaks over a long, stable
// run. The long runs (TestSoak) happen only when SOAK_DURATION is set, by the
// tasks below and the scheduled workflow; the normal suite runs a few seconds of
// the same chaos as TestChaosSlice, so a change that breaks crash recovery fails
// the pull request and not a run next week.
//
//   go tool task soak                       # leak mode: no restarts, checks RSS/goroutine stability + zero dups
//   go tool task chaos                      # chaos mode: kill/restart under load, checks no acked record is ever lost
//   go tool task chaos SOAK_DURATION=1h     # longer run
//
// The load model makes verification exact: producer p is idempotent, acks=all, and
// writes only to partition (p mod partitions) with values "P<p>:<seq>", seq counting
// from 0. Because that's a single partition with in-order idempotent acks, an
// acknowledged seq means every lower seq was acknowledged too. After all the chaos,
// the harness reads the whole log back and asserts every acknowledged record is
// present and in order.

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestSoak is the long run: SOAK_DURATION says how long, SOAK_CHAOS=0 turns the
// restarts off (leak mode). Without SOAK_DURATION it does nothing.
func TestSoak(t *testing.T) {
	raw := os.Getenv("SOAK_DURATION")
	if raw == "" {
		t.Skip("set SOAK_DURATION (go tool task soak, go tool task chaos) to run the long soak")
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("SOAK_DURATION: %v", err)
	}
	runSoak(t, duration, envDefault("SOAK_CHAOS", "1") != "0", 0)
}

// TestChaosSlice is a few seconds of the chaos run: the real binary under
// idempotent acks=all load, killed (SIGKILL) and stopped (SIGTERM) and started
// again three times, after which every acknowledged record must be on disk, in
// order. It is the crash-recovery guarantee, in the suite that every change runs.
func TestChaosSlice(t *testing.T) {
	skipShort(t)
	runSoak(t, 6*time.Second, true, 1500*time.Millisecond)
}

// runSoak runs the load for duration; with chaos, restarts the broker every
// `every` (default: a quarter of the run, at least 8s), alternating kill and stop.
func runSoak(t *testing.T, duration time.Duration, chaos bool, every time.Duration) {
	producers := atoiDefault(envDefault("SOAK_PRODUCERS", "4"))
	partitions := int32(atoiDefault(envDefault("SOAK_PARTITIONS", "4")))

	t.Logf("soak: duration=%s chaos=%v producers=%d partitions=%d", duration, chaos, producers, partitions)

	bin := buildBinary(t)
	dir := t.TempDir()
	sup := &supervisor{
		t:         t,
		bin:       bin,
		addr:      fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		dataDir:   filepath.Join(dir, "data"),
		metaDB:    filepath.Join(dir, "meta.db"),
		pprofAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}
	// SOAK_RACE=1 builds the binary with the race detector (see buildBinary). The
	// soak never looked at what the binary printed, and a race report is printed, not
	// fatal: keep stderr, and fail the run if it holds one.
	if os.Getenv("SOAK_RACE") == "1" {
		sup.stderrLog = filepath.Join(dir, "broker.stderr")
	}
	sup.launch()
	defer sup.shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Minute)
	defer cancel()

	// Create the topic up front.
	admin := newClient(t, sup.addr)
	mustCreateTopic(t, ctx, admin, "soak", partitions)
	mustCreateTopic(t, ctx, admin, "soak-tx", 2)
	createCompacted(t, ctx, admin, "soak-compact", "compact", map[string]string{"delete.retention.ms": "1000"})
	admin.Close()

	wm := &watermark{hi: map[int]int64{}}
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Producers.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go runProducer(t, &wg, sup.addr, p, int32(p)%partitions, wm, done)
	}
	// One live consumer group, for load and to exercise groups under chaos.
	wg.Add(1)
	go runLiveConsumer(t, &wg, sup.addr, done)
	// A transactional producer: its transactions are all-or-nothing whatever the
	// broker is doing.
	txOut := &txnOutcomes{byTxn: map[int]string{}}
	wg.Add(1)
	go runTxnProducer(t, &wg, sup.addr, txOut, done)
	// A producer onto a compacted topic: the broker's cleaner is rewriting segments
	// while it is killed, and the latest acknowledged value of each key must survive.
	cmp := &compactAcks{latest: map[int]int{}}
	wg.Add(1)
	go runCompactProducer(t, &wg, sup.addr, cmp, done)

	// Health monitor (RSS + goroutines).
	health := &healthMonitor{sup: sup}
	wg.Add(1)
	go health.run(&wg, done)

	// Chaos: alternate graceful and ungraceful restarts.
	var restarts, kills int
	if chaos {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if every <= 0 {
				every = maxDuration(duration/4, 8*time.Second)
			}
			tick := time.NewTicker(every)
			defer tick.Stop()
			for {
				select {
				case <-done:
					return
				case <-tick.C:
					if restarts%2 == 0 {
						sup.sigkill()
						kills++
						t.Logf("chaos: SIGKILL + restart (#%d)", restarts+1)
					} else {
						sup.sigterm()
						t.Logf("chaos: SIGTERM + restart (#%d)", restarts+1)
					}
					sup.launch()
					restarts++
				}
			}
		}()
	}

	time.Sleep(duration)
	close(done)
	wg.Wait()

	// Make sure the broker is up for the final read (chaos may have left it down mid-cycle).
	if !dialOK(sup.addr) {
		sup.launch()
	}

	// Authoritative verification: read the whole log back and check invariants.
	perPartition := drainAll(t, ctx, sup.addr, "soak", partitions)
	verifySoak(t, perPartition, wm.snapshot(), producers, partitions, chaos, restarts)
	verifyTxnSoak(t, ctx, sup.addr, txOut)
	verifyCompactSoak(t, ctx, sup.addr, cmp)

	// Health verdict.
	health.report(t, chaos)

	if sup.stderrLog != "" {
		sup.shutdown()
		out, _ := os.ReadFile(sup.stderrLog)
		if i := strings.Index(string(out), "WARNING: DATA RACE"); i >= 0 {
			end := min(len(out), i+3000)
			t.Fatalf("the race detector reported a data race in the broker binary:\n%s", out[i:end])
		}
		t.Logf("no data race reported in %d bytes of the binary's stderr", len(out))
	}

	t.Logf("soak complete: %d restarts (%d ungraceful), %d records on disk",
		restarts, kills, countAll(perPartition))
}

// --- broker subprocess supervisor ---

type supervisor struct {
	t                          *testing.T
	bin, addr, dataDir, metaDB string
	pprofAddr                  string
	stderrLog                  string // where the binary's stderr goes, when it is kept.
	mu                         sync.Mutex
	cmd                        *exec.Cmd
	gen                        int
}

func (s *supervisor) launch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := exec.Command(s.bin,
		"-listen", s.addr, "-data-dir", s.dataDir, "-meta-db", s.metaDB,
		"-pprof-addr", s.pprofAddr, "-fsync-interval", "200ms",
		"-console=false")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if s.stderrLog != "" { // a race-detector run keeps what the binary says, across every restart.
		f, err := os.OpenFile(s.stderrLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			s.t.Fatalf("open %s: %v", s.stderrLog, err)
		}
		defer f.Close() // the child holds its own descriptor.
		cmd.Stderr = f
	}
	if err := cmd.Start(); err != nil {
		s.t.Fatalf("launch broker: %v", err)
	}
	s.cmd = cmd
	s.gen++
	deadline := time.Now().Add(15 * time.Second)
	for !dialOK(s.addr) {
		if time.Now().After(deadline) {
			s.t.Fatal("broker did not start listening")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *supervisor) signal(sig syscall.Signal) {
	s.mu.Lock()
	c := s.cmd
	s.mu.Unlock()
	if c != nil && c.Process != nil {
		c.Process.Signal(sig)
		c.Wait()
	}
}

func (s *supervisor) sigkill()  { s.signal(syscall.SIGKILL) }
func (s *supervisor) sigterm()  { s.signal(syscall.SIGTERM) }
func (s *supervisor) shutdown() { s.signal(syscall.SIGTERM) }

func (s *supervisor) pid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil && s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}

func (s *supervisor) generation() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// --- load ---

func runProducer(t *testing.T, wg *sync.WaitGroup, addr string, id int, partition int32, wm *watermark, done <-chan struct{}) {
	defer wg.Done()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(1_000_000),
		kgo.RequestRetries(1_000_000),
		kgo.RetryTimeout(2*time.Minute),
		kgo.ProduceRequestTimeout(20*time.Second),
	)
	if err != nil {
		t.Errorf("producer %d: %v", id, err)
		return
	}
	defer cl.Close()

	seq := 0
	for {
		select {
		case <-done:
			cl.Flush(context.Background())
			return
		default:
		}
		s := seq
		rec := &kgo.Record{Topic: "soak", Partition: partition, Value: []byte(fmt.Sprintf("P%d:%d", id, s))}
		cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
			if err == nil {
				wm.set(id, int64(s))
			}
		})
		seq++
		time.Sleep(time.Millisecond)
	}
}

func runLiveConsumer(t *testing.T, wg *sync.WaitGroup, addr string, done <-chan struct{}) {
	defer wg.Done()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumerGroup("soak-live"),
		kgo.GroupProtocol("classic"),
		kgo.ConsumeTopics("soak"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Errorf("live consumer: %v", err)
		return
	}
	defer cl.Close()
	for {
		select {
		case <-done:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cl.PollFetches(ctx) // discard; correctness is checked by the final drain.
		cancel()
	}
}

// --- watermark of acknowledged sequences ---

type watermark struct {
	mu sync.Mutex
	hi map[int]int64
}

func (w *watermark) set(id int, seq int64) {
	w.mu.Lock()
	if cur, ok := w.hi[id]; !ok || seq > cur {
		w.hi[id] = seq
	}
	w.mu.Unlock()
}

func (w *watermark) snapshot() map[int]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[int]int64, len(w.hi))
	for k, v := range w.hi {
		out[k] = v
	}
	return out
}

// --- final verification ---

// drainAll reads every record from every partition, in offset order, up to each
// partition's current end offset (retention is off, so nothing is missing below).
func drainAll(t *testing.T, ctx context.Context, addr, topic string, partitions int32) map[int32][]string {
	admin := newClient(t, addr)
	var total int64
	ends := make(map[int32]int64)
	for p := int32(0); p < partitions; p++ {
		end := listOffset(t, ctx, admin, topic, p, -1)
		ends[p] = end
		total += end
	}
	admin.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("drain consumer: %v", err)
	}
	defer cl.Close()

	out := make(map[int32][]string)
	var got int64
	deadline := time.Now().Add(3 * time.Minute)
	for got < total {
		if time.Now().After(deadline) {
			t.Fatalf("drain timed out at %d/%d records", got, total)
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		fs.EachRecord(func(r *kgo.Record) {
			out[r.Partition] = append(out[r.Partition], string(r.Value))
			got++
		})
	}
	return out
}

func verifySoak(t *testing.T, perPartition map[int32][]string, acked map[int]int64, producers int, partitions int32, chaos bool, restarts int) {
	totalDups := 0
	for p := 0; p < producers; p++ {
		part := int32(p) % partitions
		prefix := fmt.Sprintf("P%d:", p)

		// order = this producer's sequences in partition-offset order.
		var order []int
		seen := map[int]int{}
		maxSeq := -1
		for _, v := range perPartition[part] {
			if !strings.HasPrefix(v, prefix) {
				continue
			}
			n, err := strconv.Atoi(v[len(prefix):])
			if err != nil {
				t.Fatalf("producer %d: unparseable value %q", p, v)
			}
			order = append(order, n)
			seen[n]++
			if n > maxSeq {
				maxSeq = n
			}
		}

		// No loss: every acknowledged sequence [0, watermark] must be present. This is
		// the durability guarantee for acks=all — it must hold through every crash.
		if hi, ok := acked[p]; ok {
			for n := int64(0); n <= hi; n++ {
				if seen[int(n)] == 0 {
					t.Fatalf("producer %d: LOST acknowledged seq %d (watermark %d)", p, n, hi)
				}
			}
		}

		// Contiguity: the log holds a gap-free prefix [0, maxSeq]. A hole would mean a
		// record vanished from the middle — a real corruption/loss bug.
		for n := 0; n <= maxSeq; n++ {
			if seen[n] == 0 {
				t.Fatalf("producer %d: gap at seq %d (max present %d) — a record was lost", p, n, maxSeq)
			}
		}

		for _, c := range seen {
			if c > 1 {
				totalDups += c - 1
			}
		}

		// Leak mode has no restarts, so the log must be perfect: 0,1,2,… with no
		// duplicates and no reordering.
		if !chaos {
			for i, n := range order {
				if n != i {
					t.Fatalf("leak mode: producer %d position %d holds seq %d (want %d) — dup or reorder without any restart", p, i, n, i)
				}
			}
		}
	}

	if !chaos {
		t.Logf("no data loss, no gaps, zero duplicates across the run")
		return
	}

	// With restarts, a batch in flight exactly across a crash can be re-sent (the
	// in-memory dedup state resets on restart — a documented limitation): those show
	// up as duplicates, never as loss. Bound them so a regression that duplicates
	// freely still fails.
	limit := restarts*producers*64 + 64
	if totalDups > limit {
		t.Fatalf("chaos mode: %d duplicates over %d restarts exceeds the bound %d", totalDups, restarts, limit)
	}
	t.Logf("no acknowledged record lost and no gaps across %d restarts; %d cross-restart duplicates (bound %d)", restarts, totalDups, limit)
}

// --- health monitoring ---

type sample struct {
	gen  int
	rss  float64 // MB
	goro int
}

type healthMonitor struct {
	sup     *supervisor
	mu      sync.Mutex
	samples []sample
}

func (h *healthMonitor) run(wg *sync.WaitGroup, done <-chan struct{}) {
	defer wg.Done()
	for {
		select {
		case <-done:
			return
		case <-time.After(3 * time.Second):
			rss := rssMB(h.sup.pid())
			goro := goroutineCount(h.sup.pprofAddr)
			if rss > 0 && goro > 0 {
				h.mu.Lock()
				h.samples = append(h.samples, sample{gen: h.sup.generation(), rss: rss, goro: goro})
				h.mu.Unlock()
			}
		}
	}
}

func (h *healthMonitor) report(t *testing.T, chaos bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) == 0 {
		t.Log("health: no samples collected")
		return
	}
	var maxRSS float64
	var maxGoro int
	for _, s := range h.samples {
		if s.rss > maxRSS {
			maxRSS = s.rss
		}
		if s.goro > maxGoro {
			maxGoro = s.goro
		}
	}
	first, last := h.samples[0], h.samples[len(h.samples)-1]
	t.Logf("health: RSS max=%.0fMB (%.0f→%.0f), goroutines max=%d (%d→%d) over %d samples",
		maxRSS, first.rss, last.rss, maxGoro, first.goro, last.goro, len(h.samples))

	// Gross-leak guards (generous — the point is to catch runaway growth, not tune).
	if maxRSS > 500 {
		t.Errorf("health: RSS peaked at %.0fMB — possible memory leak", maxRSS)
	}
	if maxGoro > 5000 {
		t.Errorf("health: goroutines peaked at %d — possible goroutine leak", maxGoro)
	}

	// Leak mode runs a single broker generation, so growth over the run is meaningful.
	if !chaos && first.gen == last.gen && len(h.samples) >= 6 {
		if last.goro > first.goro*3+50 {
			t.Errorf("health: goroutines grew %d→%d in one process — leak", first.goro, last.goro)
		}
		if last.rss > first.rss*3+100 {
			t.Errorf("health: RSS grew %.0f→%.0fMB in one process — leak", first.rss, last.rss)
		}
	}
}

// --- small helpers ---

func dialOK(addr string) bool {
	c, err := (&net.Dialer{Timeout: 300 * time.Millisecond}).Dial("tcp", addr)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func rssMB(pid int) float64 {
	if pid == 0 {
		return 0
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return kb / 1024
}

func goroutineCount(pprofAddr string) int {
	resp, err := http.Get("http://" + pprofAddr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// First line is "goroutine profile: total N".
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "goroutine profile: total ") {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "goroutine profile: total ")))
			return n
		}
	}
	return 0
}

func countAll(m map[int32][]string) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

func atoiDefault(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 1
	}
	return n
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// --- transactional load ---

// A transactional producer writes transactions of txnSize records, "T<n>:<i>", to a
// partition chosen by n, and ends each: every fourth is aborted on purpose, the rest
// committed. What the broker is doing meanwhile (killed, restarted) can make a commit's
// outcome unknown to the producer, and that is recorded as such. The invariant checked
// afterwards, by a read-committed reader, is the one exactly-once rests on: a
// transaction is all there or not there at all.
const txnSize = 5

const (
	txnCommitted = "committed" // the commit was acknowledged.
	txnAborted   = "aborted"   // aborted on purpose, and the abort acknowledged.
	txnUnknown   = "unknown"   // the producer does not know how it ended.
)

type txnOutcomes struct {
	mu    sync.Mutex
	byTxn map[int]string
}

func (o *txnOutcomes) set(n int, how string) { o.mu.Lock(); o.byTxn[n] = how; o.mu.Unlock() }

func runTxnProducer(t *testing.T, wg *sync.WaitGroup, addr string, out *txnOutcomes, done <-chan struct{}) {
	defer wg.Done()
	newClient := func() *kgo.Client {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID("soak-tx"), kgo.TransactionTimeout(20*time.Second),
			kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RecordRetries(1_000_000), kgo.RequestRetries(1_000_000),
			kgo.RetryTimeout(time.Minute), kgo.ProduceRequestTimeout(10*time.Second), kgo.DialTimeout(3*time.Second))
		if err != nil {
			t.Errorf("transactional producer: %v", err)
			return nil
		}
		return cl
	}
	cl := newClient()
	if cl == nil {
		return
	}
	defer func() { cl.Close() }()
	for n := 0; ; n++ {
		select {
		case <-done:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		ok := func() bool {
			if err := cl.BeginTransaction(); err != nil {
				return false
			}
			for i := 0; i < txnSize; i++ {
				rec := &kgo.Record{Topic: "soak-tx", Partition: int32(n % 2), Value: []byte(fmt.Sprintf("T%d:%d", n, i))}
				if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
					return false
				}
			}
			return true
		}()
		purposeful := n%4 == 3
		switch {
		case !ok:
			out.set(n, txnUnknown)
			_ = cl.EndTransaction(ctx, kgo.TryAbort)
		case purposeful:
			if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
				out.set(n, txnUnknown)
			} else {
				out.set(n, txnAborted)
			}
		default:
			if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
				out.set(n, txnUnknown)
			} else {
				out.set(n, txnCommitted)
			}
		}
		cancel()
		time.Sleep(5 * time.Millisecond)
	}
}

// verifyTxnSoak reads the transactional topic as a read-committed consumer and checks
// that every transaction is whole or absent, in order, once, and that what the producer
// was told matches what is there.
func verifyTxnSoak(t *testing.T, ctx context.Context, addr string, out *txnOutcomes) {
	t.Helper()
	// Wait for the last stable offset to reach the end: nothing may still be open.
	admin := newClient(t, addr)
	defer admin.Close()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		settled := true
		for p := int32(0); p < 2; p++ {
			settled = settled && listOffsetIsoPart(t, ctx, admin, "soak-tx", p, -1, 1) == listOffset(t, ctx, admin, "soak-tx", p, -1)
		}
		if settled || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics("soak-tx"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		t.Fatalf("read-committed drain: %v", err)
	}
	defer cl.Close()
	perTxn := map[int][]int{}
	lastPerPartition := map[int32][2]int{}
	idle := 0
	for idle < 3 {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			n++
			var txn, i int
			if _, err := fmt.Sscanf(string(r.Value), "T%d:%d", &txn, &i); err != nil {
				t.Errorf("unreadable value %q", r.Value)
				return
			}
			// Within a partition transactions arrive in order, each record after the one before it.
			last, seen := lastPerPartition[r.Partition]
			if seen && (txn < last[0] || (txn == last[0] && i != last[1]+1)) {
				t.Errorf("partition %d: T%d:%d after T%d:%d is out of order", r.Partition, txn, i, last[0], last[1])
			}
			lastPerPartition[r.Partition] = [2]int{txn, i}
			perTxn[txn] = append(perTxn[txn], i)
		})
		if n == 0 {
			idle++
		} else {
			idle = 0
		}
	}

	out.mu.Lock()
	defer out.mu.Unlock()
	whole, absent, partial := 0, 0, 0
	for n, how := range out.byTxn {
		got := len(perTxn[n])
		switch {
		case got != 0 && got != txnSize:
			partial++
			t.Errorf("transaction %d is partial: %d of %d records visible to a read-committed reader (%v)", n, got, txnSize, perTxn[n])
		case got == txnSize && how == txnAborted:
			t.Errorf("transaction %d was aborted on purpose and acknowledged, and is visible", n)
		case got == 0 && how == txnCommitted:
			t.Errorf("transaction %d was acknowledged as committed and is missing", n)
		case got == txnSize:
			whole++
		default:
			absent++
		}
	}
	for n := range perTxn {
		if _, ok := out.byTxn[n]; !ok {
			t.Errorf("transaction %d is visible but the producer never finished it", n)
		}
	}
	t.Logf("transactions: %d whole, %d absent, %d partial, %d in total", whole, absent, partial, len(out.byTxn))
	if len(out.byTxn) == 0 {
		t.Error("the transactional producer never completed a transaction, so the invariant was not exercised")
	}
}

// --- compacted topic ---

const compactKeys = 8

type compactAcks struct {
	mu     sync.Mutex
	latest map[int]int // key → the highest sequence acknowledged for it.
}

func (c *compactAcks) set(key, seq int) {
	c.mu.Lock()
	if seq > c.latest[key] {
		c.latest[key] = seq
	}
	c.mu.Unlock()
}

// runCompactProducer writes "K<key>:<seq>" to a compacted topic, a key per record in
// turn, one idempotent producer, so acknowledgements arrive in sequence order.
func runCompactProducer(t *testing.T, wg *sync.WaitGroup, addr string, acks *compactAcks, done <-chan struct{}) {
	defer wg.Done()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(1_000_000), kgo.RequestRetries(1_000_000), kgo.RetryTimeout(2*time.Minute), kgo.ProduceRequestTimeout(20*time.Second))
	if err != nil {
		t.Errorf("compacted producer: %v", err)
		return
	}
	defer cl.Close()
	pad := strings.Repeat("c", 60)
	for seq := 1; ; seq++ {
		select {
		case <-done:
			cl.Flush(context.Background())
			return
		default:
		}
		key, s := seq%compactKeys, seq
		cl.Produce(context.Background(), &kgo.Record{Topic: "soak-compact", Partition: 0, Key: []byte(strconv.Itoa(key)), Value: []byte(fmt.Sprintf("K%d:%d:%s", key, s, pad))},
			func(_ *kgo.Record, err error) {
				if err == nil {
					acks.set(key, s)
				}
			})
		time.Sleep(2 * time.Millisecond)
	}
}

// verifyCompactSoak reads the compacted topic back. Per key the values must run in
// increasing order, and the last must be the latest acknowledged one or later: the cleaner
// may drop older values and must never drop the newest.
func verifyCompactSoak(t *testing.T, ctx context.Context, addr string, acks *compactAcks) {
	t.Helper()
	// Give the cleaner a moment to finish a pass the last restart interrupted.
	time.Sleep(3 * time.Second)
	rows := readAllRecords(t, addr, "soak-compact", false)
	last := map[int]int{}
	for _, r := range rows {
		var key, seq int
		if _, err := fmt.Sscanf(r.vl, "K%d:%d:", &key, &seq); err != nil {
			t.Errorf("unreadable compacted value %q", r.vl)
			continue
		}
		if prev, ok := last[key]; ok && seq <= prev {
			t.Errorf("key %d: sequence %d after %d, out of order or duplicated", key, seq, prev)
		}
		last[key] = seq
	}
	acks.mu.Lock()
	defer acks.mu.Unlock()
	for key, want := range acks.latest {
		if last[key] < want {
			t.Errorf("key %d: the latest acknowledged value (%d) is gone; the newest left is %d", key, want, last[key])
		}
	}
	t.Logf("compacted topic: %d records on disk for %d keys (the producer wrote about %d)", len(rows), len(last), func() int {
		m := 0
		for _, v := range acks.latest {
			m = max(m, v)
		}
		return m
	}())
	if len(acks.latest) == 0 {
		t.Error("the compacted-topic producer never had an acknowledgement, so the invariant was not exercised")
	}
}
