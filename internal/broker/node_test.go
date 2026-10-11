package broker_test

// A node is one broker a test can stop, start, crash and freeze. It is what a
// restart or crash test is built on, so that test is three lines:
//
//	n := startNode(t, nodeSubprocess())
//	... produce with acks=all ...
//	n.Kill(); n.Start()
//
// A node runs either in this process (the default: fast, and Restart is a
// graceful stop and start over the same directory) or as the built binary
// (nodeSubprocess), which is the only kind that can be killed with SIGKILL or
// frozen with SIGSTOP, because killing this process would end the test.
//
// Either way the address and the directory are fixed for the node's life, so a
// client's seed address and the on-disk state survive a restart.

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
)

type node struct {
	t       *testing.T
	Addr    string // host:port, fixed for the life of the node.
	Dir     string // holds data/ and meta.db.
	DataDir string
	MetaDB  string

	version   int
	sub       bool
	bin       string
	fsync     string // -fsync-interval for a subprocess.
	pprofAddr string
	stderrLog string // where a subprocess's stderr goes, when it is kept.
	env       []string
	flags     []string
	cfgEdit   func(*config.Config)

	mu     sync.Mutex
	stop   func()    // in-process: stops the running broker.
	cmd    *exec.Cmd // subprocess: the running process.
	gen    int       // incremented by every start.
	paused bool
}

type nodeOpt func(*node)

// nodeVersion sets the Kafka major version the node emulates (default 4).
func nodeVersion(v int) nodeOpt { return func(n *node) { n.version = v } }

// nodeSubprocess runs the built binary instead of the broker in this process.
func nodeSubprocess() nodeOpt { return func(n *node) { n.sub = true } }

// nodeDir uses an existing directory.
func nodeDir(dir string) nodeOpt { return func(n *node) { n.Dir = dir } }

// nodeFsync sets -fsync-interval of a subprocess ("0" disables the timer).
func nodeFsync(d string) nodeOpt { return func(n *node) { n.fsync = d } }

// nodePprof serves pprof on a free port, for the soak's health samples.
func nodePprof() nodeOpt { return func(n *node) { n.pprofAddr = "x" } }

// nodeStderr keeps a subprocess's stderr, across every restart, in this file.
func nodeStderr(path string) nodeOpt { return func(n *node) { n.stderrLog = path } }

// nodeEnv adds environment variables (KEY=VALUE) to a subprocess.
func nodeEnv(kv ...string) nodeOpt { return func(n *node) { n.env = append(n.env, kv...) } }

// nodeFlags adds command-line flags to a subprocess.
func nodeFlags(f ...string) nodeOpt { return func(n *node) { n.flags = append(n.flags, f...) } }

// nodeConfig edits the config of an in-process node before every start.
func nodeConfig(f func(*config.Config)) nodeOpt { return func(n *node) { n.cfgEdit = f } }

// nodeBinary uses an already built binary (subprocess nodes).
func nodeBinary(bin string) nodeOpt { return func(n *node) { n.bin = bin } }

// startNode starts a broker on a free loopback port over a fresh directory (or
// the one given) and registers its shutdown with the test.
func startNode(t *testing.T, opts ...nodeOpt) *node {
	t.Helper()
	skipShort(t)
	n := &node{t: t, version: 4, fsync: "200ms"}
	for _, o := range opts {
		o(n)
	}
	if n.Dir == "" {
		n.Dir = t.TempDir()
	}
	n.DataDir = filepath.Join(n.Dir, "data")
	n.MetaDB = filepath.Join(n.Dir, "meta.db")
	n.Addr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	if n.pprofAddr != "" {
		n.pprofAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	if n.sub && n.bin == "" {
		n.bin = buildBinary(t)
	}
	n.Start()
	t.Cleanup(n.Stop)
	return n
}

// Start brings a stopped node up over the same address and directory.
func (n *node) Start() {
	n.t.Helper()
	n.mu.Lock()
	running := n.stop != nil || n.cmd != nil
	n.mu.Unlock()
	if running {
		n.t.Fatal("node: Start on a node that is running")
	}
	if n.sub {
		n.launch()
	} else {
		cfg := config.Default()
		cfg.Version = n.version
		cfg.ListenAddr = n.Addr
		cfg.DataDir = n.DataDir
		cfg.MetaDB = n.MetaDB
		if n.cfgEdit != nil {
			n.cfgEdit(&cfg)
		}
		_, stop := boot(n.t, cfg)
		n.mu.Lock()
		n.stop = stop
		n.mu.Unlock()
	}
	n.mu.Lock()
	n.gen++
	n.mu.Unlock()
}

func (n *node) launch() {
	args := []string{"-listen", n.Addr, "-data-dir", n.DataDir, "-meta-db", n.MetaDB,
		"-fsync-interval", n.fsync, "-kafka-version", strconv.Itoa(n.version), "-console=false"}
	if n.pprofAddr != "" {
		args = append(args, "-pprof-addr", n.pprofAddr)
	}
	args = append(args, n.flags...)
	cmd := exec.Command(n.bin, args...)
	cmd.Env = append(os.Environ(), n.env...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if n.stderrLog != "" {
		f, err := os.OpenFile(n.stderrLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			n.t.Fatalf("open %s: %v", n.stderrLog, err)
		}
		defer f.Close() // the child holds its own descriptor.
		cmd.Stderr = f
	}
	if err := cmd.Start(); err != nil {
		n.t.Fatalf("launch broker: %v", err)
	}
	n.mu.Lock()
	n.cmd = cmd
	n.mu.Unlock()
	deadline := time.Now().Add(15 * time.Second)
	for !dialOK(n.Addr) {
		if time.Now().After(deadline) {
			n.t.Fatal("broker did not start listening")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Stop shuts the node down gracefully (SIGTERM for a subprocess, which flushes).
// It is safe to call on a node that is already down.
func (n *node) Stop() {
	n.mu.Lock()
	stop, cmd := n.stop, n.cmd
	n.stop, n.cmd = nil, nil
	paused := n.paused
	n.paused = false
	n.mu.Unlock()
	if stop != nil {
		stop()
	}
	if cmd != nil {
		if paused {
			cmd.Process.Signal(syscall.SIGCONT)
		}
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
	}
}

// Restart is a graceful Stop and a Start over the same directory.
func (n *node) Restart() {
	n.t.Helper()
	n.Stop()
	n.Start()
}

// Kill ends the broker with SIGKILL and leaves it down: nothing is flushed that
// was not already on disk. Only a subprocess node can be killed. Follow it with
// Start to see what a crash left behind.
func (n *node) Kill() {
	n.t.Helper()
	n.mu.Lock()
	cmd := n.cmd
	n.cmd = nil
	n.paused = false
	n.mu.Unlock()
	if !n.sub || cmd == nil {
		n.t.Fatal("node: Kill needs a running subprocess node (nodeSubprocess)")
	}
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
}

// Pause freezes the process with SIGSTOP: connections stay open and nothing is
// answered. Resume undoes it. Only a subprocess node can be paused.
func (n *node) Pause() {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.sub || n.cmd == nil {
		n.t.Fatal("node: Pause needs a running subprocess node (nodeSubprocess)")
	}
	n.cmd.Process.Signal(syscall.SIGSTOP)
	n.paused = true
}

// Resume continues a paused node.
func (n *node) Resume() {
	n.t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || !n.paused {
		n.t.Fatal("node: Resume on a node that is not paused")
	}
	n.cmd.Process.Signal(syscall.SIGCONT)
	n.paused = false
}

// pid is the subprocess's process id, or 0.
func (n *node) pid() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && n.cmd.Process != nil {
		return n.cmd.Process.Pid
	}
	return 0
}

// generation counts starts, so a sampler can tell one process from the next.
func (n *node) generation() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.gen
}

// running reports whether the node answers on its address.
func (n *node) running() bool { return dialOK(n.Addr) }
