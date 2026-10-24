package console

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// sniffer is a TCP relay between the console and the broker that decodes every
// request the console sends, so a test can assert what reached the broker.
type sniffer struct {
	mu   sync.Mutex
	reqs []kmsg.Request
}

// sent returns the requests of one API key seen so far.
func (s *sniffer) sent(key int16) []kmsg.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []kmsg.Request
	for _, r := range s.reqs {
		if r.Key() == key {
			out = append(out, r)
		}
	}
	return out
}

func (s *sniffer) add(r kmsg.Request) {
	s.mu.Lock()
	s.reqs = append(s.reqs, r)
	s.mu.Unlock()
}

// newSniffedConsole is newTestConsole with the console dialing through a sniffer.
func newSniffedConsole(t *testing.T) (*Console, *sniffer) {
	t.Helper()
	_, brokerAddr := newTestConsoleAddr(t) // boots the broker; its own console is unused
	// A unix Addr makes the console dial the relay for every connection, not just
	// the first (a TCP seed would hand it the broker's advertised address).
	dir, err := os.MkdirTemp("", "k")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	s := &sniffer{}
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", brokerAddr)
			if err != nil {
				in.Close()
				return
			}
			go io.Copy(in, out)
			go func() {
				defer in.Close()
				defer out.Close()
				s.relay(in, out)
			}()
		}
	}()
	con, err := New(Options{Addr: "unix://" + sock, Facts: BrokerFacts{DataDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(con.Close)
	return con, s
}

func (s *sniffer) relay(in net.Conn, out net.Conn) {
	for {
		var sz [4]byte
		if _, err := io.ReadFull(in, sz[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(sz[:])
		body := make([]byte, n)
		if _, err := io.ReadFull(in, body); err != nil {
			return
		}
		if _, err := out.Write(append(sz[:], body...)); err != nil {
			return
		}
		if n < 10 {
			continue
		}
		key := int16(binary.BigEndian.Uint16(body[0:2]))
		ver := int16(binary.BigEndian.Uint16(body[2:4]))
		rest := body[8:] // after the correlation id
		cidLen := int(int16(binary.BigEndian.Uint16(rest[0:2])))
		rest = rest[2:]
		if cidLen > 0 {
			rest = rest[cidLen:]
		}
		r := kmsg.RequestForKey(key)
		if r == nil {
			continue
		}
		r.SetVersion(ver)
		if r.IsFlexible() {
			// Header v2 carries tagged fields; the console sends none.
			if len(rest) == 0 || rest[0] != 0 {
				continue
			}
			rest = rest[1:]
		}
		if err := r.ReadFrom(rest); err != nil {
			continue
		}
		s.add(r)
	}
}
