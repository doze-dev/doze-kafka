package console

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// sniffer is a relay between the console and the broker that decodes every request
// the console sends, so a test can assert what reached the broker. It can also hold
// back every request of one API key, as a broker that never answers it.
type sniffer struct {
	mu      sync.Mutex
	reqs    []kmsg.Request
	dropped map[int16]bool
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

// blackhole makes the broker never see (so never answer) requests of this key.
func (s *sniffer) blackhole(key int16) {
	s.mu.Lock()
	if s.dropped == nil {
		s.dropped = map[int16]bool{}
	}
	s.dropped[key] = true
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

// decode reads a request frame's body (after the size) into a kmsg request, or nil.
func decode(body []byte) (int16, kmsg.Request) {
	if len(body) < 10 {
		return -1, nil
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
		return key, nil
	}
	r.SetVersion(ver)
	if r.IsFlexible() {
		// Header v2 carries tagged fields; the console sends none.
		if len(rest) == 0 || rest[0] != 0 {
			return key, nil
		}
		rest = rest[1:]
	}
	if err := r.ReadFrom(rest); err != nil {
		return key, nil
	}
	return key, r
}

func (s *sniffer) relay(in net.Conn, out net.Conn) {
	for {
		var sz [4]byte
		if _, err := io.ReadFull(in, sz[:]); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(sz[:]))
		if _, err := io.ReadFull(in, body); err != nil {
			return
		}
		key, r := decode(body)
		if r != nil {
			s.add(r)
		}
		s.mu.Lock()
		held := s.dropped[key]
		s.mu.Unlock()
		if held {
			continue
		}
		if _, err := out.Write(append(sz[:], body...)); err != nil {
			return
		}
	}
}

// selfSigned is a certificate for 127.0.0.1 and the pool that trusts it.
func selfSigned(t testing.TB) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "doze-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}
