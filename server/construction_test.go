package server_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/server"
)

// corrupt writes a value that is not JSON into a bucket of the metadata database of
// a data directory that a broker has used and closed.
func corrupt(t *testing.T, dir, bucket string) {
	t.Helper()
	srv, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bbolt.Open(filepath.Join(dir, "meta.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(bucket)).Put([]byte("broken"), []byte("{not json")) }); err != nil {
		t.Fatal(err)
	}
}

// repairBucket takes the broken record out again.
func repairBucket(t *testing.T, dir, bucket string) {
	t.Helper()
	db, err := bbolt.Open(filepath.Join(dir, "meta.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("the metadata database is still locked after a failed New: %v", err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bbolt.Tx) error { return tx.Bucket([]byte(bucket)).Delete([]byte("broken")) }); err != nil {
		t.Fatal(err)
	}
}

// Whatever step of building or starting a server fails, nothing it opened outlives
// the failure: the data directory can be used again at once, and the process holds
// no more goroutines or file descriptors than before. (A failure after the lock on
// the data directory was taken used to keep the lock for the life of the process,
// and a failed bind in Serve left the AlsoListen listeners bound and accepting.)
func TestAFailureAtAnyStepOfConstructionReleasesEverything(t *testing.T) {
	// One of everything first, so lazily started runtime goroutines and descriptors
	// (the poller, the resolver) are in the baseline.
	warm, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warm.Bind(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		// fail runs the step that fails (and fails the test if it does not), and
		// returns what puts the directory right again. busy is an address in use.
		fail func(t *testing.T, dir, busy string) (repair func())
	}{
		{"an invalid configuration", func(t *testing.T, dir, busy string) func() {
			if _, err := server.New(server.Options{DataDir: dir, Version: 9}); err == nil {
				t.Fatal("Version 9 was accepted")
			}
			return nil
		}},
		{"SASL without credentials", func(t *testing.T, dir, busy string) func() {
			if _, err := server.New(server.Options{DataDir: dir, SASLMechanisms: []string{"PLAIN"}}); err == nil {
				t.Fatal("SASL with no credentials was accepted")
			}
			return nil
		}},
		{"TLS files that are not a certificate", func(t *testing.T, dir, busy string) func() {
			bad := filepath.Join(t.TempDir(), "bad.pem")
			if err := os.WriteFile(bad, []byte("not a certificate"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := config.Default()
			cfg.DataDir, cfg.ListenAddr, cfg.TLSCertFile, cfg.TLSKeyFile = dir, "127.0.0.1:0", bad, bad
			if _, err := embed.NewFromConfig(cfg, nil); err == nil {
				t.Fatal("a certificate file that is not one was accepted")
			}
			return nil
		}},
		{"the data directory is a file", func(t *testing.T, dir, busy string) func() {
			if err := os.Remove(dir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a file was accepted as the data directory")
			}
			return func() { os.Remove(dir) }
		}},
		{"the data directory is in use", func(t *testing.T, dir, busy string) func() {
			holder, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a second broker on the same data directory was accepted")
			}
			return func() { holder.Close() }
		}},
		{"a metadata database that is not one", func(t *testing.T, dir, busy string) func() {
			meta := filepath.Join(dir, "meta.db")
			if err := os.WriteFile(meta, []byte(strings.Repeat("this is not a bbolt file ", 400)), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("garbage was accepted as the metadata database")
			}
			return func() { os.Remove(meta) }
		}},
		{"a metadata database that is a directory", func(t *testing.T, dir, busy string) func() {
			meta := filepath.Join(dir, "meta.db")
			if err := os.Mkdir(meta, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a directory was accepted as the metadata database")
			}
			return func() { os.Remove(meta) }
		}},
		{"a topic record that cannot be read", func(t *testing.T, dir, busy string) func() {
			corrupt(t, dir, "topics")
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a broken topic record was accepted")
			}
			return func() { repairBucket(t, dir, "topics") }
		}},
		{"an ACL that cannot be read (the broker is not built)", func(t *testing.T, dir, busy string) func() {
			corrupt(t, dir, "acls")
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a broken ACL record was accepted")
			}
			return func() { repairBucket(t, dir, "acls") }
		}},
		{"a transaction that cannot be read (the broker is not built)", func(t *testing.T, dir, busy string) func() {
			corrupt(t, dir, "txns")
			if _, err := server.New(server.Options{DataDir: dir}); err == nil {
				t.Fatal("a broken transaction record was accepted")
			}
			return func() { repairBucket(t, dir, "txns") }
		}},
		{"Serve cannot bind, with a second listener open", func(t *testing.T, dir, busy string) func() {
			srv, err := server.New(server.Options{DataDir: dir, ListenAddr: busy})
			if err != nil {
				t.Fatal(err)
			}
			extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.Serve(context.Background()); err == nil {
				t.Fatal("Serve on a taken port returned no error")
			}
			if !refused(extra.String()) {
				t.Errorf("the AlsoListen listener %s is still bound and accepting after Serve returned its bind error", extra)
			}
			return nil
		}},
		{"Serve with a context that is already cancelled", func(t *testing.T, dir, busy string) func() {
			srv, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = srv.Serve(ctx) // an error or a clean stop: either way it is over.
			if !refused(extra.String()) {
				t.Errorf("the AlsoListen listener %s outlived Serve", extra)
			}
			return nil
		}},
		{"ServeListener on a listener that is already closed", func(t *testing.T, dir, busy string) func() {
			srv, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln.Close()
			if err := srv.ServeListener(context.Background(), ln); err == nil {
				t.Fatal("ServeListener on a closed listener returned no error")
			}
			if !refused(extra.String()) {
				t.Errorf("the AlsoListen listener %s outlived ServeListener", extra)
			}
			return nil
		}},
		{"Bind cannot bind, then Close", func(t *testing.T, dir, busy string) func() {
			srv, err := server.New(server.Options{DataDir: dir, ListenAddr: busy})
			if err != nil {
				t.Fatal(err)
			}
			extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := srv.Bind(context.Background()); err == nil {
				t.Fatal("Bind on a taken port returned no error")
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !refused(extra.String()) {
				t.Errorf("the AlsoListen listener %s outlived Close", extra)
			}
			return nil
		}},
		{"AlsoListen cannot bind, then Close", func(t *testing.T, dir, busy string) func() {
			srv, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := srv.AlsoListen(context.Background(), busy); err == nil {
				t.Fatal("AlsoListen on a taken port returned no error")
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			return nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			busy := taken(t) // an address somebody else has, opened before the count.
			goroutines, fds := runtime.NumGoroutine(), openFDs()
			repair := tc.fail(t, dir, busy)
			if repair != nil {
				repair()
			}
			settled(t, "after the failure", goroutines, fds)

			// The lock on the data directory was released: the same directory opens.
			start := time.Now()
			again, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatalf("a second New on the same data directory, after %s: %v", time.Since(start).Round(time.Millisecond), err)
			}
			if err := again.Close(); err != nil {
				t.Fatal(err)
			}
			settled(t, "after the second server was closed", goroutines, fds)
		})
	}
}
