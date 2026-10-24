package broker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
)

// What the broker's shared plumbing says: which errors are the disk's, which topic an
// id names, and who is woken when a topic is written.

func TestIsIOErrorIsTheFileSystemsNotTheBrokers(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"a path error", &fs.PathError{Op: "open", Path: "x", Err: errors.New("no")}, true},
		{"a link error", &os.LinkError{Op: "rename", Old: "a", New: "b", Err: errors.New("no")}, true},
		{"a syscall error", os.NewSyscallError("fsync", errors.New("no")), true},
		{"an errno", syscall.ENOSPC, true},
		{"storage offline", log.ErrStorageOffline, true},
		{"one of those, wrapped", fmt.Errorf("append: %w", syscall.EIO), true},
		{"an error of the broker's own", errors.New("a sequence ran backwards"), false},
		{"nothing", nil, false},
	} {
		if got := isIOError(tc.err); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	if got := b.storageFault("test", syscall.ENOSPC); got != kerr.KafkaStorageError.Code {
		t.Errorf("a full disk: code %d, want KAFKA_STORAGE_ERROR", got)
	}
	if got := b.storageFault("test", errors.New("bug")); got != kerr.UnknownServerError.Code {
		t.Errorf("the broker's own error: code %d, want UNKNOWN_SERVER_ERROR", got)
	}
	if got := b.coordinatorFault("test", syscall.ENOSPC); got != kerr.NotCoordinator.Code {
		t.Errorf("a coordinator's full disk: code %d, want NOT_COORDINATOR", got)
	}
	if got := b.coordinatorFault("test", errors.New("bug")); got != kerr.UnknownServerError.Code {
		t.Errorf("a coordinator's own error: code %d, want UNKNOWN_SERVER_ERROR", got)
	}
	if got := b.storageFaultError("test", syscall.ENOSPC); got != kerr.KafkaStorageError {
		t.Errorf("a full disk: %v", got)
	}
	if got := b.storageFaultError("test", errors.New("bug")); got != kerr.UnknownServerError {
		t.Errorf("the broker's own error: %v", got)
	}
}

func TestResolveTopicByNameOrByID(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	made, ok, err := b.meta.Topic("t")
	if err != nil || !ok {
		t.Fatal(err)
	}
	if name, got, ok := b.resolveTopic("t", [16]byte{}); !ok || name != "t" || got.ID != made.ID {
		t.Errorf("by name: %q %v %v", name, got.ID, ok)
	}
	if name, got, ok := b.resolveTopic("", made.ID); !ok || name != "t" || got.ID != made.ID {
		t.Errorf("by id: %q %v %v", name, got.ID, ok)
	}
	// A name wins over an id that names something else.
	if name, _, ok := b.resolveTopic("t", [16]byte{9}); !ok || name != "t" {
		t.Errorf("a name and a wrong id: %q %v", name, ok)
	}
	if name, _, ok := b.resolveTopic("nosuch", [16]byte{}); ok || name != "nosuch" {
		t.Errorf("an unknown name: %q %v, want it echoed and not found", name, ok)
	}
	if name, _, ok := b.resolveTopic("", [16]byte{9}); ok || name != "" {
		t.Errorf("an unknown id: %q %v", name, ok)
	}
	if got := b.TopicName(made.ID); got != "t" {
		t.Errorf("TopicName: %q", got)
	}
	if got := b.TopicName([16]byte{9}); got != "" {
		t.Errorf("TopicName of an id nothing has: %q", got)
	}
}

func pending(s *subscription) bool {
	select {
	case <-s.C:
		return true
	default:
		return false
	}
}

func TestNotifierWakesOnlyThoseWatchingTheTopic(t *testing.T) {
	n := newNotifier()
	a := n.subscribe([]string{"x", "y"})
	b := n.subscribe([]string{"y"})
	c := n.subscribe([]string{"z"})

	n.signal("x")
	if !pending(a) || pending(b) || pending(c) {
		t.Error("a signal on x must wake a, and only a")
	}
	// Signals while one is pending are one wakeup.
	n.signal("y")
	n.signal("y")
	n.signal("y")
	if !pending(a) || !pending(b) || pending(c) {
		t.Error("a signal on y must wake a and b")
	}
	if pending(a) || pending(b) {
		t.Error("three signals left more than one wakeup pending")
	}

	// Closing one watcher of a topic leaves the other, and the last close forgets the topic.
	b.Close()
	n.signal("y")
	if !pending(a) || pending(b) {
		t.Error("after b closed, a signal on y must wake a and not b")
	}
	a.Close()
	c.Close()
	n.mu.Lock()
	left := len(n.subs)
	n.mu.Unlock()
	if left != 0 {
		t.Errorf("%d topics still watched after every subscription closed", left)
	}
	n.signal("x") // nobody is there, and it must not block.
	d := n.subscribe(nil)
	n.signal("x")
	if pending(d) {
		t.Error("a subscription to no topic was woken")
	}
}

// ApiVersions is answered with the list at every version the broker serves, the
// lowest and the highest included, and a version outside them is answered in the v0
// form with the one row a client can retry from.
func TestApiVersionsIsServedAtItsWholeRange(t *testing.T) {
	var failSync bool
	b, _ := newTestBroker(t, &failSync)
	served, ok := b.profile.RangeFor(kmsg.ApiVersions)
	if !ok {
		t.Fatal("ApiVersions is not served")
	}
	ask := func(version int16) *kmsg.ApiVersionsResponse {
		t.Helper()
		req := &protocol.Request{Header: protocol.RequestHeader{APIKey: protocol.APIKey(kmsg.ApiVersions), APIVersion: version, CorrelationID: 1}, Conn: &protocol.Conn{}}
		if version >= 3 {
			q := kmsg.NewPtrApiVersionsRequest()
			q.SetVersion(version)
			q.ClientSoftwareName, q.ClientSoftwareVersion = "doze-test", "1.0"
			req.Body = q.AppendTo([]byte{0})
		}
		out, err := b.handleAPIVersions(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		resp := kmsg.NewPtrApiVersionsResponse()
		if version < served.Min || version > served.Max {
			resp.SetVersion(0)
		} else {
			resp.SetVersion(version)
		}
		// An ApiVersions response has no header tags, whatever its version.
		if err := resp.ReadFrom(out[4:]); err != nil {
			t.Fatalf("v%d: decode: %v", version, err)
		}
		return resp
	}
	for v := served.Min; v <= served.Max; v++ {
		if r := ask(v); r.ErrorCode != 0 || len(r.ApiKeys) < 10 {
			t.Errorf("v%d: code %d with %d rows, want the list", v, r.ErrorCode, len(r.ApiKeys))
		}
	}
	for _, v := range []int16{served.Max + 1, served.Max + 7} {
		r := ask(v)
		if r.ErrorCode != kerr.UnsupportedVersion.Code || len(r.ApiKeys) != 1 || r.ApiKeys[0].ApiKey != int16(kmsg.ApiVersions) ||
			r.ApiKeys[0].MinVersion != served.Min || r.ApiKeys[0].MaxVersion != served.Max {
			t.Errorf("v%d: %+v, want UNSUPPORTED_VERSION and the one row for ApiVersions itself", v, r)
		}
	}
	if served.Min > 0 {
		if r := ask(served.Min - 1); r.ErrorCode != kerr.UnsupportedVersion.Code {
			t.Errorf("below the lowest: code %d", r.ErrorCode)
		}
	}
}
