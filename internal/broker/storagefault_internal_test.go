package broker

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"syscall"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Apache Kafka answers an I/O failure on a log directory (a full disk, a read-only
// remount, a dying device) with KAFKA_STORAGE_ERROR, a retriable error that tells
// the client "this broker's storage is the problem". It is not UNKNOWN_SERVER_ERROR,
// which a client treats as a bug in the request or the broker.
func TestStorageFault_IOErrorsAreKafkaStorageError(t *testing.T) {
	b := &Broker{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for name, err := range map[string]error{
		"no space":        &os.PathError{Op: "write", Path: "/data/t-0/0.log", Err: syscall.ENOSPC},
		"quota":           &os.PathError{Op: "write", Path: "/data/t-0/0.log", Err: syscall.EDQUOT},
		"i/o error":       &os.PathError{Op: "sync", Path: "/data/t-0/0.log", Err: syscall.EIO},
		"read-only":       &os.PathError{Op: "open", Path: "/data/t-0/0.log", Err: syscall.EROFS},
		"wrapped":         fmt.Errorf("append: %w", &fs.PathError{Op: "write", Path: "x", Err: syscall.ENOSPC}),
		"bare errno":      syscall.ENOSPC,
		"wrapped bare":    fmt.Errorf("log: %w", syscall.EIO),
		"fsync (os.Sync)": &os.SyscallError{Syscall: "fsync", Err: syscall.EIO},
	} {
		if got := b.storageFault("test", err); got != kerr.KafkaStorageError.Code {
			t.Errorf("%s: code %d, want KAFKA_STORAGE_ERROR (%d)", name, got, kerr.KafkaStorageError.Code)
		}
	}
	// Anything that is not the storage's fault stays what it was.
	if got := b.storageFault("test", errors.New("a bug")); got != kerr.UnknownServerError.Code {
		t.Errorf("a plain error: code %d, want UNKNOWN_SERVER_ERROR", got)
	}
}
