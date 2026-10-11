//go:build faultinject

package faultinject

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	got, err := Parse("log.sync=error:input/output error, log.write=enospc\nmeta.write=delay:250ms\nlog.sync=error:x@3")
	if err != nil {
		t.Fatal(err)
	}
	if f := got["log.write"]; !errors.Is(f.Err, syscall.ENOSPC) {
		t.Errorf("enospc: %+v", f)
	}
	if f := got["meta.write"]; f.Delay != 250*time.Millisecond || f.Err != nil {
		t.Errorf("delay: %+v", f)
	}
	if f := got["log.sync"]; f.After != 3 || f.Err == nil || f.Err.Error() != "x" {
		t.Errorf("later items win, with their count: %+v", f)
	}
	for _, bad := range []string{"log.sync", "log.sync=boom", "log.sync=delay:fast", "log.sync=error@x"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestHitAfterAndClear(t *testing.T) {
	t.Cleanup(Reset)
	boom := errors.New("boom")
	clear := Set(LogSync, Fault{Err: boom, After: 2})
	for i := 0; i < 2; i++ {
		if err := Hit(LogSync); err != nil {
			t.Fatalf("hit %d: %v", i, err)
		}
	}
	if err := Hit(LogSync); !errors.Is(err, boom) {
		t.Fatalf("third hit: %v", err)
	}
	if err := Hit(LogWrite); err != nil {
		t.Fatalf("another point: %v", err)
	}
	clear()
	if err := Hit(LogSync); err != nil {
		t.Fatalf("after clear: %v", err)
	}
}

func TestDelay(t *testing.T) {
	t.Cleanup(Reset)
	Set(LogSync, Fault{Delay: 60 * time.Millisecond})
	start := time.Now()
	if err := Hit(LogSync); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 60*time.Millisecond {
		t.Fatalf("returned after %v", d)
	}
}

func TestFileIsReadOnEveryHit(t *testing.T) {
	t.Cleanup(func() { Reset(); file = "" })
	file = filepath.Join(t.TempDir(), "faults")
	if err := Hit(LogSync); err != nil {
		t.Fatalf("no file: %v", err)
	}
	if err := os.WriteFile(file, []byte("log.sync=error:from file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Hit(LogSync); err == nil || err.Error() != "from file" {
		t.Fatalf("with the file: %v", err)
	}
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Hit(LogSync); err != nil {
		t.Fatalf("emptied: %v", err)
	}
}
