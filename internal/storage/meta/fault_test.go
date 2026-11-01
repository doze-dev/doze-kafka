//go:build faultinject

package meta

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/faultinject"
)

// A metadata write that fails is an error to the caller and leaves the registry
// as it was.
func TestFaultSeam_MetaWriteFailsAndLeavesNoTopic(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	boom := errors.New("injected")
	defer faultinject.Set(faultinject.MetaWrite, faultinject.Fault{Err: boom})()
	if _, err := s.CreateTopic("t", 1, nil); !errors.Is(err, boom) {
		t.Fatalf("CreateTopic under a meta.write fault: %v", err)
	}
	faultinject.Reset()
	if _, ok, _ := s.Topic("t"); ok {
		t.Fatal("the topic exists though its write failed")
	}
	if _, err := s.CreateTopic("t", 1, nil); err != nil {
		t.Fatalf("after the fault: %v", err)
	}
}
