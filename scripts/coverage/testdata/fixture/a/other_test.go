package a

import "testing"

// The test the self-test deletes: without it Triple is not run at all.
func TestTriple(t *testing.T) {
	if Triple(2) != 6 || Triple(0) != 0 {
		t.Fatal("Triple")
	}
}
