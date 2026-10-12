package failing

import "testing"

func TestFailsUnmutated(t *testing.T) {
	if Two() != 3 {
		t.Fatal("this test is wrong on purpose")
	}
}
