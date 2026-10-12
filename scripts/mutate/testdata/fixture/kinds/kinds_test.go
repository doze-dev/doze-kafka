package kinds

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if !Ready(1) {
		fmt.Println("not ready: no test was run")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestJoin(t *testing.T) {
	if Join("a", "b") != "ab" {
		t.Fatal("Join")
	}
}

func TestReady(t *testing.T) {
	if !Ready(1) || Ready(0) {
		t.Fatal("Ready is above zero")
	}
}

func TestSpin(t *testing.T) {
	if Spin(3) != 6 {
		t.Fatal("Spin")
	}
}
