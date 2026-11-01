package brokenmain

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	fmt.Println("setup failed")
	os.Exit(1)
}

func TestTwo(t *testing.T) {
	if Two() != 2 {
		t.Fatal("Two")
	}
}
