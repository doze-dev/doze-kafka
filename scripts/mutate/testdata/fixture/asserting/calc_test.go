package asserting

import "testing"

// Every operator is pinned, the boundary included: every mutant must be KILLED.
func TestAdult(t *testing.T) {
	if !Adult(18) || Adult(17) || !Adult(19) {
		t.Fatal("Adult is at 18 and over")
	}
}

func TestArithmetic(t *testing.T) {
	if Area(6, 3) != 18 {
		t.Error("Area")
	}
	if Diff(5, 2) != 3 {
		t.Error("Diff")
	}
	if Neg(4) != -4 {
		t.Error("Neg")
	}
	if Next(1) != 2 {
		t.Error("Next")
	}
}

func TestSame(t *testing.T) {
	if !Same(1, 1) || Same(1, 2) {
		t.Error("Same")
	}
}
