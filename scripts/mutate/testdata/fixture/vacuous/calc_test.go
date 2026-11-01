package vacuous

import "testing"

// Every line runs and nothing is asserted: every mutant must be reported LIVED.
func TestRunsEverythingAssertsNothing(t *testing.T) {
	_ = Adult(18)
	_ = Area(6, 3)
	_ = Diff(5, 2)
	_ = Neg(4)
	_ = Next(1)
	_ = Same(1, 1)
}
