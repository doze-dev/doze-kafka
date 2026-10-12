// Package kinds has one mutant of each kind that is neither killed nor lived.
package kinds

// Join: a + on strings made a - does not compile (NOT VIABLE).
func Join(a, b string) string { return a + b }

// Ready is asked by TestMain before any test starts: its negation makes the
// test process exit there (SETUP FAILED), its boundary is killed by TestReady.
func Ready(n int) bool { return n > 0 }

// Spin: with i-- for i++ it does not end (TIMED OUT); with == for != it returns
// 0 (KILLED).
func Spin(n int) int {
	c := 0
	for i := 0; i != n; i++ {
		c += 2
	}
	return c
}

// Unreached is called by no test (NOT COVERED).
func Unreached(a int) int { return a * 2 }
