// Package asserting is a subject for the mutation tool's own test. Every
// operator here has a mutant no correct test suite can miss, and this package's
// suite pins each one.
package asserting

func Adult(age int) bool { return age >= 18 }

func Area(w, h int) int { return w * h }

func Diff(a, b int) int { return a - b }

func Neg(a int) int { return -a }

func Next(a int) int {
	a++
	return a
}

func Same(a, b int) bool { return a == b }
