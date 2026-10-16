// Package a has two functions and a test file for each.
package a

func Double(n int) int {
	if n < 0 {
		return -Double(-n)
	}
	return n * 2
}
