package broker

import "time"

// SetMaxFetchWaitForTest replaces the long-poll cap and returns the old value.
func SetMaxFetchWaitForTest(d time.Duration) time.Duration {
	old := maxFetchWait
	maxFetchWait = d
	return old
}

// SetAfterFetchReadForTest installs the fetch hook and returns a restore func.
func SetAfterFetchReadForTest(fn func()) (restore func()) {
	afterFetchRead = fn
	return func() { afterFetchRead = nil }
}
