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

// SetMinSessionTimeoutForTest lowers the smallest session.timeout.ms a classic
// member may ask for (Kafka's 6 s) and returns a restore func, for tests of the
// expiry machinery that would otherwise wait six seconds a member.
func SetMinSessionTimeoutForTest(d time.Duration) (restore func()) {
	old := minSessionTimeout
	minSessionTimeout = d
	return func() { minSessionTimeout = old }
}
