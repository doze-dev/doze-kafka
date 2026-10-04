package broker

import "time"

// expiry is a timer for something that can be overtaken: a member's session, a
// rebalance's wait for its members, a revocation's deadline. Both coordinators
// use it for every timer they keep.
//
// time.Timer.Stop cannot recall a timer that has already fired. Its callback may
// be past Stop and waiting for the coordinator's lock while a heartbeat, holding
// that lock, resets the timer; when the callback gets the lock it would evict a
// member that had proved it was alive an instant earlier. So every arming has an
// identity (gen), the callback is handed the one it was armed with, and it must
// check current(gen) under the lock before acting. Stopping or re-arming changes
// the identity, which is what makes a late callback do nothing.
//
// An expiry is used only with its coordinator's lock held.
type expiry struct {
	timer *time.Timer
	gen   uint64
}

// arm starts (or restarts) the timer: fire runs after d, with this arming's
// identity, on its own goroutine.
func (e *expiry) arm(d time.Duration, fire func(gen uint64)) {
	e.stop()
	gen := e.gen
	e.timer = time.AfterFunc(d, func() { fire(gen) })
}

// stop cancels the timer and makes any callback already on its way stale.
func (e *expiry) stop() {
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	e.gen++
}

// armed reports whether the timer is running (or has fired and not been stopped).
func (e *expiry) armed() bool { return e.timer != nil }

// current reports whether gen is the identity of the present arming, which is
// whether a callback carrying it should act.
func (e *expiry) current(gen uint64) bool { return e.timer != nil && e.gen == gen }
