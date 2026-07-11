package concurrent

import "time"

// NoTimeout, passed as a timeout argument, waits indefinitely — the Go
// equivalent of passing nil (no timeout) in concurrent-ruby's blocking methods.
const NoTimeout time.Duration = -1

// waitDone blocks on done, returning true when it closes. If timeout is
// negative it waits forever; otherwise it returns false when timeout elapses
// first. This is the single blocking primitive shared by every timed wait in
// the package.
func waitDone(done <-chan struct{}, timeout time.Duration) bool {
	if timeout < 0 {
		<-done
		return true
	}
	// Prefer an already-closed channel, so a zero (or any) timeout still reports
	// completion when the event has already happened — matching the gem, whose
	// wait(0) on an already-set latch/event returns true rather than racing the
	// timer.
	select {
	case <-done:
		return true
	default:
	}
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
