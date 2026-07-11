package concurrent

import (
	"sync"
	"time"
)

// Event models Concurrent::Event: an old-style, manually-controlled thread
// synchronisation object. It starts in the unset state; Set releases every
// current and future waiter until Reset returns it to unset. Unlike a
// CountDownLatch (one-shot), an Event may be set and reset repeatedly.
type Event struct {
	mu  sync.Mutex
	set bool
	ch  chan struct{}
}

// NewEvent returns an Event in the unset state (Ruby Concurrent::Event.new).
func NewEvent() *Event {
	return &Event{ch: make(chan struct{})}
}

// Set transitions the event to set, releasing all waiters; it is idempotent and
// always returns true (Ruby #set).
func (e *Event) Set() bool {
	e.mu.Lock()
	if !e.set {
		e.set = true
		close(e.ch)
	}
	e.mu.Unlock()
	return true
}

// TryQ sets the event only if it was unset, reporting whether this call was the
// one that set it (Ruby #try?).
func (e *Event) TryQ() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.set {
		return false
	}
	e.set = true
	close(e.ch)
	return true
}

// Reset returns the event to the unset state so it can be waited on again; a
// still-unset event is left unchanged. Always returns true (Ruby #reset).
func (e *Event) Reset() bool {
	e.mu.Lock()
	if e.set {
		e.set = false
		e.ch = make(chan struct{})
	}
	e.mu.Unlock()
	return true
}

// SetQ reports whether the event is currently set (Ruby #set?).
func (e *Event) SetQ() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.set
}

// Wait blocks until the event is set (returning true) or timeout elapses
// (returning false) (Ruby #wait). A negative timeout waits forever. An
// already-set event returns true immediately.
func (e *Event) Wait(timeout time.Duration) bool {
	e.mu.Lock()
	ch := e.ch
	set := e.set
	e.mu.Unlock()
	if set {
		return true
	}
	return waitDone(ch, timeout)
}
