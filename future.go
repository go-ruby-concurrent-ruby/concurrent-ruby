package concurrent

import (
	"sync"
	"time"
)

// event is the IVar core shared by Future and Promise: a write-once cell that
// transitions from Pending to Fulfilled or Rejected exactly once, closing a
// broadcast channel and notifying observers. It mirrors Concurrent::IVar.
type event struct {
	mu        sync.Mutex
	state     State
	value     any
	reason    error
	done      chan struct{}
	observers []func(State, any, error)
}

func newEvent() *event {
	return &event{state: Pending, done: make(chan struct{})}
}

// settle transitions the cell to a terminal state exactly once. A second
// attempt returns ErrMultipleAssignment (Concurrent::MultipleAssignmentError).
// Observers registered while pending run after the lock is released.
func (e *event) settle(st State, v any, r error) error {
	e.mu.Lock()
	if e.state != Pending {
		e.mu.Unlock()
		return ErrMultipleAssignment
	}
	e.state, e.value, e.reason = st, v, r
	obs := e.observers
	e.observers = nil
	close(e.done)
	e.mu.Unlock()
	for _, o := range obs {
		o(st, v, r)
	}
	return nil
}

// addObserver registers fn to run on completion. If the cell is already
// settled, fn runs immediately on the calling goroutine.
func (e *event) addObserver(fn func(State, any, error)) {
	e.mu.Lock()
	if e.state != Pending {
		st, v, r := e.state, e.value, e.reason
		e.mu.Unlock()
		fn(st, v, r)
		return
	}
	e.observers = append(e.observers, fn)
	e.mu.Unlock()
}

// get waits up to timeout for the cell to settle. On timeout it returns the
// still-Pending state with nil value/reason.
func (e *event) get(timeout time.Duration) (State, any, error) {
	if !waitDone(e.done, timeout) {
		e.mu.Lock()
		st := e.state
		e.mu.Unlock()
		return st, nil, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state, e.value, e.reason
}

func (e *event) snapshotState() State {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

func (e *event) snapshotReason() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.reason
}

// Future models Concurrent::Future: an asynchronous computation posted to an
// executor whose value is retrieved later. The computation is a Go func
// returning (value, error); a non-nil error rejects the future.
type Future struct {
	ev *event
}

// FutureExecute posts fn to exec and returns a Future for its result
// (Ruby Concurrent::Future.execute { ... }). fn's returned error rejects the
// future with that reason; otherwise it is fulfilled with the value.
func FutureExecute(exec Executor, fn func() (any, error)) *Future {
	f := &Future{ev: newEvent()}
	exec.Post(func() {
		v, err := fn()
		if err != nil {
			_ = f.ev.settle(Rejected, nil, err)
			return
		}
		_ = f.ev.settle(Fulfilled, v, nil)
	})
	return f
}

// Wait blocks up to timeout for the future to complete and returns the future
// (Ruby #wait). A negative timeout waits forever.
func (f *Future) Wait(timeout time.Duration) *Future {
	f.ev.get(timeout)
	return f
}

// Value blocks up to timeout and returns the value, or nil if the future timed
// out or was rejected (Ruby #value).
func (f *Future) Value(timeout time.Duration) any {
	_, v, _ := f.ev.get(timeout)
	return v
}

// ValueBang blocks up to timeout and returns the value, or the rejection reason
// as an error if the future was rejected (Ruby #value!, which re-raises).
func (f *Future) ValueBang(timeout time.Duration) (any, error) {
	st, v, r := f.ev.get(timeout)
	if st == Rejected {
		return nil, r
	}
	return v, nil
}

// State returns the current lifecycle state (Ruby #state).
func (f *Future) State() State { return f.ev.snapshotState() }

// Reason returns the rejection reason, or nil if not rejected (Ruby #reason).
func (f *Future) Reason() error { return f.ev.snapshotReason() }

// PendingQ reports whether the future is still pending (Ruby #pending?).
func (f *Future) PendingQ() bool { return f.ev.snapshotState() == Pending }

// FulfilledQ reports whether the future was fulfilled (Ruby #fulfilled?).
func (f *Future) FulfilledQ() bool { return f.ev.snapshotState() == Fulfilled }

// RejectedQ reports whether the future was rejected (Ruby #rejected?).
func (f *Future) RejectedQ() bool { return f.ev.snapshotState() == Rejected }

// CompleteQ reports whether the future has settled either way (Ruby #complete?).
func (f *Future) CompleteQ() bool { return f.ev.snapshotState() != Pending }

// Promise models the settable, composable core of Concurrent::Promise: it can
// be explicitly fulfilled or rejected, and chained with Then. Continuations run
// on the promise's executor (ImmediateExecutor by default, for deterministic
// inline resolution).
type Promise struct {
	ev   *event
	exec Executor
}

// NewPromise returns an unresolved Promise whose continuations run inline.
func NewPromise() *Promise {
	return &Promise{ev: newEvent(), exec: ImmediateExecutor{}}
}

// NewPromiseOn returns an unresolved Promise whose continuations run on exec.
func NewPromiseOn(exec Executor) *Promise {
	return &Promise{ev: newEvent(), exec: exec}
}

// Fulfill resolves the promise with v (Ruby #fulfill / #set). Resolving an
// already-resolved promise returns ErrMultipleAssignment.
func (p *Promise) Fulfill(v any) error { return p.ev.settle(Fulfilled, v, nil) }

// Reject resolves the promise as failed with reason err (Ruby #reject / #fail).
func (p *Promise) Reject(err error) error { return p.ev.settle(Rejected, nil, err) }

// Then returns a child Promise resolved by applying onFulfilled to this
// promise's value once it is fulfilled (Ruby #then). A rejection propagates to
// the child unchanged; a nil onFulfilled passes the value through; an error
// returned by onFulfilled rejects the child.
func (p *Promise) Then(onFulfilled func(any) (any, error)) *Promise {
	child := &Promise{ev: newEvent(), exec: p.exec}
	p.ev.addObserver(func(st State, v any, r error) {
		p.exec.Post(func() {
			if st == Rejected {
				_ = child.ev.settle(Rejected, nil, r)
				return
			}
			if onFulfilled == nil {
				_ = child.ev.settle(Fulfilled, v, nil)
				return
			}
			nv, err := onFulfilled(v)
			if err != nil {
				_ = child.ev.settle(Rejected, nil, err)
				return
			}
			_ = child.ev.settle(Fulfilled, nv, nil)
		})
	})
	return child
}

// Value blocks up to timeout and returns the promise's value, or nil if pending
// or rejected (Ruby #value).
func (p *Promise) Value(timeout time.Duration) any {
	_, v, _ := p.ev.get(timeout)
	return v
}

// State returns the current lifecycle state (Ruby #state).
func (p *Promise) State() State { return p.ev.snapshotState() }

// Reason returns the rejection reason, or nil if not rejected (Ruby #reason).
func (p *Promise) Reason() error { return p.ev.snapshotReason() }
