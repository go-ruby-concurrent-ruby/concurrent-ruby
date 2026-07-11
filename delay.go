package concurrent

import "sync"

// Delay models Concurrent::Delay: a lazy, memoised value. The computation is
// not run when the Delay is created but on the first call to Value/ValueBang,
// after which the result (value or rejection reason) is cached forever. It is
// the lazy sibling of Future — Future runs eagerly on an executor, Delay runs
// on first access on the calling goroutine.
type Delay struct {
	mu   sync.Mutex
	ev   *event
	fn   func() (any, error)
	done bool
}

// NewDelay returns an unforced Delay whose value is computed by fn on first
// access (Ruby Concurrent::Delay.new { ... }).
func NewDelay(fn func() (any, error)) *Delay {
	return &Delay{ev: newEvent(), fn: fn}
}

// force runs the computation exactly once, memoising its outcome.
func (d *Delay) force() (State, any, error) {
	d.mu.Lock()
	if !d.done {
		d.done = true
		v, err := d.fn()
		if err != nil {
			_ = d.ev.settle(Rejected, nil, err)
		} else {
			_ = d.ev.settle(Fulfilled, v, nil)
		}
	}
	d.mu.Unlock()
	return d.ev.get(NoTimeout)
}

// Value forces the computation and returns its value, or nil if it was rejected
// (Ruby #value).
func (d *Delay) Value() any {
	_, v, _ := d.force()
	return v
}

// ValueBang forces the computation and returns its value, or the rejection
// reason as an error (Ruby #value!, which re-raises).
func (d *Delay) ValueBang() (any, error) {
	st, v, r := d.force()
	if st == Rejected {
		return nil, r
	}
	return v, nil
}

// Reason returns the rejection reason once forced, or nil (Ruby #reason).
func (d *Delay) Reason() error { return d.ev.snapshotReason() }

// State returns the lifecycle state: Pending until forced, then Fulfilled or
// Rejected (Ruby #state).
func (d *Delay) State() State { return d.ev.snapshotState() }

// PendingQ reports whether the Delay has not been forced yet (Ruby #pending?).
func (d *Delay) PendingQ() bool { return d.ev.snapshotState() == Pending }

// FulfilledQ reports whether the forced computation succeeded (Ruby #fulfilled?).
func (d *Delay) FulfilledQ() bool { return d.ev.snapshotState() == Fulfilled }

// RejectedQ reports whether the forced computation failed (Ruby #rejected?).
func (d *Delay) RejectedQ() bool { return d.ev.snapshotState() == Rejected }
