package concurrent

import "sync"

// Atom models Concurrent::Atom: an atomically-updatable reference guarded by an
// optional validator and notifying observers on every successful change. Unlike
// a bare AtomicReference, an Atom rejects updates its validator refuses and
// keeps the previous value.
type Atom struct {
	mu        sync.Mutex
	value     any
	validator func(any) bool
	observers []func(old, new any)
}

// NewAtom returns an Atom holding initial with no validator (every value is
// valid) (Ruby Concurrent::Atom.new(initial)).
func NewAtom(initial any) *Atom { return &Atom{value: initial} }

// NewAtomWithValidator returns an Atom holding initial whose updates are
// accepted only when validator returns true (Ruby Atom.new(initial,
// validator:)). A validator that panics is treated as rejecting the value,
// mirroring the gem rescuing a raising validator into "invalid".
func NewAtomWithValidator(initial any, validator func(any) bool) *Atom {
	return &Atom{value: initial, validator: validator}
}

// valid reports whether v passes the validator, treating a panic as invalid.
func (a *Atom) valid(v any) (ok bool) {
	if a.validator == nil {
		return true
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return a.validator(v)
}

// Value returns the current value (Ruby #value / #deref).
func (a *Atom) Value() any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.value
}

// AddObserver registers fn to run, with the old and new values, after every
// successful change (Ruby #add_observer).
func (a *Atom) AddObserver(fn func(old, new any)) {
	a.mu.Lock()
	a.observers = append(a.observers, fn)
	a.mu.Unlock()
}

// Swap replaces the value with fn(current), retrying fn until it produces a
// valid value, and returns the value in effect afterwards: the new value on
// success, or the unchanged current value if fn's result is invalid (Ruby
// #swap { |old| ... }). Observers fire on a successful change.
func (a *Atom) Swap(fn func(old any) any) any {
	a.mu.Lock()
	old := a.value
	next := fn(old)
	if !a.valid(next) {
		a.mu.Unlock()
		return old
	}
	a.value = next
	obs := append([]func(old, new any){}, a.observers...)
	a.mu.Unlock()
	for _, o := range obs {
		o(old, next)
	}
	return next
}

// Reset stores newValue unconditionally when valid, returning the value now in
// effect: newValue on success, or the unchanged current value if invalid (Ruby
// #reset). Observers fire on a successful change.
func (a *Atom) Reset(newValue any) any {
	a.mu.Lock()
	old := a.value
	if !a.valid(newValue) {
		a.mu.Unlock()
		return old
	}
	a.value = newValue
	obs := append([]func(old, new any){}, a.observers...)
	a.mu.Unlock()
	for _, o := range obs {
		o(old, newValue)
	}
	return newValue
}

// CompareAndSet stores newValue only if the current value equals oldValue (by
// value equality) and newValue is valid, reporting whether it swapped (Ruby
// #compare_and_set). Observers fire on a successful change.
func (a *Atom) CompareAndSet(oldValue, newValue any) bool {
	a.mu.Lock()
	if !defaultEqual(a.value, oldValue) || !a.valid(newValue) {
		a.mu.Unlock()
		return false
	}
	a.value = newValue
	obs := append([]func(old, new any){}, a.observers...)
	a.mu.Unlock()
	for _, o := range obs {
		o(oldValue, newValue)
	}
	return true
}
