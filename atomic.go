package concurrent

import (
	"reflect"
	"sync"
	"sync/atomic"
)

// defaultEqual is the value-equality used by AtomicReference#compare_and_set
// when the host does not supply one. It never panics on incomparable values,
// unlike Go's == operator, matching the gem's reliance on Ruby's == protocol.
func defaultEqual(a, b any) bool { return reflect.DeepEqual(a, b) }

// AtomicReference models Concurrent::AtomicReference: an object reference that
// may be read and written atomically, with a compare-and-set primitive. The
// reference holds any value; compare_and_set uses value equality (the host may
// override it to Ruby's eql?/equal? semantics).
type AtomicReference struct {
	mu    sync.Mutex
	value any
	eq    func(a, b any) bool
}

// NewAtomicReference returns an AtomicReference holding initial, comparing by
// value equality on compare_and_set.
func NewAtomicReference(initial any) *AtomicReference {
	return &AtomicReference{value: initial, eq: defaultEqual}
}

// NewAtomicReferenceWith returns an AtomicReference whose compare_and_set uses
// the host-supplied equality (e.g. Ruby eql?). A nil eq falls back to value
// equality.
func NewAtomicReferenceWith(eq func(a, b any) bool, initial any) *AtomicReference {
	if eq == nil {
		eq = defaultEqual
	}
	return &AtomicReference{value: initial, eq: eq}
}

// Get returns the current value (Ruby #get / #value).
func (r *AtomicReference) Get() any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.value
}

// Set stores v (Ruby #set / #value=).
func (r *AtomicReference) Set(v any) {
	r.mu.Lock()
	r.value = v
	r.mu.Unlock()
}

// GetAndSet atomically stores v and returns the previous value (Ruby #get_and_set / #swap).
func (r *AtomicReference) GetAndSet(v any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.value
	r.value = v
	return old
}

// CompareAndSet stores next only if the current value equals old, reporting
// whether the swap happened (Ruby #compare_and_set / #compare_and_swap).
func (r *AtomicReference) CompareAndSet(old, next any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.eq(r.value, old) {
		r.value = next
		return true
	}
	return false
}

// Update atomically replaces the value with fn(current) and returns the new
// value (Ruby #update). fn runs while the reference is locked.
func (r *AtomicReference) Update(fn func(any) any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.value = fn(r.value)
	return r.value
}

// AtomicFixnum models Concurrent::AtomicFixnum: a 64-bit integer with atomic
// arithmetic, backed by sync/atomic.
type AtomicFixnum struct {
	v atomic.Int64
}

// NewAtomicFixnum returns an AtomicFixnum initialised to initial.
func NewAtomicFixnum(initial int64) *AtomicFixnum {
	f := &AtomicFixnum{}
	f.v.Store(initial)
	return f
}

// Value returns the current value (Ruby #value).
func (f *AtomicFixnum) Value() int64 { return f.v.Load() }

// SetValue stores n (Ruby #value=).
func (f *AtomicFixnum) SetValue(n int64) { f.v.Store(n) }

// Increment atomically adds delta and returns the new value (Ruby #increment,
// whose default delta is 1 — the binding passes 1).
func (f *AtomicFixnum) Increment(delta int64) int64 { return f.v.Add(delta) }

// Decrement atomically subtracts delta and returns the new value (Ruby #decrement).
func (f *AtomicFixnum) Decrement(delta int64) int64 { return f.v.Add(-delta) }

// CompareAndSet stores next only if the current value equals old, reporting
// whether the swap happened (Ruby #compare_and_set).
func (f *AtomicFixnum) CompareAndSet(old, next int64) bool {
	return f.v.CompareAndSwap(old, next)
}

// GetAndSet atomically stores n and returns the previous value (Ruby #update
// is separate; this is #swap-style get-and-set).
func (f *AtomicFixnum) GetAndSet(n int64) int64 { return f.v.Swap(n) }

// Update atomically replaces the value with fn(current) and returns the new
// value (Ruby #update), retrying under contention with a compare-and-swap loop.
func (f *AtomicFixnum) Update(fn func(int64) int64) int64 {
	for {
		old := f.v.Load()
		next := fn(old)
		if f.v.CompareAndSwap(old, next) {
			return next
		}
	}
}

// AtomicBoolean models Concurrent::AtomicBoolean, backed by sync/atomic.
type AtomicBoolean struct {
	v atomic.Int32
}

func b2i(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// NewAtomicBoolean returns an AtomicBoolean initialised to initial.
func NewAtomicBoolean(initial bool) *AtomicBoolean {
	b := &AtomicBoolean{}
	b.v.Store(b2i(initial))
	return b
}

// Value returns the current value (Ruby #value).
func (b *AtomicBoolean) Value() bool { return b.v.Load() != 0 }

// SetValue stores v (Ruby #value=).
func (b *AtomicBoolean) SetValue(v bool) { b.v.Store(b2i(v)) }

// TrueQ reports whether the value is true (Ruby #true?).
func (b *AtomicBoolean) TrueQ() bool { return b.v.Load() != 0 }

// FalseQ reports whether the value is false (Ruby #false?).
func (b *AtomicBoolean) FalseQ() bool { return b.v.Load() == 0 }

// MakeTrue sets the value to true, returning true if it changed (Ruby #make_true).
func (b *AtomicBoolean) MakeTrue() bool { return b.v.CompareAndSwap(0, 1) }

// MakeFalse sets the value to false, returning true if it changed (Ruby #make_false).
func (b *AtomicBoolean) MakeFalse() bool { return b.v.CompareAndSwap(1, 0) }

// CompareAndSet stores next only if the current value equals old, reporting
// whether the swap happened (Ruby #compare_and_set).
func (b *AtomicBoolean) CompareAndSet(old, next bool) bool {
	return b.v.CompareAndSwap(b2i(old), b2i(next))
}
