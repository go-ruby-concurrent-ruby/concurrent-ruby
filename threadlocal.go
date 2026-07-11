package concurrent

import (
	"runtime"
	"strconv"
	"sync"
)

// goid returns the current goroutine's id, parsed from the runtime stack
// header ("goroutine N [state]:"). It is the goroutine-identity a
// ThreadLocalVar keys its per-thread storage on, standing in for Ruby's
// Thread.current in the rbgo binding (where each Ruby thread is a goroutine).
// The parse is architecture-independent — it reads the textual header the Go
// runtime writes identically on every target.
func goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// buf holds "goroutine 123 [running]:\n..."; the id is between the first
	// space (index 9) and the following space.
	b := buf[10:n]
	j := 0
	for j < len(b) && b[j] != ' ' {
		j++
	}
	id, _ := strconv.ParseInt(string(b[:j]), 10, 64)
	return id
}

// ThreadLocalVar models Concurrent::ThreadLocalVar: a variable whose value is
// independent per thread (here, per goroutine). Reads and writes on one
// goroutine never affect another's; a goroutine that has not set a value sees
// the default.
type ThreadLocalVar struct {
	mu         sync.Mutex
	values     map[int64]any
	defaultVal any
	defaultFn  func() any
}

// NewThreadLocalVar returns a ThreadLocalVar whose default value (seen by any
// goroutine that has not set one) is def (Ruby ThreadLocalVar.new(default)).
func NewThreadLocalVar(def any) *ThreadLocalVar {
	return &ThreadLocalVar{values: make(map[int64]any), defaultVal: def}
}

// NewThreadLocalVarWith returns a ThreadLocalVar whose default value is computed
// per goroutine by defaultFn on first read (Ruby ThreadLocalVar.new { ... }).
func NewThreadLocalVarWith(defaultFn func() any) *ThreadLocalVar {
	return &ThreadLocalVar{values: make(map[int64]any), defaultFn: defaultFn}
}

// Value returns the calling goroutine's value, or the default if it has not set
// one (Ruby #value).
func (t *ThreadLocalVar) Value() any {
	id := goid()
	t.mu.Lock()
	defer t.mu.Unlock()
	if v, ok := t.values[id]; ok {
		return v
	}
	return t.deflt()
}

// deflt computes the default value; the caller holds t.mu.
func (t *ThreadLocalVar) deflt() any {
	if t.defaultFn != nil {
		return t.defaultFn()
	}
	return t.defaultVal
}

// SetValue stores v as the calling goroutine's value and returns it (Ruby
// #value=).
func (t *ThreadLocalVar) SetValue(v any) any {
	id := goid()
	t.mu.Lock()
	t.values[id] = v
	t.mu.Unlock()
	return v
}

// Bind sets the calling goroutine's value to v for the duration of fn, then
// restores the previous binding (Ruby #bind(value) { ... }).
func (t *ThreadLocalVar) Bind(v any, fn func()) {
	id := goid()
	t.mu.Lock()
	prev, had := t.values[id]
	t.values[id] = v
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		if had {
			t.values[id] = prev
		} else {
			delete(t.values, id)
		}
		t.mu.Unlock()
	}()
	fn()
}
