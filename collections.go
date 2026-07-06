package concurrent

import "sync"

// Array models Concurrent::Array: a thread-safe, mutex-guarded ordered list.
type Array struct {
	mu sync.RWMutex
	a  []any
}

// NewArray returns an Array seeded with items (Ruby Concurrent::Array.new).
func NewArray(items ...any) *Array {
	a := make([]any, len(items))
	copy(a, items)
	return &Array{a: a}
}

// Push appends v and returns the array (Ruby #push / #<<).
func (a *Array) Push(v any) *Array {
	a.mu.Lock()
	a.a = append(a.a, v)
	a.mu.Unlock()
	return a
}

// Pop removes and returns the last element and whether one was present (Ruby #pop).
func (a *Array) Pop() (any, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := len(a.a)
	if n == 0 {
		return nil, false
	}
	v := a.a[n-1]
	a.a = a.a[:n-1]
	return v, true
}

// Shift removes and returns the first element and whether one was present (Ruby #shift).
func (a *Array) Shift() (any, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.a) == 0 {
		return nil, false
	}
	v := a.a[0]
	a.a = a.a[1:]
	return v, true
}

// At returns the element at index i and whether i is in range (Ruby #[]).
func (a *Array) At(i int) (any, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if i < 0 || i >= len(a.a) {
		return nil, false
	}
	return a.a[i], true
}

// Set stores v at index i, returning false if i is out of range (Ruby #[]=).
func (a *Array) Set(i int, v any) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i < 0 || i >= len(a.a) {
		return false
	}
	a.a[i] = v
	return true
}

// DeleteAt removes and returns the element at index i and whether i was in
// range (Ruby #delete_at).
func (a *Array) DeleteAt(i int) (any, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i < 0 || i >= len(a.a) {
		return nil, false
	}
	v := a.a[i]
	a.a = append(a.a[:i], a.a[i+1:]...)
	return v, true
}

// Size returns the number of elements (Ruby #size / #length).
func (a *Array) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.a)
}

// Empty reports whether the array has no elements (Ruby #empty?).
func (a *Array) Empty() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.a) == 0
}

// Clear removes every element (Ruby #clear).
func (a *Array) Clear() {
	a.mu.Lock()
	a.a = nil
	a.mu.Unlock()
}

// Each calls fn for every element in order, stopping and returning fn's error
// if it returns one (Ruby #each). Elements are snapshotted under the lock.
func (a *Array) Each(fn func(any) error) error {
	for _, v := range a.ToSlice() {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

// ToSlice returns a copy of the elements (Ruby #to_a).
func (a *Array) ToSlice() []any {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]any, len(a.a))
	copy(out, a.a)
	return out
}

// Hash models Concurrent::Hash: a thread-safe, mutex-guarded key/value map. It
// is the synchronized-wrapper sibling of Map (which additionally offers atomic
// read-modify-write operations).
type Hash struct {
	mu sync.RWMutex
	m  map[any]any
}

// NewHash returns an empty Hash.
func NewHash() *Hash { return &Hash{m: make(map[any]any)} }

// Get returns the value for key, or nil if absent (Ruby #[]).
func (h *Hash) Get(key any) any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.m[key]
}

// Set stores value under key and returns value (Ruby #[]=).
func (h *Hash) Set(key, value any) any {
	h.mu.Lock()
	h.m[key] = value
	h.mu.Unlock()
	return value
}

// KeyQ reports whether key is present (Ruby #key?).
func (h *Hash) KeyQ(key any) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.m[key]
	return ok
}

// Delete removes key, returning its previous value or nil (Ruby #delete).
func (h *Hash) Delete(key any) any {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.m[key]
	if ok {
		delete(h.m, key)
		return v
	}
	return nil
}

// Size returns the number of entries (Ruby #size).
func (h *Hash) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.m)
}

// Empty reports whether the hash has no entries (Ruby #empty?).
func (h *Hash) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.m) == 0
}

// Clear removes every entry (Ruby #clear).
func (h *Hash) Clear() {
	h.mu.Lock()
	h.m = make(map[any]any)
	h.mu.Unlock()
}

// EachPair calls fn for every key/value pair, stopping and returning fn's error
// if it returns one (Ruby #each_pair).
func (h *Hash) EachPair(fn func(key, value any) error) error {
	for _, kv := range h.snapshot() {
		if err := fn(kv.k, kv.v); err != nil {
			return err
		}
	}
	return nil
}

// Keys returns a snapshot of the keys (Ruby #keys).
func (h *Hash) Keys() []any {
	pairs := h.snapshot()
	out := make([]any, len(pairs))
	for i, kv := range pairs {
		out[i] = kv.k
	}
	return out
}

// Values returns a snapshot of the values (Ruby #values).
func (h *Hash) Values() []any {
	pairs := h.snapshot()
	out := make([]any, len(pairs))
	for i, kv := range pairs {
		out[i] = kv.v
	}
	return out
}

func (h *Hash) snapshot() []pair {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]pair, 0, len(h.m))
	for k, v := range h.m {
		out = append(out, pair{k, v})
	}
	return out
}
