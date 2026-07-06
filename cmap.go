package concurrent

import "sync"

// Map models Concurrent::Map: a thread-safe hash with atomic read-modify-write
// operations (compute_if_absent, compute, put_if_absent). Keys are keyed by Go
// equality; a binding maps Ruby hash/eql? keys onto comparable Go keys, exactly
// as go-ruby-set does for Set members.
type Map struct {
	mu sync.RWMutex
	m  map[any]any
}

// NewMap returns an empty Map.
func NewMap() *Map { return &Map{m: make(map[any]any)} }

// Get returns the value for key, or nil if absent (Ruby #[]).
func (c *Map) Get(key any) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[key]
}

// GetPair returns the value for key and whether it was present (Ruby #fetch
// without a default, as a two-value form).
func (c *Map) GetPair(key any) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[key]
	return v, ok
}

// GetOrDefault returns the value for key, or def if absent (Ruby #fetch(k, def)).
func (c *Map) GetOrDefault(key, def any) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.m[key]; ok {
		return v
	}
	return def
}

// Set stores value under key and returns value (Ruby #[]=).
func (c *Map) Set(key, value any) any {
	c.mu.Lock()
	c.m[key] = value
	c.mu.Unlock()
	return value
}

// KeyQ reports whether key is present (Ruby #key?).
func (c *Map) KeyQ(key any) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.m[key]
	return ok
}

// ComputeIfAbsent returns the existing value for key, or atomically computes it
// with fn, stores, and returns it (Ruby #compute_if_absent). fn runs while the
// map is locked, so it observes a consistent view.
func (c *Map) ComputeIfAbsent(key any, fn func() any) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.m[key]; ok {
		return v
	}
	nv := fn()
	c.m[key] = nv
	return nv
}

// Compute atomically computes a new value for key from its current value
// (Ruby #compute). fn receives the current value and whether it was present,
// and returns the new value and whether to keep the entry; returning keep=false
// deletes the entry (if present) and yields nil.
func (c *Map) Compute(key any, fn func(old any, present bool) (value any, keep bool)) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, present := c.m[key]
	nv, keep := fn(old, present)
	if keep {
		c.m[key] = nv
		return nv
	}
	if present {
		delete(c.m, key)
	}
	return nil
}

// PutIfAbsent stores value under key only if absent, returning the existing
// value if present or nil if it stored (Ruby #put_if_absent).
func (c *Map) PutIfAbsent(key, value any) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.m[key]; ok {
		return v
	}
	c.m[key] = value
	return nil
}

// Delete removes key, returning its previous value or nil (Ruby #delete).
func (c *Map) Delete(key any) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	if ok {
		delete(c.m, key)
		return v
	}
	return nil
}

// Size returns the number of entries (Ruby #size).
func (c *Map) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// Empty reports whether the map has no entries (Ruby #empty?).
func (c *Map) Empty() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m) == 0
}

// Clear removes every entry (Ruby #clear).
func (c *Map) Clear() {
	c.mu.Lock()
	c.m = make(map[any]any)
	c.mu.Unlock()
}

// EachPair calls fn for every key/value pair, stopping and returning fn's error
// if it returns one (Ruby #each_pair). Pairs are snapshotted under the lock so
// fn may safely mutate the map.
func (c *Map) EachPair(fn func(key, value any) error) error {
	for _, kv := range c.snapshot() {
		if err := fn(kv.k, kv.v); err != nil {
			return err
		}
	}
	return nil
}

// Keys returns a snapshot of the keys (Ruby #keys).
func (c *Map) Keys() []any {
	pairs := c.snapshot()
	out := make([]any, len(pairs))
	for i, kv := range pairs {
		out[i] = kv.k
	}
	return out
}

// Values returns a snapshot of the values (Ruby #values).
func (c *Map) Values() []any {
	pairs := c.snapshot()
	out := make([]any, len(pairs))
	for i, kv := range pairs {
		out[i] = kv.v
	}
	return out
}

type pair struct{ k, v any }

func (c *Map) snapshot() []pair {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]pair, 0, len(c.m))
	for k, v := range c.m {
		out = append(out, pair{k, v})
	}
	return out
}
