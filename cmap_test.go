package concurrent

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestMapBasics(t *testing.T) {
	m := NewMap()
	if !m.Empty() {
		t.Fatal("new map empty")
	}
	if m.Get("a") != nil {
		t.Fatal("absent get nil")
	}
	if m.Set("a", 1) != 1 {
		t.Fatal("set returns value")
	}
	if m.Get("a") != 1 {
		t.Fatal("get after set")
	}
	if v, ok := m.GetPair("a"); !ok || v != 1 {
		t.Fatal("get_pair present")
	}
	if _, ok := m.GetPair("z"); ok {
		t.Fatal("get_pair absent")
	}
	if m.GetOrDefault("a", 99) != 1 {
		t.Fatal("get_or_default present")
	}
	if m.GetOrDefault("z", 99) != 99 {
		t.Fatal("get_or_default absent")
	}
	if !m.KeyQ("a") || m.KeyQ("z") {
		t.Fatal("key?")
	}
	if m.Size() != 1 || m.Empty() {
		t.Fatal("size")
	}
}

func TestMapComputeIfAbsent(t *testing.T) {
	m := NewMap()
	calls := 0
	got := m.ComputeIfAbsent("a", func() any { calls++; return 10 })
	if got != 10 || calls != 1 {
		t.Fatal("compute added")
	}
	got = m.ComputeIfAbsent("a", func() any { calls++; return 20 })
	if got != 10 || calls != 1 {
		t.Fatal("compute should not recompute present")
	}
}

func TestMapCompute(t *testing.T) {
	m := NewMap()
	// absent + keep -> add
	if m.Compute("a", func(old any, present bool) (any, bool) {
		if present {
			t.Fatal("should be absent")
		}
		return 1, true
	}) != 1 {
		t.Fatal("compute add")
	}
	// present + keep -> update
	if m.Compute("a", func(old any, present bool) (any, bool) {
		if !present || old != 1 {
			t.Fatalf("present old: %v %v", present, old)
		}
		return 2, true
	}) != 2 {
		t.Fatal("compute update")
	}
	// present + !keep -> delete
	if m.Compute("a", func(old any, present bool) (any, bool) { return nil, false }) != nil {
		t.Fatal("compute delete returns nil")
	}
	if m.KeyQ("a") {
		t.Fatal("should be deleted")
	}
	// absent + !keep -> no-op
	if m.Compute("b", func(old any, present bool) (any, bool) { return nil, false }) != nil {
		t.Fatal("compute absent no-op")
	}
	if m.KeyQ("b") {
		t.Fatal("should not have added b")
	}
}

func TestMapPutIfAbsent(t *testing.T) {
	m := NewMap()
	if m.PutIfAbsent("a", 1) != nil {
		t.Fatal("put_if_absent new returns nil")
	}
	if m.PutIfAbsent("a", 2) != 1 {
		t.Fatal("put_if_absent existing returns old")
	}
	if m.Get("a") != 1 {
		t.Fatal("value not overwritten")
	}
}

func TestMapDelete(t *testing.T) {
	m := NewMap()
	m.Set("a", 1)
	if m.Delete("a") != 1 {
		t.Fatal("delete returns old")
	}
	if m.Delete("a") != nil {
		t.Fatal("delete absent returns nil")
	}
}

func TestMapEachAndClear(t *testing.T) {
	m := NewMap()
	m.Set("a", 1)
	m.Set("b", 2)
	sum := 0
	if err := m.EachPair(func(k, v any) error { sum += v.(int); return nil }); err != nil {
		t.Fatal(err)
	}
	if sum != 3 {
		t.Fatalf("each_pair sum: %d", sum)
	}
	// early error stops iteration
	boom := errors.New("stop")
	if err := m.EachPair(func(k, v any) error { return boom }); err != boom {
		t.Fatal("each_pair should return error")
	}
	keys := m.Keys()
	vals := m.Values()
	sort.Slice(keys, func(i, j int) bool { return keys[i].(string) < keys[j].(string) })
	sort.Slice(vals, func(i, j int) bool { return vals[i].(int) < vals[j].(int) })
	if len(keys) != 2 || keys[0] != "a" || vals[1] != 2 {
		t.Fatalf("keys/values: %v %v", keys, vals)
	}
	m.Clear()
	if !m.Empty() {
		t.Fatal("clear")
	}
}

func TestMapRace(t *testing.T) {
	m := NewMap()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.ComputeIfAbsent(i%5, func() any { return i })
			m.Set(i, i)
			m.Get(i)
			_ = m.Keys()
		}(i)
	}
	wg.Wait()
	if m.Size() == 0 {
		t.Fatal("expected entries")
	}
}
