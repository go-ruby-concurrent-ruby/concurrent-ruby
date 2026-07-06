package concurrent

import (
	"sync"
	"testing"
)

func TestAtomicReference(t *testing.T) {
	r := NewAtomicReference("a")
	if r.Get() != "a" {
		t.Fatalf("get: %v", r.Get())
	}
	r.Set("b")
	if r.Get() != "b" {
		t.Fatalf("set: %v", r.Get())
	}
	if old := r.GetAndSet("c"); old != "b" {
		t.Fatalf("get_and_set old: %v", old)
	}
	if r.Get() != "c" {
		t.Fatalf("get_and_set new: %v", r.Get())
	}
	if !r.CompareAndSet("c", "d") {
		t.Fatal("cas should succeed")
	}
	if r.CompareAndSet("c", "e") {
		t.Fatal("cas should fail")
	}
	if r.Get() != "d" {
		t.Fatalf("after cas: %v", r.Get())
	}
	if nv := r.Update(func(v any) any { return v.(string) + "!" }); nv != "d!" {
		t.Fatalf("update: %v", nv)
	}
}

func TestAtomicReferenceWith(t *testing.T) {
	// nil comparator falls back to value equality.
	r := NewAtomicReferenceWith(nil, []int{1, 2})
	if !r.CompareAndSet([]int{1, 2}, []int{3}) {
		t.Fatal("deep-equal cas should succeed")
	}
	// custom comparator: everything equal.
	eqAll := func(a, b any) bool { return true }
	r2 := NewAtomicReferenceWith(eqAll, 1)
	if !r2.CompareAndSet(999, 2) {
		t.Fatal("custom eq cas should succeed")
	}
}

func TestAtomicFixnum(t *testing.T) {
	f := NewAtomicFixnum(10)
	if f.Value() != 10 {
		t.Fatalf("value: %d", f.Value())
	}
	f.SetValue(5)
	if f.Value() != 5 {
		t.Fatalf("set_value: %d", f.Value())
	}
	if f.Increment(1) != 6 {
		t.Fatal("increment")
	}
	if f.Decrement(2) != 4 {
		t.Fatal("decrement")
	}
	if !f.CompareAndSet(4, 8) {
		t.Fatal("cas success")
	}
	if f.CompareAndSet(4, 9) {
		t.Fatal("cas fail")
	}
	if old := f.GetAndSet(1); old != 8 {
		t.Fatalf("get_and_set: %d", old)
	}
	if f.Value() != 1 {
		t.Fatalf("after get_and_set: %d", f.Value())
	}
}

func TestAtomicFixnumUpdate(t *testing.T) {
	f := NewAtomicFixnum(0)
	// Simple success path.
	if nv := f.Update(func(x int64) int64 { return x + 3 }); nv != 3 {
		t.Fatalf("update: %d", nv)
	}
	// Force the compare-and-swap retry branch: the first application mutates the
	// underlying value so the initial CAS fails and the loop retries.
	first := true
	nv := f.Update(func(x int64) int64 {
		if first {
			first = false
			f.v.Add(10) // now 13; makes the pending CAS(3, ...) fail
		}
		return x + 1
	})
	if nv != 14 { // retried: old=13, +1
		t.Fatalf("update retry: %d", nv)
	}
}

func TestAtomicBoolean(t *testing.T) {
	b := NewAtomicBoolean(false)
	if b.Value() || b.TrueQ() || !b.FalseQ() {
		t.Fatal("initial false")
	}
	if !b.MakeTrue() {
		t.Fatal("make_true should change")
	}
	if b.MakeTrue() {
		t.Fatal("make_true again should not change")
	}
	if !b.Value() || !b.TrueQ() || b.FalseQ() {
		t.Fatal("now true")
	}
	if !b.MakeFalse() {
		t.Fatal("make_false should change")
	}
	if b.MakeFalse() {
		t.Fatal("make_false again should not change")
	}
	b.SetValue(true)
	if !b.Value() {
		t.Fatal("set_value true")
	}
	if !b.CompareAndSet(true, false) {
		t.Fatal("cas success")
	}
	if b.CompareAndSet(true, false) {
		t.Fatal("cas fail")
	}
}

func TestAtomicFixnumRace(t *testing.T) {
	f := NewAtomicFixnum(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.Increment(1)
			f.Update(func(x int64) int64 { return x + 1 })
		}()
	}
	wg.Wait()
	if f.Value() != 100 {
		t.Fatalf("concurrent total: %d", f.Value())
	}
}
