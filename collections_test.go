package concurrent

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

func TestArray(t *testing.T) {
	a := NewArray(1, 2)
	if a.Size() != 2 || a.Empty() {
		t.Fatal("initial")
	}
	a.Push(3)
	if a.Size() != 3 {
		t.Fatal("push")
	}
	if v, ok := a.At(0); !ok || v != 1 {
		t.Fatal("at in range")
	}
	if _, ok := a.At(99); ok {
		t.Fatal("at out of range high")
	}
	if _, ok := a.At(-1); ok {
		t.Fatal("at out of range low")
	}
	if !a.Set(1, 20) {
		t.Fatal("set in range")
	}
	if a.Set(99, 0) {
		t.Fatal("set out of range")
	}
	if v, _ := a.At(1); v != 20 {
		t.Fatal("value after set")
	}
	if v, ok := a.Pop(); !ok || v != 3 {
		t.Fatal("pop")
	}
	if v, ok := a.Shift(); !ok || v != 1 {
		t.Fatal("shift")
	}
	if v, ok := a.DeleteAt(0); !ok || v != 20 {
		t.Fatal("delete_at")
	}
	if _, ok := a.DeleteAt(0); ok {
		t.Fatal("delete_at empty")
	}
	if _, ok := a.Pop(); ok {
		t.Fatal("pop empty")
	}
	if _, ok := a.Shift(); ok {
		t.Fatal("shift empty")
	}
}

func TestArrayEach(t *testing.T) {
	a := NewArray(1, 2, 3)
	sum := 0
	if err := a.Each(func(v any) error { sum += v.(int); return nil }); err != nil {
		t.Fatal(err)
	}
	if sum != 6 {
		t.Fatalf("each sum: %d", sum)
	}
	boom := errors.New("stop")
	if err := a.Each(func(v any) error { return boom }); err != boom {
		t.Fatal("each error")
	}
	if got := a.ToSlice(); len(got) != 3 {
		t.Fatal("to_slice")
	}
	a.Clear()
	if !a.Empty() {
		t.Fatal("clear")
	}
}

func TestHash(t *testing.T) {
	h := NewHash()
	if !h.Empty() {
		t.Fatal("empty")
	}
	if h.Get("a") != nil {
		t.Fatal("absent get")
	}
	if h.Set("a", 1) != 1 {
		t.Fatal("set")
	}
	if h.Get("a") != 1 {
		t.Fatal("get")
	}
	if !h.KeyQ("a") || h.KeyQ("z") {
		t.Fatal("key?")
	}
	if h.Size() != 1 || h.Empty() {
		t.Fatal("size")
	}
	h.Set("b", 2)
	sum := 0
	if err := h.EachPair(func(k, v any) error { sum += v.(int); return nil }); err != nil {
		t.Fatal(err)
	}
	if sum != 3 {
		t.Fatalf("each_pair: %d", sum)
	}
	boom := errors.New("stop")
	if err := h.EachPair(func(k, v any) error { return boom }); err != boom {
		t.Fatal("each_pair error")
	}
	keys := h.Keys()
	vals := h.Values()
	sort.Slice(keys, func(i, j int) bool { return keys[i].(string) < keys[j].(string) })
	sort.Slice(vals, func(i, j int) bool { return vals[i].(int) < vals[j].(int) })
	if keys[0] != "a" || vals[0] != 1 {
		t.Fatalf("keys/values: %v %v", keys, vals)
	}
	if h.Delete("a") != 1 {
		t.Fatal("delete present")
	}
	if h.Delete("a") != nil {
		t.Fatal("delete absent")
	}
	h.Clear()
	if !h.Empty() {
		t.Fatal("clear")
	}
}

func TestCollectionsRace(t *testing.T) {
	a := NewArray()
	h := NewHash()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a.Push(i)
			_ = a.Size()
			_ = a.ToSlice()
			h.Set(i, i)
			_ = h.Get(i)
			_ = h.Keys()
		}(i)
	}
	wg.Wait()
	if a.Size() != 50 || h.Size() != 50 {
		t.Fatalf("sizes: %d %d", a.Size(), h.Size())
	}
}
