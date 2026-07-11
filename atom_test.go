package concurrent

import "testing"

func TestAtomBasic(t *testing.T) {
	a := NewAtom(1)
	if a.Value() != 1 {
		t.Fatalf("initial: %v", a.Value())
	}
	if got := a.Swap(func(o any) any { return o.(int) + 1 }); got != 2 {
		t.Fatalf("swap: %v", got)
	}
	if a.Value() != 2 {
		t.Fatalf("after swap: %v", a.Value())
	}
	if got := a.Reset(10); got != 10 {
		t.Fatalf("reset: %v", got)
	}
	if !a.CompareAndSet(10, 20) {
		t.Fatal("cas should swap")
	}
	if a.CompareAndSet(999, 30) {
		t.Fatal("cas with wrong old must fail")
	}
	if a.Value() != 20 {
		t.Fatalf("final: %v", a.Value())
	}
}

func TestAtomObservers(t *testing.T) {
	a := NewAtom(0)
	var seen [][2]int
	a.AddObserver(func(o, n any) { seen = append(seen, [2]int{o.(int), n.(int)}) })
	a.Swap(func(o any) any { return o.(int) + 1 }) // 0 -> 1
	a.Reset(5)                                     // 1 -> 5
	a.CompareAndSet(5, 6)                          // 5 -> 6
	want := [][2]int{{0, 1}, {1, 5}, {5, 6}}
	if len(seen) != len(want) {
		t.Fatalf("observer count: %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("observer %d: got %v want %v", i, seen[i], want[i])
		}
	}
}

func TestAtomValidator(t *testing.T) {
	even := func(v any) bool { return v.(int)%2 == 0 }
	a := NewAtomWithValidator(0, even)
	// Invalid results leave the value unchanged and return the current value.
	if got := a.Swap(func(o any) any { return 3 }); got != 0 || a.Value() != 0 {
		t.Fatalf("invalid swap: got=%v value=%v", got, a.Value())
	}
	if got := a.Reset(3); got != 0 || a.Value() != 0 {
		t.Fatalf("invalid reset: got=%v value=%v", got, a.Value())
	}
	if a.CompareAndSet(0, 3) {
		t.Fatal("cas to invalid must fail")
	}
	// Valid results go through.
	if got := a.Swap(func(o any) any { return 4 }); got != 4 {
		t.Fatalf("valid swap: %v", got)
	}
}

func TestAtomValidatorPanicIsInvalid(t *testing.T) {
	a := NewAtomWithValidator(0, func(v any) bool { panic("boom") })
	if got := a.Reset(1); got != 0 || a.Value() != 0 {
		t.Fatalf("panicking validator must reject: got=%v value=%v", got, a.Value())
	}
}
