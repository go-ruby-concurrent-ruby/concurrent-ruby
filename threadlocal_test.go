package concurrent

import "testing"

func TestThreadLocalDefaultValue(t *testing.T) {
	tl := NewThreadLocalVar(7)
	if tl.Value() != 7 {
		t.Fatalf("default: %v", tl.Value())
	}
	if tl.SetValue(9) != 9 {
		t.Fatal("SetValue returns v")
	}
	if tl.Value() != 9 {
		t.Fatalf("after set: %v", tl.Value())
	}
}

func TestThreadLocalDefaultFunc(t *testing.T) {
	tl := NewThreadLocalVarWith(func() any { return "d" })
	if tl.Value() != "d" {
		t.Fatalf("default func: %v", tl.Value())
	}
}

func TestThreadLocalIsolation(t *testing.T) {
	tl := NewThreadLocalVar(0)
	tl.SetValue(100)
	other := make(chan any, 1)
	go func() { other <- tl.Value() }()
	if v := <-other; v != 0 {
		t.Fatalf("other goroutine should see default 0, got %v", v)
	}
	if tl.Value() != 100 {
		t.Fatalf("this goroutine keeps 100, got %v", tl.Value())
	}
}

func TestThreadLocalBind(t *testing.T) {
	tl := NewThreadLocalVar(1)

	// In a goroutine that never set a value, Bind restores to "unset" (default).
	res := make(chan int, 2)
	go func() {
		tl.Bind(5, func() { res <- tl.Value().(int) })
		res <- tl.Value().(int)
	}()
	if inside := <-res; inside != 5 {
		t.Fatalf("inside bind: %v", inside)
	}
	if after := <-res; after != 1 {
		t.Fatalf("after bind (was unset) should be default 1, got %v", after)
	}

	// When a prior value exists, Bind restores it.
	tl.SetValue(2)
	tl.Bind(8, func() {
		if tl.Value() != 8 {
			t.Fatalf("inside bind: %v", tl.Value())
		}
	})
	if tl.Value() != 2 {
		t.Fatalf("after bind (had value) should restore 2, got %v", tl.Value())
	}
}
