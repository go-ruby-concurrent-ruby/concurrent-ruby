package concurrent

import (
	"errors"
	"testing"
	"time"
)

func TestPromiseRescue(t *testing.T) {
	// Fulfilled parent passes its value through.
	p := NewPromise()
	c := p.Rescue(func(error) (any, error) { return "recovered", nil })
	p.Fulfill("ok")
	if c.Value(NoTimeout) != "ok" || c.State() != Fulfilled {
		t.Fatalf("passthrough: %v", c.Value(NoTimeout))
	}

	// Rejected parent is recovered to a value.
	boom := errors.New("boom")
	p2 := NewPromise()
	c2 := p2.Rescue(func(r error) (any, error) {
		if r != boom {
			t.Fatalf("reason: %v", r)
		}
		return "rec", nil
	})
	p2.Reject(boom)
	if c2.Value(NoTimeout) != "rec" || c2.State() != Fulfilled {
		t.Fatalf("recover: %v", c2.Value(NoTimeout))
	}

	// A handler that errors rejects the child.
	other := errors.New("other")
	p3 := NewPromise()
	c3 := p3.Rescue(func(error) (any, error) { return nil, other })
	p3.Reject(boom)
	if c3.State() != Rejected || c3.Reason() != other {
		t.Fatalf("handler error: %v %v", c3.State(), c3.Reason())
	}
}

func TestPromiseChain(t *testing.T) {
	p := NewPromise()
	c := p.Chain(func(st State, v any, _ error) (any, error) {
		if st != Fulfilled {
			t.Fatalf("state: %v", st)
		}
		return v.(int) + 1, nil
	})
	p.Fulfill(1)
	if c.Value(NoTimeout) != 2 {
		t.Fatalf("chain fulfilled: %v", c.Value(NoTimeout))
	}

	boom := errors.New("boom")
	p2 := NewPromise()
	c2 := p2.Chain(func(st State, _ any, r error) (any, error) {
		if st != Rejected || r != boom {
			t.Fatalf("state/reason: %v %v", st, r)
		}
		return "handled", nil
	})
	p2.Reject(boom)
	if c2.Value(NoTimeout) != "handled" {
		t.Fatalf("chain on rejection: %v", c2.Value(NoTimeout))
	}

	e := errors.New("e")
	p3 := NewPromise()
	c3 := p3.Chain(func(State, any, error) (any, error) { return nil, e })
	p3.Fulfill(0)
	if c3.State() != Rejected || c3.Reason() != e {
		t.Fatalf("chain fn error: %v %v", c3.State(), c3.Reason())
	}
}

func TestPromiseFlatMap(t *testing.T) {
	p := NewPromise()
	c := p.FlatMap(func(v any) *Promise { return PromisesFulfilledFuture(v.(int) * 10) })
	p.Fulfill(4)
	if c.Value(NoTimeout) != 40 {
		t.Fatalf("flat_map: %v", c.Value(NoTimeout))
	}

	boom := errors.New("boom")
	p2 := NewPromise()
	c2 := p2.FlatMap(func(any) *Promise { return PromisesRejectedFuture(boom) })
	p2.Fulfill(1)
	if c2.State() != Rejected || c2.Reason() != boom {
		t.Fatalf("inner rejection: %v %v", c2.State(), c2.Reason())
	}

	p3 := NewPromise()
	called := false
	c3 := p3.FlatMap(func(any) *Promise { called = true; return nil })
	p3.Reject(boom)
	if c3.State() != Rejected || called {
		t.Fatalf("parent rejection skips fn: state=%v called=%v", c3.State(), called)
	}
}

func TestPromiseExecuteAndZip(t *testing.T) {
	c := PromiseExecute(ImmediateExecutor{}, func() (any, error) { return 7, nil })
	if c.Value(NoTimeout) != 7 {
		t.Fatalf("execute: %v", c.Value(NoTimeout))
	}
	boom := errors.New("boom")
	c2 := PromiseExecute(ImmediateExecutor{}, func() (any, error) { return nil, boom })
	if c2.State() != Rejected || c2.Reason() != boom {
		t.Fatalf("execute error: %v %v", c2.State(), c2.Reason())
	}

	z := PromiseZip(PromisesFulfilledFuture(1), PromisesFulfilledFuture(2), PromisesFulfilledFuture(3))
	vals := z.Value(NoTimeout).([]any)
	if len(vals) != 3 || vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Fatalf("zip values: %v", vals)
	}
	z2 := PromiseZip(PromisesFulfilledFuture(1), PromisesRejectedFuture(boom))
	if z2.State() != Rejected || z2.Reason() != boom {
		t.Fatalf("zip rejection: %v %v", z2.State(), z2.Reason())
	}
	if len(PromiseZip().Value(NoTimeout).([]any)) != 0 {
		t.Fatal("empty zip is an empty slice")
	}
}

func TestPromisesFactories(t *testing.T) {
	if PromisesFulfilledFuture(5).Value(NoTimeout) != 5 {
		t.Fatal("fulfilled_future")
	}
	boom := errors.New("boom")
	if PromisesRejectedFuture(boom).Reason() != boom {
		t.Fatal("rejected_future")
	}
	rf := PromisesResolvableFuture()
	rf.Fulfill(9)
	if rf.Value(NoTimeout) != 9 {
		t.Fatal("resolvable_future")
	}
	fut := PromisesFuture(ImmediateExecutor{}, func() (any, error) { return 3, nil })
	if fut.Value(NoTimeout) != 3 {
		t.Fatal("future_on")
	}
	if PromisesZip(PromisesFulfilledFuture(1)).Value(NoTimeout).([]any)[0] != 1 {
		t.Fatal("promises zip")
	}

	pool := NewFixedThreadPool(2)
	defer func() { pool.Shutdown(); pool.WaitForTermination(2 * time.Second) }()
	rfo := PromisesResolvableFutureOn(pool)
	c := rfo.Then(func(v any) (any, error) { return v.(int) + 1, nil })
	rfo.Fulfill(1)
	if c.Value(2*time.Second) != 2 {
		t.Fatalf("resolvable_future_on: %v", c.Value(2*time.Second))
	}
}
