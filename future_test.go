package concurrent

import (
	"errors"
	"testing"
	"time"
)

func TestFutureFulfilled(t *testing.T) {
	p := NewFixedThreadPool(2)
	defer func() { p.Shutdown(); p.WaitForTermination(2 * time.Second) }()

	f := FutureExecute(p, func() (any, error) { return 42, nil })
	if f.Value(2*time.Second) != 42 {
		t.Fatalf("value: %v", f.Value(NoTimeout))
	}
	f.Wait(2 * time.Second)
	if !f.FulfilledQ() || !f.CompleteQ() || f.PendingQ() || f.RejectedQ() {
		t.Fatal("state predicates for fulfilled")
	}
	if f.State() != Fulfilled {
		t.Fatalf("state: %v", f.State())
	}
	if f.Reason() != nil {
		t.Fatal("fulfilled has no reason")
	}
	v, err := f.ValueBang(2 * time.Second)
	if err != nil || v != 42 {
		t.Fatalf("value!: %v %v", v, err)
	}
}

func TestFutureRejected(t *testing.T) {
	boom := errors.New("boom")
	// Use the immediate executor for deterministic inline resolution.
	f := FutureExecute(ImmediateExecutor{}, func() (any, error) { return nil, boom })
	if !f.RejectedQ() || f.FulfilledQ() {
		t.Fatal("should be rejected")
	}
	if f.Value(NoTimeout) != nil {
		t.Fatal("rejected value is nil")
	}
	if f.Reason() != boom {
		t.Fatalf("reason: %v", f.Reason())
	}
	if _, err := f.ValueBang(NoTimeout); err != boom {
		t.Fatalf("value! err: %v", err)
	}
}

func TestFutureTimeout(t *testing.T) {
	// A future whose body blocks until we release it; Value with a short timeout
	// returns nil while still pending, then we release and shut down cleanly.
	p := NewFixedThreadPool(1)
	release := NewCountDownLatch(1)
	f := FutureExecute(p, func() (any, error) {
		release.Wait(2 * time.Second)
		return 7, nil
	})
	if f.Value(20*time.Millisecond) != nil {
		t.Fatal("pending future should time out to nil")
	}
	if !f.PendingQ() {
		t.Fatal("should still be pending")
	}
	release.CountDown()
	if f.Value(2*time.Second) != 7 {
		t.Fatal("value after release")
	}
	p.Shutdown()
	p.WaitForTermination(2 * time.Second)
}

func TestPromiseFulfill(t *testing.T) {
	p := NewPromise()
	if p.State() != Pending {
		t.Fatal("initial pending")
	}
	if err := p.Fulfill(1); err != nil {
		t.Fatalf("fulfill: %v", err)
	}
	if p.Value(NoTimeout) != 1 || p.State() != Fulfilled {
		t.Fatal("fulfilled")
	}
	// Second assignment is a MultipleAssignmentError.
	if err := p.Fulfill(2); err != ErrMultipleAssignment {
		t.Fatalf("expected multiple assignment: %v", err)
	}
	if err := p.Reject(errors.New("x")); err != ErrMultipleAssignment {
		t.Fatalf("expected multiple assignment on reject: %v", err)
	}
}

func TestPromiseReject(t *testing.T) {
	p := NewPromise()
	boom := errors.New("boom")
	if err := p.Reject(boom); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if p.State() != Rejected || p.Reason() != boom || p.Value(NoTimeout) != nil {
		t.Fatal("rejected state")
	}
}

func TestPromiseThenFulfilled(t *testing.T) {
	// Register-before-resolve: Then observes a pending parent.
	p := NewPromise()
	c := p.Then(func(v any) (any, error) { return v.(int) * 2, nil })
	p.Fulfill(21)
	if c.Value(NoTimeout) != 42 || c.State() != Fulfilled {
		t.Fatalf("then value: %v", c.Value(NoTimeout))
	}
}

func TestPromiseThenAfterResolved(t *testing.T) {
	// Resolve-before-register: Then observes an already-fulfilled parent and
	// runs the continuation immediately.
	p := NewPromise()
	p.Fulfill(10)
	c := p.Then(func(v any) (any, error) { return v.(int) + 5, nil })
	if c.Value(NoTimeout) != 15 {
		t.Fatalf("then after resolved: %v", c.Value(NoTimeout))
	}
}

func TestPromiseThenPassthrough(t *testing.T) {
	// nil continuation passes the value through.
	p := NewPromise()
	c := p.Then(nil)
	p.Fulfill("x")
	if c.Value(NoTimeout) != "x" {
		t.Fatalf("passthrough: %v", c.Value(NoTimeout))
	}
}

func TestPromiseThenRejectionPropagates(t *testing.T) {
	p := NewPromise()
	boom := errors.New("boom")
	c := p.Then(func(v any) (any, error) { return v, nil })
	p.Reject(boom)
	if c.State() != Rejected || c.Reason() != boom {
		t.Fatalf("rejection should propagate: %v %v", c.State(), c.Reason())
	}
}

func TestPromiseThenContinuationError(t *testing.T) {
	p := NewPromise()
	boom := errors.New("in continuation")
	c := p.Then(func(v any) (any, error) { return nil, boom })
	p.Fulfill(1)
	if c.State() != Rejected || c.Reason() != boom {
		t.Fatalf("continuation error should reject child: %v", c.Reason())
	}
}

func TestPromiseOnExecutor(t *testing.T) {
	pool := NewFixedThreadPool(2)
	defer func() { pool.Shutdown(); pool.WaitForTermination(2 * time.Second) }()

	p := NewPromiseOn(pool)
	c := p.Then(func(v any) (any, error) { return v.(int) + 1, nil })
	p.Fulfill(1)
	if c.Value(2*time.Second) != 2 {
		t.Fatalf("executor-backed then: %v", c.Value(2*time.Second))
	}
}
