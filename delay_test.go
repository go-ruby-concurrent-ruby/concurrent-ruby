package concurrent

import (
	"errors"
	"testing"
)

func TestDelayFulfilled(t *testing.T) {
	calls := 0
	d := NewDelay(func() (any, error) { calls++; return 42, nil })
	if !d.PendingQ() || d.State() != Pending {
		t.Fatal("unforced delay should be pending")
	}
	if d.Value() != 42 {
		t.Fatalf("value: %v", d.Value())
	}
	// Second access is memoised: the body runs exactly once.
	if d.Value() != 42 || calls != 1 {
		t.Fatalf("memoisation broke: value=%v calls=%d", d.Value(), calls)
	}
	if !d.FulfilledQ() || d.RejectedQ() || d.PendingQ() {
		t.Fatal("state predicates for fulfilled delay")
	}
	if d.State() != Fulfilled || d.Reason() != nil {
		t.Fatalf("state/reason: %v %v", d.State(), d.Reason())
	}
	v, err := d.ValueBang()
	if v != 42 || err != nil {
		t.Fatalf("value!: %v %v", v, err)
	}
}

func TestDelayRejected(t *testing.T) {
	boom := errors.New("boom")
	d := NewDelay(func() (any, error) { return nil, boom })
	if d.Value() != nil {
		t.Fatal("rejected delay value is nil")
	}
	if !d.RejectedQ() || d.FulfilledQ() {
		t.Fatal("state predicates for rejected delay")
	}
	if d.Reason() != boom || d.State() != Rejected {
		t.Fatalf("reason/state: %v %v", d.Reason(), d.State())
	}
	if _, err := d.ValueBang(); err != boom {
		t.Fatalf("value! err: %v", err)
	}
}
