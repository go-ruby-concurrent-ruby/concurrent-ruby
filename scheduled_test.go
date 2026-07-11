package concurrent

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestScheduledTaskFulfilled(t *testing.T) {
	// A negative delay is clamped to zero and fires promptly.
	task := ScheduledTaskExecute(ImmediateExecutor{}, -1, func() (any, error) { return 42, nil })
	task.Wait(2 * time.Second)
	if task.Value(2*time.Second) != 42 {
		t.Fatalf("value: %v", task.Value(NoTimeout))
	}
	if !task.FulfilledQ() || task.RejectedQ() || task.PendingQ() {
		t.Fatal("state predicates for fulfilled task")
	}
	if task.State() != Fulfilled || task.Reason() != nil {
		t.Fatalf("state/reason: %v %v", task.State(), task.Reason())
	}
	v, err := task.ValueBang(2 * time.Second)
	if v != 42 || err != nil {
		t.Fatalf("value!: %v %v", v, err)
	}
	if task.Cancel() { // already started/completed
		t.Fatal("cannot cancel a completed task")
	}
	if task.CancelledQ() {
		t.Fatal("completed task is not cancelled")
	}
}

func TestScheduledTaskRejected(t *testing.T) {
	boom := errors.New("boom")
	task := ScheduledTaskExecute(ImmediateExecutor{}, 0, func() (any, error) { return nil, boom })
	task.Wait(2 * time.Second)
	if task.Value(2*time.Second) != nil {
		t.Fatal("rejected task value is nil")
	}
	if !task.RejectedQ() || task.Reason() != boom {
		t.Fatalf("rejected: %v %v", task.RejectedQ(), task.Reason())
	}
	if _, err := task.ValueBang(2 * time.Second); err != boom {
		t.Fatalf("value! err: %v", err)
	}
}

func TestScheduledTaskCancel(t *testing.T) {
	ran := NewAtomicBoolean(false)
	task := ScheduledTaskExecute(ImmediateExecutor{}, time.Hour, func() (any, error) {
		ran.MakeTrue()
		return 1, nil
	})
	if !task.Cancel() {
		t.Fatal("should cancel before firing")
	}
	if !task.CancelledQ() || !task.RejectedQ() {
		t.Fatal("cancelled task is rejected with ErrCancelled")
	}
	if task.Cancel() { // second cancel: already cancelled
		t.Fatal("second cancel returns false")
	}
	// White-box: a timer that fires after cancellation is a no-op.
	task.fire()
	if ran.Value() {
		t.Fatal("cancelled task body must not run")
	}
	if _, err := task.ValueBang(NoTimeout); err != ErrCancelled {
		t.Fatalf("cancelled reason: %v", err)
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTimerTaskRuns(t *testing.T) {
	// interval <= 0 is clamped to 1ms.
	tt := NewTimerTask(0, func() (any, error) { return "tick", nil })
	if tt.ExecutionInterval() != time.Millisecond {
		t.Fatalf("interval: %v", tt.ExecutionInterval())
	}
	var mu sync.Mutex
	var got []any
	fired := make(chan struct{}, 1)
	tt.SetObserver(func(v any, err error) {
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	if !tt.Execute() {
		t.Fatal("execute should start")
	}
	if tt.Execute() {
		t.Fatal("second execute returns false")
	}
	if !tt.RunningQ() {
		t.Fatal("should be running")
	}
	<-fired
	if !tt.Shutdown() {
		t.Fatal("shutdown of running task returns true")
	}
	if tt.Shutdown() {
		t.Fatal("shutdown of stopped task returns false")
	}
	if tt.RunningQ() {
		t.Fatal("should not be running after shutdown")
	}
	if tt.TimesRun() < 1 {
		t.Fatalf("times run: %d", tt.TimesRun())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) < 1 || got[0] != "tick" {
		t.Fatalf("observer values: %v", got)
	}
}

func TestTimerTaskNoObserverAndPanic(t *testing.T) {
	// No observer registered: the loop's obs==nil path runs.
	silent := NewTimerTask(time.Millisecond, func() (any, error) { return 1, nil })
	silent.Execute()
	waitUntil(t, func() bool { return silent.TimesRun() >= 1 })
	silent.Shutdown()

	// A panicking body surfaces ErrExecution to the observer.
	tt := NewTimerTask(time.Millisecond, func() (any, error) { panic("boom") })
	errc := make(chan error, 1)
	tt.SetObserver(func(v any, err error) {
		select {
		case errc <- err:
		default:
		}
	})
	tt.Execute()
	if err := <-errc; err != ErrExecution {
		t.Fatalf("panic should report ErrExecution, got %v", err)
	}
	tt.Shutdown()
}
