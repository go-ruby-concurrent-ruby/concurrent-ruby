package concurrent

import (
	"sync"
	"time"
)

// ScheduledTask models Concurrent::ScheduledTask: a one-shot computation that
// runs after a delay and whose result is retrieved like a Future. Until it
// fires it can be cancelled.
type ScheduledTask struct {
	ev    *event
	exec  Executor
	fn    func() (any, error)
	timer *time.Timer

	mu        sync.Mutex
	cancelled bool
	started   bool
}

// ScheduledTaskExecute schedules fn to run on exec after delay and returns the
// task (Ruby Concurrent::ScheduledTask.execute(delay) { ... }). A non-negative
// delay is honoured; the task fires once, fulfilling with fn's value or
// rejecting with its error.
func ScheduledTaskExecute(exec Executor, delay time.Duration, fn func() (any, error)) *ScheduledTask {
	if delay < 0 {
		delay = 0
	}
	t := &ScheduledTask{ev: newEvent(), exec: exec, fn: fn}
	t.timer = time.AfterFunc(delay, t.fire)
	return t
}

// fire is invoked by the timer; it posts the body to the executor unless the
// task was cancelled first.
func (t *ScheduledTask) fire() {
	t.mu.Lock()
	if t.cancelled {
		t.mu.Unlock()
		return
	}
	t.started = true
	t.mu.Unlock()
	t.exec.Post(func() {
		v, err := t.fn()
		if err != nil {
			_ = t.ev.settle(Rejected, nil, err)
			return
		}
		_ = t.ev.settle(Fulfilled, v, nil)
	})
}

// Cancel prevents a not-yet-started task from running, reporting whether it was
// cancelled in time (Ruby #cancel). A task that has already started or
// completed cannot be cancelled.
func (t *ScheduledTask) Cancel() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started || t.cancelled {
		return false
	}
	t.cancelled = true
	t.timer.Stop()
	_ = t.ev.settle(Rejected, nil, ErrCancelled)
	return true
}

// Wait blocks up to timeout for the task to complete and returns it (Ruby
// #wait). A negative timeout waits forever.
func (t *ScheduledTask) Wait(timeout time.Duration) *ScheduledTask {
	t.ev.get(timeout)
	return t
}

// Value blocks up to timeout and returns the task's value, or nil if it timed
// out, was rejected, or was cancelled (Ruby #value).
func (t *ScheduledTask) Value(timeout time.Duration) any {
	_, v, _ := t.ev.get(timeout)
	return v
}

// ValueBang blocks up to timeout and returns the value, or the rejection reason
// as an error (Ruby #value!).
func (t *ScheduledTask) ValueBang(timeout time.Duration) (any, error) {
	st, v, r := t.ev.get(timeout)
	if st == Rejected {
		return nil, r
	}
	return v, nil
}

// State returns the lifecycle state (Ruby #state).
func (t *ScheduledTask) State() State { return t.ev.snapshotState() }

// Reason returns the rejection reason, or nil (Ruby #reason).
func (t *ScheduledTask) Reason() error { return t.ev.snapshotReason() }

// PendingQ reports whether the task has not completed yet (Ruby #pending?).
func (t *ScheduledTask) PendingQ() bool { return t.ev.snapshotState() == Pending }

// FulfilledQ reports whether the task completed successfully (Ruby #fulfilled?).
func (t *ScheduledTask) FulfilledQ() bool { return t.ev.snapshotState() == Fulfilled }

// RejectedQ reports whether the task failed or was cancelled (Ruby #rejected?).
func (t *ScheduledTask) RejectedQ() bool { return t.ev.snapshotState() == Rejected }

// CancelledQ reports whether the task was cancelled (Ruby #cancelled?).
func (t *ScheduledTask) CancelledQ() bool {
	return t.ev.snapshotReason() == ErrCancelled
}

// TimerTask models Concurrent::TimerTask: a task run repeatedly on a fixed
// interval until shut down. Each run's outcome is reported to an optional
// observer.
type TimerTask struct {
	interval time.Duration
	fn       func() (any, error)
	observer func(value any, reason error)

	mu       sync.Mutex
	running  bool
	stop     chan struct{}
	done     chan struct{}
	runCount int
}

// NewTimerTask returns an unstarted TimerTask that will run fn every interval
// once executed (Ruby Concurrent::TimerTask.new(execution_interval: ...) {}).
func NewTimerTask(interval time.Duration, fn func() (any, error)) *TimerTask {
	if interval <= 0 {
		interval = time.Millisecond
	}
	return &TimerTask{interval: interval, fn: fn}
}

// SetObserver registers fn to receive each run's (value, reason); a run that
// returns an error reports (nil, err) (Ruby #add_observer). It must be set
// before Execute.
func (t *TimerTask) SetObserver(fn func(value any, reason error)) {
	t.mu.Lock()
	t.observer = fn
	t.mu.Unlock()
}

// ExecutionInterval returns the interval between runs (Ruby #execution_interval).
func (t *TimerTask) ExecutionInterval() time.Duration { return t.interval }

// Execute starts the timer loop, reporting whether it started (false if it was
// already running) (Ruby #execute). The first run happens after one interval.
func (t *TimerTask) Execute() bool {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return false
	}
	t.running = true
	t.stop = make(chan struct{})
	t.done = make(chan struct{})
	stop, done := t.stop, t.done
	t.mu.Unlock()
	go t.loop(stop, done)
	return true
}

func (t *TimerTask) loop(stop, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			v, err := t.runOnce()
			t.mu.Lock()
			obs := t.observer
			t.mu.Unlock()
			if obs != nil {
				obs(v, err)
			}
		}
	}
}

// runOnce invokes the body, isolating a panicking body as a rejection reason.
func (t *TimerTask) runOnce() (v any, err error) {
	defer func() {
		if r := recover(); r != nil {
			v, err = nil, ErrExecution
		}
		t.mu.Lock()
		t.runCount++
		t.mu.Unlock()
	}()
	return t.fn()
}

// RunningQ reports whether the timer loop is active (Ruby #running?).
func (t *TimerTask) RunningQ() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.running
}

// TimesRun returns how many times the body has run (test/observability aid).
func (t *TimerTask) TimesRun() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runCount
}

// Shutdown stops the timer loop and blocks until it has exited, reporting
// whether it was running (Ruby #shutdown). It is idempotent.
func (t *TimerTask) Shutdown() bool {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return false
	}
	t.running = false
	close(t.stop)
	done := t.done
	t.mu.Unlock()
	<-done
	return true
}
