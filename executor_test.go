package concurrent

import (
	"testing"
	"time"
)

func TestImmediateExecutor(t *testing.T) {
	ran := false
	if !(ImmediateExecutor{}).Post(func() { ran = true }) {
		t.Fatal("immediate post should accept")
	}
	if !ran {
		t.Fatal("immediate should run inline")
	}
}

func TestThreadPoolBasics(t *testing.T) {
	p := NewFixedThreadPool(3)
	latch := NewCountDownLatch(5)
	for i := 0; i < 5; i++ {
		if !p.Post(func() { latch.CountDown() }) {
			t.Fatal("post rejected")
		}
	}
	if !latch.Wait(2 * time.Second) {
		t.Fatal("tasks did not complete")
	}
	if !p.RunningQ() {
		t.Fatal("should be running")
	}
	if p.ShutdownQ() {
		t.Fatal("should not be shut down yet")
	}
	p.Shutdown()
	p.Shutdown() // idempotent
	if !p.WaitForTermination(2 * time.Second) {
		t.Fatal("did not terminate")
	}
	if p.RunningQ() {
		t.Fatal("should not be running after shutdown")
	}
	if !p.ShutdownQ() {
		t.Fatal("should be shut down")
	}
	if p.CompletedTaskCount() != 5 {
		t.Fatalf("completed: %d", p.CompletedTaskCount())
	}
	// Posting after shutdown is rejected.
	if p.Post(func() {}) {
		t.Fatal("post after shutdown should be rejected")
	}
}

func TestThreadPoolClamps(t *testing.T) {
	// workers < 1 clamps to 1; queueCap < 0 clamps to 0 (synchronous handoff).
	p := NewThreadPoolExecutor(0, -1)
	if !p.RunningQ() {
		t.Fatal("clamped pool should be running")
	}
	// A rendezvous (cap-0) queue accepts only when a worker is parked, so retry
	// until the single worker is ready; then the task runs.
	done := NewCountDownLatch(1)
	for !p.Post(func() { done.CountDown() }) {
	}
	if !done.Wait(2 * time.Second) {
		t.Fatal("task not run")
	}
	p.Shutdown()
	p.WaitForTermination(NoTimeout)
}

func TestThreadPoolQueueFull(t *testing.T) {
	// One worker, queue capacity one. Occupy the worker, fill the queue, then
	// the next post is rejected — deterministically, via a latch that tells us
	// the worker has dequeued and is blocked.
	p := NewThreadPoolExecutor(1, 1)
	entered := NewCountDownLatch(1)
	release := NewCountDownLatch(1)
	if !p.Post(func() { entered.CountDown(); release.Wait(2 * time.Second) }) {
		t.Fatal("first post rejected")
	}
	entered.Wait(2 * time.Second) // worker is now blocked; queue is empty
	if !p.Post(func() {}) {       // fills the capacity-1 queue
		t.Fatal("second post should be queued")
	}
	if p.QueueLength() != 1 {
		t.Fatalf("queue length: %d", p.QueueLength())
	}
	if p.Post(func() {}) { // queue full -> rejected
		t.Fatal("third post should be rejected")
	}
	release.CountDown()
	p.Shutdown()
	if !p.WaitForTermination(2 * time.Second) {
		t.Fatal("did not terminate")
	}
}

func TestThreadPoolPanicIsolation(t *testing.T) {
	p := NewFixedThreadPool(1)
	done := NewCountDownLatch(2)
	p.Post(func() { done.CountDown(); panic("boom") }) // recovered
	p.Post(func() { done.CountDown() })                // worker survives
	if !done.Wait(2 * time.Second) {
		t.Fatal("worker did not survive panic")
	}
	p.Shutdown()
	p.WaitForTermination(2 * time.Second)
	if p.CompletedTaskCount() != 2 {
		t.Fatalf("completed: %d", p.CompletedTaskCount())
	}
}

func TestThreadPoolWaitTimeout(t *testing.T) {
	p := NewFixedThreadPool(1)
	if p.WaitForTermination(20 * time.Millisecond) {
		t.Fatal("running pool should not report terminated")
	}
	p.Shutdown()
	p.WaitForTermination(2 * time.Second)
}
