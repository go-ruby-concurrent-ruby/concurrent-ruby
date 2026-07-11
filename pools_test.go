package concurrent

import (
	"testing"
	"time"
)

func TestCachedThreadPoolRunsConcurrently(t *testing.T) {
	p := NewCachedThreadPool()
	if !p.RunningQ() {
		t.Fatal("new pool is running")
	}
	start := NewCountDownLatch(1)
	done := NewCountDownLatch(3)
	for i := 0; i < 3; i++ {
		if !p.Post(func() { start.Wait(2 * time.Second); done.CountDown() }) {
			t.Fatal("post should accept")
		}
	}
	start.CountDown()
	done.Wait(2 * time.Second)
	p.Shutdown()
	p.Shutdown() // idempotent
	if p.RunningQ() {
		t.Fatal("shut-down pool is not running")
	}
	if !p.WaitForTermination(2 * time.Second) {
		t.Fatal("should terminate")
	}
	if p.CompletedTaskCount() != 3 {
		t.Fatalf("completed: %d", p.CompletedTaskCount())
	}
	if p.LargestPoolSize() < 1 {
		t.Fatalf("largest: %d", p.LargestPoolSize())
	}
	if p.Post(func() {}) {
		t.Fatal("post after shutdown returns false")
	}
}

func TestCachedThreadPoolShutdownDrainsRunning(t *testing.T) {
	p := NewCachedThreadPool()
	running := NewCountDownLatch(1)
	release := NewCountDownLatch(1)
	p.Post(func() { running.CountDown(); release.Wait(2 * time.Second) })
	running.Wait(2 * time.Second) // task is active

	p.Shutdown() // active != 0: does not terminate yet
	if p.WaitForTermination(20 * time.Millisecond) {
		t.Fatal("should not terminate while a task runs")
	}
	release.CountDown()
	if !p.WaitForTermination(2 * time.Second) {
		t.Fatal("last task should terminate the pool")
	}
}

func TestCachedThreadPoolPanicIsolatedAndLargest(t *testing.T) {
	p := NewCachedThreadPool()
	done := NewCountDownLatch(1)
	p.Post(func() { defer done.CountDown(); panic("isolated") })
	done.Wait(2 * time.Second)

	// A sequential second task: active (1) is not greater than the peak (1),
	// exercising the "no new peak" path.
	done2 := NewCountDownLatch(1)
	p.Post(func() { done2.CountDown() })
	done2.Wait(2 * time.Second)

	p.Shutdown()
	p.WaitForTermination(2 * time.Second)
	if p.LargestPoolSize() != 1 {
		t.Fatalf("largest: %d", p.LargestPoolSize())
	}
	if p.CompletedTaskCount() != 2 {
		t.Fatalf("completed (panic still counts): %d", p.CompletedTaskCount())
	}
}

func TestFastPoolSize(t *testing.T) {
	if fastPoolSize(1) != 2 {
		t.Fatal("floor of two")
	}
	if fastPoolSize(8) != 8 {
		t.Fatal("cpu count")
	}
}

func TestGlobalsAndSingleThread(t *testing.T) {
	done := NewCountDownLatch(2)
	if !GlobalIOExecutor().Post(func() { done.CountDown() }) {
		t.Fatal("io executor post")
	}
	if !GlobalFastExecutor().Post(func() { done.CountDown() }) {
		t.Fatal("fast executor post")
	}
	ran := NewAtomicBoolean(false)
	GlobalImmediateExecutor().Post(func() { ran.MakeTrue() })
	if !ran.Value() {
		t.Fatal("immediate executor runs inline")
	}
	done.Wait(2 * time.Second)
	// Singletons are stable across calls.
	if GlobalIOExecutor() != GlobalIOExecutor() || GlobalFastExecutor() != GlobalFastExecutor() {
		t.Fatal("global executors are singletons")
	}

	se := NewSingleThreadExecutor()
	defer func() { se.Shutdown(); se.WaitForTermination(2 * time.Second) }()
	d := NewCountDownLatch(1)
	se.Post(func() { d.CountDown() })
	if !d.Wait(2 * time.Second) {
		t.Fatal("single-thread executor runs the task")
	}
}
