package concurrent

import (
	"sync"
	"testing"
	"time"
)

func TestCountDownLatch(t *testing.T) {
	l := NewCountDownLatch(2)
	if l.Count() != 2 {
		t.Fatalf("count: %d", l.Count())
	}
	// Timeout while still latched.
	if l.Wait(20 * time.Millisecond) {
		t.Fatal("should time out")
	}
	released := NewCountDownLatch(1)
	go func() {
		l.Wait(2 * time.Second)
		released.CountDown()
	}()
	l.CountDown()
	if l.Count() != 1 {
		t.Fatalf("count after one: %d", l.Count())
	}
	l.CountDown()
	if !released.Wait(2 * time.Second) {
		t.Fatal("waiter not released")
	}
	if l.Count() != 0 {
		t.Fatal("count zero")
	}
	l.CountDown() // below zero is a no-op
	if l.Count() != 0 {
		t.Fatal("count stays zero")
	}
	if !l.Wait(NoTimeout) {
		t.Fatal("already-released wait returns true")
	}
}

func TestCountDownLatchZeroStart(t *testing.T) {
	l := NewCountDownLatch(0)
	if !l.Wait(NoTimeout) {
		t.Fatal("zero-count latch starts released")
	}
	l.CountDown() // no-op
}

func TestSemaphore(t *testing.T) {
	s := NewSemaphore(-1) // clamps to 0
	if s.AvailablePermits() != 0 {
		t.Fatal("negative clamps to zero")
	}
	s = NewSemaphore(2)
	if s.AvailablePermits() != 2 {
		t.Fatal("available")
	}
	if !s.TryAcquire(2) {
		t.Fatal("try_acquire success")
	}
	if s.AvailablePermits() != 0 {
		t.Fatal("drained by try_acquire")
	}
	if s.TryAcquire(1) {
		t.Fatal("try_acquire should fail when empty")
	}
	s.Release(2)
	if s.TryAcquire(3) { // partial then rollback
		t.Fatal("try_acquire more than available should fail")
	}
	if s.AvailablePermits() != 2 {
		t.Fatal("rolled back to 2")
	}
}

func TestSemaphoreBlockingAcquire(t *testing.T) {
	s := NewSemaphore(0)
	acquired := NewCountDownLatch(1)
	go func() {
		s.Acquire(1) // blocks until Release
		acquired.CountDown()
	}()
	s.Release(1)
	if !acquired.Wait(2 * time.Second) {
		t.Fatal("blocking acquire not released")
	}
}

func TestSemaphoreDrainAndReduce(t *testing.T) {
	s := NewSemaphore(5)
	if n := s.Drain(); n != 5 {
		t.Fatalf("drain: %d", n)
	}
	if s.AvailablePermits() != 0 {
		t.Fatal("empty after drain")
	}
	s.Release(4)
	s.ReducePermits(2) // enough available
	if s.AvailablePermits() != 2 {
		t.Fatalf("after reduce: %d", s.AvailablePermits())
	}
	s.ReducePermits(5) // more than available -> stops at empty
	if s.AvailablePermits() != 0 {
		t.Fatalf("after over-reduce: %d", s.AvailablePermits())
	}
}

func TestSemaphoreAsMutex(t *testing.T) {
	s := NewSemaphore(1)
	var counter int
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Acquire(1)
			counter++
			s.Release(1)
		}()
	}
	wg.Wait()
	if counter != 50 {
		t.Fatalf("counter: %d", counter)
	}
}

func TestCyclicBarrierTrip(t *testing.T) {
	b := NewCyclicBarrier(2)
	if b.Parties() != 2 {
		t.Fatalf("parties: %d", b.Parties())
	}
	if b.NumberWaiting() != 0 || b.BrokenQ() {
		t.Fatal("initial state")
	}
	result := make(chan bool, 1)
	go func() { result <- b.Wait(2 * time.Second) }()
	if !b.Wait(2 * time.Second) {
		t.Fatal("last party should return true")
	}
	if !<-result {
		t.Fatal("waiting party should return true")
	}
	// Reusable: a second cycle works.
	go func() { result <- b.Wait(2 * time.Second) }()
	if !b.Wait(2 * time.Second) {
		t.Fatal("second cycle last party")
	}
	if !<-result {
		t.Fatal("second cycle waiting party")
	}
}

func TestCyclicBarrierAction(t *testing.T) {
	ran := 0
	// parties=1 with action: a single Wait trips immediately and runs the action.
	b := NewCyclicBarrierWithAction(1, func() { ran++ })
	if !b.Wait(2 * time.Second) {
		t.Fatal("single-party barrier trips")
	}
	if ran != 1 {
		t.Fatalf("action runs once: %d", ran)
	}
	// parties=1 without action (nil-action branch).
	b2 := NewCyclicBarrier(1)
	if !b2.Wait(2 * time.Second) {
		t.Fatal("single-party no-action trips")
	}
}

func TestCyclicBarrierClamp(t *testing.T) {
	b := NewCyclicBarrierWithAction(0, nil) // clamps to 1
	if b.Parties() != 1 {
		t.Fatalf("clamped parties: %d", b.Parties())
	}
}

func TestCyclicBarrierTimeoutBreaks(t *testing.T) {
	b := NewCyclicBarrier(2)
	// Only one party arrives; it times out and breaks the barrier.
	if b.Wait(20 * time.Millisecond) {
		t.Fatal("timeout should return false")
	}
	if !b.BrokenQ() {
		t.Fatal("barrier should be broken after timeout")
	}
	// A subsequent Wait sees a broken generation and returns false immediately.
	if b.Wait(2 * time.Second) {
		t.Fatal("broken barrier returns false")
	}
	// Reset on an already-broken barrier installs a fresh, usable generation.
	b.Reset()
	if b.BrokenQ() {
		t.Fatal("reset clears broken")
	}
	// The fresh generation trips normally on a full cycle.
	result := make(chan bool, 1)
	go func() { result <- b.Wait(2 * time.Second) }()
	if !b.Wait(2*time.Second) || !<-result {
		t.Fatal("barrier usable after reset")
	}
}

func TestCyclicBarrierResetReleasesWaiter(t *testing.T) {
	b := NewCyclicBarrier(2)
	result := make(chan bool, 1)
	entered := NewCountDownLatch(1)
	go func() {
		entered.CountDown()
		result <- b.Wait(2 * time.Second) // waits, then Reset breaks it
	}()
	entered.Wait(2 * time.Second)
	// Reset while a party is waiting: it is released with a false (broken) result.
	// Retry Reset until the waiter has actually parked, to make the broken-release
	// branch deterministic without sleeping.
	deadline := time.Now().Add(2 * time.Second)
	for b.NumberWaiting() == 0 && time.Now().Before(deadline) {
	}
	b.Reset()
	if <-result {
		t.Fatal("reset should release waiter with false")
	}
	if b.BrokenQ() {
		t.Fatal("reset installed a fresh generation")
	}
}
