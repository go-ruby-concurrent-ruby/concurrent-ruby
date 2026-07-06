package concurrent

import (
	"sync"
	"time"
)

// CountDownLatch models Concurrent::CountDownLatch: a one-shot gate that
// releases every waiter once the count reaches zero.
type CountDownLatch struct {
	mu    sync.Mutex
	count int
	done  chan struct{}
}

// NewCountDownLatch returns a latch with the given initial count. A count <= 0
// starts already released.
func NewCountDownLatch(count int) *CountDownLatch {
	l := &CountDownLatch{count: count, done: make(chan struct{})}
	if count <= 0 {
		l.count = 0
		close(l.done)
	}
	return l
}

// CountDown decrements the count, releasing all waiters when it reaches zero.
// Counting below zero is a no-op (Ruby #count_down).
func (l *CountDownLatch) CountDown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count == 0 {
		return
	}
	l.count--
	if l.count == 0 {
		close(l.done)
	}
}

// Count returns the current count (Ruby #count).
func (l *CountDownLatch) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// Wait blocks until the count reaches zero (returning true) or timeout elapses
// (returning false) (Ruby #wait). A negative timeout waits forever.
func (l *CountDownLatch) Wait(timeout time.Duration) bool {
	return waitDone(l.done, timeout)
}

// maxSemaphorePermits bounds the token channel. struct{} elements make the
// channel buffer allocation-free regardless of capacity, so a large bound lets
// Release grow the permit count without ever blocking.
const maxSemaphorePermits = 1 << 30

// Semaphore models Concurrent::Semaphore: a counting semaphore. Permits are
// held as tokens in a buffered channel, so Acquire blocks naturally when none
// are available and the race detector stays satisfied.
type Semaphore struct {
	tokens chan struct{}
}

// NewSemaphore returns a Semaphore with the given number of permits. A negative
// count is treated as zero.
func NewSemaphore(permits int) *Semaphore {
	if permits < 0 {
		permits = 0
	}
	s := &Semaphore{tokens: make(chan struct{}, maxSemaphorePermits)}
	for i := 0; i < permits; i++ {
		s.tokens <- struct{}{}
	}
	return s
}

// Acquire blocks until n permits are available, then takes them (Ruby #acquire).
func (s *Semaphore) Acquire(n int) {
	for i := 0; i < n; i++ {
		<-s.tokens
	}
}

// Release returns n permits (Ruby #release).
func (s *Semaphore) Release(n int) {
	for i := 0; i < n; i++ {
		s.tokens <- struct{}{}
	}
}

// TryAcquire takes n permits without blocking, reporting whether it succeeded;
// on failure it returns any partially taken permits (Ruby #try_acquire).
func (s *Semaphore) TryAcquire(n int) bool {
	got := 0
	for got < n {
		select {
		case <-s.tokens:
			got++
		default:
			s.Release(got)
			return false
		}
	}
	return true
}

// AvailablePermits returns the number of permits currently available
// (Ruby #available_permits).
func (s *Semaphore) AvailablePermits() int { return len(s.tokens) }

// Drain takes all available permits and returns how many it took
// (Ruby #drain_permits).
func (s *Semaphore) Drain() int {
	c := 0
	for {
		select {
		case <-s.tokens:
			c++
		default:
			return c
		}
	}
}

// ReducePermits removes up to n available permits without blocking
// (Ruby #reduce_permits).
func (s *Semaphore) ReducePermits(n int) {
	for i := 0; i < n; i++ {
		select {
		case <-s.tokens:
		default:
			return
		}
	}
}

// barrierGeneration is one cycle of a CyclicBarrier. Its done channel releases
// the waiters of that cycle exactly once, guarded by a sync.Once so a trip and
// a concurrent break can never double-close it.
type barrierGeneration struct {
	done   chan struct{}
	broken bool
	once   sync.Once
}

func (g *barrierGeneration) release() { g.once.Do(func() { close(g.done) }) }

// CyclicBarrier models Concurrent::CyclicBarrier: a reusable barrier that
// releases a fixed number of parties once they have all arrived, optionally
// running a barrier action on the tripping party.
type CyclicBarrier struct {
	parties int
	action  func()
	mu      sync.Mutex
	count   int
	gen     *barrierGeneration
}

// NewCyclicBarrier returns a barrier for the given number of parties. A count
// < 1 is clamped to 1.
func NewCyclicBarrier(parties int) *CyclicBarrier {
	return NewCyclicBarrierWithAction(parties, nil)
}

// NewCyclicBarrierWithAction returns a barrier that runs action once, on the
// party that trips it, before the parties are released (Ruby's barrier action).
func NewCyclicBarrierWithAction(parties int, action func()) *CyclicBarrier {
	if parties < 1 {
		parties = 1
	}
	return &CyclicBarrier{
		parties: parties,
		action:  action,
		gen:     &barrierGeneration{done: make(chan struct{})},
	}
}

// Parties returns the number of parties required to trip the barrier
// (Ruby #parties).
func (b *CyclicBarrier) Parties() int { return b.parties }

// NumberWaiting returns how many parties are currently waiting (Ruby #number_waiting).
func (b *CyclicBarrier) NumberWaiting() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// BrokenQ reports whether the current generation is broken (Ruby #broken?).
func (b *CyclicBarrier) BrokenQ() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gen.broken
}

// Wait blocks until all parties arrive (returning true), or until timeout
// elapses or the barrier breaks (returning false) (Ruby #wait). The tripping
// party runs the barrier action and starts a fresh generation. A negative
// timeout waits forever.
func (b *CyclicBarrier) Wait(timeout time.Duration) bool {
	b.mu.Lock()
	g := b.gen
	if g.broken {
		b.mu.Unlock()
		return false
	}
	b.count++
	if b.count == b.parties {
		if b.action != nil {
			b.action()
		}
		b.nextGeneration()
		b.mu.Unlock()
		return true
	}
	b.mu.Unlock()

	if waitDone(g.done, timeout) {
		b.mu.Lock()
		broken := g.broken
		b.mu.Unlock()
		return !broken
	}

	// Timed out before the barrier tripped: break this generation so any other
	// waiters are released with a false result.
	b.mu.Lock()
	g.broken = true
	g.release()
	b.mu.Unlock()
	return false
}

// nextGeneration releases the current generation's waiters and installs a fresh
// one. The caller holds b.mu.
func (b *CyclicBarrier) nextGeneration() {
	b.gen.release()
	b.gen = &barrierGeneration{done: make(chan struct{})}
	b.count = 0
}

// Reset breaks the current generation, releasing any waiters with a false
// result, and starts a fresh one (Ruby #reset).
func (b *CyclicBarrier) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.gen.broken {
		b.gen.broken = true
		b.gen.release()
	}
	b.gen = &barrierGeneration{done: make(chan struct{})}
	b.count = 0
}
