package concurrent

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// CachedThreadPool models Concurrent::CachedThreadPool: an unbounded pool that
// runs each posted task on its own goroutine, so it never rejects a task for a
// full queue (only for being shut down). It is the executor of choice for
// blocking, IO-bound work.
type CachedThreadPool struct {
	mu         sync.Mutex
	shutdown   bool
	active     int
	terminated chan struct{}
	termOnce   sync.Once
	completed  atomic.Int64
	largest    atomic.Int64
}

// NewCachedThreadPool returns a running CachedThreadPool (Ruby
// Concurrent::CachedThreadPool.new).
func NewCachedThreadPool() *CachedThreadPool {
	return &CachedThreadPool{terminated: make(chan struct{})}
}

// Post runs task on a fresh goroutine, returning false only if the pool is shut
// down (Ruby #post). It never blocks and never rejects for capacity.
func (e *CachedThreadPool) Post(task func()) bool {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return false
	}
	e.active++
	if int64(e.active) > e.largest.Load() {
		e.largest.Store(int64(e.active))
	}
	e.mu.Unlock()

	go func() {
		defer e.finishTask()
		task()
	}()
	return true
}

// finishTask records a completion and, if the pool is draining its last task,
// signals termination.
func (e *CachedThreadPool) finishTask() {
	_ = recover()
	e.completed.Add(1)
	e.mu.Lock()
	e.active--
	if e.shutdown && e.active == 0 {
		e.closeTerminated()
	}
	e.mu.Unlock()
}

// closeTerminated closes the terminated channel exactly once; the caller holds
// e.mu.
func (e *CachedThreadPool) closeTerminated() {
	e.termOnce.Do(func() { close(e.terminated) })
}

// Shutdown stops accepting new tasks; already-running tasks finish. It is
// idempotent (Ruby #shutdown).
func (e *CachedThreadPool) Shutdown() {
	e.mu.Lock()
	if !e.shutdown {
		e.shutdown = true
		if e.active == 0 {
			e.closeTerminated()
		}
	}
	e.mu.Unlock()
}

// WaitForTermination blocks until all running tasks finish (returning true) or
// timeout elapses (returning false) (Ruby #wait_for_termination). A negative
// timeout waits forever.
func (e *CachedThreadPool) WaitForTermination(timeout time.Duration) bool {
	return waitDone(e.terminated, timeout)
}

// CompletedTaskCount returns the number of tasks that have finished (Ruby
// #completed_task_count).
func (e *CachedThreadPool) CompletedTaskCount() int64 { return e.completed.Load() }

// LargestPoolSize returns the peak number of tasks that ran concurrently (Ruby
// #largest_length).
func (e *CachedThreadPool) LargestPoolSize() int64 { return e.largest.Load() }

// RunningQ reports whether the pool still accepts tasks (Ruby #running?).
func (e *CachedThreadPool) RunningQ() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.shutdown
}

// NewSingleThreadExecutor returns an executor backed by exactly one worker,
// running tasks in submission order (Ruby Concurrent::SingleThreadExecutor.new).
func NewSingleThreadExecutor() *ThreadPoolExecutor {
	return NewThreadPoolExecutor(1, defaultQueueCap)
}

// Global executors mirror Concurrent.global_io_executor,
// global_fast_executor, and global_immediate_executor: process-wide singletons
// the gem's higher-level abstractions default to. They are never shut down.
var (
	globalIOOnce   sync.Once
	globalIO       *CachedThreadPool
	globalFastOnce sync.Once
	globalFast     *ThreadPoolExecutor
)

// GlobalIOExecutor returns the shared unbounded IO executor
// (Concurrent.global_io_executor). It is a process-wide singleton.
func GlobalIOExecutor() Executor {
	globalIOOnce.Do(func() { globalIO = NewCachedThreadPool() })
	return globalIO
}

// GlobalFastExecutor returns the shared fixed executor sized to the CPU count,
// for short non-blocking tasks (Concurrent.global_fast_executor). It is a
// process-wide singleton.
func GlobalFastExecutor() Executor {
	globalFastOnce.Do(func() {
		globalFast = NewThreadPoolExecutor(fastPoolSize(runtime.NumCPU()), defaultQueueCap)
	})
	return globalFast
}

// fastPoolSize sizes the global fast executor to the CPU count, with a floor of
// two workers so a single-core host still runs callbacks concurrently.
func fastPoolSize(numCPU int) int {
	if numCPU < 2 {
		return 2
	}
	return numCPU
}

// GlobalImmediateExecutor returns an executor that runs tasks inline on the
// caller (Concurrent.global_immediate_executor).
func GlobalImmediateExecutor() Executor { return ImmediateExecutor{} }
