package concurrent

import (
	"sync"
	"sync/atomic"
	"time"
)

// defaultQueueCap is the bounded-queue capacity used by FixedThreadPool, which
// in the gem defaults to an effectively unbounded queue; we use a generous
// bound so Post rarely rejects while keeping the queue finite and observable.
const defaultQueueCap = 1 << 16

// Executor is the seam every asynchronous primitive posts work through. It
// mirrors Concurrent::ExecutorService#post: a task is a niladic callable, and
// Post reports whether the executor accepted it. A binding supplies the Ruby
// block wrapped as a Go func.
type Executor interface {
	Post(task func()) bool
}

// ImmediateExecutor models Concurrent::ImmediateExecutor: it runs each posted
// task synchronously on the caller's goroutine. It makes Future/Promise
// resolution fully deterministic and is the default executor for Promise.
type ImmediateExecutor struct{}

// Post runs task inline and always accepts it.
func (ImmediateExecutor) Post(task func()) bool {
	task()
	return true
}

// ThreadPoolExecutor models Concurrent::ThreadPoolExecutor / FixedThreadPool: a
// fixed set of worker goroutines draining a bounded FIFO queue. It supports
// graceful shutdown, termination waiting, and task accounting.
type ThreadPoolExecutor struct {
	tasks      chan func()
	wg         sync.WaitGroup
	terminated chan struct{}
	completed  atomic.Int64

	mu       sync.Mutex
	shutdown bool
}

// NewThreadPoolExecutor returns a pool with the given number of worker
// goroutines and bounded queue capacity. workers < 1 is clamped to 1 and a
// negative queueCap to 0 (a synchronous handoff queue).
func NewThreadPoolExecutor(workers, queueCap int) *ThreadPoolExecutor {
	if workers < 1 {
		workers = 1
	}
	if queueCap < 0 {
		queueCap = 0
	}
	e := &ThreadPoolExecutor{
		tasks:      make(chan func(), queueCap),
		terminated: make(chan struct{}),
	}
	e.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go e.worker()
	}
	go func() {
		e.wg.Wait()
		close(e.terminated)
	}()
	return e
}

// NewFixedThreadPool returns a ThreadPoolExecutor with n workers and the
// default bounded queue (Ruby Concurrent::FixedThreadPool.new(n)).
func NewFixedThreadPool(n int) *ThreadPoolExecutor {
	return NewThreadPoolExecutor(n, defaultQueueCap)
}

func (e *ThreadPoolExecutor) worker() {
	defer e.wg.Done()
	for task := range e.tasks {
		e.run(task)
	}
}

// run executes one task, isolating a panicking task from the worker (the gem
// discards a task's uncaught exception) and counting the completion.
func (e *ThreadPoolExecutor) run(task func()) {
	defer func() {
		_ = recover()
		e.completed.Add(1)
	}()
	task()
}

// Post enqueues task, returning false if the pool is shut down or its queue is
// saturated (the gem's :abort fallback policy). It never blocks.
func (e *ThreadPoolExecutor) Post(task func()) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return false
	}
	select {
	case e.tasks <- task:
		return true
	default:
		return false
	}
}

// Shutdown initiates a graceful shutdown: no new tasks are accepted, already
// queued tasks still run, and workers exit once the queue drains. It is
// idempotent (Ruby #shutdown).
func (e *ThreadPoolExecutor) Shutdown() {
	e.mu.Lock()
	if e.shutdown {
		e.mu.Unlock()
		return
	}
	e.shutdown = true
	close(e.tasks)
	e.mu.Unlock()
}

// WaitForTermination blocks until every worker has exited, returning true, or
// until timeout elapses, returning false (Ruby #wait_for_termination). A
// negative timeout waits forever.
func (e *ThreadPoolExecutor) WaitForTermination(timeout time.Duration) bool {
	return waitDone(e.terminated, timeout)
}

// QueueLength returns the number of tasks currently queued and not yet picked
// up by a worker (Ruby #queue_length).
func (e *ThreadPoolExecutor) QueueLength() int { return len(e.tasks) }

// CompletedTaskCount returns the number of tasks that have finished running
// (Ruby #completed_task_count).
func (e *ThreadPoolExecutor) CompletedTaskCount() int64 { return e.completed.Load() }

// RunningQ reports whether the pool is still accepting tasks (Ruby #running?).
func (e *ThreadPoolExecutor) RunningQ() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.shutdown
}

// ShutdownQ reports whether the pool has fully terminated (Ruby #shutdown?).
func (e *ThreadPoolExecutor) ShutdownQ() bool {
	select {
	case <-e.terminated:
		return true
	default:
		return false
	}
}
