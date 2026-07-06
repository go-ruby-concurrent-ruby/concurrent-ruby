// Package concurrent is a pure-Go (no cgo), MRI-4.0.5-faithful model of the
// core of Ruby's concurrent-ruby gem — the Concurrent:: toolbox of thread-safe
// data structures, atomics, futures, thread pools, and synchronization
// primitives.
//
// It reproduces the observable behaviour and vocabulary of the gem
// (Concurrent::AtomicReference, Concurrent::Map, Concurrent::Future,
// Concurrent::ThreadPoolExecutor, Concurrent::CountDownLatch, …) without any
// Ruby runtime, targeting a later rbgo binding where `require "concurrent"`
// maps Ruby blocks and values onto the Go func seams exposed here.
//
// Every point where the gem takes a Ruby block — a Future body, a Promise
// continuation, a Map#compute_if_absent computation, an Enumerable iteration —
// is expressed as a Go func parameter so the binding can thread the host VM's
// callables through unchanged. Every point where the gem runs work on an
// executor takes an Executor, so the binding chooses where callbacks run.
//
// The package is CGO-free, dependency-free, and safe under the race detector.
package concurrent

import "errors"

// State is the lifecycle state of a Future, Promise, or other IVar-like value,
// mirroring the Ruby symbols :pending, :fulfilled, and :rejected.
type State string

const (
	// Pending is the initial state, before a value or reason is assigned.
	Pending State = "pending"
	// Fulfilled means a value was assigned successfully.
	Fulfilled State = "fulfilled"
	// Rejected means the computation failed and a reason (error) was assigned.
	Rejected State = "rejected"
)

// Package-level sentinel errors mirroring concurrent-ruby's exception classes.
// A binding maps each to the corresponding Ruby exception when raising into the
// host VM.
var (
	// ErrMultipleAssignment mirrors Concurrent::MultipleAssignmentError: an
	// IVar/Future/Promise may only be completed once.
	ErrMultipleAssignment = errors.New("concurrent: multiple assignment")

	// ErrRejectedExecution mirrors Concurrent::RejectedExecutionError: a task
	// was posted to an executor that could not accept it (shut down, or its
	// bounded queue is saturated).
	ErrRejectedExecution = errors.New("concurrent: rejected execution")

	// ErrTimeout mirrors Concurrent::TimeoutError.
	ErrTimeout = errors.New("concurrent: operation timed out")

	// ErrBrokenBarrier mirrors the broken state of a CyclicBarrier
	// (Java's BrokenBarrierException), reached on timeout or reset.
	ErrBrokenBarrier = errors.New("concurrent: broken barrier")
)
