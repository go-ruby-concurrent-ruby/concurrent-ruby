package concurrent

import "sync/atomic"

// This file completes the composable Promise surface (Concurrent::Promise's
// rescue/flat_map/zip) and the Concurrent::Promises factory functions
// (Promises.future, .fulfilled_future, .rejected_future, .resolvable_future,
// .zip), all built on the same write-once event core as Promise/Future.

// Rescue returns a child Promise that recovers from this promise's rejection:
// if this promise is fulfilled, its value passes through unchanged; if it is
// rejected, handler runs with the reason and its (value, error) resolves the
// child — a nil error fulfils the child with the recovered value, a non-nil
// error rejects it (Ruby #rescue / #catch / #on_error).
func (p *Promise) Rescue(handler func(reason error) (any, error)) *Promise {
	child := &Promise{ev: newEvent(), exec: p.exec}
	p.ev.addObserver(func(st State, v any, r error) {
		p.exec.Post(func() {
			if st != Rejected {
				_ = child.ev.settle(Fulfilled, v, nil)
				return
			}
			nv, err := handler(r)
			if err != nil {
				_ = child.ev.settle(Rejected, nil, err)
				return
			}
			_ = child.ev.settle(Fulfilled, nv, nil)
		})
	})
	return child
}

// Chain returns a child Promise resolved by fn, which runs on any outcome and
// receives this promise's full result (state, value, reason). fn's (value,
// error) resolves the child (Ruby Promises::Future#chain, which yields on both
// fulfilment and rejection).
func (p *Promise) Chain(fn func(state State, value any, reason error) (any, error)) *Promise {
	child := &Promise{ev: newEvent(), exec: p.exec}
	p.ev.addObserver(func(st State, v any, r error) {
		p.exec.Post(func() {
			nv, err := fn(st, v, r)
			if err != nil {
				_ = child.ev.settle(Rejected, nil, err)
				return
			}
			_ = child.ev.settle(Fulfilled, nv, nil)
		})
	})
	return child
}

// FlatMap returns a child Promise that adopts the resolution of the inner
// Promise returned by fn(value). A rejection of this promise propagates to the
// child without calling fn (Ruby Promises::Future#flat_map).
func (p *Promise) FlatMap(fn func(value any) *Promise) *Promise {
	child := &Promise{ev: newEvent(), exec: p.exec}
	p.ev.addObserver(func(st State, v any, r error) {
		if st == Rejected {
			p.exec.Post(func() { _ = child.ev.settle(Rejected, nil, r) })
			return
		}
		p.exec.Post(func() {
			inner := fn(v)
			inner.ev.addObserver(func(ist State, iv any, ir error) {
				if ist == Rejected {
					_ = child.ev.settle(Rejected, nil, ir)
					return
				}
				_ = child.ev.settle(Fulfilled, iv, nil)
			})
		})
	})
	return child
}

// PromiseExecute posts fn to exec and returns a Promise for its result (Ruby
// Concurrent::Promise.execute { ... }). fn's error rejects the promise.
func PromiseExecute(exec Executor, fn func() (any, error)) *Promise {
	p := &Promise{ev: newEvent(), exec: exec}
	exec.Post(func() {
		v, err := fn()
		if err != nil {
			_ = p.ev.settle(Rejected, nil, err)
			return
		}
		_ = p.ev.settle(Fulfilled, v, nil)
	})
	return p
}

// PromiseZip returns a Promise fulfilled with the ordered slice of every input
// promise's value once all are fulfilled, or rejected with the reason of the
// first input to reject (Ruby Concurrent::Promise.zip / Promises.zip). Zipping
// no promises fulfils immediately with an empty slice.
func PromiseZip(ps ...*Promise) *Promise {
	child := &Promise{ev: newEvent(), exec: ImmediateExecutor{}}
	n := len(ps)
	if n == 0 {
		_ = child.ev.settle(Fulfilled, []any{}, nil)
		return child
	}
	values := make([]any, n)
	var remaining atomic.Int32
	remaining.Store(int32(n))
	for i, p := range ps {
		i, p := i, p
		p.ev.addObserver(func(st State, v any, r error) {
			if st == Rejected {
				_ = child.ev.settle(Rejected, nil, r)
				return
			}
			values[i] = v
			if remaining.Add(-1) == 0 {
				_ = child.ev.settle(Fulfilled, values, nil)
			}
		})
	}
	return child
}

// Concurrent::Promises factory functions. -----------------------------------

// PromisesFuture posts fn to exec and returns its Future-like Promise (Ruby
// Concurrent::Promises.future_on(executor) { ... }).
func PromisesFuture(exec Executor, fn func() (any, error)) *Promise {
	return PromiseExecute(exec, fn)
}

// PromisesFulfilledFuture returns an already-fulfilled Promise holding v (Ruby
// Concurrent::Promises.fulfilled_future(v)).
func PromisesFulfilledFuture(v any) *Promise {
	p := NewPromise()
	_ = p.Fulfill(v)
	return p
}

// PromisesRejectedFuture returns an already-rejected Promise with reason err
// (Ruby Concurrent::Promises.rejected_future(err)).
func PromisesRejectedFuture(err error) *Promise {
	p := NewPromise()
	_ = p.Reject(err)
	return p
}

// PromisesResolvableFuture returns an unresolved Promise the caller resolves
// with Fulfill/Reject, its continuations running inline (Ruby
// Concurrent::Promises.resolvable_future).
func PromisesResolvableFuture() *Promise { return NewPromise() }

// PromisesResolvableFutureOn returns an unresolved Promise whose continuations
// run on exec (Ruby Concurrent::Promises.resolvable_future_on(executor)).
func PromisesResolvableFutureOn(exec Executor) *Promise { return NewPromiseOn(exec) }

// PromisesZip is the Concurrent::Promises spelling of PromiseZip.
func PromisesZip(ps ...*Promise) *Promise { return PromiseZip(ps...) }
