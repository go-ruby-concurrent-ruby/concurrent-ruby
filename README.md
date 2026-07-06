<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-concurrent-ruby/brand/main/social/go-ruby-concurrent-ruby-concurrent-ruby.png" alt="go-ruby-concurrent-ruby/concurrent-ruby" width="720"></p>

# concurrent-ruby — go-ruby-concurrent-ruby

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-concurrent-ruby.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) reimplementation of the core of Ruby's
[`concurrent-ruby`](https://github.com/ruby-concurrency/concurrent-ruby) gem** —
the `Concurrent::` toolbox of thread-safe data structures, atomics, futures,
thread pools, and synchronization primitives — modelling MRI 4.0.5's observable
behaviour and vocabulary **without any Ruby runtime**.

It is the `concurrent` backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby) (a later
`require "concurrent"` binding), but is a **standalone, reusable** Go module — a
sibling of [go-ruby-set](https://github.com/go-ruby-set/set) and
[go-ruby-bigdecimal](https://github.com/go-ruby-bigdecimal/bigdecimal).

## The block/value seam

Everywhere the gem takes a Ruby **block** — a `Future` body, a `Promise#then`
continuation, `Map#compute_if_absent`, an `each_pair` iteration — this package
takes a Go **func**, so the rbgo binding threads the host VM's callables through
unchanged. Everywhere the gem runs work on an **executor**, the API takes an
`Executor` interface, so the binding chooses where callbacks run:

```go
type Executor interface{ Post(task func()) bool }
```

An `ImmediateExecutor` runs tasks inline (deterministic resolution); a
`ThreadPoolExecutor` / `FixedThreadPool` runs them on a bounded worker pool.

## Install

```sh
go get github.com/go-ruby-concurrent-ruby/concurrent-ruby
```

## Usage

```go
package main

import (
	"fmt"
	"time"

	concurrent "github.com/go-ruby-concurrent-ruby/concurrent-ruby"
)

func main() {
	// Atomics
	n := concurrent.NewAtomicFixnum(0)
	n.Increment(1)
	fmt.Println(n.Value()) // 1

	// Concurrent::Map with compute_if_absent
	m := concurrent.NewMap()
	fmt.Println(m.ComputeIfAbsent("k", func() any { return 42 })) // 42

	// Thread pool + Future
	pool := concurrent.NewFixedThreadPool(4)
	defer func() { pool.Shutdown(); pool.WaitForTermination(concurrent.NoTimeout) }()

	f := concurrent.FutureExecute(pool, func() (any, error) { return 6 * 7, nil })
	fmt.Println(f.Value(time.Second)) // 42

	// Promise chaining
	p := concurrent.NewPromise()
	c := p.Then(func(v any) (any, error) { return v.(int) + 1, nil })
	p.Fulfill(1)
	fmt.Println(c.Value(concurrent.NoTimeout)) // 2

	// Synchronization primitives
	latch := concurrent.NewCountDownLatch(1)
	latch.CountDown()
	fmt.Println(latch.Wait(concurrent.NoTimeout)) // true
}
```

## The `Concurrent::` surface

| Gem class | Go type | Notable methods |
|---|---|---|
| `Concurrent::AtomicReference` | `AtomicReference` | `Get` `Set` `GetAndSet` `CompareAndSet` `Update` |
| `Concurrent::AtomicFixnum` | `AtomicFixnum` | `Value` `Increment` `Decrement` `CompareAndSet` `Update` |
| `Concurrent::AtomicBoolean` | `AtomicBoolean` | `Value` `TrueQ` `FalseQ` `MakeTrue` `MakeFalse` `CompareAndSet` |
| `Concurrent::Map` | `Map` | `Get` `Set` `ComputeIfAbsent` `Compute` `PutIfAbsent` `Delete` `EachPair` `Size` |
| `Concurrent::Array` | `Array` | `Push` `Pop` `Shift` `At` `Set` `DeleteAt` `Each` `ToSlice` |
| `Concurrent::Hash` | `Hash` | `Get` `Set` `Delete` `KeyQ` `EachPair` `Keys` `Values` |
| `Concurrent::Future` | `Future` | `FutureExecute` `Value` `ValueBang` `Wait` `State` `Reason` `PendingQ`/`FulfilledQ`/`RejectedQ` |
| `Concurrent::Promise` | `Promise` | `Fulfill` `Reject` `Then` `Value` `State` `Reason` |
| `Concurrent::ThreadPoolExecutor` / `FixedThreadPool` | `ThreadPoolExecutor` | `Post` `Shutdown` `WaitForTermination` `QueueLength` `CompletedTaskCount` |
| `Concurrent::ImmediateExecutor` | `ImmediateExecutor` | `Post` |
| `Concurrent::CountDownLatch` | `CountDownLatch` | `CountDown` `Count` `Wait` |
| `Concurrent::Semaphore` | `Semaphore` | `Acquire` `Release` `TryAcquire` `AvailablePermits` `Drain` `ReducePermits` |
| `Concurrent::CyclicBarrier` | `CyclicBarrier` | `Wait` `Parties` `NumberWaiting` `BrokenQ` `Reset` |

State symbols map to `State` constants (`Pending`/`Fulfilled`/`Rejected`), and
the gem's exceptions map to package sentinel errors — `ErrMultipleAssignment`
(`Concurrent::MultipleAssignmentError`), `ErrRejectedExecution`, `ErrTimeout`,
`ErrBrokenBarrier` — which the binding re-raises as the corresponding Ruby class.
Ruby predicate methods (`true?`, `fulfilled?`, `broken?`) are spelled with a `Q`
suffix (`TrueQ`, `FulfilledQ`, `BrokenQ`), following the go-ruby naming
convention. Ruby's optional `nil` timeout (wait forever) is the `NoTimeout`
constant.

## Scope

This models a faithful **core**, not the entire gem. Deferred for now: the
higher-level dataflow layer (`Concurrent::Promises`, `dataflow`, `Async`,
`Actor`), `TVar`/software-transactional memory, `Agent`, `TimerTask`/scheduled
executors, `ThreadLocalVar`, `Delay`, `MVar`, `AtomicMarkableReference`, and the
tuple/exchanger primitives. The seams here (the `Executor` interface and the Go
func callbacks) are the foundation those layers would build on.

## Tests & coverage

The suite is deterministic and race-clean: every state transition and error
path is driven with latches and channels rather than `time.Sleep`, so there is
no timing flakiness, and every pool is shut down so no goroutine leaks. The
coverage gate holds at **100%** of statements.

```sh
GOWORK=off CGO_ENABLED=1 go test -race -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

CGO-free, dependency-free, `gofmt` + `go vet` clean, and green across the six
64-bit Go targets (amd64, arm64, riscv64, loong64, ppc64le, s390x — including
big-endian) and three OSes (Linux, macOS, Windows).

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the
go-ruby-concurrent-ruby/concurrent-ruby authors.

## WebAssembly

Being pure Go (CGO=0), this library also compiles to **WebAssembly** — both
`GOOS=js GOARCH=wasm` (browser / Node.js) and `GOOS=wasip1 GOARCH=wasm` (WASI).
CI builds both targets on every push, alongside the six 64-bit native/qemu arches.

```sh
GOOS=js     GOARCH=wasm go build ./...   # browser / Node
GOOS=wasip1 GOARCH=wasm go build ./...   # WASI (wasmtime, wasmer, wasmedge, …)
```
