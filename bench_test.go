package concurrent

import "testing"

func BenchmarkAtomicFixnumIncrement(b *testing.B) {
	f := NewAtomicFixnum(0)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.Increment(1)
		}
	})
}

func BenchmarkAtomicReferenceCompareAndSet(b *testing.B) {
	r := NewAtomicReference(0)
	for i := 0; i < b.N; i++ {
		r.CompareAndSet(i, i+1)
	}
}

func BenchmarkMapComputeIfAbsent(b *testing.B) {
	m := NewMap()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.ComputeIfAbsent(i&1023, func() any { return i })
			i++
		}
	})
}

func BenchmarkMapGetSet(b *testing.B) {
	m := NewMap()
	for i := 0; i < 1024; i++ {
		m.Set(i, i)
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			m.Get(i & 1023)
			i++
		}
	})
}

func BenchmarkAtomSwap(b *testing.B) {
	a := NewAtom(0)
	for i := 0; i < b.N; i++ {
		a.Swap(func(o any) any { return o.(int) + 1 })
	}
}

func BenchmarkSemaphoreAcquireRelease(b *testing.B) {
	s := NewSemaphore(1)
	for i := 0; i < b.N; i++ {
		s.Acquire(1)
		s.Release(1)
	}
}

func BenchmarkTVarAtomicallyIncrement(b *testing.B) {
	v := NewTVar(0)
	for i := 0; i < b.N; i++ {
		_ = Atomically(func(tx *Transaction) error {
			tx.Write(v, tx.Read(v).(int)+1)
			return nil
		})
	}
}

func BenchmarkPromiseThenChain(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := NewPromise()
		c := p.Then(func(v any) (any, error) { return v.(int) + 1, nil })
		p.Fulfill(0)
		_ = c.Value(NoTimeout)
	}
}

func BenchmarkCachedThreadPoolPost(b *testing.B) {
	p := NewCachedThreadPool()
	defer func() { p.Shutdown(); p.WaitForTermination(NoTimeout) }()
	done := NewCountDownLatch(b.N)
	for i := 0; i < b.N; i++ {
		p.Post(func() { done.CountDown() })
	}
	done.Wait(NoTimeout)
}
