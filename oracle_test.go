package concurrent

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// rubyOracle runs a concurrent-ruby snippet under the real MRI gem and returns
// its trimmed stdout, so a Go result can be differentially checked against the
// gem's observable behaviour. It skips (never fails) when no Ruby/gem oracle is
// available — on Windows, under qemu cross-arch, or when the gem is absent — so
// the deterministic ruby-free suite alone keeps the coverage gate satisfied.
func rubyOracle(t *testing.T, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("no ruby oracle on windows")
	}
	path, err := exec.LookPath("ruby")
	if err != nil {
		t.Skip("ruby not installed")
	}
	out, err := exec.Command(path, "-e", "require 'concurrent'\n"+body).CombinedOutput()
	if err != nil {
		t.Skipf("ruby/concurrent-ruby oracle unavailable: %v: %s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestOracleAtomicFixnum(t *testing.T) {
	want := rubyOracle(t, `f=Concurrent::AtomicFixnum.new(0); 5.times{ f.increment }; f.decrement; puts f.value`)
	f := NewAtomicFixnum(0)
	for i := 0; i < 5; i++ {
		f.Increment(1)
	}
	f.Decrement(1)
	if got := fmt.Sprint(f.Value()); got != want {
		t.Fatalf("AtomicFixnum: go=%s ruby=%s", got, want)
	}
}

func TestOracleAtomicBoolean(t *testing.T) {
	want := rubyOracle(t, `b=Concurrent::AtomicBoolean.new(false); r=b.make_true; puts "#{r} #{b.value} #{b.make_true}"`)
	b := NewAtomicBoolean(false)
	got := fmt.Sprintf("%v %v %v", b.MakeTrue(), b.Value(), b.MakeTrue())
	if got != want {
		t.Fatalf("AtomicBoolean: go=%q ruby=%q", got, want)
	}
}

func TestOracleMapComputeIfAbsent(t *testing.T) {
	want := rubyOracle(t, `m=Concurrent::Map.new; a=m.compute_if_absent(:k){42}; b=m.compute_if_absent(:k){99}; puts "#{a} #{b} #{m.size}"`)
	m := NewMap()
	a := m.ComputeIfAbsent("k", func() any { return 42 })
	b := m.ComputeIfAbsent("k", func() any { return 99 })
	got := fmt.Sprintf("%v %v %v", a, b, m.Size())
	if got != want {
		t.Fatalf("Map: go=%q ruby=%q", got, want)
	}
}

func TestOracleAtomValidator(t *testing.T) {
	want := rubyOracle(t, `
a=Concurrent::Atom.new(0, validator: ->(v){ v.even? })
inv_swap=a.swap{|o| 3}
inv_reset=a.reset(3)
ok=a.reset(4)
cas=a.compare_and_set(4,5)
puts "#{inv_swap} #{inv_reset} #{ok} #{cas} #{a.value}"`)
	a := NewAtomWithValidator(0, func(v any) bool { return v.(int)%2 == 0 })
	invSwap := a.Swap(func(o any) any { return 3 })
	invReset := a.Reset(3)
	ok := a.Reset(4)
	cas := a.CompareAndSet(4, 5)
	got := fmt.Sprintf("%v %v %v %v %v", invSwap, invReset, ok, cas, a.Value())
	if got != want {
		t.Fatalf("Atom: go=%q ruby=%q", got, want)
	}
}

func TestOracleTVarAtomically(t *testing.T) {
	want := rubyOracle(t, `
a=Concurrent::TVar.new(1); b=Concurrent::TVar.new(2)
Concurrent::atomically { b.value = a.value + b.value; a.value = 100 }
puts "#{a.value} #{b.value}"`)
	a := NewTVar(1)
	b := NewTVar(2)
	_ = Atomically(func(tx *Transaction) error {
		tx.Write(b, tx.Read(a).(int)+tx.Read(b).(int))
		tx.Write(a, 100)
		return nil
	})
	got := fmt.Sprintf("%v %v", a.Value(), b.Value())
	if got != want {
		t.Fatalf("TVar: go=%q ruby=%q", got, want)
	}
}

func TestOracleDelayMemoises(t *testing.T) {
	want := rubyOracle(t, `
c=0
d=Concurrent::Delay.new { c += 1; 42 }
p0=d.pending?
v1=d.value; v2=d.value
puts "#{p0} #{v1} #{v2} #{c} #{d.fulfilled?}"`)
	c := 0
	d := NewDelay(func() (any, error) { c++; return 42, nil })
	p0 := d.PendingQ()
	v1, v2 := d.Value(), d.Value()
	got := fmt.Sprintf("%v %v %v %v %v", p0, v1, v2, c, d.FulfilledQ())
	if got != want {
		t.Fatalf("Delay: go=%q ruby=%q", got, want)
	}
}

func TestOracleCountDownLatch(t *testing.T) {
	want := rubyOracle(t, `
l=Concurrent::CountDownLatch.new(2)
l.count_down
c1=l.count
l.count_down
puts "#{c1} #{l.count} #{l.wait(0)}"`)
	l := NewCountDownLatch(2)
	l.CountDown()
	c1 := l.Count()
	l.CountDown()
	got := fmt.Sprintf("%v %v %v", c1, l.Count(), l.Wait(0))
	if got != want {
		t.Fatalf("CountDownLatch: go=%q ruby=%q", got, want)
	}
}

func TestOracleSemaphore(t *testing.T) {
	want := rubyOracle(t, `
s=Concurrent::Semaphore.new(2)
a=s.try_acquire(1)
b=s.try_acquire(2)
puts "#{a} #{b} #{s.available_permits}"`)
	s := NewSemaphore(2)
	a := s.TryAcquire(1)
	b := s.TryAcquire(2)
	got := fmt.Sprintf("%v %v %v", a, b, s.AvailablePermits())
	if got != want {
		t.Fatalf("Semaphore: go=%q ruby=%q", got, want)
	}
}

func TestOracleEvent(t *testing.T) {
	want := rubyOracle(t, `
e=Concurrent::Event.new
s0=e.set?
e.set
puts "#{s0} #{e.set?} #{e.wait(0)}"`)
	e := NewEvent()
	s0 := e.SetQ()
	e.Set()
	got := fmt.Sprintf("%v %v %v", s0, e.SetQ(), e.Wait(0))
	if got != want {
		t.Fatalf("Event: go=%q ruby=%q", got, want)
	}
}

func TestOraclePromiseThen(t *testing.T) {
	want := rubyOracle(t, `
p=Concurrent::Promise.fulfill(21)
c=p.then{|v| v*2}
c.wait
puts "#{c.value} #{c.state}"`)
	p := PromisesFulfilledFuture(21)
	c := p.Then(func(v any) (any, error) { return v.(int) * 2, nil })
	got := fmt.Sprintf("%v %v", c.Value(2*time.Second), c.State())
	if got != want {
		t.Fatalf("Promise#then: go=%q ruby=%q", got, want)
	}
}
