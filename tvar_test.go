package concurrent

import (
	"errors"
	"testing"
	"time"
)

func TestTVarSingleOps(t *testing.T) {
	v := NewTVar(10)
	if v.Value() != 10 {
		t.Fatalf("value: %v", v.Value())
	}
	if v.SetValue(20) != 20 {
		t.Fatal("SetValue returns v")
	}
	if v.Value() != 20 {
		t.Fatalf("after set: %v", v.Value())
	}
}

func TestTVarAtomicallyReadsAndWrites(t *testing.T) {
	a := NewTVar(1)
	b := NewTVar(2)
	c := NewTVar(5)
	err := Atomically(func(tx *Transaction) error {
		// Fresh read then repeatable read of the same tvar (readVals cache).
		if tx.Read(c) != 5 || tx.Read(c) != 5 {
			t.Fatal("repeatable read")
		}
		sum := tx.Read(a).(int) + tx.Read(b).(int) // 1 + 2
		tx.Write(a, 100)
		// Read-after-write in the same transaction sees the buffered write.
		if tx.Read(a) != 100 {
			t.Fatal("read-after-write")
		}
		tx.Write(b, sum)
		return nil
	})
	if err != nil {
		t.Fatalf("atomically: %v", err)
	}
	if a.Value() != 100 || b.Value() != 3 {
		t.Fatalf("committed a=%v b=%v", a.Value(), b.Value())
	}
}

func TestTVarAtomicallyAbort(t *testing.T) {
	d := NewTVar(0)
	sentinel := errors.New("abort")
	err := Atomically(func(tx *Transaction) error {
		tx.Write(d, 99)
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("abort should surface error, got %v", err)
	}
	if d.Value() != 0 {
		t.Fatalf("aborted write must not commit, got %v", d.Value())
	}
}

func TestTVarConflictRetries(t *testing.T) {
	x := NewTVar(0)
	firstAttempt := true
	committed := NewCountDownLatch(1)

	err := Atomically(func(tx *Transaction) error {
		cur := tx.Read(x).(int)
		if firstAttempt {
			firstAttempt = false
			// A concurrent transaction commits x=5 before we try to commit,
			// invalidating our read and forcing a deterministic retry.
			go func() {
				Atomically(func(tx2 *Transaction) error {
					tx2.Write(x, 5)
					return nil
				})
				committed.CountDown()
			}()
			committed.Wait(2 * time.Second)
		}
		tx.Write(x, cur+1)
		return nil
	})
	if err != nil {
		t.Fatalf("atomically: %v", err)
	}
	// First attempt read 0 but conflicted; retry read 5 and wrote 6.
	if x.Value() != 6 {
		t.Fatalf("expected retry to land 6, got %v", x.Value())
	}
}
