package concurrent

import "sync"

// stmLock guards every access to a TVar's committed value and version, so reads
// and the commit phase are race-free, and (together with per-TVar versioning)
// provides the isolation and atomicity of Concurrent::TVar /
// Concurrent.atomically. Reads are optimistic and validated at commit; a stale
// read retries the whole block, exactly as the gem does on a write conflict.
var stmLock sync.Mutex

// TVar models Concurrent::TVar: a transactional variable that may only be read
// or written inside a transaction (Atomically). Outside a transaction, Value
// and SetValue run a one-operation transaction.
type TVar struct {
	value   any
	version uint64
}

// NewTVar returns a TVar holding initial (Ruby Concurrent::TVar.new(initial)).
func NewTVar(initial any) *TVar { return &TVar{value: initial} }

// Value returns the committed value via a single-read transaction (Ruby
// #value, when called outside an explicit transaction).
func (t *TVar) Value() any {
	var out any
	_ = Atomically(func(tx *Transaction) error {
		out = tx.Read(t)
		return nil
	})
	return out
}

// SetValue stores v via a single-write transaction and returns it (Ruby
// #value=, outside an explicit transaction).
func (t *TVar) SetValue(v any) any {
	_ = Atomically(func(tx *Transaction) error {
		tx.Write(t, v)
		return nil
	})
	return v
}

// Transaction is the handle threaded through an Atomically block. Reads and
// writes to TVars go through it so the runtime can buffer writes and validate
// reads at commit time.
type Transaction struct {
	reads    map[*TVar]uint64
	readVals map[*TVar]any
	writes   map[*TVar]any
}

// Read returns t's value as seen by this transaction: a value written earlier
// in the same transaction, then a value already read in it (repeatable read),
// otherwise a fresh committed value whose version is recorded for commit-time
// validation (Ruby TVar#value inside atomically).
func (tx *Transaction) Read(t *TVar) any {
	if v, ok := tx.writes[t]; ok {
		return v
	}
	if v, ok := tx.readVals[t]; ok {
		return v
	}
	stmLock.Lock()
	ver, val := t.version, t.value
	stmLock.Unlock()
	tx.reads[t] = ver
	tx.readVals[t] = val
	return val
}

// Write buffers v as t's new value within this transaction; it becomes visible
// to other transactions only at commit (Ruby TVar#value= inside atomically).
func (tx *Transaction) Write(t *TVar, v any) {
	tx.writes[t] = v
}

// Atomically runs fn as an atomic, isolated transaction (Ruby
// Concurrent.atomically { ... }). If fn returns an error the transaction is
// aborted, no writes are applied, and the error is returned without retrying.
// If another transaction commits a conflicting write to a TVar this one read,
// the whole block is retried against the fresh committed state. On success all
// buffered writes are applied atomically and nil is returned.
func Atomically(fn func(tx *Transaction) error) error {
	for {
		tx := &Transaction{
			reads:    map[*TVar]uint64{},
			readVals: map[*TVar]any{},
			writes:   map[*TVar]any{},
		}
		if err := fn(tx); err != nil {
			return err
		}
		stmLock.Lock()
		conflict := false
		for t, ver := range tx.reads {
			if t.version != ver {
				conflict = true
				break
			}
		}
		if conflict {
			stmLock.Unlock()
			continue // retry the whole block against the fresh committed state
		}
		for t, v := range tx.writes {
			t.value = v
			t.version++
		}
		stmLock.Unlock()
		return nil
	}
}
