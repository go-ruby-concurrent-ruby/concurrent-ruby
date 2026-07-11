package concurrent

import (
	"testing"
	"time"
)

func TestEventSetAndWait(t *testing.T) {
	e := NewEvent()
	if e.SetQ() {
		t.Fatal("new event is unset")
	}
	if e.Wait(20 * time.Millisecond) {
		t.Fatal("unset event should time out")
	}
	done := make(chan bool, 1)
	go func() { done <- e.Wait(2 * time.Second) }()
	if !e.Set() {
		t.Fatal("set returns true")
	}
	if !<-done {
		t.Fatal("waiter should be released by set")
	}
	if !e.SetQ() {
		t.Fatal("event should be set")
	}
	if !e.Wait(NoTimeout) {
		t.Fatal("already-set event returns immediately")
	}
	if !e.Set() {
		t.Fatal("set is idempotent")
	}
}

func TestEventResetAndTry(t *testing.T) {
	e := NewEvent()
	if !e.Reset() {
		t.Fatal("reset on unset event is a no-op returning true")
	}
	if !e.TryQ() {
		t.Fatal("first try? sets the event")
	}
	if e.TryQ() {
		t.Fatal("second try? finds it already set")
	}
	if !e.Reset() {
		t.Fatal("reset on set event returns true")
	}
	if e.SetQ() {
		t.Fatal("event should be unset after reset")
	}
	if !e.TryQ() {
		t.Fatal("try? sets again after reset")
	}
}
