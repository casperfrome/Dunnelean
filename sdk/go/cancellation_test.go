package dunnelean

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// A controlled native receipt isolates the Go cancellation boundary: the
// accepted submission outlives ctx, owns frozen input and frees its late buffer.
func TestCancelledSubmitOwnsInputAndFreesLateReceipt(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	freed := make(chan struct{})
	var storage []byte
	var inputText string
	api := &nativeAPI{}
	api.invoke = func(_ uint64, op uint32, input *byte, length uintptr, out **byte, outLen *uintptr) int32 {
		if op != opSubmit {
			panic("unexpected operation")
		}
		close(entered)
		<-gate
		inputText = string(unsafe.Slice(input, int(length)))
		storage = []byte(`{"run_id":"accepted","state":"QUEUED","config":{}}`)
		*out = &storage[0]
		*outLen = uintptr(len(storage))
		return 0
	}
	api.free = func(*byte, uintptr) { close(freed) }
	e := &Engine{owner: &handleOwner{api: api, handle: 1}}
	ctx, cancel := context.WithCancel(background)
	spec := json.RawMessage(`{"request_id":"stable"}`)
	done := make(chan error, 1)
	go func() { _, err := e.Submit(ctx, spec); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for i := range spec {
		spec[i] = 'x'
	}
	close(gate)
	select {
	case <-freed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled submit leaked late native receipt")
	}
	if inputText != `{"request_id":"stable"}` {
		t.Fatal("cancelled call borrowed mutable caller input")
	}
}
func TestCancelledCloseCompletesAndReleasesHandle(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	released := make(chan struct{})
	var receipt []byte
	var count atomic.Int32
	api := &nativeAPI{free: func(*byte, uintptr) {}, release: func(uint64) int32 {
		if count.Add(1) == 1 {
			close(released)
		}
		return 0
	}}
	api.close = func(_ uint64, _ uint64, out **byte, length *uintptr) int32 {
		close(entered)
		<-gate
		receipt = []byte(`{"closed":true}`)
		*out = &receipt[0]
		*length = uintptr(len(receipt))
		return 0
	}
	e := &Engine{owner: &handleOwner{api: api, handle: 1}, shutdown: time.Second}
	ctx, cancel := context.WithCancel(background)
	done := make(chan error, 1)
	go func() { done <- e.Close(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(gate)
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("close cancellation retained engine handle")
	}
	if err := e.Close(background); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatal("handle released repeatedly")
	}
}
func TestConcurrentCloseBudgetIncludesSerialization(t *testing.T) {
	entered := make(chan struct{})
	gate := make(chan struct{})
	var receipt []byte
	api := &nativeAPI{free: func(*byte, uintptr) {}, release: func(uint64) int32 { return 0 }}
	api.close = func(_ uint64, _ uint64, out **byte, length *uintptr) int32 {
		close(entered)
		<-gate
		receipt = []byte(`{"closed":true}`)
		*out = &receipt[0]
		*length = uintptr(len(receipt))
		return 0
	}
	owner := &handleOwner{api: api, handle: 1}
	done := make(chan error, 1)
	go func() { done <- owner.close(time.Second) }()
	<-entered
	started := time.Now()
	var timeout *CloseTimeoutError
	if err := owner.close(5 * time.Millisecond); !errors.As(err, &timeout) {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("serialized close ignored its timeout")
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestCancelledNativeCloseReleasesRealStore(t *testing.T) {
	p := stalled(t)
	e, path := openTest(t)
	validation := make(chan error, 1)
	go func() { _, err := e.Validate(background, peerJob(p, "close-cancel")); validation <- err }()
	p.connected(t)
	ctx, cancel := context.WithCancel(background)
	done := make(chan error, 1)
	go func() { done <- e.Close(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p.release()
	<-validation
	deadline := time.Now().Add(5 * time.Second)
	for {
		reopened, err := Open(background, path)
		if err == nil {
			reopened.Close(background)
			break
		}
		requireCode(t, err, "STATE_STORE")
		if time.Now().After(deadline) {
			t.Fatal("background close did not release real store")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := e.Close(background); err != nil {
		t.Fatal(err)
	}
}
func TestCancelledOpeningReleasesLateRealInstance(t *testing.T) {
	// Delay native loading, cancel the local waiter, then permit the late open.
	// The original context remains retained throughout to expose ownership leaks.
	libraries.Lock()
	path := t.TempDir() + "/late.sqlite"
	ctx, cancel := context.WithCancel(background)
	done := make(chan error, 1)
	go func() { _, err := Open(ctx, path); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		libraries.Unlock()
		t.Fatal(err)
	}
	libraries.Unlock()
	time.Sleep(50 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for {
		engine, err := Open(background, path)
		if err == nil {
			engine.Close(background)
			break
		}
		requireCode(t, err, "STATE_STORE")
		if time.Now().After(deadline) {
			t.Fatal("cancelled open retained late engine")
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.KeepAlive(ctx)
}

func TestClosedEngineRejectsCallsWithoutNativeBuffers(t *testing.T) {
	e := &Engine{owner: &handleOwner{}}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.GetRun(background, "absent")
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != "ENGINE_CLOSED" {
				t.Errorf("unexpected error %v", err)
			}
		}()
	}
	wg.Wait()
}
