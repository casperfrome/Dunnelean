package dunnelean

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type handleOwner struct {
	sync.RWMutex
	closer sync.Mutex
	api    *nativeAPI
	handle uint64
}

func (o *handleOwner) abandon() {
	o.Lock()
	handle := o.handle
	o.handle = 0
	o.Unlock()
	if handle != 0 {
		o.api.release(handle)
	}
}
func (o *handleOwner) invoke(op uint32, input []byte) ([]byte, error) {
	o.RLock()
	defer o.RUnlock()
	if o.handle == 0 {
		return nil, &Error{Code: "ENGINE_CLOSED", Message: "engine is closed"}
	}
	return o.api.call(o.handle, op, input)
}
func (o *handleOwner) close(timeout time.Duration) error {
	started := time.Now()
	for !o.closer.TryLock() {
		o.RLock()
		closed := o.handle == 0
		o.RUnlock()
		if closed {
			return nil
		}
		remaining := timeout - time.Since(started)
		if remaining <= 0 {
			return &CloseTimeoutError{Timeout: timeout}
		}
		time.Sleep(min(remaining, time.Millisecond))
	}
	defer o.closer.Unlock()
	o.RLock()
	handle := o.handle
	o.RUnlock()
	if handle == 0 {
		return nil
	}
	budget := max(timeout-time.Since(started), 0)
	var out *byte
	var length uintptr
	status := o.api.close(handle, milliseconds(budget), &out, &length)
	data, err := o.api.result(status, out, length)
	if err != nil {
		return err
	}
	var result struct {
		Closed bool `json:"closed"`
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return nativeError("NATIVE_ABI", err)
	}
	if !result.Closed {
		return &CloseTimeoutError{Timeout: timeout}
	}
	// Native closure waits all Rust leases; this also waits Go calls scheduled
	// before close but not yet entered into the registry. Publish closure only
	// after those callers finish and release the registry handle once.
	o.Lock()
	if o.handle == handle {
		o.handle = 0
		o.api.release(handle)
	}
	o.Unlock()
	return nil
}

// Engine owns an independent Rust engine, SQLite store and Tokio runtime.
// Call Close explicitly. Garbage collection supplies background cleanup only.
type Engine struct {
	owner        *handleOwner
	stateStoreID string
	shutdown     time.Duration
	cleanup      runtime.Cleanup
}
type openResult struct {
	owner *handleOwner
	id    string
	err   error
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return invalid("context cannot be nil")
	}
	return ctx.Err()
}
func milliseconds(timeout time.Duration) uint64 {
	n := uint64(timeout / time.Millisecond)
	if timeout%time.Millisecond != 0 {
		n++
	}
	return n
}

// Open starts the engine without connecting to source or target databases.
// Cancellation ends only the local opening wait; a late instance is cleaned up.
func Open(ctx context.Context, statePath string, options ...Option) (*Engine, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if statePath == "" || statePath == ":memory:" || strings.HasPrefix(statePath, "file:") {
		return nil, invalid("state_path must name a persistent SQLite file")
	}
	s := settings{running: 2, queued: 16, shutdown: 60 * time.Second}
	for _, option := range options {
		if option == nil {
			return nil, invalid("nil engine option")
		}
		if err := option(&s); err != nil {
			return nil, err
		}
	}
	path, err := filepath.Abs(statePath)
	if err != nil {
		return nil, invalid(err.Error())
	}
	input, _ := json.Marshal(struct {
		Path    string `json:"state_path"`
		Running int    `json:"max_running"`
		Queued  int    `json:"max_queued"`
	}{path, s.running, s.queued})
	result := make(chan openResult)
	go func() {
		api, err := loadNative(s.cache)
		if err != nil {
			select {
			case result <- openResult{err: err}:
			case <-ctx.Done():
			}
			return
		}
		var handle uint64
		var out *byte
		var length uintptr
		status := api.open(&input[0], uintptr(len(input)), &handle, &out, &length)
		runtime.KeepAlive(input)
		data, err := api.result(status, out, length)
		var opened struct {
			ID string `json:"state_store_id"`
		}
		if err == nil {
			err = json.Unmarshal(data, &opened)
			if err != nil {
				err = nativeError("NATIVE_ABI", err)
			}
		}
		owner := &handleOwner{api: api, handle: handle}
		if err != nil {
			owner.abandon()
			select {
			case result <- openResult{err: err}:
			case <-ctx.Done():
			}
			return
		}
		if handle == 0 || opened.ID == "" {
			owner.abandon()
			select {
			case result <- openResult{err: &Error{Code: "NATIVE_ABI", Message: "native open returned invalid handle or identity"}}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case result <- openResult{owner: owner, id: opened.ID}:
		case <-ctx.Done():
			owner.abandon()
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case opened := <-result:
		if opened.err != nil {
			return nil, opened.err
		}
		if err = ctx.Err(); err != nil {
			opened.owner.abandon()
			return nil, err
		}
		engine := &Engine{owner: opened.owner, stateStoreID: opened.id, shutdown: s.shutdown}
		engine.cleanup = runtime.AddCleanup(engine, func(owner *handleOwner) { owner.abandon() }, opened.owner)
		return engine, nil
	}
}

// StateStoreID is cached and remains available after Close.
func (e *Engine) StateStoreID() string { return e.stateStoreID }

type callResult struct {
	data []byte
	err  error
}

func (e *Engine) call(ctx context.Context, op uint32, input []byte, target any) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if e == nil || e.owner == nil {
		return &Error{Code: "ENGINE_CLOSED", Message: "engine is not open"}
	}
	// Own the input before returning on cancellation; caller may reuse its slice.
	input = append([]byte(nil), input...)
	owner := e.owner
	result := make(chan callResult, 1)
	go func() { data, err := owner.invoke(op, input); result <- callResult{data, err} }()
	defer runtime.KeepAlive(e)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case got := <-result:
		if got.err != nil {
			return got.err
		}
		if target != nil {
			if err := json.Unmarshal(got.data, target); err != nil {
				return nativeError("NATIVE_ABI", err)
			}
		}
		return nil
	}
}
func idInput(id string) []byte {
	data, _ := json.Marshal(struct {
		ID string `json:"id"`
	}{id})
	return data
}

func (e *Engine) Ready(ctx context.Context) error { return e.call(ctx, opReady, nil, nil) }
func (e *Engine) Connectors(ctx context.Context) (*ConnectorInfo, error) {
	var value ConnectorInfo
	if err := e.call(ctx, opConnectors, nil, &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) RequestSchema(ctx context.Context) (json.RawMessage, error) {
	info, err := e.Connectors(ctx)
	if err != nil {
		return nil, err
	}
	return info.RequestSchema, nil
}
func (e *Engine) Validate(ctx context.Context, spec json.RawMessage) (*Validation, error) {
	var value Validation
	if err := e.call(ctx, opValidate, spec, &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) Submit(ctx context.Context, spec json.RawMessage) (*Run, error) {
	var value Run
	if err := e.call(ctx, opSubmit, spec, &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) GetRun(ctx context.Context, runID string) (*Run, error) {
	var value Run
	if err := e.call(ctx, opGetRun, idInput(runID), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) CancelRun(ctx context.Context, runID string) (*Run, error) {
	var value Run
	if err := e.call(ctx, opCancelRun, idInput(runID), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) GetRequest(ctx context.Context, requestID string) (*RequestStatus, error) {
	var value RequestStatus
	if err := e.call(ctx, opGetRequest, idInput(requestID), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) CancelRequest(ctx context.Context, requestID string) (*RequestStatus, error) {
	var value RequestStatus
	if err := e.call(ctx, opCancelRequest, idInput(requestID), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
func (e *Engine) ListBatches(ctx context.Context, runID string) ([]Batch, error) {
	var value []Batch
	if err := e.call(ctx, opListBatches, idInput(runID), &value); err != nil {
		return nil, err
	}
	return value, nil
}
func (e *Engine) ListRuns(ctx context.Context, limit, offset int) ([]Run, error) {
	if limit < 1 || limit > 500 || offset < 0 {
		return nil, invalid("limit must be 1..500 and offset nonnegative")
	}
	input, _ := json.Marshal(struct {
		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}{limit, offset})
	var value []Run
	if err := e.call(ctx, opListRuns, input, &value); err != nil {
		return nil, err
	}
	return value, nil
}

// Close cancels jobs and drains native calls. If ctx ends first, the ongoing
// close still finishes in the background; a successful finish releases the lock.
func (e *Engine) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if e == nil || e.owner == nil {
		return nil
	}
	budget := e.shutdown
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < budget {
			budget = max(remaining, 0)
		}
	}
	owner := e.owner
	result := make(chan error, 1)
	go func() { result <- owner.close(budget) }()
	defer runtime.KeepAlive(e)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		if err == nil {
			e.cleanup.Stop()
		}
		return err
	}
}

// WaitForCompletion polls until a terminal audit record. Cancellation and
// timeout only stop this wait. The last observed Run accompanies a wait error.
func (e *Engine) WaitForCompletion(ctx context.Context, runID string, options ...WaitOption) (*Run, error) {
	s, err := waitOptions(options)
	if err != nil {
		return nil, err
	}
	return e.wait(ctx, runID, s, nil)
}
func (e *Engine) wait(ctx context.Context, runID string, s waitSettings, last *Run) (*Run, error) {
	if err := checkContext(ctx); err != nil {
		return last, err
	}
	deadline := time.Now().Add(s.timeout)
	for {
		queryCtx := ctx
		cancel := func() {}
		if s.timeout > 0 {
			queryCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		run, err := e.GetRun(queryCtx, runID)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return last, ctx.Err()
			}
			if !time.Now().Before(deadline) {
				return last, &WaitTimeoutError{runID, s.timeout}
			}
			return last, err
		}
		last = run
		if last.Terminal() {
			return last, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return last, &WaitTimeoutError{runID, s.timeout}
		}
		timer := time.NewTimer(min(s.poll, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, ctx.Err()
		case <-timer.C:
		}
	}
}

// Run submits then waits, retaining the accepted run on any wait failure.
// Failed and cancelled jobs return a terminal Run with Run.Error and nil error.
func (e *Engine) Run(ctx context.Context, spec json.RawMessage, options ...WaitOption) (*Run, error) {
	s, err := waitOptions(options)
	if err != nil {
		return nil, err
	}
	run, err := e.Submit(ctx, spec)
	if err != nil {
		return nil, err
	}
	return e.wait(ctx, run.RunID, s, run)
}

// Connectors returns capabilities and schemas without opening a state database.
func Connectors(ctx context.Context) (*ConnectorInfo, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	result := make(chan callResult, 1)
	go func() {
		api, err := loadNative("")
		if err != nil {
			result <- callResult{err: err}
			return
		}
		data, err := api.call(0, opConnectors, nil)
		result <- callResult{data, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case got := <-result:
		if got.err != nil {
			return nil, got.err
		}
		var info ConnectorInfo
		if err := json.Unmarshal(got.data, &info); err != nil {
			return nil, nativeError("NATIVE_ABI", err)
		}
		return &info, nil
	}
}
func RequestSchema(ctx context.Context) (json.RawMessage, error) {
	info, err := Connectors(ctx)
	if err != nil {
		return nil, err
	}
	return info.RequestSchema, nil
}
