package dunnelean

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var background = context.Background()

func openTest(t *testing.T, options ...Option) (*Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	engine, err := Open(background, path, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(background); err != nil {
			t.Error(err)
		}
	})
	return engine, path
}
func requireCode(t *testing.T, err error, code string) *Error {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
	return failure
}

type pendingPeer struct {
	listener    net.Listener
	accepted    chan struct{}
	stop        chan struct{}
	once        sync.Once
	mu          sync.Mutex
	connections []net.Conn
}

func stalled(t *testing.T) *pendingPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &pendingPeer{listener: listener, accepted: make(chan struct{}), stop: make(chan struct{})}
	go func() {
		defer close(p.stop)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.connections = append(p.connections, conn)
			p.mu.Unlock()
			p.once.Do(func() { close(p.accepted) })
		}
	}()
	t.Cleanup(p.release)
	return p
}
func (p *pendingPeer) release() {
	p.listener.Close()
	<-p.stop
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.connections {
		c.Close()
	}
	p.connections = nil
}
func (p *pendingPeer) connected(t *testing.T) {
	t.Helper()
	select {
	case <-p.accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("native engine never connected")
	}
}
func job(port int, request string) json.RawMessage {
	connection := map[string]any{"host": "127.0.0.1", "port": port, "database": "unit", "credentials": map[string]any{"username": "unit", "password": "unit-secret"}, "timeouts": map[string]any{"connect_ms": 5000, "read_ms": 5000, "write_ms": 5000}}
	spec := map[string]any{"request_id": request, "reader": map[string]any{"type": "mysql", "connection": connection, "source": map[string]any{"table": "source"}}, "writer": map[string]any{"type": "doris", "sql": connection, "table": "target", "mode": "append", "fe_http_urls": []string{"http://127.0.0.1:1"}, "be_http_urls": []string{"http://127.0.0.1:1"}}}
	data, _ := json.Marshal(spec)
	return data
}
func peerJob(p *pendingPeer, request string) json.RawMessage {
	return job(p.listener.Addr().(*net.TCPAddr).Port, request)
}

func TestLifecycleIdentityExclusiveStore(t *testing.T) {
	e, path := openTest(t)
	id := e.StateStoreID()
	if id == "" {
		t.Fatal("empty store identity")
	}
	if err := e.Ready(background); err != nil {
		t.Fatal(err)
	}
	runs, err := e.ListRuns(background, 50, 0)
	if err != nil || len(runs) != 0 {
		t.Fatalf("%v %v", runs, err)
	}
	second, err := Open(background, path)
	if second != nil {
		second.Close(background)
		t.Fatal("state lock was not exclusive")
	}
	requireCode(t, err, "STATE_STORE")
	if err = e.Close(background); err != nil {
		t.Fatal(err)
	}
	if err = e.Close(background); err != nil {
		t.Fatal(err)
	}
	_, err = e.ListRuns(background, 50, 0)
	requireCode(t, err, "ENGINE_CLOSED")
	reopened, err := Open(background, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(background)
	if reopened.StateStoreID() != id || e.StateStoreID() != id {
		t.Fatal("identity changed across close/reopen")
	}
}
func TestCrossProcessStateStoreExclusive(t *testing.T) {
	if path := os.Getenv("DUNNELEAN_GO_LOCK_SMOKE"); path != "" {
		engine, err := Open(background, path)
		if os.Getenv("DUNNELEAN_GO_LOCK_ID") == "" {
			if engine != nil {
				engine.Close(background)
				t.Fatal("another process acquired live store")
			}
			requireCode(t, err, "STATE_STORE")
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close(background)
		if engine.StateStoreID() != os.Getenv("DUNNELEAN_GO_LOCK_ID") {
			t.Fatal("reopen identity differs")
		}
		return
	}
	e, path := openTest(t)
	check := func(id string) {
		child := exec.Command(os.Args[0], "-test.run=^TestCrossProcessStateStoreExclusive$", "-test.v")
		child.Env = append(os.Environ(), "DUNNELEAN_GO_LOCK_SMOKE="+path, "DUNNELEAN_GO_LOCK_ID="+id)
		if output, err := child.CombinedOutput(); err != nil {
			t.Fatalf("state owner consumer: %v\n%s", err, output)
		}
	}
	check("")
	if err := e.Close(background); err != nil {
		t.Fatal(err)
	}
	check(e.StateStoreID())
}
func TestProcessInterruptionPersistsAuditWithoutReplay(t *testing.T) {
	if path := os.Getenv("DUNNELEAN_GO_INTERRUPT_SMOKE"); path != "" {
		engine, err := Open(background, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = engine.Submit(background, json.RawMessage(os.Getenv("DUNNELEAN_GO_INTERRUPT_JOB"))); err != nil {
			t.Fatal(err)
		}
		// Model application termination without running deferred Close or GC.
		os.Exit(0)
	}
	p := stalled(t)
	spec := peerJob(p, "interrupted-request")
	path := filepath.Join(t.TempDir(), "interrupted.sqlite")
	child := exec.Command(os.Args[0], "-test.run=^TestProcessInterruptionPersistsAuditWithoutReplay$", "-test.v")
	child.Env = append(os.Environ(), "DUNNELEAN_GO_INTERRUPT_SMOKE="+path, "DUNNELEAN_GO_INTERRUPT_JOB="+string(spec))
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("interrupted consumer: %v\n%s", err, output)
	}
	e, err := Open(background, path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(background)
	request, err := e.GetRequest(background, "interrupted-request")
	if err != nil || request.Run == nil {
		t.Fatalf("missing interrupted request: %+v %v", request, err)
	}
	if request.Run.State != "INTERRUPTED" || request.Run.Error == nil || request.Run.Error.Code != "INTERRUPTED" || request.Run.RowsCommitted != 0 || request.Run.CommitUnknown {
		t.Fatalf("wrong interruption audit: %+v", request.Run)
	}
	replayed, err := e.Submit(background, spec)
	if err != nil || replayed.RunID != request.Run.RunID || replayed.State != "INTERRUPTED" {
		t.Fatalf("interrupted request replayed: %+v %v", replayed, err)
	}
}
func TestConfigContractAndProcessEnvironment(t *testing.T) {
	e, _ := openTest(t)
	t.Setenv("JAVA_SDK_TEST_PASSWORD", "go-contract-secret")
	for _, name := range []string{"all-fields-mysql-to-doris.json", "all-fields-doris-to-mysql.json"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			var spec map[string]any
			if err = json.Unmarshal(data, &spec); err != nil {
				t.Fatal(err)
			}
			id := spec["request_id"].(string) + name
			spec["request_id"] = id
			if _, err = e.CancelRequest(background, id); err != nil {
				t.Fatal(err)
			}
			data, _ = json.Marshal(spec)
			_, err = e.Submit(background, data)
			requireCode(t, err, "REQUEST_CANCELLED")
		})
	}
	// The Rust core must see Go's os.Setenv updates in CGO_ENABLED=0 builds.
	spec := job(1, "env-request")
	var object map[string]any
	json.Unmarshal(spec, &object)
	reader := object["reader"].(map[string]any)["connection"].(map[string]any)
	reader["credentials"] = map[string]any{"username": "unit", "password_env": "GO_NATIVE_PASSWORD"}
	data, _ := json.Marshal(object)
	os.Unsetenv("GO_NATIVE_PASSWORD")
	_, err := e.Submit(background, data)
	requireCode(t, err, "INVALID_CONFIG")
	t.Setenv("GO_NATIVE_PASSWORD", "native-env-秘密")
	e.CancelRequest(background, "env-request")
	_, err = e.Submit(background, data)
	requireCode(t, err, "REQUEST_CANCELLED")
}
func TestInvalidConfigurations(t *testing.T) {
	e, _ := openTest(t)
	for _, data := range []json.RawMessage{nil, []byte("null"), []byte("[]"), []byte("{\"reader\":"), []byte("{\"unexpected\":1}")} {
		_, err := e.Submit(background, data)
		var failure *Error
		if !errors.As(err, &failure) || failure.Code == "" || failure.Message == "" || failure.CommitUnknown || failure.Retryable {
			t.Fatalf("invalid JSON returned wrong error: %v", err)
		}
	}
	var object map[string]any
	json.Unmarshal(job(1, "unknown"), &object)
	object["unknown_field"] = true
	data, _ := json.Marshal(object)
	_, err := e.Submit(background, data)
	if err == nil {
		t.Fatal("unknown field accepted")
	}
	runs, err := e.ListRuns(background, 50, 0)
	if err != nil || len(runs) != 0 {
		t.Fatal("invalid configs created a run")
	}
}
func TestCancellationTombstonePersists(t *testing.T) {
	e, path := openTest(t)
	request, err := e.CancelRequest(background, "unsubmitted-request")
	if err != nil || !request.CancelRequested || request.Run != nil {
		t.Fatalf("%+v %v", request, err)
	}
	e.Close(background)
	next, err := Open(background, path)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(background)
	request, err = next.GetRequest(background, "unsubmitted-request")
	if err != nil || !request.CancelRequested || request.Run != nil {
		t.Fatal("tombstone lost")
	}
	_, err = next.Submit(background, job(1, "unsubmitted-request"))
	requireCode(t, err, "REQUEST_CANCELLED")
}
func TestIdempotenceConflictRedactionAndCancel(t *testing.T) {
	p := stalled(t)
	e, _ := openTest(t)
	spec := peerJob(p, "stable-request")
	run, err := e.Submit(background, spec)
	if err != nil {
		t.Fatal(err)
	}
	p.connected(t)
	repeated, err := e.Submit(background, spec)
	if err != nil || repeated.RunID != run.RunID {
		t.Fatalf("stable request replay: %v %v", repeated, err)
	}
	if strings.Contains(string(run.Config), "unit-secret") {
		t.Fatal("configuration leaked credentials")
	}
	var change map[string]any
	json.Unmarshal(spec, &change)
	change["writer"].(map[string]any)["table"] = "different"
	conflict, _ := json.Marshal(change)
	_, err = e.Submit(background, conflict)
	requireCode(t, err, "CONFLICT")
	request, err := e.GetRequest(background, "stable-request")
	if err != nil || request.Run == nil || request.Run.RunID != run.RunID {
		t.Fatal("request lookup did not find accepted run")
	}
	if _, err = e.CancelRun(background, run.RunID); err != nil {
		t.Fatal(err)
	}
	terminal, err := e.WaitForCompletion(background, run.RunID, WithWaitTimeout(10*time.Second), WithPollInterval(10*time.Millisecond))
	if err != nil || terminal.State != "CANCELLED" {
		t.Fatalf("cancelled state: %+v %v", terminal, err)
	}
	if terminal.RowsCommitted != 0 || terminal.CommitUnknown || terminal.PartialWrite {
		t.Fatal("uncommitted cancellation changed audit counts")
	}
	if _, err = e.ListBatches(background, run.RunID); err != nil {
		t.Fatal(err)
	}
}
func TestWaitTimeoutPreservesLastRun(t *testing.T) {
	p := stalled(t)
	e, _ := openTest(t)
	run, err := e.Run(background, peerJob(p, "timed-wait"), WithWaitTimeout(30*time.Millisecond), WithPollInterval(time.Millisecond))
	var timeout *WaitTimeoutError
	if run == nil || !errors.As(err, &timeout) || !errors.Is(err, context.DeadlineExceeded) || timeout.RunID != run.RunID {
		t.Fatalf("last run lost: %+v %v", run, err)
	}
	p.connected(t)
	current, err := e.GetRun(background, run.RunID)
	if err != nil || current.Terminal() {
		t.Fatal("timeout stopped job")
	}
	e.CancelRun(background, run.RunID)
}
func TestContextCancellationStopsOnlyWait(t *testing.T) {
	p := stalled(t)
	e, _ := openTest(t)
	run, err := e.Submit(background, peerJob(p, "ctx-wait"))
	if err != nil {
		t.Fatal(err)
	}
	p.connected(t)
	ctx, cancel := context.WithCancel(background)
	done := make(chan callResult, 1)
	go func() {
		last, err := e.WaitForCompletion(ctx, run.RunID, WithPollInterval(time.Millisecond))
		if last == nil {
			err = errors.New("last known run missing")
		}
		done <- callResult{err: err}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if got := <-done; !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	current, err := e.GetRun(background, run.RunID)
	if err != nil || current.Terminal() {
		t.Fatal("context cancellation stopped job")
	}
	e.CancelRequest(background, "ctx-wait")
}
func TestCloseTimeoutRetryDuringValidation(t *testing.T) {
	p := stalled(t)
	e, path := openTest(t, WithShutdownTimeout(time.Millisecond))
	done := make(chan error, 1)
	go func() { _, err := e.Validate(background, peerJob(p, "validate")); done <- err }()
	p.connected(t)
	var timeout *CloseTimeoutError
	if err := e.Close(background); !errors.As(err, &timeout) {
		t.Fatalf("close didn't retain pending validation: %v", err)
	}
	if _, err := e.ListRuns(background, 50, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.CancelRequest(background, "after-close-timeout"); err != nil {
		t.Fatal(err)
	}
	_, err := e.Submit(background, peerJob(p, "closed-admission"))
	requireCode(t, err, "ENGINE_CLOSING")
	_, err = Open(background, path)
	requireCode(t, err, "STATE_STORE")
	p.release()
	<-done
	deadline := time.Now().Add(3 * time.Second)
	for {
		err = e.Close(background)
		if err == nil {
			break
		}
		if !errors.As(err, &timeout) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	other, err := Open(background, path)
	if err != nil {
		t.Fatal(err)
	}
	other.Close(background)
}
func TestCancelledValidationKeepsGoroutinesResponsive(t *testing.T) {
	p := stalled(t)
	e, _ := openTest(t)
	ctx, cancel := context.WithCancel(background)
	done := make(chan error, 1)
	var ticks atomic.Uint64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				ticks.Add(1)
				runtime.Gosched()
			}
		}
	}()
	defer close(stop)
	go func() { _, err := e.Validate(ctx, peerJob(p, "validation-cancel")); done <- err }()
	p.connected(t)
	before := ticks.Load()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if ticks.Load() <= before {
		t.Fatal("blocking native call stalled Go scheduling")
	}
	if err := e.Ready(background); err != nil {
		t.Fatal(err)
	}
	p.release()
}
func TestConcurrentSubmitAndClose(t *testing.T) {
	for i := 0; i < 8; i++ {
		e, path := openTest(t)
		p := stalled(t)
		start := make(chan struct{})
		done := make(chan struct {
			run *Run
			err error
		}, 1)
		go func() {
			<-start
			run, err := e.Submit(background, peerJob(p, "race"))
			done <- struct {
				run *Run
				err error
			}{run, err}
		}()
		close(start)
		if err := e.Close(background); err != nil {
			t.Fatal(err)
		}
		result := <-done
		if result.err != nil {
			var failure *Error
			if !errors.As(result.err, &failure) {
				t.Fatal(result.err)
			}
			// A binding lease can enter before host closing, then the core's
			// admission recheck observes shutdown. Preserve that existing BUSY
			// result while distinguishing it from queue-capacity failures.
			shutdownBusy := failure.Code == "BUSY" && failure.Message == "Service is shutting down"
			if (!shutdownBusy && failure.Code != "ENGINE_CLOSED" && failure.Code != "ENGINE_CLOSING") || failure.CommitUnknown || failure.Retryable {
				t.Fatal(result.err)
			}
		}
		// Successful close must release the store and fully audit every accepted
		// run. Reopening would reveal INTERRUPTED if shutdown missed a submit.
		reopened, err := Open(background, path)
		if err != nil {
			t.Fatal(err)
		}
		runs, err := reopened.ListRuns(background, 50, 0)
		closeErr := reopened.Close(background)
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if result.err == nil {
			if result.run == nil || len(runs) != 1 || runs[0].RunID != result.run.RunID || runs[0].State != "CANCELLED" || runs[0].RowsCommitted != 0 || runs[0].CommitUnknown {
				t.Fatalf("close missed accepted submission: %+v", runs)
			}
		} else if len(runs) != 0 {
			t.Fatalf("rejected submission persisted a run: %+v", runs)
		}
	}
}
func TestConcurrentCloseIsIdempotent(t *testing.T) {
	e, _ := openTest(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Close(background); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
func TestCancelledOpenAndGCReleaseStateStore(t *testing.T) {
	ctx, cancel := context.WithCancel(background)
	cancel()
	_, err := Open(ctx, filepath.Join(t.TempDir(), "cancelled.sqlite"))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "cleanup.sqlite")
	func() {
		engine, err := Open(background, path)
		if err != nil {
			t.Fatal(err)
		}
		if engine.StateStoreID() == "" {
			t.Fatal("empty identity")
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		engine, err := Open(background, path)
		if err == nil {
			engine.Close(background)
			break
		}
		requireCode(t, err, "STATE_STORE")
		if time.Now().After(deadline) {
			t.Fatal("GC cleanup retained store")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestMetadataAndExactUint64JSON(t *testing.T) {
	e, _ := openTest(t)
	info, err := e.Connectors(background)
	if err != nil || len(info.Connectors) != 2 || len(info.RequestSchema) == 0 || Version != "0.1.0" {
		t.Fatalf("metadata: %v %v", info, err)
	}
	schema, err := RequestSchema(background)
	if err != nil || !json.Valid(schema) {
		t.Fatal(err)
	}
	var run Run
	if err = json.Unmarshal([]byte(`{"run_id":"counter","request_id":null,"state":"FAILED","rows_read":18446744073709551615,"rows_committed":9007199254740993,"error":{"code":"MYSQL","message":"lost commit","commit_unknown":true,"retryable":true},"config":{"integer":18446744073709551615}}`), &run); err != nil {
		t.Fatal(err)
	}
	if run.RowsRead != math.MaxUint64 || run.RowsCommitted != 9007199254740993 || run.RequestID != nil || run.Error == nil || !run.Error.CommitUnknown || !run.Terminal() || !strings.Contains(string(run.Config), "18446744073709551615") {
		t.Fatal("typed JSON lost exact audit values")
	}
}
func TestOptionsAndPreCancelledCalls(t *testing.T) {
	for _, options := range [][]Option{{WithMaxRunning(0)}, {WithMaxRunning(65)}, {WithMaxQueued(-1)}, {WithMaxQueued(4097)}, {WithShutdownTimeout(-1)}, {WithNativeCacheDir("")}, {nil}} {
		_, err := Open(background, filepath.Join(t.TempDir(), "state.sqlite"), options...)
		requireCode(t, err, "INVALID_CONFIG")
	}
	for _, path := range []string{"", ":memory:", "file:state.sqlite?mode=memory"} {
		_, err := Open(background, path)
		requireCode(t, err, "INVALID_CONFIG")
	}
	e, _ := openTest(t)
	ctx, cancel := context.WithCancel(background)
	cancel()
	_, err := e.Submit(ctx, job(1, "never-accepted"))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, err = e.GetRequest(background, "never-accepted")
	requireCode(t, err, "NOT_FOUND")
	for _, opts := range [][]WaitOption{{WithWaitTimeout(-1)}, {WithPollInterval(0)}, {nil}} {
		_, err = e.Run(background, job(1, "invalid-wait"), opts...)
		requireCode(t, err, "INVALID_CONFIG")
	}
	_, err = e.ListRuns(background, 0, 0)
	requireCode(t, err, "INVALID_CONFIG")
}
