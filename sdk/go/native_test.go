package dunnelean

import (
	"errors"
	"testing"
)

func TestNativeABIVersionMismatch(t *testing.T) {
	api := &nativeAPI{abi: func() uint32 { return 99 }}
	requireCode(t, api.checkVersion(), "NATIVE_ABI")
	data := []byte(`{"version":"99.0.0","abi_version":1}`)
	freed := false
	api = &nativeAPI{abi: func() uint32 { return 1 }, version: func(out **byte, length *uintptr) int32 { *out = &data[0]; *length = uintptr(len(data)); return 0 }, free: func(*byte, uintptr) { freed = true }}
	requireCode(t, api.checkVersion(), "NATIVE_ABI")
	if !freed {
		t.Fatal("version mismatch leaked receipt")
	}
}
func TestNativeErrorFieldsAndPanicReceipt(t *testing.T) {
	for _, status := range []int32{1, 2} {
		data := []byte(`{"code":"NATIVE_PANIC","message":"operation failed","commit_unknown":true,"retryable":true}`)
		freed := false
		api := &nativeAPI{free: func(*byte, uintptr) { freed = true }}
		_, err := api.result(status, &data[0], uintptr(len(data)))
		var failure *Error
		if !errors.As(err, &failure) || failure.Message != "operation failed" || !failure.CommitUnknown || !failure.Retryable || !freed {
			t.Fatalf("native error lost fields or buffer: %+v", err)
		}
	}
}
func TestTimeoutsExposeStructuredErrors(t *testing.T) {
	for _, item := range []struct {
		err  error
		code string
	}{{&WaitTimeoutError{RunID: "pending"}, "WAIT_TIMEOUT"}, {&CloseTimeoutError{}, "CLOSE_TIMEOUT"}} {
		failure := requireCode(t, item.err, item.code)
		if failure.Message == "" || failure.CommitUnknown || failure.Retryable {
			t.Fatal("local timeout reported native commit uncertainty")
		}
	}
}
