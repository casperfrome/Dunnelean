package dunnelean

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

const (
	opValidate uint32 = iota + 1
	opSubmit
	opGetRun
	opCancelRun
	opListRuns
	opGetRequest
	opCancelRequest
	opListBatches
	opStateStoreID
	opReady
	opConnectors
)

type nativeAPI struct {
	library uintptr
	abi     func() uint32
	version func(**byte, *uintptr) int32
	open    func(*byte, uintptr, *uint64, **byte, *uintptr) int32
	invoke  func(uint64, uint32, *byte, uintptr, **byte, *uintptr) int32
	close   func(uint64, uint64, **byte, *uintptr) int32
	release func(uint64) int32
	free    func(*byte, uintptr)
}

var libraries = struct {
	sync.Mutex
	entries map[string]*nativeAPI
}{entries: map[string]*nativeAPI{}}

func loadNative(base string) (*nativeAPI, error) {
	path, err := nativePath(base)
	if err != nil {
		return nil, err
	}
	libraries.Lock()
	defer libraries.Unlock()
	if api := libraries.entries[path]; api != nil {
		return api, nil
	}
	handle, err := loadLibrary(path)
	if err != nil {
		return nil, nativeError("NATIVE_LOAD", err)
	}
	api := &nativeAPI{library: handle}
	for _, binding := range []struct {
		name string
		fn   any
	}{
		{"dunnelean_abi_version", &api.abi}, {"dunnelean_native_version", &api.version},
		{"dunnelean_engine_open", &api.open}, {"dunnelean_engine_invoke", &api.invoke},
		{"dunnelean_engine_close", &api.close}, {"dunnelean_engine_release", &api.release}, {"dunnelean_buffer_free", &api.free},
	} {
		symbol, err := librarySymbol(handle, binding.name)
		if err != nil {
			return nil, nativeError("NATIVE_ABI", err)
		}
		purego.RegisterFunc(binding.fn, symbol)
	}
	if err := api.checkVersion(); err != nil {
		return nil, err
	}
	// Native threads and process-global code may outlive an Engine. Never unload.
	libraries.entries[path] = api
	return api, nil
}
func (api *nativeAPI) checkVersion() error {
	if api.abi() != abiVersion {
		return &Error{Code: "NATIVE_ABI", Message: "bundled engine ABI version does not match Go package"}
	}
	var out *byte
	var length uintptr
	status := api.version(&out, &length)
	data, err := api.result(status, out, length)
	if err != nil {
		return err
	}
	var info struct {
		Version string `json:"version"`
		ABI     uint32 `json:"abi_version"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return nativeError("NATIVE_ABI", err)
	}
	if info.Version != Version || info.ABI != abiVersion {
		return &Error{Code: "NATIVE_ABI", Message: "bundled engine version does not match Go package"}
	}
	return nil
}
func (api *nativeAPI) result(status int32, out *byte, length uintptr) ([]byte, error) {
	if out != nil {
		defer api.free(out, length)
	}
	if out == nil || length == 0 || length > uintptr(^uint(0)>>1) {
		return nil, &Error{Code: "NATIVE_ABI", Message: "native engine returned an invalid JSON buffer"}
	}
	data := append([]byte(nil), unsafe.Slice(out, int(length))...)
	if status != 0 {
		var failure Error
		if err := json.Unmarshal(data, &failure); err != nil {
			return nil, nativeError("NATIVE_ABI", err)
		}
		if failure.Code == "" {
			return nil, nativeError("NATIVE_ABI", fmt.Errorf("native failure omitted its code"))
		}
		return nil, &failure
	}
	return data, nil
}
func (api *nativeAPI) call(handle uint64, op uint32, input []byte) ([]byte, error) {
	var out *byte
	var length uintptr
	var ptr *byte
	if len(input) > 0 {
		ptr = &input[0]
	}
	status := api.invoke(handle, op, ptr, uintptr(len(input)), &out, &length)
	runtime.KeepAlive(input)
	return api.result(status, out, length)
}
