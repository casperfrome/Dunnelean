//go:build windows

package dunnelean

import (
	"syscall"
	"unsafe"
)

var kernel32 = syscall.NewLazyDLL("kernel32.dll")
var loadLibraryExW = kernel32.NewProc("LoadLibraryExW")

func loadLibrary(path string) (uintptr, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	// Only this library's directory and Windows system libraries participate.
	handle, _, err := loadLibraryExW.Call(uintptr(unsafe.Pointer(name)), 0, 0x100|0x800)
	if handle == 0 {
		return 0, err
	}
	return handle, nil
}
func librarySymbol(handle uintptr, name string) (uintptr, error) {
	return syscall.GetProcAddress(syscall.Handle(handle), name)
}
