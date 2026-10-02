//go:build linux || darwin

package dunnelean

import "github.com/ebitengine/purego"

func loadLibrary(path string) (uintptr, error) {
	return purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_LOCAL)
}
func librarySymbol(handle uintptr, name string) (uintptr, error) { return purego.Dlsym(handle, name) }
