//go:build !windows && !linux && !darwin

package dunnelean

func loadLibrary(string) (uintptr, error) {
	return 0, &Error{Code: "UNSUPPORTED_PLATFORM", Message: "unsupported native platform"}
}
func librarySymbol(uintptr, string) (uintptr, error) {
	return 0, &Error{Code: "UNSUPPORTED_PLATFORM", Message: "unsupported native platform"}
}
