package dunnelean

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func compressedAsset(t *testing.T) (asset, []byte) {
	t.Helper()
	payload := bytes.Repeat([]byte("deterministic-native-library"), 1024)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return asset{GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Library: "test-native.bin", Size: int64(len(payload)), SHA256: digest(payload), CompressedSize: int64(compressed.Len()), CompressedSHA256: digest(compressed.Bytes())}, compressed.Bytes()
}
func TestConcurrentCachePublicationAndIntegrity(t *testing.T) {
	a, compressed := compressedAsset(t)
	base := t.TempDir()
	var wg sync.WaitGroup
	paths := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path, err := extractAsset(base, a, compressed)
			if err != nil {
				t.Error(err)
				return
			}
			paths <- path
		}()
	}
	wg.Wait()
	close(paths)
	var path string
	for got := range paths {
		if path != "" && path != got {
			t.Fatal("concurrent extraction used different paths")
		}
		path = got
	}
	if path == "" {
		t.Fatal("no library extracted")
	}
	if err := verifyFile(path, a); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), int(a.Size)), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := extractAsset(base, a, compressed)
	requireCode(t, err, "NATIVE_INTEGRITY")
}
func TestCacheRejectsCorruptEmbeddedAssets(t *testing.T) {
	a, compressed := compressedAsset(t)
	changed := append([]byte(nil), compressed...)
	changed[len(changed)/2] ^= 1
	_, err := extractAsset(t.TempDir(), a, changed)
	requireCode(t, err, "NATIVE_INTEGRITY")
	wrong := a
	wrong.Size++
	_, err = extractAsset(t.TempDir(), wrong, compressed)
	requireCode(t, err, "NATIVE_INTEGRITY")
	wrong = a
	wrong.SHA256 = digest([]byte("wrong"))
	_, err = extractAsset(t.TempDir(), wrong, compressed)
	requireCode(t, err, "NATIVE_INTEGRITY")
}
func TestCacheDirectoryFailure(t *testing.T) {
	a, compressed := compressedAsset(t)
	base := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(base, []byte("cannot create child directories"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := extractAsset(base, a, compressed)
	requireCode(t, err, "NATIVE_CACHE")
}
func TestUnwritableCacheDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows directory permissions require ACLs; path access failures covered separately")
	}
	a, compressed := compressedAsset(t)
	base := t.TempDir()
	if err := os.Chmod(base, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(base, 0700)
	probe, err := os.CreateTemp(base, "permission-probe-*")
	if err == nil {
		probe.Close()
		os.Remove(probe.Name())
		t.Skip("current account bypasses filesystem write permissions")
	}
	_, err = extractAsset(base, a, compressed)
	requireCode(t, err, "NATIVE_CACHE")
}
func TestFreshCustomCacheLoadsActualEngine(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "libraries")
	e, _ := openTest(t, WithNativeCacheDir(cache))
	if err := e.Ready(background); err != nil {
		t.Fatal(err)
	}
	_, a, err := platformAsset()
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyFile(filepath.Join(cache, Version, a.GOOS+"_"+a.GOARCH, a.SHA256, a.Library), a); err != nil {
		t.Fatal(err)
	}
}
