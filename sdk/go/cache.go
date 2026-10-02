package dunnelean

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/casperfrome/Dunnelean/sdk/go/internal/nativeassets"
)

const abiVersion = 1

type asset struct {
	GOOS             string `json:"goos"`
	GOARCH           string `json:"goarch"`
	File             string `json:"file"`
	Library          string `json:"library"`
	Size             int64  `json:"size"`
	SHA256           string `json:"sha256"`
	CompressedSize   int64  `json:"compressed_size"`
	CompressedSHA256 string `json:"compressed_sha256"`
}
type manifest struct {
	SchemaVersion int     `json:"schema_version"`
	Version       string  `json:"version"`
	ABIVersion    uint32  `json:"abi_version"`
	SourceCommit  string  `json:"source_commit"`
	SourceHash    string  `json:"source_hash"`
	Assets        []asset `json:"assets"`
}

func nativeError(code string, err error) error { return &Error{Code: code, Message: err.Error()} }
func digest(data []byte) string                { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}
func platformAsset() (manifest, asset, error) {
	var m manifest
	if err := json.Unmarshal(nativeassets.Manifest, &m); err != nil {
		return m, asset{}, nativeError("NATIVE_MANIFEST", err)
	}
	if m.SchemaVersion != 1 || m.Version != Version || m.ABIVersion != abiVersion || len(m.SourceCommit) != 40 || !validDigest(m.SourceHash) {
		return m, asset{}, invalid("bundled native manifest version or source metadata is invalid")
	}
	for _, a := range m.Assets {
		if a.GOOS == runtime.GOOS && a.GOARCH == runtime.GOARCH {
			if a.Library == "" || filepath.Base(a.Library) != a.Library || strings.ContainsAny(a.Library, "/\\") || a.Size <= 0 || a.Size > 512*1024*1024 || !validDigest(a.SHA256) || !validDigest(a.CompressedSHA256) {
				return m, a, invalid("bundled native asset metadata is invalid")
			}
			return m, a, nil
		}
	}
	return m, asset{}, &Error{Code: "UNSUPPORTED_PLATFORM", Message: runtime.GOOS + "/" + runtime.GOARCH + " has no bundled native engine"}
}
func verifyFile(path string, a asset) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != a.Size {
		return fmt.Errorf("native cache file %s has invalid type or size", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return fmt.Errorf("native cache file %s failed SHA-256 verification; remove this cache entry before retrying", path)
	}
	return nil
}

// extractAsset never overwrites a published library. Concurrent processes use a
// hard link to publish a fully written file atomically; every loser revalidates.
func extractAsset(base string, a asset, compressed []byte) (string, error) {
	if int64(len(compressed)) != a.CompressedSize || digest(compressed) != a.CompressedSHA256 {
		return "", nativeError("NATIVE_INTEGRITY", fmt.Errorf("embedded compressed engine failed length or SHA-256 verification"))
	}
	dir := filepath.Join(base, Version, a.GOOS+"_"+a.GOARCH, a.SHA256)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	path := filepath.Join(dir, a.Library)
	if _, err := os.Lstat(path); err == nil {
		if err = verifyFile(path, a); err != nil {
			return "", nativeError("NATIVE_INTEGRITY", err)
		}
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", nativeError("NATIVE_CACHE", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return "", nativeError("NATIVE_INTEGRITY", err)
	}
	defer reader.Close()
	temp, err := os.CreateTemp(dir, ".extract-*")
	if err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, h), io.LimitReader(reader, a.Size+1))
	if err != nil {
		return "", nativeError("NATIVE_INTEGRITY", err)
	}
	if written != a.Size || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return "", nativeError("NATIVE_INTEGRITY", fmt.Errorf("embedded engine failed uncompressed length or SHA-256 verification"))
	}
	if err = temp.Sync(); err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	mode := os.FileMode(0500)
	if runtime.GOOS == "windows" {
		mode = 0600
	}
	if err = temp.Chmod(mode); err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	if err = temp.Close(); err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	if err = os.Link(temp.Name(), path); err != nil {
		if _, statErr := os.Lstat(path); statErr != nil {
			return "", nativeError("NATIVE_CACHE", err)
		}
	}
	if err = verifyFile(path, a); err != nil {
		return "", nativeError("NATIVE_INTEGRITY", err)
	}
	return path, nil
}
func nativePath(base string) (string, error) {
	_, a, err := platformAsset()
	if err != nil {
		return "", err
	}
	if base == "" {
		base, err = os.UserCacheDir()
		if err != nil {
			return "", nativeError("NATIVE_CACHE", err)
		}
		base = filepath.Join(base, "dunnelean", "native")
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return "", nativeError("NATIVE_CACHE", err)
	}
	return extractAsset(base, a, nativeassets.Library)
}
