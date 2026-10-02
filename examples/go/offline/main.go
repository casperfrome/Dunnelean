// Offline smoke test: opens the embedded Rust engine without database servers.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
)

func run() error {
	cache := flag.String("native-cache", "", "directory for bundled native libraries")
	flag.Parse()
	directory, err := os.MkdirTemp("", "dunnelean-go-offline-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	options := []dunnelean.Option{}
	if *cache != "" {
		options = append(options, dunnelean.WithNativeCacheDir(*cache))
	}
	ctx := context.Background()
	path := filepath.Join(directory, "state.sqlite")
	engine, err := dunnelean.Open(ctx, path, options...)
	if err != nil {
		return err
	}
	defer engine.Close(ctx)
	if err := engine.Ready(ctx); err != nil {
		return err
	}
	id := engine.StateStoreID()
	if id == "" {
		return fmt.Errorf("native engine returned an empty state identity")
	}
	request, err := engine.CancelRequest(ctx, "offline-smoke-cancel")
	if err != nil || !request.CancelRequested || request.Run != nil {
		return fmt.Errorf("native cancellation audit failed: %v", err)
	}
	if err := engine.Close(ctx); err != nil {
		return err
	}
	reopened, err := dunnelean.Open(ctx, path, options...)
	if err != nil {
		return err
	}
	defer reopened.Close(ctx)
	if reopened.StateStoreID() != id {
		return fmt.Errorf("native state identity changed after reopening")
	}
	persisted, err := reopened.GetRequest(ctx, "offline-smoke-cancel")
	if err != nil || !persisted.CancelRequested {
		return fmt.Errorf("native cancellation audit did not persist: %v", err)
	}
	if err := reopened.Close(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"version": dunnelean.Version, "state_store_id": id,
		"pid": os.Getpid(), "offline": true, "reopened": true,
	})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
