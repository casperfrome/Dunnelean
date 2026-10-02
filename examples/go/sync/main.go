// Run from examples/go: go run ./sync -spec ../mysql-to-doris.json
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
)

func run() (resultErr error) {
	config := flag.String("spec", "", "UTF-8 RunSpec JSON file (required)")
	state := flag.String("state", "var/go-jobs.sqlite", "persistent SQLite state file")
	requestID := flag.String("request-id", "", "stable request ID; generated when omitted")
	validateOnly := flag.Bool("validate-only", false, "validate connections and mappings without submitting")
	flag.Parse()
	if *config == "" {
		return fmt.Errorf("-spec is required")
	}
	data, err := os.ReadFile(*config)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("-spec must contain a JSON object")
	}
	if *requestID == "" {
		*requestID = "go_" + rand.Text()
	}
	fields["request_id"], err = json.Marshal(*requestID)
	if err != nil {
		return err
	}
	spec, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	ctx := context.Background()
	engine, err := dunnelean.Open(ctx, *state)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := engine.Close(closeCtx); err != nil && resultErr == nil {
			resultErr = err
		}
	}()
	fmt.Fprintf(os.Stderr, "request_id=%s state_store_id=%s\n", *requestID, engine.StateStoreID())
	if *validateOnly {
		validation, err := engine.Validate(ctx, spec)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(validation)
	}
	run, err := engine.Run(ctx, spec)
	if run != nil {
		if outputErr := json.NewEncoder(os.Stdout).Encode(run); outputErr != nil {
			return outputErr
		}
	}
	if err != nil {
		return err
	}
	if run.State != "SUCCEEDED" {
		return fmt.Errorf("run %s ended in %s: %v", run.RunID, run.State, run.Error)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
