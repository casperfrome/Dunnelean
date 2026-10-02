// Package dunnelean embeds the Rust Arrow synchronization engine in a Go process.
// It does not need an HTTP service, cgo, or an installed Rust toolchain.
package dunnelean

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Version is the version of this Go package and its bundled engine.
const Version = "0.1.0"

// Error describes an API failure or a failed run. Retryable does not authorize
// replaying a whole job, particularly when CommitUnknown is true.
type Error struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CommitUnknown bool   `json:"commit_unknown"`
	Retryable     bool   `json:"retryable"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// WaitTimeoutError ends the local wait; it does not cancel the job.
type WaitTimeoutError struct {
	RunID   string
	Timeout time.Duration
}

func (e *WaitTimeoutError) Error() string {
	return fmt.Sprintf("WAIT_TIMEOUT: run %s did not finish within %s", e.RunID, e.Timeout)
}
func (e *WaitTimeoutError) Unwrap() error { return context.DeadlineExceeded }

// As exposes the same structured error contract as native call failures.
func (e *WaitTimeoutError) As(target any) bool {
	value, ok := target.(**Error)
	if ok {
		*value = &Error{Code: "WAIT_TIMEOUT", Message: fmt.Sprintf("run %s did not finish within %s", e.RunID, e.Timeout)}
	}
	return ok
}

// CloseTimeoutError means the engine still owns its state database. Queries,
// cancellation and another Close remain available, but new work is rejected.
type CloseTimeoutError struct{ Timeout time.Duration }

func (e *CloseTimeoutError) Error() string {
	return fmt.Sprintf("CLOSE_TIMEOUT: engine did not finish closing within %s", e.Timeout)
}
func (e *CloseTimeoutError) Unwrap() error { return context.DeadlineExceeded }
func (e *CloseTimeoutError) As(target any) bool {
	value, ok := target.(**Error)
	if ok {
		*value = &Error{Code: "CLOSE_TIMEOUT", Message: fmt.Sprintf("engine did not finish closing within %s", e.Timeout)}
	}
	return ok
}

// Run is a complete persisted audit record. Config contains redacted JSON.
type Run struct {
	RunID              string          `json:"run_id"`
	RequestID          *string         `json:"request_id"`
	State              string          `json:"state"`
	Stage              string          `json:"stage"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
	RowsRead           uint64          `json:"rows_read"`
	BytesRead          uint64          `json:"bytes_read"`
	RowsSubmitted      uint64          `json:"rows_submitted"`
	RowsCommitted      uint64          `json:"rows_committed"`
	RowsFiltered       uint64          `json:"rows_filtered"`
	ServerAffectedRows uint64          `json:"server_affected_rows"`
	BatchesCommitted   uint64          `json:"batches_committed"`
	PartialWrite       bool            `json:"partial_write"`
	CommitUnknown      bool            `json:"commit_unknown"`
	Error              *Error          `json:"error"`
	Config             json.RawMessage `json:"config"`
}

// Terminal reports whether the engine has finished auditing this run.
func (r *Run) Terminal() bool {
	if r == nil {
		return false
	}
	switch r.State {
	case "SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED":
		return true
	}
	return false
}

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type Validation struct {
	Valid        bool    `json:"valid"`
	SourceSchema []Field `json:"source_schema"`
	TargetSchema []Field `json:"target_schema"`
	Semantics    string  `json:"semantics"`
}
type RequestStatus struct {
	RequestID       string `json:"request_id"`
	CancelRequested bool   `json:"cancel_requested"`
	Run             *Run   `json:"run"`
}

// Batch holds commit receipts, not the data transported by the engine.
type Batch struct {
	BatchID uint64  `json:"batch_id"`
	State   string  `json:"state"`
	Rows    uint64  `json:"rows"`
	Bytes   uint64  `json:"bytes"`
	Label   *string `json:"label"`
	Detail  *string `json:"detail"`
}

// ConnectorInfo retains Rust-generated capability and JSON Schema documents.
// RawMessage fields preserve integer precision and future connector properties.
type ConnectorInfo struct {
	Connectors    []json.RawMessage `json:"connectors"`
	RequestSchema json.RawMessage   `json:"request_schema"`
	Limits        json.RawMessage   `json:"limits"`
}
