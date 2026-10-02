# Dunnelean for Go

Run the Rust Arrow synchronization engine directly inside a Go process. No HTTP
service, Python environment, Rust installation, C compiler, or `cgo` is required
by applications installing this module. The module bundles the native engine for
Windows/Linux amd64 and macOS amd64/arm64.

```sh
go get github.com/casperfrome/Dunnelean/sdk/go@v0.1.0
```

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"

    dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
)

func main() {
    ctx := context.Background()
    engine, err := dunnelean.Open(ctx, "var/jobs.sqlite")
    if err != nil { log.Fatal(err) }
    defer func() {
        if err := engine.Close(context.Background()); err != nil { log.Print(err) }
    }()
    spec, err := os.ReadFile("mysql-to-doris.json")
    if err != nil { log.Print(err); return }
    run, err := engine.Run(ctx, json.RawMessage(spec))
    if err != nil { log.Print(err); return }
    log.Printf("run=%s state=%s committed=%d error=%v", run.RunID, run.State, run.RowsCommitted, run.Error)
}
```

Go 1.25+ is required. Linux requires glibc 2.17+; Alpine/musl is outside the first
release. Windows requires Windows 10+/Server 2016+. macOS requires 12+ with Go
1.25/1.26, or 13+ with Go 1.27. The runtime extracts a verified embedded library
to the user cache; `WithNativeCacheDir` can choose another writable location.

Configuration is JSON, validated by Rust. A failed job is a terminal `Run` with
`Run.Error`; an API failure is a Go `error`. Context cancellation stops a local
wait. Stop a job explicitly with `CancelRun` or `CancelRequest`. Keep stable
request IDs to recover submissions whose results were not received.

See the [complete guide](https://github.com/casperfrome/Dunnelean/blob/main/docs/go-sdk.md)
for installation, configuration, goroutines, idempotency, cancellation, reliable
shutdown, cache rules, examples, and maintainer release checks.

Licensed under Apache-2.0; see [LICENSE](LICENSE).
