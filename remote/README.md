# remote

A generic RPC library over TCP with `gob` serialization. Allows a caller to invoke methods on an object instance hosted on a remote machine, with the network interaction fully hidden behind a stub interface.

Connections are wrapped in `LeakySocket`, which can emulate packet loss and propagation delay for stress testing.

## Concepts

A **service interface** is a Go struct whose fields are function signatures. Every function must return `remote.RemoteError` as its last value, which signals a stub-level failure distinct from application-level errors.

```go
type MyService struct {
    Add func(int, int) (int, remote.RemoteError)
}
```

## Usage

**Callee (server)**

```go
callee, err := remote.NewCalleeStub(&MyService{}, &MyServiceImpl{}, "localhost:9999", false, false)
callee.Start()
// ...
callee.Stop()
```

**Caller (client)**

```go
stub := &MyService{}
err := remote.CallerStubCreator(stub, "localhost:9999", false, false)
result, remoteErr := stub.Add(1, 2)
```

`CallerStubCreator` uses reflection to populate each function field. After the call, `stub` is a fully functional caller — no further setup needed.

## Error handling

- `RemoteError` indicates a stub-level failure (connection dropped, serialization error, etc.)
- Standard `error` return values come from the service implementation itself
- A method can return both; callers should check both

## Tests

```bash
go test -v -timeout 120s -race -cover ./...
```

The suite is split for clarity:

- `testkit_test.go`: shared fixtures, a kernel-assigned free-port allocator (no guessed ports, so no cross-test collisions), and start/dial helpers.
- `stub_validation_test.go`: table-driven interface-validation rules (fast, no network).
- `leakysocket_test.go`: direct `LeakySocket` and `RemoteError` unit tests (send/recv, loss, delay, setters).
- `lifecycle_test.go`: callee start/stop/restart and listening state.
- `rpc_test.go`: end-to-end calls, application errors, composite types, concurrency, lossy channels, mismatched interfaces.
- `errorpaths_test.go`: error-return round trips and bind/decode failures.

Goroutine failures are reported over channels rather than calling `t.Fatal` off the test goroutine (which `go vet` flags and which can be silently dropped).
