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
go test -v -timeout 120s -race ./...
```
