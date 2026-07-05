// Package remote implements a small, generic RPC library over TCP with gob
// serialization. It lets a caller invoke methods on an object instance that
// lives in another process, with all of the network interaction hidden behind
// an ordinary Go struct of function fields (the "service interface").
//
// # Service interfaces
//
// A service interface is a struct whose every field is a function. Each
// function's final return value must be [RemoteError]; this is how the library
// distinguishes a stub-level failure (connection dropped, decode error, method
// not found) from an application-level error returned by the implementation.
//
//	type MyService struct {
//	    Add func(int, int) (int, remote.RemoteError)
//	}
//
// The same struct type is shared by both endpoints: the callee validates that
// its concrete object implements the named methods, and the caller has each
// function field populated with a network-backed implementation via reflection.
//
// # Callee (server)
//
//	callee, err := remote.NewCalleeStub(&MyService{}, &MyServiceImpl{}, "localhost:9999", false, false)
//	if err != nil { /* interface was not a valid service interface */ }
//	callee.Start()        // non-blocking; begins accepting connections
//	defer callee.Stop()   // graceful shutdown
//
// [NewCalleeStub] rejects nil arguments and any interface whose fields are not
// functions ending in [RemoteError]. [CalleeStub.Start] spins up a TCP listener
// and serves each accepted connection in its own goroutine, so multiple callers
// may be in flight simultaneously. [CalleeStub.Stop] closes the listener; the
// server may be restarted with a subsequent Start.
//
// # Caller (client)
//
//	stub := &MyService{}
//	err := remote.CallerStubCreator(stub, "localhost:9999", false, false)
//	result, rerr := stub.Add(1, 2)
//
// [CallerStubCreator] uses reflection to replace each function field with an
// implementation that: encodes the arguments, dials the callee, sends a
// [RequestMsg], and decodes the [ReplyMsg] into the declared return types. The
// caller retries transparently on packet loss (see LeakySocket) until a reply
// is decoded, so a lossy channel is handled without caller involvement.
//
// # Wire format
//
// Each call opens a fresh TCP connection. The caller sends one gob-encoded
// [RequestMsg] (method name plus per-argument gob blobs) and reads back one
// gob-encoded [ReplyMsg]. Return values that satisfy the error interface are
// transmitted as their message string, because concrete error values are not
// generally gob-serializable; the caller reconstructs them with fmt.Errorf.
//
// # Reliability emulation
//
// [LeakySocket] wraps a net.Conn and can emulate packet loss and propagation
// delay, which the test suite uses to stress-test the retry logic. It is
// configured through the isLossy/isDelayed flags passed to the constructors and
// tuned with SetLossRate, SetDelay, and SetTimeout.
//
// # Error handling
//
//   - A non-empty [RemoteError] signals a stub-level failure and means the
//     application method may not have run at all.
//   - An application error is returned in the interface's own error position and
//     is independent of RemoteError.
//   - Callers should check both: a method can return an application error while
//     RemoteError is empty, and vice versa.
package remote
