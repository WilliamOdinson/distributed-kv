package remote

// testkit_test.go holds the shared fixtures used across the remote test suite:
// the sample service interfaces/instances that stand in for a real application,
// a collision-free port allocator, and helpers that spin a CalleeStub up and
// tear it down. Keeping this scaffolding in one place lets the actual test
// files read as a list of behaviors rather than a wall of setup.

import (
	"net"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Sample service interface and implementation
// ---------------------------------------------------------------------------

// testService is a well-formed service interface: every field is a function
// whose last return value is RemoteError. It exercises argument passing,
// application errors, concurrency, and non-trivial composite types.
type testService struct {
	// Echo returns value and "" normally, or -value and a non-empty error
	// string when returnError is true. RemoteError is always empty.
	Echo func(value int, returnError bool) (int, string, RemoteError)
	// PairUp blocks until a second concurrent call arrives, proving the
	// callee serves connections concurrently rather than serially.
	PairUp func() RemoteError
	// SumLengths returns, for each map entry, the total length of its strings.
	SumLengths func(map[string][]string) ([]int, RemoteError)
	// MayFail returns a real error value (fail==true) or nil (fail==false).
	// It exercises the special handling for return values that satisfy the
	// error interface on both the callee (encode as string) and caller
	// (reconstruct via fmt.Errorf) sides.
	MayFail func(fail bool) (error, RemoteError)
}

// testInstance implements the methods named by testService.
type testInstance struct {
	mu   sync.Mutex
	wake bool
	wg   sync.WaitGroup
}

func (o *testInstance) Echo(value int, returnError bool) (int, string, RemoteError) {
	if returnError {
		return -value, "This is an error.", RemoteError{}
	}
	return value, "", RemoteError{}
}

func (o *testInstance) PairUp() RemoteError {
	o.mu.Lock()
	if !o.wake {
		o.wg.Add(1)
		o.wake = true
		o.mu.Unlock()
		o.wg.Wait()
	} else {
		o.mu.Unlock()
		o.wg.Done()
	}
	return RemoteError{}
}

func (o *testInstance) SumLengths(dict map[string][]string) ([]int, RemoteError) {
	var lengths []int
	for _, entry := range dict {
		c := 0
		for _, val := range entry {
			c += len(val)
		}
		lengths = append(lengths, c)
	}
	return lengths, RemoteError{}
}

func (o *testInstance) MayFail(fail bool) (error, RemoteError) {
	if fail {
		return errFromMethod, RemoteError{}
	}
	return nil, RemoteError{}
}

// errFromMethod is the sentinel returned by testInstance.MayFail(true).
var errFromMethod = errorString("method failed")

type errorString string

func (e errorString) Error() string { return string(e) }

// ---------------------------------------------------------------------------
// Invalid / mismatched interfaces used for negative tests
// ---------------------------------------------------------------------------

// invalidService has a method that does not end in RemoteError, so both stub
// constructors must reject it.
type invalidService struct {
	Echo func(int, bool) (int, string)
}

type invalidInstance struct{}

func (o *invalidInstance) Echo(value int, returnError bool) (int, string) {
	if returnError {
		return -value, "This is an error."
	}
	return value, ""
}

// mismatchService has, relative to testService, a wrong argument type on Echo,
// an extra return value on PairUp, and a method the callee does not implement.
type mismatchService struct {
	Echo        func(int, int) (int, string, RemoteError)
	PairUp      func() (int, RemoteError)
	ExtraMethod func() RemoteError
}

// mismatchService2 has an extra argument on Echo relative to testService.
type mismatchService2 struct {
	Echo   func(int, bool, int) (int, string, RemoteError)
	PairUp func() RemoteError
}

// notAStruct is used to prove the constructors reject non-struct interfaces.
type notAStruct int

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// freeAddr asks the kernel for an unused localhost port and returns it as an
// "ip:port" string. Binding to :0 and reading back the assigned port avoids the
// flaky "pick a random port and hope" pattern that can collide across tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("could not reserve a free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// probe reports whether a TCP listener is accepting connections at addr.
func probe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// waitFor polls cond until it is true or the deadline elapses. It returns the
// final value of cond so callers can assert on it.
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// startedCallee creates a CalleeStub for testService/testInstance, starts it,
// waits until it is accepting connections, and registers cleanup so the caller
// never has to remember to Stop it. It fails the test on any error.
//
// Progress is narrated with t.Logf; that output is buffered and shown only
// under `go test -v` (or when the test fails), giving a step-by-step trace for
// debugging without cluttering ordinary runs.
func startedCallee(t *testing.T, addr string, lossy, delayed bool) *CalleeStub {
	t.Helper()
	t.Logf("callee: creating stub at %s (lossy=%v delayed=%v)", addr, lossy, delayed)
	callee, err := NewCalleeStub(&testService{}, &testInstance{}, addr, lossy, delayed)
	if err != nil {
		t.Fatalf("NewCalleeStub failed for a valid service interface: %v", err)
	}
	if callee == nil {
		t.Fatal("NewCalleeStub returned a nil callee for a valid service interface")
	}
	if err := callee.Start(); err != nil {
		t.Fatalf("CalleeStub.Start failed: %v", err)
	}
	if !waitFor(callee.IsRunning, 5*time.Second) {
		t.Fatal("timed out waiting for CalleeStub to start")
	}
	t.Logf("callee: started and accepting connections at %s", addr)
	cs := callee.(*CalleeStub)
	t.Cleanup(func() { cs.Stop() })
	return cs
}

// dialCaller builds a caller stub bound to addr and fails the test on error.
func dialCaller(t *testing.T, addr string, lossy, delayed bool) *testService {
	t.Helper()
	t.Logf("caller: creating stub targeting %s (lossy=%v delayed=%v)", addr, lossy, delayed)
	caller := &testService{}
	if err := CallerStubCreator(caller, addr, lossy, delayed); err != nil {
		t.Fatalf("CallerStubCreator failed for a valid service interface: %v", err)
	}
	return caller
}
