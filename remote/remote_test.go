// summary of tests
// -- TestCheckpoint_CalleeStubInterface: CalleeStub rejects nils and non-remote interfaces
// -- TestCheckpoint_CalleeStubRuns: CalleeStub can be started, accept connections, and stopped
// -- TestFinal_CallerStubInterface: CallerStubCreator rejects nils and non-remote interfaces
// -- TestFinal_CallerStubConnects: CallerStub can connect to given address
// -- TestFinal_Connection: verifies argument passing, transmission of return values and remote exceptions
// -- TestFinal_LossyConnection: runs many calls over unreliable channel to ensure errors are handled
// -- TestFinal_Reconnection: verifies continued connection after multiple stop/start calls
// -- TestFinal_Multithread: checks that the CalleeStub supports multiple simultaneous connections
// -- TestFinal_MismatchedInterface: checks error handling for mismatched interfaces in CallerStub and CalleeStub
package remote

import (
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// interface methods supported by a remote instance
type RemoteTestInterface struct {
	// test method for sending arguments, return values, and errors.
	// @param value -- an integer value
	// @param return_error -- boolean value indicating an error string request
	// @return if return_error is true, returns -value and an Error, otherwise value and ""
	RemoteTestMethod func(int, bool) (int, string, RemoteError)
	// method used to coordinate concurrent calls to the same CalleeStub.
	PairUp func() RemoteError
	// method for testing more complex data structures as parameters
	ComplexTestMethod func(map[string][]string) ([]int, RemoteError)
}

// instance type supporting RemoteTestInterface methods
type RemoteTestInstance struct {
	mu sync.Mutex
	// set by first call, then wait until second call.
	wake bool
	// use a WaitGroup to synchronize two threads
	wg sync.WaitGroup
}

// CalleeStub implementation of the RemoteTestMethod method in RemoteTestInterface
func (obj *RemoteTestInstance) RemoteTestMethod(value int, return_error bool) (int, string, RemoteError) {
	if return_error {
		err := "This is an error."
		return -value, err, RemoteError{}
	}
	return value, "", RemoteError{}
}

// CalleeStub implementation of the PairUp method in RemoteTestInterface
func (obj *RemoteTestInstance) PairUp() RemoteError {
	obj.mu.Lock()
	if !obj.wake {
		obj.wg.Add(1)
		obj.wake = true
		obj.mu.Unlock()
		obj.wg.Wait()
	} else {
		obj.mu.Unlock()
		obj.wg.Done()
	}
	return RemoteError{}
}

// CalleeStub implementation of the ComplexTestMethod method in RemoteTestInterface
func (obj *RemoteTestInstance) ComplexTestMethod(dict map[string][]string) ([]int, RemoteError) {
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

// non-remote interface for error checking
type InvalidInterface struct {
	// test method that doesn't return a RemoteError, should be rejected by stubs.
	RemoteTestMethod func(int, bool) (int, string)
}

// instance type supporting the InvalidInterface methods
type InvalidObject struct{}

// CalleeStub implementation of the RemoteTestMethod method in InvalidInterface
func (obj *InvalidObject) RemoteTestMethod(value int, return_error bool) (int, string) {
	if return_error {
		err := "This is an error."
		return -value, err
	}
	return value, ""
}

// mismatched interface to test error handling at CalleeStub
type MismatchInterface struct {
	// mismatched method with (int, int) instead of (int, bool) parameters
	RemoteTestMethod func(int, int) (int, string, RemoteError)
	// mismatched method with (int, RemoteError) instead of RemoteError return type
	PairUp func() (int, RemoteError)
	// extra method not included in RemoteTestInterface interface
	ExtraMethod func() RemoteError
}

// another mismatched interface to test error handling at CalleeStub
type MismatchInterface2 struct {
	// mismatched method with (int, bool, int) instead of (int, bool) parameters
	RemoteTestMethod func(int, bool, int) (int, string, RemoteError)
	// mismatched method with (int, RemoteError) instead of RemoteError return type
	PairUp func() RemoteError
}

// helper function for testing whether listening socket is active
func probe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// TestCheckpoint_CalleeStubInterface -- perform basic tests on CalleeStub creation
// -- verifies that NewCalleeStub rejects non-remote interfaces and nulls
// -- verifies that NewCalleeStub accepts remote interfaces
func TestCheckpoint_CalleeStubInterface(t *testing.T) {

	// choose a large-ish random port number for each test
	addr := "localhost:" + strconv.Itoa(rand.Intn(10000)+20000)

	// should throw an error due to non-remote interface definition and instance
	_, err := NewCalleeStub(&InvalidInterface{}, &InvalidObject{}, addr, false, false)
	if err == nil {
		t.Fatalf("NewCalleeStub accepted non-remote service interface and instance")
	}

	// should throw an error due to nil interface
	_, err = NewCalleeStub(nil, &RemoteTestInstance{}, addr, false, false)
	if err == nil {
		t.Fatalf("NewCalleeStub accepted nil service interface")
	}

	// should throw an error due to nil instance
	_, err = NewCalleeStub(&RemoteTestInterface{}, nil, addr, false, false)
	if err == nil {
		t.Fatalf("NewCalleeStub accepted nil service instance")
	}

	// should work correctly with no error
	_, err = NewCalleeStub(&RemoteTestInterface{}, &RemoteTestInstance{}, addr, false, false)
	if err != nil {
		t.Fatalf("NewCalleeStub did not accept proper service interface and instance")
	}
}

// TestCheckpoint_CalleeStubRuns -- perform basic tests on CalleeStub running
// -- verifies that CalleeStub can be started, stopped, and connected to
func TestCheckpoint_CalleeStubRuns(t *testing.T) {

	// choose a large-ish random port number for each test
	addr := "localhost:" + strconv.Itoa(rand.Intn(10000)+20000)

	// create the CalleeStub, should work if the previous test passed
	callee, err := NewCalleeStub(&RemoteTestInterface{}, &RemoteTestInstance{}, addr, false, false)
	if err != nil {
		t.Fatalf("Error in NewCalleeStub: %s", err.Error())
	}
	if callee == nil {
		t.Fatalf("NewCalleeStub returned nil")
	}

	if probe(addr) {
		t.Fatalf("CalleeStub accepts connections before start")
	}

	err = callee.Start()
	if err != nil {
		t.Fatalf("Error in CalleeStub Start(): %s", err.Error())
	}

	// wait for CalleeStub to start...or timeout
	ddln := time.Now().Add(5 * time.Second)
	for !callee.IsRunning() && time.Now().Before(ddln) {
	}
	if !callee.IsRunning() {
		t.Fatalf("Timeout waiting for CalleeStub to start")
	}

	if !probe(addr) {
		t.Fatalf("CalleeStub refuses connections after start")
	}

	callee.Stop()

	// wait for CalleeStub to stop...or timeout
	ddln = time.Now().Add(5 * time.Second)
	for callee.IsRunning() && time.Now().Before(ddln) {
	}
	if callee.IsRunning() {
		t.Fatalf("Timeout waiting for CalleeStub to stop")
	}

	if probe(addr) {
		t.Fatalf("CalleeStub accepts connections after stop")
	}
}

// TestFinal_CallerStubInterface -- perform basic tests on CallerStub creation
// -- verifies that CallerStubCreator rejects non-remote interfaces and nulls
// -- verifies that CallerStubCreator accepts remote interfaces
