package remote

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
)

// CalleeStub -- stub that receives remote calls and hosts an object/instance
// A CalleeStub encapsulates a multithreaded TCP server that manages a single
// remote object on a single TCP port, which is a simplification to ease management
// of remote objects and interaction with callers.  Each CalleeStub is built
// around a single struct of function declarations. All remote calls are
// handled synchronously, meaning the lifetime of a connection is that of a
// single method call.  A CalleeStub can encounter a number of different issues,
// and most of them will result in sending a failure response to the caller,
// including a RemoteError with suitable details.
type CalleeStub struct {
	// TODO: populate with needed contents including, but not limited to:
	//       - reflect.Type of the CalleeStub's service interface (struct of Fields)
	//       - reflect.Value of the CalleeStub's service interface
	//       - reflect.Value of the CalleeStub's remote object instance
	//       - status and configuration parameters, as needed
	serviceType reflect.Type  // reflected Type of the service interface (struct of Fields)
	serviceVal  reflect.Value // reflected Value of the service interface
	objectVal   reflect.Value // reflected Value of the remote object instance
	address     string        // TCP address to listen on
	isLossy     bool          // control whether LeakySocket simulates packet loss
	isDelayed   bool          // control whether LeakySocket simulates delayed packets
	listener    net.Listener  // TCP listener
	isRunning   atomic.Bool   // indicator for whether the server is running, supported graceful shutdown
	callCount   int64         // the number of calls handled (across restarts)
	mu          sync.Mutex    // mutex to protect isRunning and callCount
}

// Callee defines the minimum contract our
// CalleeStub implementation must satisfy.
type Callee interface {
	Start() error      // start a TCP server, then return
	Stop() error       // close the TCP server, then return
	IsRunning() bool   // is the TCP server running?
	GetCallCount() int // how many calls has the TCP server handled (across restarts)?
}

// build a new CalleeStub instance around a given struct of supported functions,
// a local instance of a corresponding object that supports these functions,
// and arguments to support creation and use of LeakySocket-wrapped connections.
// performs the following:
// -- returns a local error if function struct or object is nil
// -- returns a local error if any function in the struct is not a remote function
// -- if neither error, creates and populates a CalleeStub and returns a pointer
func NewCalleeStub(serviceInterface any, serviceObject any, address string, isLossy bool, isDelayed bool) (Callee, error) {
	// if function struct or object is nil, return an error
	if serviceInterface == nil || serviceObject == nil {
		return nil, fmt.Errorf("serviceInterface or serviceObject is nil")
	}

	// if serviceInterface is not a pointer to a struct or itself is not a struct, return an error
	serviceType := reflect.TypeOf(serviceInterface)

	// dereference pointer types to get to the struct type
	if serviceType.Kind() == reflect.Pointer {
		serviceType = serviceType.Elem()
	}
	// get the reflect.Value of serviceInterface
	serviceValue := reflect.ValueOf(serviceInterface)

	if serviceType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("service interface must be a struct or pointer to struct")
	}

	for i := 0; i < serviceType.NumField(); i++ {
		field := serviceType.Field(i)
		// check if the field is a function
		if field.Type.Kind() != reflect.Func {
			return nil, fmt.Errorf("field %s is not a function", field.Name)
		}
		// check if the function has the correct signature
		if field.Type.NumOut() == 0 || field.Type.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
			return nil, fmt.Errorf("function %s does not have the correct signature", field.Name)
		}
	}

	// at this point, we have verified that:
	// - serviceInterface is a struct or pointer to struct
	// - all fields in the struct are functions with correct signatures

	// create and populate a CalleeStub instance
	return &CalleeStub{
		serviceType: serviceType,
		serviceVal:  serviceValue,
		objectVal:   reflect.ValueOf(serviceObject),
		address:     address,
		isLossy:     isLossy,
		isDelayed:   isDelayed,
		callCount:   0,
	}, nil
}

// Start launches the TCP server for Callee.
// It performs the following steps:
// -- binds to the configured TCP address and starts listening
// -- continuously accepts new connections
// -- for each accepted connection, spawns a new goroutine to handle it
func (cs *CalleeStub) Start() error {
	// resolve the TCP address from string format
	tcpAddr, err := net.ResolveTCPAddr("tcp", cs.address)
	if err != nil {
		return fmt.Errorf("failed to resolve TCP address: %w", err)
	}

	// bind to the address and start listening
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}

	cs.listener = listener
	cs.isRunning.Store(true)

	// continuously accept new connections
	// use one goroutine per connection for not blocking
	for cs.isRunning.Load() {
		conn, err := listener.Accept()
		if err != nil && !cs.isRunning.Load() {
			// server has been stopped, exit the loop
			return nil
		}
		// otherwise, the error might just be end of connection
		if err != io.EOF {
			continue
		}

		go cs.handleConnection(conn)
	}
	return nil
}

func (cs *CalleeStub) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Wrap the connection in a LeakySocket
	socket := NewLeakySocket(conn, cs.isLossy, cs.isDelayed)

	data, err := socket.Recv()
	if err != nil {
		return
	}
	// Process the received data
	_ = string(data)

	// increase call count by 1
	atomic.AddInt64(&cs.callCount, 1)
}

// Stop gracefully shuts down the TCP server for Callee.
func (cs *CalleeStub) Stop() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// if the server is not running, nothing to do
	if !cs.isRunning.Load() {
		return nil
	}

	// otherwise, set isRunning to false and
	// close the listener to stop accepting new connections
	cs.isRunning.Store(false)
	if cs.listener != nil {
		err := cs.listener.Close()
		if err != nil {
			return fmt.Errorf("Failed to close listener: %w", err)
		}
	}

	return nil
}

// IsRunning indicates whether the TCP server is currently running.
func (cs *CalleeStub) IsRunning() bool {
	return cs.isRunning.Load()
}

// GetCallCount returns the total number of calls handled by the TCP server across all restarts.
func (cs *CalleeStub) GetCallCount() int {
	return int(atomic.LoadInt64(&cs.callCount))
}
