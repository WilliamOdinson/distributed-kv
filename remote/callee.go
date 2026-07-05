package remote

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
)

// CalleeStub is a struct that implements the Callee interface and serves as a TCP server to handle remote procedure calls.
type CalleeStub struct {
	// TODO: populate with needed contents including, but not limited to:
	//       - reflect.Type of the CalleeStub's service interface (struct of Fields)
	//       - reflect.Value of the CalleeStub's service interface
	//       - reflect.Value of the CalleeStub's remote object instance
	//       - status and configuration parameters, as needed
	serviceType reflect.Type  // type descriptor of the service interface struct
	serviceVal  reflect.Value // reflected value of the service interface struct
	objectVal   reflect.Value // reflected value of the concrete service object
	address     string        // TCP address to listen on (e.g., "localhost:8080")
	isLossy     bool          // whether LeakySocket simulates packet loss
	isDelayed   bool          // whether LeakySocket simulates delayed packets
	listener    net.Listener  // TCP listener, used to accept incoming connections
	isRunning   atomic.Bool   // indicator for whether the server is running for graceful shutdown
	callCount   int64         // total number of successful calls (across restarts)
	mu          sync.Mutex    // mutex to protect isRunning and callCount
}

// Callee defines the interface that any CalleeStub implementation must satisfy.
type Callee interface {
	Start() error      // non-blocking start a TCP server (returns immediately)
	Stop() error       // graceful shutdown of the TCP server
	IsRunning() bool   // report whether the server is currently accepting connections
	GetCallCount() int // return the total number of calls handled across all restarts
}

// build a new CalleeStub instance around a given struct of supported functions,
// a local instance of a corresponding object that supports these functions,
// and arguments to support creation and use of LeakySocket-wrapped connections.
// performs the following:
// -- returns a local error if function struct or object is nil
// -- returns a local error if any function in the struct is not a remote function
// -- if neither error, creates and populates a CalleeStub and returns a pointer

// NewCalleeStub validates the provided service interface and object, then creates and returns a new CalleeStub instance configured with the given parameters.
//
// It checks the following conditions:
// 1. serviceInterface or serviceObject should not be nil
// 2. serviceInterface should be a struct or pointer to struct
// 3. each field of the serviceInterface should be a function with the correct signature: last return value must be RemoteError
func NewCalleeStub(serviceInterface any, serviceObject any, address string, isLossy bool, isDelayed bool) (Callee, error) {
	// Check 1. if function struct or object is nil, return an error
	if serviceInterface == nil || serviceObject == nil {
		return nil, fmt.Errorf("serviceInterface or serviceObject is nil")
	}

	// Check 2. if serviceInterface is not a pointer to a struct or itself is not a struct, return an error
	serviceType := reflect.TypeOf(serviceInterface)

	// dereference if serviceInterface is a pointer
	if serviceType.Kind() == reflect.Pointer {
		serviceType = serviceType.Elem()
	}
	serviceValue := reflect.ValueOf(serviceInterface)

	// if serviceType is not a struct, return an error
	if serviceType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("service interface must be a struct or pointer to struct")
	}

	// Check 3. for each field in the struct, if it is not a function or does not have the correct signature, return an error
	for i := 0; i < serviceType.NumField(); i++ {
		field := serviceType.Field(i)
		// use Type.Kind() to check if the field is a function
		if field.Type.Kind() != reflect.Func {
			return nil, fmt.Errorf("field %s is not a function", field.Name)
		}
		// check if the function's last return value is an instance of RemoteError
		if field.Type.NumOut() == 0 || field.Type.Out(field.Type.NumOut()-1) != reflect.TypeOf(RemoteError{}) {
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

// Start launches the TCP server for Callee. It begins listening for incoming connections and handles them concurrently in separate goroutines. This method is non-blocking and returns immediately after starting the server.
func (cs *CalleeStub) Start() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// guard against a double Start(), which would leak the previous listener
	if cs.isRunning.Load() {
		return nil
	}

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

	// cs.listener is read by Stop() under the same mutex, so writing it here
	// while holding the lock avoids a data race between Start and Stop.
	cs.listener = listener
	cs.isRunning.Store(true)

	// continuously accept new connections, use one goroutine per connection for not blocking
	go func() {
		for cs.isRunning.Load() {
			conn, err := listener.Accept()
			if err != nil && !cs.isRunning.Load() {
				// server has been stopped, exit the loop
				return
			}
			// otherwise, the error might just be end of connection
			if err != nil {
				continue
			}

			go cs.handleConnection(conn)
		}
	}()
	return nil
}

// handleConnection processes a single remote method call in an connection.
// It reads the request, decodes it. After validation, it invokes the specific method on the service object, and sends back the response after encoding it to ReplyMsg.
// Any errors encountered during this process are reported back to the caller through a ReplyMsg with Success set to false and Err containing the error message.
func (cs *CalleeStub) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Wrap the connection in a LeakySocket
	socket := NewLeakySocket(conn, cs.isLossy, cs.isDelayed)

	// receive the request message from the caller
	data, err := socket.Recv()
	if err != nil {
		return
	}

	// decode the request message
	var req RequestMsg
	var reqBuf bytes.Buffer
	reqBuf.Write(data)
	if err := gob.NewDecoder(&reqBuf).Decode(&req); err != nil {
		cs.sendErrorMessage(conn, "[Callee] failed to decode request: "+err.Error())
		return
	}

	// lookup the corresponding function in the service object
	methodVal := cs.objectVal.MethodByName(req.Method)
	if !methodVal.IsValid() {
		// if method is not found, send an error message back to the caller
		cs.sendErrorMessage(conn, "[Callee] method not found: "+req.Method)
		return
	}

	// verify argument count matches the method signature
	methodType := methodVal.Type()
	if methodType.NumIn() != len(req.Args) {
		cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] argument count mismatch for method %s: expected %d, got %d", req.Method, methodType.NumIn(), len(req.Args)))
		return
	}

	// decode each argument from the gob-encoded byte slice and prepare for local method call
	args := make([]reflect.Value, len(req.Args))
	for i, argData := range req.Args {
		argType := methodType.In(i)
		argPtr := reflect.New(argType)
		argBuf := bytes.NewBuffer(argData)
		if err := gob.NewDecoder(argBuf).Decode(argPtr.Interface()); err != nil {
			cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] failed to decode argument %d for method %s: %v", i, req.Method, err))
			return
		}
		args[i] = argPtr.Elem()
	}

	// invoke the method with the decoded arguments
	results := methodVal.Call(args)

	// prepare the reply message
	reply := ReplyMsg{
		Success: true,
		Err:     RemoteError{},
	}

	// encode the results into the reply message
	replyMsg := make([][]byte, len(results))
	for i, result := range results {
		var buf bytes.Buffer
		// if the result is an error type, we need to encode the error message string instead of the error object itself because the error object is not serializable
		if result.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
			errMessage := ""
			if !result.IsNil() {
				errMessage = result.Interface().(error).Error()
			}

			// encode the error message string instead of the error object itself
			err := gob.NewEncoder(&buf).Encode(errMessage)
			if err != nil {
				cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] failed to encode result %d for method %s: %v", i, req.Method, err))
				return
			}
		} else {
			// for non-error results, we can encode them directly
			err := gob.NewEncoder(&buf).EncodeValue(result)
			if err != nil {
				cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] failed to encode result %d for method %s: %v", i, req.Method, err))
				return
			}
		}
		replyMsg[i] = buf.Bytes()
	}
	reply.Reply = replyMsg

	// encode the reply message and send it back to the caller
	var replyBuf bytes.Buffer
	if err := gob.NewEncoder(&replyBuf).Encode(reply); err != nil {
		cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] failed to encode reply message: %v", err))
		return
	}
	sent, err := socket.Send(replyBuf.Bytes())
	if err != nil {
		// conn already broken, actually this will also fail.
		// cs.sendErrorMessage(conn, fmt.Sprintf("[Callee] failed to send reply message: %v", err))
		return
	}
	if sent {
		// at the end of handling this connection, we have successfully handled one call, so increase call count by 1
		atomic.AddInt64(&cs.callCount, 1)
	}
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

// GetCallCount returns the total number of calls handled by the TCP server (total number of successful remote procedure calls). across all restarts.
func (cs *CalleeStub) GetCallCount() int {
	return int(atomic.LoadInt64(&cs.callCount))
}

// sendErrorMessage creates a failed ReplyMsg, and sends it back to the caller with the connection.
// This simplifies the error handling in handleConnection.
func (cs *CalleeStub) sendErrorMessage(conn net.Conn, errMsg string) error {
	reply := ReplyMsg{
		Success: false,
		Err:     RemoteError{Err: errMsg},
	}
	var replyBuf bytes.Buffer
	if err := gob.NewEncoder(&replyBuf).Encode(reply); err != nil {
		return fmt.Errorf("failed to encode reply message: %w", err)
	}

	socket := NewLeakySocket(conn, cs.isLossy, cs.isDelayed)
	_, err := socket.Send(replyBuf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send reply message: %w", err)
	}

	return nil
}
