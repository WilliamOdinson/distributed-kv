package remote

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
	"time"
)

// CallerStubCreator uses reflection to populate the interface functions to create the
// caller's stub interface. Once created, the interface masks remote calls to a CalleeStub
// that hosts the object instance that the functions are invoked on.
//
// It checks the following conditions:
// 1. serviceInterface must be a struct or pointer to struct type, and all fields must be functions.
// 2. All functions must be exported and have RemoteError as their last return type (is a remote function).
func CallerStubCreator(serviceInterface any, address string, isLossy bool, isDelayed bool) error {
	// returns a local error if function struct is nil
	if serviceInterface == nil {
		return fmt.Errorf("serviceInterface should not be nil")
	}

	// if the serviceInterface is a pointer to a struct, first dereference to get the element type
	serviceType := reflect.TypeOf(serviceInterface)
	if serviceType.Kind() == reflect.Pointer {
		serviceType = serviceType.Elem()
	}

	if serviceType.Kind() != reflect.Struct {
		return fmt.Errorf("service interface must be a struct or pointer to struct")
	}

	// Check 1. the functions must be exported and have RemoteError as their last return type.
	for i := 0; i < serviceType.NumField(); i++ {
		field := serviceType.Field(i)
		// check if the field is a function
		if field.Type.Kind() != reflect.Func {
			return fmt.Errorf("field %s is not a function", field.Name)
		}
		// check if the function's last return type is RemoteError
		if field.Type.NumOut() == 0 || field.Type.Out(field.Type.NumOut()-1) != reflect.TypeOf(RemoteError{}) {
			return fmt.Errorf("function %s does not have the correct signature, last return type must be RemoteError{}", field.Name)
		}
	}

	// Main Part: for each function field, create a function implementation that performs the remote call.
	for i := 0; i < serviceType.NumField(); i++ {
		field := serviceType.Field(i)
		funcName := field.Name

		// create a function (stub) that matches the signature of the field
		funcType := field.Type
		funcImpl := reflect.MakeFunc(funcType, func(args []reflect.Value) []reflect.Value {
			// Step 1. create a request message instance, populated with the method name and input arguments
			reqMsg := RequestMsg{
				Method: funcName,
				Args:   make([][]byte, len(args)),
			}
			for i, arg := range args {
				var buf bytes.Buffer
				if err := gob.NewEncoder(&buf).Encode(arg.Interface()); err != nil {
					return makeCallerErrorResponse(funcType, fmt.Sprintf("[Caller] failed to encode argument: %v", err))
				}
				reqMsg.Args[i] = buf.Bytes()
			}

			// Step 2: encode the request message into a byte-string with gob encoding
			var reqBuf bytes.Buffer
			if err := gob.NewEncoder(&reqBuf).Encode(reqMsg); err != nil {
				return makeCallerErrorResponse(funcType, fmt.Sprintf("[Caller] failed to encode request message: %v", err))
			}

			// Step 3: connect to the CalleeStub's tcp server, and wrap the connection in an LeakySocket. Since the LeakySocket may 'fail' to send packets, we simply retry sending the request until we get a reply from the server.
			// indicate whether the request has been successfully sent and a reply has been received
			flag := false
			var reply ReplyMsg

			for !flag { // connect to remote callee
				conn, err := net.Dial("tcp", address)
				if err != nil {
					return makeCallerErrorResponse(funcType, fmt.Sprintf("[Caller] failed to connect to remote callee: %v", err))
				}
				defer conn.Close()

				// wrap connection in leaky socket
				leakyConn := NewLeakySocket(conn, isLossy, isDelayed)

				// attempt to send; if the packet is dropped, retry with a new connection
				sent, err := leakyConn.Send(reqBuf.Bytes())
				if err != nil || !sent {
					continue // packet lost, should retry
				}

				// blocking (non-async) wait for reply with a 1-second read deadline; if no reply is received within the deadline, retry with a new connection wait for a reply
				conn.SetReadDeadline(time.Now().Add(time.Second))
				replyMsg, err := leakyConn.Recv()

				if err != nil {
					continue // packet lost, should retry
				}

				// decode the received byte-string according to the expected return types
				if err := gob.NewDecoder(bytes.NewReader(replyMsg)).Decode(&reply); err != nil {
					return makeCallerErrorResponse(funcType, fmt.Sprintf("[Caller] failed to decode reply message from remote callee: %v", err))
				}
				flag = true
			}

			// if the reply indicates a remote error, construct the return values with the error message.
			if !reply.Success {
				return makeCallerErrorResponse(funcType, reply.Err.Err)
			}

			// Step 4: decode the return values from the reply message according to the expected return types.
			returnCount := funcType.NumOut()
			results := make([]reflect.Value, returnCount)
			for i := 0; i < returnCount-1; i++ {
				outType := funcType.Out(i)
				// rrror types are encoded as strings, so we need to decode the string and convert it back to an error type.
				if outType.Implements(reflect.TypeOf((*error)(nil)).Elem()) {
					errMessage := ""
					if err := gob.NewDecoder(bytes.NewReader(reply.Reply[i])).Decode(&errMessage); err != nil {
						return makeCallerErrorResponse(funcType, "[Caller] failed to decode return value: "+err.Error())
					}
					if errMessage != "" {
						results[i] = reflect.ValueOf(fmt.Errorf("%s", errMessage))
					} else {
						results[i] = reflect.Zero(outType)

					}
				} else {
					// for non-error types, we can directly decode into the expected type.
					ptr := reflect.New(outType)
					if err := gob.NewDecoder(bytes.NewReader(reply.Reply[i])).DecodeValue(ptr); err != nil {
						return makeCallerErrorResponse(funcType, "[Caller] failed to decode return value: "+err.Error())
					}
					results[i] = ptr.Elem()
				}
			}
			// Since there is no failures within the RPC, final return value should be set as a zero RemoteError{}
			results[returnCount-1] = reflect.Zero(reflect.TypeOf(RemoteError{}))
			return results
		})

		// set the function implementation to the field so that the caller can invoke the function as if it were a local call.
		reflect.ValueOf(serviceInterface).Elem().FieldByName(funcName).Set(funcImpl)
	}

	return nil
}

// makeCallerErrorResponse is a helper function to construct the return values for a function when an error occurs. It takes in the function type and an error message, and returns a slice of reflect.Value where all return values except the last one are zero values, and the last one is a RemoteError with the given message.
func makeCallerErrorResponse(funcType reflect.Type, errMsg string) []reflect.Value {
	returnCount := funcType.NumOut()
	results := make([]reflect.Value, returnCount)
	for i := 0; i < returnCount-1; i++ {
		results[i] = reflect.Zero(funcType.Out(i))
	}
	results[returnCount-1] = reflect.ValueOf(RemoteError{Err: errMsg})
	return results
}
