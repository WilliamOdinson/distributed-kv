package remote

import (
	"fmt"
	"reflect"
)

// CallerStubCreator -- use reflection to populate the interface functions to create the
// caller's stub interface. Only works if all functions are exported. Once created,
// the interface masks remote calls to a CalleeStub that hosts the object instance that
// the functions are invoked on.  The network address of the remote CalleeStub must be
// provided with the stub is created, and it may not change later. Arguments include:
// -- a struct of function declarations to act as the stub's interface
// -- the remote address of the CalleeStub as "<ip-address>:<port-number>"
// -- indicator of whether caller-to-callee channel has emulated packet loss
// -- indicator of whether caller-to-callee channel has emulated propagation delay
// Performs the following:
// -- returns a local error if function struct is nil
// -- returns a local error if any function in the struct is not a remote function
// -- otherwise, uses reflection to access the functions in the given struct and
//
//	populate their function definitions with the required CallerStub functionality
func CallerStubCreator(serviceInterface any, address string, isLossy bool, isDelayed bool) error {
	// if serviceInterface is a pointer to a struct with function declarations,
	// then reflect.TypeOf(serviceInterface).Elem() is the reflected struct's reflect.Type
	// and reflect.ValueOf(serviceInterface).Elem() is the reflected struct's reflect.Value
	//
	// Here's what it needs to do (not strictly in this order):
	//
	//    1. create a request message populated with the method name and input
	//       arguments to send to the CalleeStub
	//
	//    2. create a []reflect.Value of correct size to hold the result to be
	//       returned back to the program
	//
	//    3. connect to the CalleeStub's tcp server, and wrap the connection in an
	//       appropriate LeakySocket using the parameters given to the CallerStubCreator
	//
	//    4. encode the request message into a byte-string to send over the connection
	//
	//    5. send the encoded message, noting that the LeakySocket is not guaranteed
	//       to succeed depending on the given parameters
	//
	//    6. wait for a reply to be received using Recv, which is blocking
	//        -- if Recv returns an error, populate and return error output
	//
	//    7. decode the received byte-string according to the expected return types

	// returns a local error if function struct is nil
	if serviceInterface == nil {
		return fmt.Errorf("serviceInterface should not be nil")
	}

	// returns a local error if any function in the struct is not a remote function
	serviceType := reflect.TypeOf(serviceInterface)

	// dereference pointer types to get to the struct type
	if serviceType.Kind() == reflect.Pointer {
		serviceType = serviceType.Elem()
	}

	if serviceType.Kind() != reflect.Struct {
		return fmt.Errorf("service interface must be a struct or pointer to struct")
	}

	for i := 0; i < serviceType.NumField(); i++ {
		field := serviceType.Field(i)
		// check if the field is a function
		if field.Type.Kind() != reflect.Func {
			return fmt.Errorf("field %s is not a function", field.Name)
		}
		// check if the function has the correct signature
		if field.Type.NumOut() == 0 || field.Type.Out(field.Type.NumOut()-1) != reflect.TypeOf(RemoteError{}) {
			return fmt.Errorf("function %s does not have the correct signature", field.Name)
		}
	}

	return nil
}
