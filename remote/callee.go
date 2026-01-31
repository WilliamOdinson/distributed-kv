package remote

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
func NewCalleeStub(sv interface{}, sobj interface{}, address string, lossy bool, delayed bool) (Callee, error) {

	// if ifc is a pointer to a struct with function declarations,
	// then reflect.TypeOf(ifc).Elem() is the reflected struct's Type

	// if sobj is a pointer to an object instance, then
	// reflect.ValueOf(sobj) is the reflected object's Value

	// TODO: get the CalleeStub ready to start
	return nil, nil
}
