// support for generic remote interaction over sockets
// including a socket wrapper that can drop and/or delay messages arbitrarily
// works with any* objects that can be gob-encoded for serialization
//
// the LeakySocket wrapper for net.Conn is provided in its entirety, and should
// not be changed, though you may extend it with additional helper functions as
// desired.  it is used directly by the test code.
//
// the RemoteError type is also provided in its entirety, and should not
// be changed.
//
// suggested RequestMsg and ReplyMsg types are included to get you started,
// but they are only used internally to the remote library, so you can use
// something else if you prefer
//
// the CalleeStub type represents the callee that manages remote objects, invokes
// calls from callers, and returns suitable results and/or remote errors
//
// the CallerStubCreator converts a struct of function declarations into a functional
// caller stub by using reflection to populate the function definitions.
//
// USAGE:
// the desired usage of this library is as follows (not showing all error-checking
// for clarity and brevity):
//
//  example ServiceInterface known to both caller and callee, defined as
//  type ServiceInterface struct {
//      ExampleMethod func(int, int) (int, remote.RemoteError)
//  }
//
//  1. callee program calls NewCalleeStub with interface and connection details, e.g.,
//     inst := &ServiceInstance{}
//     callee, err := remote.NewCalleeStub(&ServiceInterface{}, inst, "147.36.147.36:9999", true, true)
//
//  2. caller program calls CallerStubCreator, e.g.,
//     caller := &ServiceInterface{}
//     err := remote.CallerStubCreator(caller, "147.36.147.36:9999", true, true)
//
//  3. caller makes calls, e.g.,
//     n, r := caller.ExampleMethod(42, 14736)
//
//
// TODO *** here's what needs to be done for Lab 1:
//  1. create the CalleeStub type and supporting functions, including but not
//     limited to: NewCalleeStub, Start, Stop, IsRunning, and GetCallCount (see below)
//
//  2. create the CallerStubCreator which uses reflection to transparently define each
//     method call in the caller stub (see below)
//

package remote

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"time"
)

// LeakySocket is a wrapper for a net.Conn connection that emulates
// transmission delays and random packet loss. it has its own send
// and receive functions that together mimic an unreliable connection
// that can be customized to stress-test remote service interactions.
type LeakySocket struct {
	s         net.Conn
	isLossy   bool
	lossRate  float32
	isDelayed bool
	msTimeout int
	usTimeout int
	msDelay   int
	usDelay   int
}

// builder for a LeakySocket given a normal socket and indicators
// of whether the connection should experience loss and delay.
// uses default loss and delay values that can be changed using setters.
func NewLeakySocket(conn net.Conn, lossy bool, delayed bool) *LeakySocket {
	return &LeakySocket{
		s:         conn,
		isLossy:   lossy,
		lossRate:  0.05,
		isDelayed: delayed,
		msTimeout: 500,
		usTimeout: 0,
		msDelay:   2,
		usDelay:   0,
	}
}

// send a byte-string over the socket mimicking unreliability.
// delay is emulated using time.Sleep, packet loss is emulated using RNG
// coupled with time.Sleep to emulate a timeout
func (ls *LeakySocket) Send(obj []byte) (bool, error) {
	if obj == nil {
		return true, nil
	}

	if ls.s != nil {
		rand.Seed(time.Now().UnixNano())
		if ls.isLossy && rand.Float32() < ls.lossRate {
			time.Sleep(time.Duration(ls.msTimeout)*time.Millisecond + time.Duration(ls.usTimeout)*time.Microsecond)
			return false, nil
		} else {
			if ls.isDelayed {
				time.Sleep(time.Duration(ls.msDelay)*time.Millisecond + time.Duration(ls.usDelay)*time.Microsecond)
			}
			_, err := ls.s.Write(obj)
			if err != nil {
				return false, errors.New("Send Write error: " + err.Error())
			}
			return true, nil
		}
	}
	return false, errors.New("Send failed, nil socket")
}

// receive a byte-string over the socket connection.
// no significant change to normal socket receive.
func (ls *LeakySocket) Recv() ([]byte, error) {
	if ls.s != nil {
		buf := make([]byte, 4096)
		n := 0
		var err error
		for n <= 0 {
			n, err = ls.s.Read(buf)
			if n > 0 {
				return buf[:n], nil
			}
			if err != nil {
				if err != io.EOF {
					return nil, errors.New("Recv Read error: " + err.Error())
				}
			}
		}
	}
	return nil, errors.New("Recv failed, nil socket")
}

// enable/disable emulated transmission delay and/or change the delay parameter
func (ls *LeakySocket) SetDelay(delayed bool, ms int, us int) {
	ls.isDelayed = delayed
	ls.msDelay = ms
	ls.usDelay = us
}

// change the emulated timeout period used with packet loss
func (ls *LeakySocket) SetTimeout(ms int, us int) {
	ls.msTimeout = ms
	ls.usTimeout = us
}

// enable/disable emulated packet loss and/or change the loss rate
func (ls *LeakySocket) SetLossRate(lossy bool, rate float32) {
	ls.isLossy = lossy
	ls.lossRate = rate
}

// close the socket (can also be done on original net.Conn passed to builder)
func (ls *LeakySocket) Close() error {
	return ls.s.Close()
}

// RemoteError is a custom error type used for this library to identify remote methods.
// it is used by both caller and callee endpoints.
type RemoteError struct {
	Err string
}

// getter for the error message included inside the custom error type
func (e *RemoteError) Error() string {
	return e.Err
}

// RequestMsg (this is only a suggestion, can be changed)
//
// RequestMsg represents the request message sent from caller to callee.
// it is used by both endpoints, and uses the reflect package to carry
// arbitrary argument types across the network.
type RequestMsg struct {
	Method string
	Args   [][]byte
}

// ReplyMsg (this is only a suggestion, can be changed)
//
// ReplyMsg represents the reply message sent from callee back to caller
// in response to a RequestMsg. it similarly uses reflection to carry
// arbitrary return types along with a success indicator to tell the caller
// whether the call was correctly handled by the callee. also includes
// a RemoteError to specify details of any encountered failure.
type ReplyMsg struct {
	Success bool
	Reply   [][]byte
	Err     RemoteError
}
