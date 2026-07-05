package remote

// Tests that drive the error-handling and edge branches of the caller and
// callee: transmitting real error return values, malformed requests, and
// failures to bind a listener. These target the defensive paths that the
// happy-path RPC tests don't reach.

import (
	"net"
	"testing"
)

// A method returning a real error interface value must round-trip: nil stays
// nil, and a non-nil error arrives with its message preserved. This exercises
// the "result implements error" branch on the callee (encode message string)
// and the matching decode-and-rebuild branch on the caller.
func TestRPC_ErrorReturnRoundTrip(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	t.Run("nil error", func(t *testing.T) {
		t.Log("step: MayFail(false) should return a nil error and no RemoteError")
		appErr, r := caller.MayFail(false)
		if r.Error() != "" {
			t.Fatalf("MayFail(false) RemoteError = %q, want empty", r.Error())
		}
		if appErr != nil {
			t.Fatalf("MayFail(false) error = %v, want nil", appErr)
		}
		t.Log("ok: nil error round-tripped as nil")
	})

	t.Run("non-nil error", func(t *testing.T) {
		t.Logf("step: MayFail(true) should return an error with message %q", string(errFromMethod))
		appErr, r := caller.MayFail(true)
		if r.Error() != "" {
			t.Fatalf("MayFail(true) RemoteError = %q, want empty", r.Error())
		}
		if appErr == nil || appErr.Error() != string(errFromMethod) {
			t.Fatalf("MayFail(true) error = %v, want %q", appErr, string(errFromMethod))
		}
		t.Log("ok: non-nil error round-tripped with its message preserved")
	})
}

// Start must return an error when the address cannot be resolved or bound.
func TestCalleeStub_StartBadAddress(t *testing.T) {
	t.Log("step: constructing a callee with an unresolvable address; NewCalleeStub should defer validation")
	callee, err := NewCalleeStub(&testService{}, &testInstance{}, "this is not an address", false, false)
	if err != nil {
		t.Fatalf("NewCalleeStub should defer address validation to Start, got: %v", err)
	}
	t.Log("step: Start on the unresolvable address should return an error")
	if err := callee.Start(); err == nil {
		callee.Stop()
		t.Fatal("Start on an unresolvable address returned no error")
	}
	if callee.IsRunning() {
		t.Fatal("callee reports running after a failed Start")
	}
	t.Log("ok: Start failed and the callee is not running")
}

// Start must fail if the port is already occupied.
func TestCalleeStub_StartAddressInUse(t *testing.T) {
	// Occupy a port ourselves, then ask a callee to bind the same one.
	t.Log("step: occupying a port ourselves before asking a callee to bind it")
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("could not occupy a port: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	callee, err := NewCalleeStub(&testService{}, &testInstance{}, addr, false, false)
	if err != nil {
		t.Fatalf("NewCalleeStub failed: %v", err)
	}
	t.Logf("step: Start on the occupied address %s should return an error", addr)
	if err := callee.Start(); err == nil {
		callee.Stop()
		t.Fatal("Start on an occupied port returned no error")
	}
	t.Log("ok: Start failed on the in-use port")
}

// A malformed request (raw bytes that are not a gob-encoded RequestMsg) must be
// answered with a failed ReplyMsg rather than a hang or a crash. We drive this
// at the wire level with a plain TCP connection.
func TestCalleeStub_MalformedRequestGetsErrorReply(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)

	t.Logf("step: opening a raw TCP connection to the callee at %s", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	t.Log("step: writing raw non-gob bytes as a malformed request")
	if _, err := conn.Write([]byte("this is not a gob stream")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// The callee should send back *something* (an encoded failure ReplyMsg)
	// and then close, rather than leaving us to block forever.
	t.Log("step: reading the reply; expect a non-empty failure ReplyMsg, not a hang")
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	if n == 0 {
		t.Fatal("callee sent no reply to a malformed request")
	}
	t.Logf("ok: callee replied with %d bytes to the malformed request", n)
}
