package remote

// Tests for the CalleeStub lifecycle: it must not accept connections before
// Start, must accept them after Start, must stop accepting after Stop, and must
// survive repeated Start/Stop cycles.

import (
	"testing"
	"time"
)

func TestCalleeStub_NotListeningBeforeStart(t *testing.T) {
	addr := freeAddr(t)
	callee, err := NewCalleeStub(&testService{}, &testInstance{}, addr, false, false)
	if err != nil {
		t.Fatalf("NewCalleeStub failed: %v", err)
	}
	if callee.IsRunning() {
		t.Fatal("callee reports running before Start")
	}
	if probe(addr) {
		t.Fatal("callee accepts connections before Start")
	}
}

func TestCalleeStub_StartStopToggleListening(t *testing.T) {
	addr := freeAddr(t)
	cs := startedCallee(t, addr, false, false)

	t.Log("step: after Start, the address should accept connections")
	if !probe(addr) {
		t.Fatal("callee refuses connections after Start")
	}

	t.Log("step: stopping the callee")
	if err := cs.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	if !waitFor(func() bool { return !cs.IsRunning() }, 5*time.Second) {
		t.Fatal("timed out waiting for callee to stop")
	}
	t.Log("step: after Stop, the address should refuse connections")
	if probe(addr) {
		t.Fatal("callee still accepts connections after Stop")
	}
	t.Log("ok: listening state tracked Start/Stop correctly")
}

func TestCalleeStub_StopWhenNotRunningIsNoop(t *testing.T) {
	addr := freeAddr(t)
	callee, err := NewCalleeStub(&testService{}, &testInstance{}, addr, false, false)
	if err != nil {
		t.Fatalf("NewCalleeStub failed: %v", err)
	}
	if err := callee.Stop(); err != nil {
		t.Fatalf("Stop on a never-started callee should be a no-op, got: %v", err)
	}
}

// A second Start() must not leak the first listener or error out; it should be
// a no-op while the server is already running.
func TestCalleeStub_DoubleStartIsSafe(t *testing.T) {
	addr := freeAddr(t)
	cs := startedCallee(t, addr, false, false)
	if err := cs.Start(); err != nil {
		t.Fatalf("second Start returned error: %v", err)
	}
	if !cs.IsRunning() {
		t.Fatal("callee not running after redundant Start")
	}
	if !probe(addr) {
		t.Fatal("callee not accepting connections after redundant Start")
	}
}

func TestCalleeStub_Reconnection(t *testing.T) {
	addr := freeAddr(t)
	cs := startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	// Works while running.
	t.Log("step: baseline call before any restart should succeed")
	if v, _, r := caller.Echo(1, false); v != 1 || r.Error() != "" {
		t.Fatalf("Echo before restart = (%d, _, %q), want (1, _, \"\")", v, r.Error())
	}

	for cycle := 0; cycle < 2; cycle++ {
		t.Logf("step: restart cycle %d: stopping callee", cycle)
		if err := cs.Stop(); err != nil {
			t.Fatalf("Stop (cycle %d) returned error: %v", cycle, err)
		}
		if !waitFor(func() bool { return !cs.IsRunning() }, 5*time.Second) {
			t.Fatalf("callee did not stop (cycle %d)", cycle)
		}
		// A call to a stopped callee must surface a RemoteError.
		t.Logf("step: restart cycle %d: call to stopped callee must return a RemoteError", cycle)
		if _, _, r := caller.Echo(1, false); r.Error() == "" {
			t.Fatalf("call to stopped callee (cycle %d) returned no RemoteError", cycle)
		}

		t.Logf("step: restart cycle %d: restarting callee", cycle)
		if err := cs.Start(); err != nil {
			t.Fatalf("Start (cycle %d) returned error: %v", cycle, err)
		}
		if !waitFor(cs.IsRunning, 5*time.Second) {
			t.Fatalf("callee did not restart (cycle %d)", cycle)
		}
		t.Logf("step: restart cycle %d: call after restart must succeed again", cycle)
		if v, _, r := caller.Echo(1, false); v != 1 || r.Error() != "" {
			t.Fatalf("Echo after restart (cycle %d) = (%d, _, %q), want (1, _, \"\")", cycle, v, r.Error())
		}
	}
	t.Log("ok: caller survived repeated callee restarts")
}
