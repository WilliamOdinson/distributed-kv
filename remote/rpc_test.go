package remote

// End-to-end RPC behavior: argument passing, return values, application errors,
// composite types, concurrency, unreliable channels, and mismatched interfaces.

import (
	"testing"
	"time"
)

func TestRPC_CallerConnectsAndCalls(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	// A bare call should complete without panicking or blocking.
	t.Log("step: invoking Echo(1, false) over a fresh connection")
	if _, _, r := caller.Echo(1, false); r.Error() != "" {
		t.Fatalf("Echo returned RemoteError: %q", r.Error())
	}
	t.Log("ok: bare call completed with no RemoteError")
}

func TestRPC_ArgumentsReturnsAndAppErrors(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	t.Run("normal value", func(t *testing.T) {
		t.Log("step: Echo(1, false) should return the value unchanged")
		v, e, r := caller.Echo(1, false)
		if v != 1 || e != "" || r.Error() != "" {
			t.Fatalf("Echo(1,false) = (%d,%q,%q), want (1,\"\",\"\")", v, e, r.Error())
		}
	})

	t.Run("application error", func(t *testing.T) {
		t.Log("step: Echo(1, true) should return -value plus an application error")
		v, e, r := caller.Echo(1, true)
		if v != -1 || e == "" || r.Error() != "" {
			t.Fatalf("Echo(1,true) = (%d,%q,%q), want (-1, non-empty, \"\")", v, e, r.Error())
		}
	})

	t.Run("composite map argument", func(t *testing.T) {
		t.Log("step: SumLengths over a map[string][]string should round-trip the composite type")
		d := map[string][]string{
			"apple": {"a delicious fruit", "a company that doesn't make delicious fruit"},
			"gold":  {"a precious metal", "the color of the precious metal with the same name"},
		}
		got, r := caller.SumLengths(d)
		if r.Error() != "" {
			t.Fatalf("SumLengths returned RemoteError: %q", r.Error())
		}
		if len(got) != 2 {
			t.Fatalf("SumLengths returned %d lengths, want 2", len(got))
		}
		// Map iteration order is unspecified, so accept either ordering.
		ok := (got[0] == 60 && got[1] == 66) || (got[0] == 66 && got[1] == 60)
		if !ok {
			t.Fatalf("SumLengths = %v, want the multiset {60, 66}", got)
		}
	})
}

// Over a lossy+delayed channel, the caller's retry loop must still deliver
// every call correctly. This is the core reliability guarantee of the library.
func TestRPC_LossyConnectionEventuallySucceeds(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, true, true)
	caller := dialCaller(t, addr, true, true)

	const calls = 100
	t.Logf("step: issuing %d calls over a lossy+delayed channel; each must eventually succeed via retry", calls)
	for j := 0; j < calls; j++ {
		v, e, r := caller.Echo(j, false)
		if v != j || e != "" || r.Error() != "" {
			t.Fatalf("Echo(%d) over lossy channel = (%d,%q,%q)", j, v, e, r.Error())
		}
	}
	t.Logf("ok: all %d lossy calls returned correct results", calls)
}

// Two concurrent PairUp calls must both complete, which can only happen if the
// callee serves connections concurrently. The goroutine reports its result over
// a channel so the assertion runs on the test goroutine (t.Fatal from a
// non-test goroutine is unreliable and is flagged by go vet).
func TestRPC_ConcurrentConnections(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	t.Log("step: launching two PairUp calls that each block until the other arrives")
	errCh := make(chan string, 1)
	go func() { r := caller.PairUp(); errCh <- r.Error() }()

	if r := caller.PairUp(); r.Error() != "" {
		t.Fatalf("PairUp (main) returned RemoteError: %q", r.Error())
	}
	t.Log("step: main PairUp returned; waiting for the concurrent one to pair up")

	select {
	case e := <-errCh:
		if e != "" {
			t.Fatalf("PairUp (goroutine) returned RemoteError: %q", e)
		}
		t.Log("ok: both PairUp calls completed, confirming concurrent service")
	case <-time.After(10 * time.Second):
		t.Fatal("second PairUp never completed; callee may not be concurrent")
	}
}

// A caller whose interface disagrees with the callee's implementation must get
// a RemoteError rather than silent corruption. Goroutine results are funneled
// back over a channel to keep assertions on the test goroutine.
func TestRPC_MismatchedInterfaces(t *testing.T) {
	addr := freeAddr(t)
	startedCallee(t, addr, false, false)

	t.Run("unknown method, wrong arg type, wrong return arity", func(t *testing.T) {
		caller := &mismatchService{}
		if err := CallerStubCreator(caller, addr, false, false); err != nil {
			t.Fatalf("CallerStubCreator failed: %v", err)
		}

		t.Log("step: calling a method the callee does not implement should error")
		if r := caller.ExtraMethod(); r.Error() == "" {
			t.Fatal("callee accepted a method it does not implement")
		}
		t.Log("step: calling Echo with the wrong argument type should error")
		if _, _, r := caller.Echo(1, 1); r.Error() == "" {
			t.Fatal("callee accepted a call with the wrong argument type")
		}

		t.Log("step: calling PairUp expecting the wrong return arity should error (both goroutines)")
		errCh := make(chan string, 1)
		go func() { _, r := caller.PairUp(); errCh <- r.Error() }()
		if _, r := caller.PairUp(); r.Error() == "" {
			t.Fatal("caller accepted a reply with the wrong number of return values")
		}
		if e := <-errCh; e == "" {
			t.Fatal("goroutine caller accepted a reply with the wrong return arity")
		}
	})

	t.Run("wrong argument count", func(t *testing.T) {
		caller := &mismatchService2{}
		if err := CallerStubCreator(caller, addr, false, false); err != nil {
			t.Fatalf("CallerStubCreator failed: %v", err)
		}
		t.Log("step: calling Echo with an extra argument should error")
		if _, _, r := caller.Echo(1, true, 1); r.Error() == "" {
			t.Fatal("callee accepted a call with the wrong argument count")
		}
	})
}

// A caller that cannot even dial the callee must get a RemoteError promptly.
func TestRPC_DialFailureReturnsRemoteError(t *testing.T) {
	addr := freeAddr(t) // reserved then released, so nothing is listening
	caller := dialCaller(t, addr, false, false)
	t.Logf("step: calling Echo against %s where no callee is listening", addr)
	if _, _, r := caller.Echo(1, false); r.Error() == "" {
		t.Fatal("call to an address with no listener returned no RemoteError")
	}
	t.Log("ok: dial failure surfaced as a RemoteError")
}

// GetCallCount should increase as successful calls are handled and persist
// across a Stop/Start cycle (it is a lifetime counter).
func TestRPC_CallCountAccumulates(t *testing.T) {
	addr := freeAddr(t)
	cs := startedCallee(t, addr, false, false)
	caller := dialCaller(t, addr, false, false)

	const n = 5
	t.Logf("step: issuing %d successful calls, then checking GetCallCount", n)
	for i := 0; i < n; i++ {
		if _, _, r := caller.Echo(i, false); r.Error() != "" {
			t.Fatalf("Echo(%d) failed: %q", i, r.Error())
		}
	}
	got := cs.GetCallCount()
	if got < n {
		t.Fatalf("GetCallCount = %d after %d calls, want >= %d", got, n, n)
	}
	t.Logf("ok: GetCallCount = %d after %d calls", got, n)
}
