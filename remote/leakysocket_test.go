package remote

// Unit tests for LeakySocket and RemoteError. LeakySocket itself is provided
// and must not change, but exercising its send/recv/loss/delay behavior
// directly (without a full RPC round trip) gives fast, deterministic coverage
// of the reliability-emulation layer that the integration tests lean on.

import (
	"net"
	"testing"
	"time"
)

// pipePair returns two connected in-memory sockets and registers their cleanup.
func pipePair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func TestRemoteError_Error(t *testing.T) {
	t.Log("step: stringifying a zero-value RemoteError, expecting empty string")
	empty := RemoteError{}
	if empty.Error() != "" {
		t.Fatalf("zero RemoteError should stringify to empty, got %q", empty.Error())
	}
	t.Logf("step: stringifying RemoteError{Err: %q}, expecting %q", "boom", "boom")
	filled := RemoteError{Err: "boom"}
	if filled.Error() != "boom" {
		t.Fatalf("RemoteError.Error() = %q, want %q", filled.Error(), "boom")
	}
	t.Log("ok: RemoteError stringifies as expected")
}

func TestLeakySocket_SendRecvRoundTrip(t *testing.T) {
	t.Log("step: creating a lossless, delay-free leaky socket over an in-memory pipe")
	client, server := pipePair(t)
	ls := NewLeakySocket(client, false, false)

	payload := []byte("hello wire")
	// net.Pipe is synchronous, so read on a goroutine while we send.
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		n, _ := server.Read(buf)
		got <- buf[:n]
	}()

	t.Logf("step: sending %q and expecting the server to receive it verbatim", payload)
	sent, err := ls.Send(payload)
	if err != nil || !sent {
		t.Fatalf("Send returned sent=%v err=%v, want true/nil", sent, err)
	}
	if received := string(<-got); received != string(payload) {
		t.Fatalf("server received %q, want %q", received, payload)
	}
	t.Log("ok: payload round-tripped through the leaky socket")
}

func TestLeakySocket_RecvReadsData(t *testing.T) {
	t.Logf("step: creating a leaky socket and writing %q from the server side", "payload")
	client, server := pipePair(t)
	ls := NewLeakySocket(client, false, false)

	go func() { server.Write([]byte("payload")) }()

	t.Logf("step: receiving on the leaky socket, expecting %q", "payload")
	data, err := ls.Recv()
	if err != nil {
		t.Fatalf("Recv returned error: %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("Recv = %q, want %q", data, "payload")
	}
	t.Log("ok: Recv returned the bytes written by the server")
}

func TestLeakySocket_SendNilPayloadIsNoop(t *testing.T) {
	client, _ := pipePair(t)
	ls := NewLeakySocket(client, false, false)
	t.Log("step: sending a nil payload, expecting a no-op success (true, nil)")
	sent, err := ls.Send(nil)
	if !sent || err != nil {
		t.Fatalf("Send(nil) = (%v, %v), want (true, nil)", sent, err)
	}
	t.Log("ok: Send(nil) reported success without touching the wire")
}

func TestLeakySocket_NilConnErrors(t *testing.T) {
	t.Log("step: building a leaky socket with a nil underlying conn")
	ls := &LeakySocket{s: nil}
	t.Log("step: expecting Send on the nil socket to error")
	if _, err := ls.Send([]byte("x")); err == nil {
		t.Fatal("Send on nil socket should error")
	}
	t.Log("step: expecting Recv on the nil socket to error")
	if _, err := ls.Recv(); err == nil {
		t.Fatal("Recv on nil socket should error")
	}
	t.Log("ok: both Send and Recv errored on a nil conn")
}

// With a 100% loss rate, Send must report the packet as dropped (sent=false)
// without surfacing an error, and must not deliver any bytes.
func TestLeakySocket_TotalLossDropsPacket(t *testing.T) {
	t.Log("step: creating a leaky socket with loss enabled")
	client, server := pipePair(t)
	ls := NewLeakySocket(client, true, false)
	t.Logf("step: forcing loss rate to %v and timeout to %dms", 1.0, 1)
	ls.SetLossRate(true, 1.0)
	ls.SetTimeout(1, 0) // keep the emulated timeout short

	// Nothing should ever arrive; fail if the server reads any bytes.
	server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	done := make(chan int, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := server.Read(buf)
		done <- n
	}()

	t.Log("step: sending under total loss, expecting sent=false with no error")
	sent, err := ls.Send([]byte("dropped"))
	if err != nil {
		t.Fatalf("Send under total loss returned error: %v", err)
	}
	if sent {
		t.Fatal("Send under total loss reported the packet as delivered")
	}
	t.Log("step: verifying the server read 0 bytes")
	if n := <-done; n != 0 {
		t.Fatalf("server received %d bytes under total loss, want 0", n)
	}
	t.Log("ok: packet was dropped and no bytes reached the server")
}

// With loss disabled, delay enabled, and a measurable delay, Send should still
// deliver but take at least the configured time.
func TestLeakySocket_DelayIsApplied(t *testing.T) {
	t.Log("step: creating a leaky socket with delay enabled")
	client, server := pipePair(t)
	ls := NewLeakySocket(client, false, true)
	t.Logf("step: configuring a %dms delay", 30)
	ls.SetDelay(true, 30, 0)

	go func() {
		buf := make([]byte, 16)
		server.Read(buf)
	}()

	t.Log("step: sending and timing the delayed delivery, expecting success and >=~30ms")
	start := time.Now()
	sent, err := ls.Send([]byte("slow"))
	elapsed := time.Since(start)
	if err != nil || !sent {
		t.Fatalf("delayed Send = (%v, %v), want (true, nil)", sent, err)
	}
	if elapsed < 25*time.Millisecond {
		t.Fatalf("delayed Send took %v, expected at least ~30ms", elapsed)
	}
	t.Logf("ok: delayed Send delivered after %v", elapsed)
}

func TestLeakySocket_SettersDoNotPanic(t *testing.T) {
	client, _ := pipePair(t)
	ls := NewLeakySocket(client, false, false)
	// Exercise every setter; the point is coverage plus a smoke check that the
	// stored fields are updated and later used without panicking.
	t.Log("step: calling SetDelay, SetTimeout, and SetLossRate")
	ls.SetDelay(true, 5, 10)
	ls.SetTimeout(250, 15)
	ls.SetLossRate(true, 0.25)
	t.Logf("step: verifying delay fields are msDelay=%d, usDelay=%d", 5, 10)
	if ls.msDelay != 5 || ls.usDelay != 10 {
		t.Fatalf("SetDelay did not update delay fields: %+v", ls)
	}
	t.Logf("step: verifying timeout fields are msTimeout=%d, usTimeout=%d", 250, 15)
	if ls.msTimeout != 250 || ls.usTimeout != 15 {
		t.Fatalf("SetTimeout did not update timeout fields: %+v", ls)
	}
	t.Logf("step: verifying loss fields are isLossy=%v, lossRate=%v", true, 0.25)
	if !ls.isLossy || ls.lossRate != 0.25 {
		t.Fatalf("SetLossRate did not update loss fields: %+v", ls)
	}
	t.Log("ok: all setters updated their fields without panicking")
}

func TestLeakySocket_Close(t *testing.T) {
	client, _ := pipePair(t)
	ls := NewLeakySocket(client, false, false)
	t.Log("step: closing the leaky socket, expecting no error")
	if err := ls.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	// After Close, sending should fail because the underlying conn is closed.
	t.Log("step: sending after Close, expecting failure on the closed conn")
	if sent, err := ls.Send([]byte("x")); sent && err == nil {
		t.Fatal("Send after Close unexpectedly succeeded")
	}
	t.Log("ok: Close succeeded and a subsequent Send did not succeed")
}
