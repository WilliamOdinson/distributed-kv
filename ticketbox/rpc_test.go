package ticketbox

// An end-to-end test that serves a TicketBoxService through a remote callee and
// drives it with a caller stub, over a lossy+delayed socket. This confirms the
// service composes correctly with the remote library: gob serialization of the
// composite EventDetail type, error propagation, and retry under packet loss.

import (
	"net"
	"remote"
	"testing"
	"time"
)

// freeAddr reserves an unused localhost address for the callee to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("could not reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestTicketBox_OverRemote(t *testing.T) {
	addr := freeAddr(t)
	t.Logf("step: reserved callee address %q", addr)

	// A lossy+delayed socket stresses the caller's retry loop end to end.
	t.Log("step: starting callee stub over a lossy+delayed socket (big=10, solo=1)")
	svc := NewTicketBoxService(map[string]int{"big": 10, "solo": 1})
	callee, err := remote.NewCalleeStub(&TicketBoxInterface{}, svc, addr, true, true)
	if err != nil {
		t.Fatalf("NewCalleeStub failed: %v", err)
	}
	if err := callee.Start(); err != nil {
		t.Fatalf("callee Start failed: %v", err)
	}
	t.Cleanup(func() { callee.Stop() })

	t.Log("step: waiting for callee to report running (5s deadline)")
	deadline := time.Now().Add(5 * time.Second)
	for !callee.IsRunning() && time.Now().Before(deadline) {
	}
	if !callee.IsRunning() {
		t.Fatal("callee never started")
	}
	t.Log("ok: callee is running")

	t.Logf("step: creating caller stub connected to %q", addr)
	client := &TicketBoxInterface{}
	if err := remote.CallerStubCreator(client, addr, true, true); err != nil {
		t.Fatalf("CallerStubCreator failed: %v", err)
	}

	// Buy a valid ticket over the wire.
	t.Log("step: remote BuyTickets(alice, [big]) and expecting no error")
	if _, appErr, rerr := client.BuyTickets("alice", []string{"big"}); appErr != nil || rerr.Error() != "" {
		t.Fatalf("remote BuyTickets: appErr=%v rerr=%q", appErr, rerr.Error())
	}
	t.Log("ok: remote buy for big succeeded")

	// The composite return type must survive the round trip.
	t.Log("step: remote GetAllEvents and checking big inventory == 9")
	events, appErr, rerr := client.GetAllEvents()
	if appErr != nil || rerr.Error() != "" {
		t.Fatalf("remote GetAllEvents: appErr=%v rerr=%q", appErr, rerr.Error())
	}
	if remainingFor(events, "big") != 9 {
		t.Fatalf("remote inventory for big = %d, want 9", remainingFor(events, "big"))
	}
	t.Log("ok: composite EventDetail survived the round trip, big=9")

	// An application error (buying a sold-out event) must arrive as a non-nil
	// error with an empty RemoteError.
	t.Log("step: exhausting solo then buying it again to force an application error")
	client.BuyTickets("first", []string{"solo"}) // exhaust the single ticket
	_, appErr, rerr = client.BuyTickets("second", []string{"solo"})
	if rerr.Error() != "" {
		t.Fatalf("unexpected RemoteError on sold-out buy: %q", rerr.Error())
	}
	if appErr == nil {
		t.Fatal("sold-out buy should return an application error over the wire")
	}
	t.Logf("ok: sold-out buy returned appErr=%v with empty RemoteError", appErr)
}
