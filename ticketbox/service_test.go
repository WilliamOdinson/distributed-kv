package ticketbox

// Unit tests for TicketBoxService. Because the service logic is decoupled from
// the RPC transport, these tests call it directly, with no network, and so run
// instantly and deterministically. A separate concurrency test runs under the
// race detector to confirm the mutex actually protects shared state.

import (
	"sort"
	"sync"
	"testing"
)

func newTestService() *TicketBoxService {
	return NewTicketBoxService(map[string]int{
		"solo": 1,  // single ticket -> exercises sold-out
		"big":  10, // plenty
		"alt":  10,
	})
}

func TestBuyTickets_Success(t *testing.T) {
	s := newTestService()
	t.Log("step: alice buys tickets for events [big alt]")
	msg, err, rerr := s.BuyTickets("alice", []string{"big", "alt"})
	if err != nil || rerr.Error() != "" {
		t.Fatalf("BuyTickets failed: err=%v rerr=%q", err, rerr.Error())
	}
	if msg == "" {
		t.Fatal("BuyTickets returned an empty success message")
	}
	t.Log("step: alice's held tickets should be exactly [alt big]")
	tickets, _, _ := s.GetMyTickets("alice")
	sort.Strings(tickets)
	if len(tickets) != 2 || tickets[0] != "alt" || tickets[1] != "big" {
		t.Fatalf("alice holds %v, want [alt big]", tickets)
	}
	t.Log("ok: purchase succeeded and both tickets are held")
}

func TestBuyTickets_RejectsDuplicateHold(t *testing.T) {
	s := newTestService()
	t.Log("step: bob buys event \"big\" once (should succeed)")
	if _, err, _ := s.BuyTickets("bob", []string{"big"}); err != nil {
		t.Fatalf("first buy failed: %v", err)
	}
	// Buying "big" again must fail...
	t.Log("step: bob buys the already-held event \"big\" again (should error)")
	_, err, _ := s.BuyTickets("bob", []string{"big"})
	if err == nil {
		t.Fatal("buying an already-held event should error")
	}
	// ...and inventory must be untouched (still 9 after the one successful buy).
	t.Log("step: inventory for \"big\" should be untouched at 9 after the rejected re-buy")
	events, _, _ := s.GetAllEvents()
	if remaining := remainingFor(events, "big"); remaining != 9 {
		t.Fatalf("big remaining = %d after rejected re-buy, want 9", remaining)
	}
	t.Log("ok: duplicate hold rejected and inventory unchanged")
}

func TestBuyTickets_SoldOut(t *testing.T) {
	s := newTestService()
	t.Log("step: first buyer claims the single \"solo\" ticket (exhausts inventory)")
	if _, err, _ := s.BuyTickets("first", []string{"solo"}); err != nil {
		t.Fatalf("first buy of solo failed: %v", err)
	}
	// "solo" now has 0 tickets; a second buyer must be rejected.
	t.Log("step: second buyer tries sold-out \"solo\" (should error)")
	if _, err, _ := s.BuyTickets("second", []string{"solo"}); err == nil {
		t.Fatal("buying a sold-out event should error")
	}
	t.Log("ok: sold-out event rejected the second buyer")
}

// A multi-event purchase must be all-or-nothing: if one event in the batch is
// unavailable, no inventory changes at all.
func TestBuyTickets_AtomicOnPartialFailure(t *testing.T) {
	s := newTestService()
	// Sell out "solo" first.
	t.Log("step: selling out \"solo\" so a later batch will hit an unavailable event")
	if _, err, _ := s.BuyTickets("first", []string{"solo"}); err != nil {
		t.Fatalf("setup buy failed: %v", err)
	}
	// "carol" tries to buy {big, solo}; solo is sold out, so the whole thing
	// must fail and "big" must not be decremented or granted.
	t.Log("step: carol buys batch [big solo] where solo is sold out (whole batch must fail)")
	if _, err, _ := s.BuyTickets("carol", []string{"big", "solo"}); err == nil {
		t.Fatal("batch with a sold-out event should fail")
	}
	t.Log("step: carol should hold no tickets after the failed batch")
	tickets, _, _ := s.GetMyTickets("carol")
	if len(tickets) != 0 {
		t.Fatalf("carol holds %v after a failed batch, want none", tickets)
	}
	t.Log("step: \"big\" inventory should remain at 10 (not decremented by the failed batch)")
	events, _, _ := s.GetAllEvents()
	if remaining := remainingFor(events, "big"); remaining != 10 {
		t.Fatalf("big remaining = %d after a failed batch, want 10", remaining)
	}
	t.Log("ok: partial failure was atomic; no tickets granted and inventory intact")
}

func TestRefundTicket_Success(t *testing.T) {
	s := newTestService()
	t.Log("step: dave buys \"big\" then refunds it")
	if _, err, _ := s.BuyTickets("dave", []string{"big"}); err != nil {
		t.Fatalf("buy failed: %v", err)
	}
	if _, err, _ := s.RefundTicket("dave", "big"); err != nil {
		t.Fatalf("refund failed: %v", err)
	}
	t.Log("step: dave should hold no tickets after the refund")
	tickets, _, _ := s.GetMyTickets("dave")
	if len(tickets) != 0 {
		t.Fatalf("dave still holds %v after refund", tickets)
	}
	t.Log("step: \"big\" inventory should be restored to 10 after the refund")
	events, _, _ := s.GetAllEvents()
	if remaining := remainingFor(events, "big"); remaining != 10 {
		t.Fatalf("big remaining = %d after refund, want 10 (restored)", remaining)
	}
	t.Log("ok: refund released the ticket and restored inventory")
}

func TestRefundTicket_NotHeld(t *testing.T) {
	s := newTestService()
	t.Log("step: erin refunds \"big\" without ever holding it (should error)")
	if _, err, _ := s.RefundTicket("erin", "big"); err == nil {
		t.Fatal("refunding a ticket the user never held should error")
	}
	// Inventory must be unchanged (no phantom increment).
	t.Log("step: \"big\" inventory should stay at 10 (no phantom increment)")
	events, _, _ := s.GetAllEvents()
	if remaining := remainingFor(events, "big"); remaining != 10 {
		t.Fatalf("big remaining = %d after bogus refund, want 10", remaining)
	}
	t.Log("ok: bogus refund rejected and inventory unchanged")
}

func TestGetAllEvents_ReportsAttendees(t *testing.T) {
	s := newTestService()
	t.Log("step: alice and bob each buy \"big\" so it has two attendees")
	s.BuyTickets("alice", []string{"big"})
	s.BuyTickets("bob", []string{"big"})

	t.Log("step: GetAllEvents should report all 3 seeded events")
	events, _, _ := s.GetAllEvents()
	if len(events) != 3 {
		t.Fatalf("GetAllEvents returned %d events, want 3", len(events))
	}
	t.Log("step: \"big\" attendees should be exactly [alice bob]")
	for _, e := range events {
		if e.Name == "big" {
			attendees := append([]string(nil), e.Attendees...)
			sort.Strings(attendees)
			if len(attendees) != 2 || attendees[0] != "alice" || attendees[1] != "bob" {
				t.Fatalf("big attendees = %v, want [alice bob]", attendees)
			}
		}
	}
}

func TestGetMyTickets_UnknownUserIsEmpty(t *testing.T) {
	s := newTestService()
	t.Log("step: GetMyTickets for unknown user \"nobody\" should return no error and no tickets")
	tickets, err, rerr := s.GetMyTickets("nobody")
	if err != nil || rerr.Error() != "" {
		t.Fatalf("GetMyTickets errored: %v / %q", err, rerr.Error())
	}
	if len(tickets) != 0 {
		t.Fatalf("unknown user holds %v, want none", tickets)
	}
	t.Log("ok: unknown user holds no tickets")
}

func TestNewTicketBoxService_CopiesInventory(t *testing.T) {
	inventory := map[string]int{"e": 5}
	s := NewTicketBoxService(inventory)
	// Mutating the caller's map must not affect the service.
	t.Log("step: mutating the caller's inventory map to 999 after constructing the service")
	inventory["e"] = 999
	t.Log("step: service inventory for \"e\" should still be 5 (constructor copied the map)")
	events, _, _ := s.GetAllEvents()
	if remaining := remainingFor(events, "e"); remaining != 5 {
		t.Fatalf("service inventory = %d, want 5 (map should have been copied)", remaining)
	}
	t.Log("ok: service state is isolated from the caller's map")
}

// Concurrent buyers and refunders must not corrupt state or race. Under -race
// this fails loudly if the mutex is missing or misused. Afterwards, inventory
// plus outstanding tickets must equal the original supply for each event.
func TestConcurrentBuyRefund_NoRace(t *testing.T) {
	const supply = 50
	s := NewTicketBoxService(map[string]int{"concert": supply})

	var wg sync.WaitGroup
	const workers = 40
	t.Logf("step: launching %d workers that each buy+refund \"concert\" 5 times (supply=%d)", workers, supply)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			user := "u" + string(rune('A'+id%26)) + string(rune('0'+id/26))
			// Buy, then refund, a few times.
			for r := 0; r < 5; r++ {
				if _, err, _ := s.BuyTickets(user, []string{"concert"}); err == nil {
					s.RefundTicket(user, "concert")
				}
			}
		}(i)
	}
	t.Log("step: waiting for all workers to finish")
	wg.Wait()

	// After all activity settles, remaining + still-held must equal supply.
	t.Log("step: checking conservation: remaining + still-held must equal supply")
	events, _, _ := s.GetAllEvents()
	remaining := remainingFor(events, "concert")
	held := 0
	for _, e := range events {
		if e.Name == "concert" {
			held = len(e.Attendees)
		}
	}
	if remaining+held != supply {
		t.Fatalf("conservation violated: remaining %d + held %d != supply %d", remaining, held, supply)
	}
	if remaining < 0 {
		t.Fatalf("remaining went negative: %d", remaining)
	}
	t.Logf("ok: conservation holds (remaining %d + held %d == supply %d)", remaining, held, supply)
}

// remainingFor returns the TicketsRemaining for the named event, or -1 if
// absent.
func remainingFor(events []EventDetail, name string) int {
	for _, e := range events {
		if e.Name == name {
			return e.TicketsRemaining
		}
	}
	return -1
}
