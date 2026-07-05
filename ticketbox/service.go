// Package ticketbox implements a concurrent ticket-booking service that is
// exposed to clients through the remote RPC library. A single TicketBoxService
// instance owns an independent inventory of events and the per-user tickets
// bought against it; several instances can run side by side on different
// addresses, each with its own state.
//
// The service logic lives here, decoupled from any process entry point, so it
// can be unit tested directly (without a network) and also served remotely by
// the cmd/server binary and driven by the cmd/client binary.
package ticketbox

import (
	"fmt"
	"remote"
	"sync"
)

// EventDetail describes one event: its name, how many tickets remain, and who
// currently holds a ticket. It is shared, via gob, between client and server.
type EventDetail struct {
	Name             string
	TicketsRemaining int
	Attendees        []string
}

// TicketBoxInterface is the remote service contract shared by client and
// server. Every method ends in remote.RemoteError as the remote library
// requires.
type TicketBoxInterface struct {
	GetAllEvents func() ([]EventDetail, error, remote.RemoteError)
	GetMyTickets func(user string) ([]string, error, remote.RemoteError)
	BuyTickets   func(user string, events []string) (string, error, remote.RemoteError)
	RefundTicket func(user string, event string) (string, error, remote.RemoteError)
}

// TicketBoxService is the concrete implementation of TicketBoxInterface. All
// exported methods are safe for concurrent use by multiple callers.
type TicketBoxService struct {
	mu      sync.Mutex          // guards events and tickets
	events  map[string]int      // event name -> tickets remaining
	tickets map[string][]string // user -> events they hold tickets for
}

// NewTicketBoxService builds a service seeded with the given event inventory.
// The provided map is copied, so the caller may reuse or mutate it afterwards
// without affecting the service.
func NewTicketBoxService(inventory map[string]int) *TicketBoxService {
	events := make(map[string]int, len(inventory))
	for name, n := range inventory {
		events[name] = n
	}
	return &TicketBoxService{
		events:  events,
		tickets: make(map[string][]string),
	}
}

// GetAllEvents returns every event with its remaining count and attendee list.
func (s *TicketBoxService) GetAllEvents() ([]EventDetail, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := make([]EventDetail, 0, len(s.events))
	for event, remaining := range s.events {
		attendees := make([]string, 0)
		for user, held := range s.tickets {
			for _, ticket := range held {
				if ticket == event {
					attendees = append(attendees, user)
				}
			}
		}
		events = append(events, EventDetail{
			Name:             event,
			TicketsRemaining: remaining,
			Attendees:        attendees,
		})
	}
	return events, nil, remote.RemoteError{}
}

// GetMyTickets returns the events for which user holds a ticket.
func (s *TicketBoxService) GetMyTickets(user string) ([]string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tickets[user], nil, remote.RemoteError{}
}

// BuyTickets atomically buys one ticket per named event for user. It is
// idempotent-safe against double-holding: if the user already holds any of the
// events, or any event is sold out, the whole purchase is rejected and no
// inventory changes. On success the user receives one ticket per event.
func (s *TicketBoxService) BuyTickets(user string, events []string) (string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate the entire request before mutating any state, so a rejected
	// purchase leaves inventory untouched.
	for _, event := range events {
		for _, held := range s.tickets[user] {
			if held == event {
				return "", fmt.Errorf("[%s] user %s already has a ticket for event: %s", user, user, event), remote.RemoteError{}
			}
		}
		if s.events[event] <= 0 {
			return "", fmt.Errorf("[%s] event %s is sold out", user, event), remote.RemoteError{}
		}
	}

	for _, event := range events {
		s.events[event]--
		s.tickets[user] = append(s.tickets[user], event)
	}
	return fmt.Sprintf("[%s] bought %d tickets for events: %v", user, len(events), events), nil, remote.RemoteError{}
}

// RefundTicket returns user's ticket for event to inventory. It only mutates
// state on a successful refund; refunding a ticket the user does not hold is an
// error and leaves inventory unchanged.
func (s *TicketBoxService) RefundTicket(user string, event string) (string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	held := s.tickets[user]
	for i, ticket := range held {
		if ticket == event {
			s.tickets[user] = append(held[:i], held[i+1:]...)
			s.events[event]++
			return fmt.Sprintf("[%s] refund successful for event: %s", user, event), nil, remote.RemoteError{}
		}
	}
	return "", fmt.Errorf("[%s] user %s does not have a ticket for event: %s", user, user, event), remote.RemoteError{}
}
