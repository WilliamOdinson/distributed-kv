//go:build server

package main

import (
	"fmt"
	"log"
	"remote"
	"sync"
)

type TicketBoxInterface struct {
	GetAllEvents func() ([]string, error, remote.RemoteError)
	GetMyTickets func(user string) ([]string, error, remote.RemoteError)
	BuyTicket    func(user string, event string) (string, error, remote.RemoteError)
	RefundTicket func(user string, event string) (string, error, remote.RemoteError)
}

const (
	address   = "localhost:14736"
	isLossy   = true
	isDelayed = true
)

type TicketBoxService struct {
	events  map[string]int
	tickets map[string][]string
	mu      sync.Mutex
}

// GetAllEvents returns a list of all events.
// It is designed to not show clients the number of tickets remaining for each event
func (s *TicketBoxService) GetAllEvents() ([]string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]string, 0, len(s.events))
	for event := range s.events {
		events = append(events, event)
	}
	return events, nil, remote.RemoteError{}
}

// GetMyTickets returns a list of tickets for a given user.
func (s *TicketBoxService) GetMyTickets(user string) ([]string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tickets[user], nil, remote.RemoteError{}
}

// BuyTicket allows a user to buy a ticket for an event, if tickets are available and the user doesn't already have a ticket for that event.
func (s *TicketBoxService) BuyTicket(user string, event string) (string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if the user already has a ticket for the event
	for _, ticket := range s.tickets[user] {
		if ticket == event {
			return "", fmt.Errorf("[%s] user %s already has a ticket for event: %s", user, user, event), remote.RemoteError{}
		}
	}

	// check if the event is sold out
	if s.events[event] <= 0 {
		return "", fmt.Errorf("[%s] event %s is sold out", user, event), remote.RemoteError{}
	}

	// sell the ticket, thread safe decrement
	s.events[event]--
	s.tickets[user] = append(s.tickets[user], event)

	// return success message
	return fmt.Sprintf("[%s] bought 1 ticket for event: %s", user, event), nil, remote.RemoteError{}
}

// RefundTicket allows a user to refund a ticket for an event, if they have a ticket for that event.
func (s *TicketBoxService) RefundTicket(user string, event string) (string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tickets := s.tickets[user]
	for i, ticket := range tickets {
		if ticket == event {
			s.tickets[user] = append(tickets[:i], tickets[i+1:]...)
			s.events[event]++
			return fmt.Sprintf("[%s] refund successful for event: %s", user, event), nil, remote.RemoteError{}
		}
	}
	return "", fmt.Errorf("[%s] user %s does not have a ticket for event: %s", user, user, event), remote.RemoteError{}
}

func main() {
	server := &TicketBoxInterface{}
	service := &TicketBoxService{
		events: map[string]int{
			"14736": 1, // only 1 ticket for this event to test the sold out case
			"15513": 10,
			"15619": 10,
		},
		tickets: make(map[string][]string),
	}

	// create a new CalleeStub with our DIY library and start the server
	callee, err := remote.NewCalleeStub(server, service, address, isLossy, isDelayed)
	if err != nil {
		log.Printf("Failed to register server: %v\n", err)
		return
	}

	callee.Start()

	// block forever to keep the server running
	select {}
}
