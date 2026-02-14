//go:build server

package main

import (
	"fmt"
	"log"
	"os"
	"remote"
	"sync"
)

// EventDetails struct defines the details of an event, including its name, the number of tickets remaining, and the list of attendees. This struct is used to represent the information about each event in the ticket box system, shared between client and server.
type EventDetail struct {
	Name             string
	TicketsRemaining int
	Attendees        []string
}

// TicketBoxInterface defines the remote service contract, shared between client and server.
type TicketBoxInterface struct {
	GetAllEvents func() ([]EventDetail, error, remote.RemoteError)
	GetMyTickets func(user string) ([]string, error, remote.RemoteError)
	BuyTickets   func(user string, events []string) (string, error, remote.RemoteError)
	RefundTicket func(user string, event string) (string, error, remote.RemoteError)
}

// Configuration constants for remote service connection, shared between client and server.
const (
	defaultAddress1 = "localhost:14736"
	defaultAddress2 = "localhost:14737"
	isLossy         = true
	isDelayed       = true
)

// TicketBoxService is the implementation of the TicketBoxInterface. It implements all the methods defined in the interface for clients to invoke remotely.
type TicketBoxService struct {
	events  map[string]int      // map event name to number of tickets remaining
	tickets map[string][]string // map user name to list of events they have tickets
	mu      sync.Mutex          // mutex to protect concurrent access to events and tickets
}

// GetAllEvents returns a list of all events.
// It is designed to not show clients the number of tickets remaining for each event
func (s *TicketBoxService) GetAllEvents() ([]EventDetail, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]EventDetail, 0, len(s.events))

	for event, remainingTicket := range s.events {
		attendees := make([]string, 0)
		for user, tickets := range s.tickets {
			for _, ticket := range tickets {
				if ticket == event {
					attendees = append(attendees, user)
				}
			}
		}
		events = append(events, EventDetail{
			Name:             event,
			TicketsRemaining: remainingTicket,
			Attendees:        attendees,
		})
	}
	return events, nil, remote.RemoteError{}
}

// GetMyTickets returns the list of events for which the user has tickets.
func (s *TicketBoxService) GetMyTickets(user string) ([]string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tickets[user], nil, remote.RemoteError{}
}

// BuyTickets allows a user to attempt to buy tickets for multiple events.
// It checks whether the user doesn't already have a ticket for each event and if there are still tickets available.
func (s *TicketBoxService) BuyTickets(user string, events []string) (string, error, remote.RemoteError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check if the user already has a ticket for any of the events
	for _, event := range events {
		for _, ticket := range s.tickets[user] {
			if ticket == event {
				return "", fmt.Errorf("[%s] user %s already has a ticket for event: %s", user, user, event), remote.RemoteError{}
			}
		}

		// check if the event is sold out
		if s.events[event] <= 0 {
			return "", fmt.Errorf("[%s] event %s is sold out", user, event), remote.RemoteError{}
		}
	}

	// sell the ticket, thread safe decrement
	for _, event := range events {
		s.events[event]--
		s.tickets[user] = append(s.tickets[user], event)
	}

	// return success message
	return fmt.Sprintf("[%s] bought %d tickets for events: %v", user, len(events), events), nil, remote.RemoteError{}
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
	address1 := defaultAddress1
	if len(os.Args) > 1 {
		address1 = os.Args[1]
	}

	address2 := defaultAddress2
	if len(os.Args) > 2 {
		address2 = os.Args[2]
	}

	// Instance 1
	service1 := &TicketBoxService{
		events: map[string]int{
			"14736": 1, // only 1 ticket for this event to test the sold out case
			"15513": 10,
			"15619": 10,
		},
		tickets: make(map[string][]string),
	}
	callee1, err := remote.NewCalleeStub(&TicketBoxInterface{}, service1, address1, isLossy, isDelayed)
	if err != nil {
		log.Fatalf("Failed to start instance 1: %v\n", err)
	}
	callee1.Start()
	log.Printf("Instance 1 running on %s\n", address1)

	// Instance 2
	service2 := &TicketBoxService{
		events: map[string]int{
			"14736": 1, // only 1 ticket for this event to test the sold out case
			"15513": 10,
			"15619": 10,
		},
		tickets: make(map[string][]string),
	}
	callee2, err := remote.NewCalleeStub(&TicketBoxInterface{}, service2, address2, isLossy, isDelayed)
	if err != nil {
		log.Fatalf("Failed to start instance 2: %v\n", err)
	}
	callee2.Start()
	log.Printf("Instance 2 running on %s\n", address2)

	// block forever to keep the server running
	select {}
}
