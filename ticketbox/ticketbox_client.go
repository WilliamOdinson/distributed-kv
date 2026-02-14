//go:build client

package main

import (
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

func main() {
	address1 := defaultAddress1
	address2 := defaultAddress2
	if len(os.Args) > 1 {
		address1 = os.Args[1]
	}
	if len(os.Args) > 2 {
		address2 = os.Args[2]
	}

	log.Printf("Starting TicketBox client connecting to %s", address1)
	// create a client stub for the TicketBoxInterface using the remote package's CallerStubCreator.
	client1 := &TicketBoxInterface{}
	if err := remote.CallerStubCreator(client1, address1, isLossy, isDelayed); err != nil {
		log.Printf("Failed to register client: %v\n", err)
		return
	}

	// test the client by performing a series of operations for multiple users.
	var wg sync.WaitGroup
	testUser := []string{"Alice", "Bob", "Charlie"}
	for _, user := range testUser {
		log.Printf("Testing client for user: %s\n", user)
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			TestClient(u, client1)
		}(user)
	}
	wg.Wait()
	TestClient("David", client1) // deterministically buy the last ticket for event "14736" on client1

	// create another client stub to test the independent state of the second server instance.
	log.Printf("Starting TicketBox client connecting to %s", address2)
	client2 := &TicketBoxInterface{}
	if err := remote.CallerStubCreator(client2, address2, isLossy, isDelayed); err != nil {
		log.Printf("Failed to register client: %v\n", err)
		return
	}
	TestClient("William", client2) // William should be able to buy the ticket for event "14736" on client2 even client1 has sold out the ticket
}

// TestClient performs a series of operations using the TicketBoxInterface client for a given user.
// It tests:
// -- get all events
// -- get my tickets
// -- buy ticket for event "14736" (the event with insufficient tickets for all users)
// -- get my tickets again (check if ticket was added)
// -- refund ticket for event "14736" for other users to be able to buy it
func TestClient(user string, client *TicketBoxInterface) {
	// 1. get all events
	events, err, remoteErr := client.GetAllEvents()
	if err != nil {
		log.Printf("Error calling GetAllEvents: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling GetAllEvents: %v\n", remoteErr)
	} else {
		for _, event := range events {
			log.Printf("[%s] Event: %s, Tickets Remaining: %d, Attendees: %v\n", user, event.Name, event.TicketsRemaining, event.Attendees)
		}
	}

	// 2. get current tickets for this user
	tickets, err, remoteErr := client.GetMyTickets(user)
	if err != nil {
		log.Printf("Error calling GetMyTickets: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling GetMyTickets: %v\n", remoteErr)
	} else {
		log.Printf("[%s] has tickets: %v\n", user, tickets)
	}

	// 3. attempt to buy ticket for event "14736" (limited availability)
	buyResult, err, remoteErr := client.BuyTickets(user, []string{"14736", "15513", "15619"})
	if err != nil {
		log.Printf("Error calling BuyTickets: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling BuyTickets: %v\n", remoteErr)
	} else {
		log.Printf("[%s] bought ticket: %v\n", user, buyResult)
	}

	// 4. verify ticket purchase by getting tickets again
	tickets, err, remoteErr = client.GetMyTickets(user)
	if err != nil {
		log.Printf("Error calling GetMyTickets: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling GetMyTickets: %v\n", remoteErr)
	} else {
		log.Printf("[%s] has tickets: %v\n", user, tickets)
	}

	// 5. get all events again to see the updated tickets remaining and attendees list after purchase
	events, err, remoteErr = client.GetAllEvents()
	if err != nil {
		log.Printf("Error calling GetAllEvents: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling GetAllEvents: %v\n", remoteErr)
	} else {
		for _, event := range events {
			log.Printf("[%s] Event: %s, Tickets Remaining: %d, Attendees: %v\n", user, event.Name, event.TicketsRemaining, event.Attendees)
		}
	}

	// 6. refund ticket for event "14736" for other users to be able to buy it
	refundResult, err, remoteErr := client.RefundTicket(user, "14736")
	if err != nil {
		log.Printf("Error calling RefundTicket: %v\n", err)
	} else if remoteErr.Error() != "" {
		log.Printf("Remote error calling RefundTicket: %v\n", remoteErr)
	} else {
		log.Printf("[%s] successfully refunded ticket: %v\n", user, refundResult)
	}
}
