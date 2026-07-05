// Command client exercises a running TicketBox server through the remote RPC
// library, driving a series of buy/refund operations for several users against
// two independent server instances.
//
// Usage:
//
//	go run ./cmd/client [addr1] [addr2]
//
// addr1 and addr2 default to localhost:14736 and localhost:14737.
package main

import (
	"log"
	"os"
	"remote"
	"sync"
	"ticketbox"
)

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

	log.Printf("connecting to %s", address1)
	client1 := &ticketbox.TicketBoxInterface{}
	if err := remote.CallerStubCreator(client1, address1, isLossy, isDelayed); err != nil {
		log.Printf("failed to register client for %s: %v", address1, err)
		return
	}

	var wg sync.WaitGroup
	for _, user := range []string{"Alice", "Bob", "Charlie"} {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			exerciseClient(u, client1)
		}(user)
	}
	wg.Wait()
	exerciseClient("David", client1) // deterministically claims the last "14736" ticket

	log.Printf("connecting to %s", address2)
	client2 := &ticketbox.TicketBoxInterface{}
	if err := remote.CallerStubCreator(client2, address2, isLossy, isDelayed); err != nil {
		log.Printf("failed to register client for %s: %v", address2, err)
		return
	}
	// The second instance has its own inventory, so William can still buy
	// "14736" even after instance 1 sold out.
	exerciseClient("William", client2)
}

// exerciseClient runs a representative sequence of calls for one user: list
// events, check tickets, buy, re-check, list again, and refund.
func exerciseClient(user string, client *ticketbox.TicketBoxInterface) {
	if events, err, rerr := client.GetAllEvents(); err != nil {
		log.Printf("GetAllEvents error: %v", err)
	} else if rerr.Error() != "" {
		log.Printf("GetAllEvents remote error: %v", rerr)
	} else {
		for _, e := range events {
			log.Printf("[%s] event %s, remaining %d, attendees %v", user, e.Name, e.TicketsRemaining, e.Attendees)
		}
	}

	if tickets, err, rerr := client.GetMyTickets(user); err == nil && rerr.Error() == "" {
		log.Printf("[%s] holds %v", user, tickets)
	}

	if result, err, rerr := client.BuyTickets(user, []string{"14736", "15513", "15619"}); err != nil {
		log.Printf("[%s] BuyTickets error: %v", user, err)
	} else if rerr.Error() != "" {
		log.Printf("[%s] BuyTickets remote error: %v", user, rerr)
	} else {
		log.Printf("[%s] %s", user, result)
	}

	if tickets, err, rerr := client.GetMyTickets(user); err == nil && rerr.Error() == "" {
		log.Printf("[%s] now holds %v", user, tickets)
	}

	if result, err, rerr := client.RefundTicket(user, "14736"); err != nil {
		log.Printf("[%s] RefundTicket error: %v", user, err)
	} else if rerr.Error() != "" {
		log.Printf("[%s] RefundTicket remote error: %v", user, rerr)
	} else {
		log.Printf("[%s] %s", user, result)
	}
}
