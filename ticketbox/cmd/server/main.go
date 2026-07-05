// Command server runs one or two independent TicketBox service instances over
// the remote RPC library. Each instance manages its own inventory, which
// demonstrates that several services can coexist on different addresses.
//
// Usage:
//
//	go run ./cmd/server [addr1] [addr2]
//
// addr1 and addr2 default to localhost:14736 and localhost:14737.
package main

import (
	"log"
	"os"
	"remote"
	"ticketbox"
)

const (
	defaultAddress1 = "localhost:14736"
	defaultAddress2 = "localhost:14737"
	isLossy         = true
	isDelayed       = true
)

// seedInventory is the starting inventory each instance is created with. Event
// "14736" has a single ticket so the sold-out path is easy to demonstrate.
func seedInventory() map[string]int {
	return map[string]int{
		"14736": 1,
		"15513": 10,
		"15619": 10,
	}
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

	for i, addr := range []string{address1, address2} {
		callee, err := remote.NewCalleeStub(&ticketbox.TicketBoxInterface{}, ticketbox.NewTicketBoxService(seedInventory()), addr, isLossy, isDelayed)
		if err != nil {
			log.Fatalf("failed to start instance %d: %v", i+1, err)
		}
		if err := callee.Start(); err != nil {
			log.Fatalf("failed to start instance %d listener: %v", i+1, err)
		}
		log.Printf("instance %d running on %s", i+1, addr)
	}

	select {} // block forever, serving requests
}
