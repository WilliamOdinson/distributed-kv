// Command hkvc-cluster launches a local, single-group HKVC cluster of N
// participants on fixed ports, activates them, and blocks so you can drive it
// with hkvcctl or curl. It is meant for demos and manual exploration, not
// production.
//
// Usage:
//
//	hkvc-cluster [-n 3] [-base 15440]
//
// It prints each participant's client address (what hkvcctl -addrs wants).
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"hkvc"
	"remote"
)

func main() {
	n := flag.Int("n", 3, "number of participants")
	base := flag.Int("base", 15440, "base TCP port; each participant uses 3 consecutive ports")
	flag.Parse()

	if *n < 1 {
		log.Fatal("-n must be >= 1")
	}

	// Each participant needs a client, control, and one raft (group 0) port.
	// Lay them out as base + 3*i + {0,1,2}.
	setup := make([]hkvc.HKVCSetupInfo, *n)
	clientAddrs := make([]string, *n)
	for i := 0; i < *n; i++ {
		client := "localhost:" + strconv.Itoa(*base+3*i)
		control := "localhost:" + strconv.Itoa(*base+3*i+1)
		raft0 := "localhost:" + strconv.Itoa(*base+3*i+2)
		setup[i] = hkvc.HKVCSetupInfo{
			Id:          1000 + i,
			ClientAddr:  client,
			ControlAddr: control,
			RaftAddrs:   map[int]string{0: raft0},
		}
		clientAddrs[i] = client
	}

	// Single group 0 containing everyone.
	members := make([]int, *n)
	for i := range members {
		members[i] = i
	}
	groups := map[int][]int{0: members}

	// Launch participants.
	for i := range setup {
		go hkvc.NewHKVCParticipant(setup, i, groups)
	}

	// Connect to each participant's control interface and activate it.
	log.Printf("waiting for %d participants to come up...", *n)
	time.Sleep(2 * time.Second)
	stubs := make([]*hkvc.HKVCControlInterface, *n)
	for i := range setup {
		stubs[i] = &hkvc.HKVCControlInterface{}
		if err := remote.CallerStubCreator(stubs[i], setup[i].ControlAddr, false, false); err != nil {
			log.Fatalf("cannot connect to participant %d control: %v", i, err)
		}
		for {
			if re := stubs[i].Activate(); re.Error() == "" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	fmt.Println("HKVC cluster is up. Client addresses:")
	fmt.Printf("  %s\n", joinComma(clientAddrs))
	fmt.Println("Try:")
	fmt.Printf("  hkvcctl -addrs %s set / hello world\n", joinComma(clientAddrs))
	fmt.Printf("  hkvcctl -addrs %s get / hello\n", joinComma(clientAddrs))
	fmt.Println("Press Ctrl-C to shut down.")

	// Terminate cleanly on Ctrl-C.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("shutting down cluster...")
	for i := range stubs {
		stubs[i].Terminate()
	}
}

func joinComma(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}
