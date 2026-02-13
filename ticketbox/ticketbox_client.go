//go:build client

package main

import (
	"remote"
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

func main() {
}
