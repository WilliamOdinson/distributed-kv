# ticketbox

A concurrent ticket-booking service that demonstrates the [`remote`](../remote/README.md) RPC library. A single `TicketBoxService` instance owns an independent event inventory; multiple clients may connect at once, and multiple server instances can run side by side with separate state.

## Layout

The service logic lives in a plain, importable package so it can be unit tested without a network. The two executables are thin wrappers around it.

```
ticketbox/
  service.go            # package ticketbox: TicketBoxService + TicketBoxInterface (the logic)
  service_test.go       # fast unit tests (no network)
  rpc_test.go           # end-to-end test over a real remote callee/caller (lossy socket)
  cmd/server/main.go    # server executable
  cmd/client/main.go    # client executable
```

## Service interface

```go
GetAllEvents() ([]EventDetail, error, remote.RemoteError)
GetMyTickets(user string) ([]string, error, remote.RemoteError)
BuyTickets(user string, events []string) (string, error, remote.RemoteError)
RefundTicket(user string, event string) (string, error, remote.RemoteError)
```

- `BuyTickets` is all-or-nothing: if the user already holds any listed event, or any event is sold out, the whole purchase is rejected and inventory is untouched.
- `RefundTicket` only returns a ticket to inventory on a successful refund.

## Run

```bash
# server, addresses default to localhost:14736 and localhost:14737
go run ./cmd/server [addr1] [addr2]

# client
go run ./cmd/client [addr1] [addr2]
```

Each server instance manages its own inventory, so a ticket sold out on one instance can still be bought on another.

## Tests

```bash
go test -v -race -cover ./...
```

The `ticketbox` package itself has full statement coverage; the two `cmd` mains are process entry points and are exercised by running them, not by unit tests.
