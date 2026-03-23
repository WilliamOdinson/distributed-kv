# ticketbox

A concurrent ticket booking service demonstrating the `remote` library. A single server instance manages event inventory; multiple clients can connect simultaneously.

## Service interface

```go
GetAllEvents() ([]EventDetail, error, remote.RemoteError)
GetMyTickets(user string) ([]string, error, remote.RemoteError)
BuyTickets(user string, events []string) (string, error, remote.RemoteError)
RefundTicket(user string, event string) (string, error, remote.RemoteError)
```

`BuyTickets` is idempotent — duplicate purchases for the same user/event are detected and ignored. `RefundTicket` only decrements inventory on a successful refund.

## Run

```bash
# server — address defaults to localhost:15440 localhost:15640 if omitted
go run ticketbox_server.go <addr>

# client
go run ticketbox_client.go <server-addr>
```

Multiple server instances can run on different ports; each manages its own independent inventory.
