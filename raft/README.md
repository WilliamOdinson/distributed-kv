# raft

A Raft consensus implementation built on the `remote` library. Manages a replicated log across a fixed set of peers, providing leader election, log replication, and fault tolerance as long as a majority of peers are reachable.

Does not implement persistence, log compaction, or cluster membership changes.

## How it works

Each peer exposes two remote interfaces:

- **`RaftInterface`**: peer-to-peer RPCs (`RequestVote`, `AppendEntries`)
- **`ControlInterface`**: used by the controller/client to drive and observe the peer (`Activate`, `Deactivate`, `Terminate`, `NewCommand`, `GetStatus`, `GetCommittedCmd`)

The `RaftInterface` callee is started and stopped via `Activate`/`Deactivate` to simulate failure and recovery. The `ControlInterface` callee runs for the full lifetime of the peer.

## Usage

```go
peers := []raft.RaftSetupInfo{
    {Id: 0, Address: "localhost:8080"},
    {Id: 1, Address: "localhost:8081"},
    {Id: 2, Address: "localhost:8082"},
}

// Spawned in its own goroutine by the controller; blocks until Terminate().
go raft.NewRaftPeer(peers, 0)
```

Clients interact exclusively through `ControlInterface` remote calls. Direct in-process access is not the intended usage.

## Key behaviors

- Elections complete within ~1s; election timeouts are tuned to avoid split votes
- `NewCommand` returns only after the command is committed to a majority
- `GetCommittedCmd(index)` returns the committed entry at that log index
- Peers tolerate concurrent failures of up to `(n-1)/2` nodes

## Tests

```bash
go test -v -timeout 600s -race -cover ./...
```

The suite has two layers:

- **Unit tests** (`logic_test.go`) build a peer in memory and call its pure methods directly (`RequestVote`, `AppendEntries`, `calculateCommitIndex`, `isUpToDate`, `NewCommand`, `WaitForCommit`, ...). No network, no goroutines, no timing: they pin the election-restriction and log-consistency rules and run in ~1s.
- **Integration tests** (`election_test.go`, `commit_test.go`, `hkvcpeer_test.go`) stand up live clusters via the `Controller` harness in `testkit_test.go` and drive them through failures. Addresses come from the kernel (`:0`), reserved as a batch, to avoid port collisions.

Run only the fast unit tests with:

```bash
go test -run 'TestRequestVote|TestAppendEntries|TestCalculateCommitIndex|TestNewCommand|TestIsUpToDate|TestWaitForCommit|TestGetLogEntry|TestGetCommittedCmd|TestResetElectionTimeout|TestSubmitCommand' -race ./...
```
