package raft

import (
	"log"
	"math/rand"
	"remote"
	"slices"
	"time"
)

// run is the main event loop for the Raft peer, started as a goroutine by NewRaftPeer.
// It periodically checks whether the peer should send heartbeats (if leader) or start
// an election (if follower/candidate and election timeout has elapsed). All shared state
// access is protected by rp's mutex. The loop exits when the peer is terminated.
func (rp *RaftPeer) run() {
	for {
		time.Sleep(PollInterval)
		rp.mu.Lock()
		if rp.isTerminated {
			rp.mu.Unlock()
			return
		}
		if !rp.isActivate {
			rp.mu.Unlock()
			continue
		}
		now := time.Now()

		term := rp.currentTerm
		leaderId := rp.id

		if rp.isLeader && now.Sub(rp.lastHeartbeatTime) >= HeartbeatInterval {
			// send heartbeats to followers
			for idx, stub := range rp.peerStubs {
				prevLogIndex := rp.nextIndex[idx] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rp.log) {
					prevLogTerm = rp.log[prevLogIndex].Term
				}
				if prevLogIndex >= len(rp.log) {
					prevLogIndex = len(rp.log) - 1
				}
				entries := make([]LogEntry, len(rp.log)-rp.nextIndex[idx])
				copy(entries, rp.log[rp.nextIndex[idx]:])
				commitIdx := rp.commitIndex

				go func(stub *RaftInterface) {
					replyTerm, replyOK, remoteErr := stub.AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, commitIdx)
					if remoteErr.Error() != "" {
						// handle remote error: simply return and wait for the next heartbeat
						return
					}
					rp.mu.Lock()
					if replyTerm > rp.currentTerm {
						// step down to follower
						rp.currentTerm = replyTerm
						rp.isLeader = false
						rp.isCandidate = false
						rp.votedFor = -1
					}
					if replyOK {
						rp.nextIndex[idx] = prevLogIndex + len(entries) + 1
						rp.matchIndex[idx] = rp.nextIndex[idx] - 1
						rp.commitIndex = rp.calculateCommitIndex()
					} else {
						rp.nextIndex[idx] = max(1, rp.nextIndex[idx]-1)
					}
					rp.mu.Unlock()
				}(stub)
			}
			rp.lastHeartbeatTime = now
			rp.mu.Unlock()
		} else if !rp.isLeader && now.Sub(rp.lastHeartbeatTime) >= rp.electionTimeout {
			rp.mu.Unlock()
			rp.StartElection()
			log.Println("\t", "Peer", rp.id, "\t", "starts election for term", rp.currentTerm)
		} else {
			rp.mu.Unlock()
		}
	}
}

// calculateCommitIndex determines the highest log index N that a majority of peers
// have replicated, per Raft paper §5.3. It first collects all matchIndex values and
// plus the leader's own last log index, sorts them, and picks the median.
// N is only adopted if the entry at N belongs to the current term.
func (rp *RaftPeer) calculateCommitIndex() int {
	if len(rp.log) == 0 {
		return rp.commitIndex
	}
	matchIndexes := make([]int, len(rp.matchIndex)+1)
	matchIndexes[0] = len(rp.log) - 1
	copy(matchIndexes[1:], rp.matchIndex)
	slices.Sort(matchIndexes)
	N := matchIndexes[len(matchIndexes)/2]

	// only update commitIndex if the entry at N is from current term
	if N > rp.commitIndex && N >= 0 && N < len(rp.log) && rp.log[N].Term == rp.currentTerm {
		return N
	}
	return rp.commitIndex
}

// StartElection starts a new election for the peer as described in §5.2 of the Raft paper.
// It first transitions this peer to candidate state, increments the current term, votes for itself, and sends RequestVote RPCs to all other existing peers.
//
// If:
//   - a majority of votes is received, the peer becomes leader.
//   - a higher term is discovered, the peer steps down to follower.
func (rp *RaftPeer) StartElection() {
	rp.mu.Lock()

	if !rp.isActivate || rp.isTerminated {
		rp.mu.Unlock()
		return
	}

	rp.isCandidate = true
	rp.currentTerm++
	rp.votedFor = rp.id
	rp.resetElectionTimeout()
	term := rp.currentTerm
	leaderId := rp.id
	lastLogIndex := len(rp.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rp.log[lastLogIndex].Term
	}
	rp.mu.Unlock()

	votesReceived := 1
	var voteCh = make(chan bool, len(rp.peerStubs))
	for _, stub := range rp.peerStubs {
		go func(stub *RaftInterface) {
			replyTerm, voteGranted, remoteErr := stub.RequestVote(term, leaderId, lastLogIndex, lastLogTerm)
			if remoteErr.Error() != "" {
				voteCh <- false
				// handle remote error: simply return and wait for the next election timeout
				return
			}
			rp.mu.Lock()
			if replyTerm > rp.currentTerm {
				// step down to follower
				rp.currentTerm = replyTerm
				rp.isLeader = false
				rp.isCandidate = false
				rp.votedFor = -1
				voteCh <- false
			} else if voteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
			rp.mu.Unlock()
		}(stub)
	}

	for i := 0; i < rp.totalPeers-1 && votesReceived < (rp.totalPeers+1)/2; i++ {
		if <-voteCh {
			votesReceived++
		}
	}

	rp.mu.Lock()
	if rp.isCandidate && rp.currentTerm == term && votesReceived >= (rp.totalPeers+1)/2 {
		// become leader
		rp.isLeader = true
		rp.isCandidate = false
		rp.nextIndex = make([]int, rp.totalPeers-1)
		for i := range rp.nextIndex {
			rp.nextIndex[i] = len(rp.log)
			rp.matchIndex[i] = 0
		}
		rp.lastHeartbeatTime = time.Time{}
	}
	rp.mu.Unlock()
}

// resetElectionTimeout is a helper function that picks a new random election timeout
// and resets the heartbeat timer.
// The timeout is a random value [ElectionTimeoutMin, ElectionTimeoutMax)
func (rp *RaftPeer) resetElectionTimeout() {
	seed := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin

	rp.electionTimeout = time.Duration(seed) * time.Millisecond
	rp.lastHeartbeatTime = time.Now()
}

// RequestVote handles an RequestVote RPC from a candidate as described in §5.2 of the Raft paper.
// It grants a vote if:
//   - this peer has not already voted for a different candidate in the same term
//   - the candidate's term is at least as large as (>=) this peer's current term
//   - the candidate's log is at least as up-to-date as (>=) this peer's log
//
// If the vote is granted, this peer's election timeout is reset.
// If the peer reject the vote for any upper conditions unmet, it returns its current term to the candidate for him to step down.
func (rp *RaftPeer) RequestVote(term int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if term < rp.currentTerm {
		return rp.currentTerm, false, remote.RemoteError{}
	}

	if term > rp.currentTerm {
		rp.currentTerm = term
		rp.isLeader = false
		rp.isCandidate = false
		rp.votedFor = -1
	}

	if (rp.votedFor == -1 || rp.votedFor == candidateId) && rp.isUpToDate(lastLogIndex, lastLogTerm) {
		rp.votedFor = candidateId
		rp.resetElectionTimeout()
		return rp.currentTerm, true, remote.RemoteError{}
	}

	return rp.currentTerm, false, remote.RemoteError{}
}

// isUpToDate checks whether a candidate's log is at least as up-to-date as this peer's
// log, per the election restriction in Raft paper §5.4.
func (rp *RaftPeer) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if len(rp.log) == 0 {
		return true
	}
	lastEntry := rp.log[len(rp.log)-1]
	if lastLogTerm != lastEntry.Term {
		return lastLogTerm > lastEntry.Term
	}
	return lastLogIndex >= len(rp.log)-1
}

// AppendEntries handles an incoming AppendEntries RPC from the leader, implements Raft paper §5.3.
// The RPC performs two different functions:
//   - heartbeat: if entries is empty, the RPC is a heartbeat to maintain the leader's authority.
//   - log replication: if entries is non-empty, the RPC replicates log entries from the leader.
//     if the leader's log is inconsistent with this peer's log, the RPC should delete any
//     conflicting entries and append the new entries from the leader.
func (rp *RaftPeer) AppendEntries(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) (int, bool, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if term < rp.currentTerm {
		return rp.currentTerm, false, remote.RemoteError{}
	}
	if term > rp.currentTerm {
		rp.currentTerm = term
		rp.isLeader = false
		rp.isCandidate = false
		rp.votedFor = -1
	}

	rp.isLeader = false
	rp.isCandidate = false

	logIndex := len(rp.log) - 1
	if prevLogIndex > logIndex || (prevLogIndex >= 0 && rp.log[prevLogIndex].Term != prevLogTerm) {
		return rp.currentTerm, false, remote.RemoteError{}
	}

	for i, entry := range entries {
		index := prevLogIndex + 1 + i
		if index < len(rp.log) {
			if rp.log[index].Term != entry.Term {
				rp.log = rp.log[:index]
				rp.log = append(rp.log, LogEntry{Term: entry.Term, Command: entry.Command})
			}
		} else {
			rp.log = append(rp.log, LogEntry{Term: entry.Term, Command: entry.Command})
		}
	}

	if leaderCommit > rp.commitIndex {
		rp.commitIndex = min(leaderCommit, len(rp.log)-1)
	}
	rp.resetElectionTimeout()

	return rp.currentTerm, true, remote.RemoteError{}
}

// NewRaftPeer creates and initialize a Raft peer. The function will:
//   - initialize the Raft peer's state based on the provided peerInfo and index
//   - create the Callee stub for the ControlInterface and start it
//   - create the Callee stub for the RaftInterface
//   - create Caller stubs for all other Raft peers (len(peerInfo)-1) in the group
//
// After initialization, the peer starts run() as a goroutine for the main loop.
func NewRaftPeer(peerInfo []RaftSetupInfo, index int) {
	rp := &RaftPeer{
		id:           peerInfo[index].Id,
		totalPeers:   len(peerInfo),
		isActivate:   false,
		isTerminated: false,
		isLeader:     false,
		isCandidate:  false,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peerInfo)-1),
		matchIndex:  make([]int, len(peerInfo)-1),

		ch: make(chan struct{}),
	}

	// create Callee stubs for ControlInterface, should start immediately
	controlStub, err := remote.NewCalleeStub(&ControlInterface{}, rp, peerInfo[index].Caddr, false, false)
	if err != nil {
		panic(err)
	}
	rp.controlCalleeStub = controlStub
	rp.controlCalleeStub.Start()

	// create Callee stubs for RaftInterface, should not start until Activate is called
	raftStub, err := remote.NewCalleeStub(&RaftInterface{}, rp, peerInfo[index].Addr, false, false)
	if err != nil {
		panic(err)
	}
	rp.raftCalleeStub = raftStub

	for i, info := range peerInfo {
		if i == index {
			continue
		}
		stub := &RaftInterface{}
		remote.CallerStubCreator(stub, info.Addr, false, false)
		rp.peerStubs = append(rp.peerStubs, stub)
	}

	go rp.run()
	<-rp.ch
}

//// method implementations for the ControlInterface

// Activate starts the RaftInterface so that to allow this peer to make or receive RPC calls.
func (rp *RaftPeer) Activate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.isActivate = true
	rp.resetElectionTimeout()
	rp.raftCalleeStub.Start()

	return remote.RemoteError{}
}

// Deactivate stops the RaftInterface so that to emulate disconnection/failure of the Raft peer.
func (rp *RaftPeer) Deactivate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.isActivate = false

	rp.raftCalleeStub.Stop()
	return remote.RemoteError{}
}

// Terminate permanently shuts down this Raft peer and clear the states.
func (rp *RaftPeer) Terminate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.isTerminated {
		return remote.RemoteError{}
	}
	rp.isTerminated = true
	rp.isActivate = false

	rp.raftCalleeStub.Stop()
	rp.controlCalleeStub.Stop()

	rp.ch <- struct{}{}

	return remote.RemoteError{}
}

// GetStatus returns a StatusReport, including the peer's:
//   - the last log index
//   - the current term
//   - whether the peer is the leader
//   - whether it is still alive, not terminated
//   - the total number of RPCs calls
func (rp *RaftPeer) GetStatus() (StatusReport, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	callCount := rp.raftCalleeStub.GetCallCount()

	return StatusReport{
		Index:     len(rp.log) - 1,
		Term:      rp.currentTerm,
		Leader:    rp.isLeader,
		Active:    rp.isActivate,
		CallCount: callCount,
	}, remote.RemoteError{}
}

// GetCommittedCmd returns the command at the given log index if the entry exists and
// has been committed. Returns nil if no committed entry exists at that index.
func (rp *RaftPeer) GetCommittedCmd(index int) ([]byte, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if index >= 0 && index < len(rp.log) && index <= rp.commitIndex {
		return rp.log[index].Command, remote.RemoteError{}
	}

	return nil, remote.RemoteError{}
}

// NewCommand accepts a client command from outside the raft cluster.
// The command rules that are ONLY accepted by the leader of the raft cluster.
func (rp *RaftPeer) NewCommand(command []byte) (StatusReport, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if !rp.isActivate {
		return StatusReport{
			Index:     0,
			Term:      rp.currentTerm,
			Leader:    false,
			Active:    rp.isActivate,
			CallCount: 0,
		}, remote.RemoteError{}
	}

	if rp.isActivate && !rp.isTerminated && rp.isLeader {
		rp.log = append(rp.log, LogEntry{
			Term:    rp.currentTerm,
			Command: command,
		})
	}

	return StatusReport{
		Index:     len(rp.log) - 1,
		Term:      rp.currentTerm,
		Leader:    rp.isLeader,
		Active:    rp.isActivate,
		CallCount: 0,
	}, remote.RemoteError{}
}
