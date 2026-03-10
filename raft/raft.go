package raft

// Raft implementation
//
// general notes:
//
// - you are welcome to use additional helper functions to handle aspects of the Raft algorithm
//   logic within the scope of a single Raft peer. you should not need to create any additional
//   remote calls between Raft peers or the Controller. if there is a desire to create additional
//   remote calls, please talk with the course staff before doing so.
//
// - Raft peers are not able to share information with each other or with the Controller in any
//   way other than through remote calls, allowing peers the potential to operate on physically
//   distinct machines
//
// - please make sure to read the Raft paper (https://raft.github.io/raft.pdf) before attempting
//   any coding for this lab. you will most likely need to refer to it many times during your
//   implementation and testing tasks, so please consult the paper for algorithm details.
//
// - each Raft peer will accept a lot of concurrent remote calls from other Raft peers and the
//   Controller, so use of concurrency controls is essential. you are expected to use such
//   controls to prevent race conditions in your implementation. the Makefile supports testing
//   both without and with go's race condition detector, and the testing system will enable the
//   race condition detector, which will cause tests to fail if any race conditions are
//   encountered.
//
// - don't forget to ask for help!

import (
	"math/rand"
	"remote"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const ()

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
		rp.lastHeartBeatSentTime = now

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
				entries := rp.log[rp.nextIndex[idx]:]
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
					} else {
						rp.nextIndex[idx] = max(1, rp.nextIndex[idx]-1)
					}
					rp.mu.Unlock()
				}(stub)
			}
			rp.commitIndex = rp.calculateCommitIndex()
			rp.lastHeartbeatTime = now
			rp.mu.Unlock()
		} else if !rp.isLeader && now.Sub(rp.lastHeartbeatTime) >= rp.electionTimeout {
			rp.mu.Unlock()
			rp.StartElection()
		} else {
			rp.mu.Unlock()
		}
	}
}

func (rp *RaftPeer) calculateCommitIndex() int {
	matchIndexes := append([]int{len(rp.log) - 1}, rp.matchIndex...)
	// sort matchIndexes in descending order
	slices.Sort(matchIndexes)
	N := matchIndexes[len(matchIndexes)/2]
	if N > rp.commitIndex && rp.log[N].Term == rp.currentTerm {
		return N
	}
	return rp.commitIndex
}

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
	rp.mu.Unlock()

	var votesReceived int64 = 1 // vote for self
	var wg sync.WaitGroup

	for _, stub := range rp.peerStubs {
		wg.Add(1)
		go func(stub *RaftInterface) {
			defer wg.Done()
			replyTerm, voteGranted, remoteErr := stub.RequestVote(term, leaderId, 0, 0)
			if remoteErr.Error() != "" {
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
			} else if voteGranted {
				atomic.AddInt64(&votesReceived, 1)
				if int(votesReceived) >= (rp.totalPeers+1)/2 {
					// become leader
					rp.isLeader = true
					rp.isCandidate = false
					rp.lastHeartBeatSentTime = time.Now()
					rp.nextIndex = make([]int, rp.totalPeers-1)
					for i := range rp.nextIndex {
						rp.nextIndex[i] = len(rp.log)
						rp.matchIndex[i] = 0
					}
				}
			}
			rp.mu.Unlock()
		}(stub)
	}
	wg.Wait()
}

func (rp *RaftPeer) resetElectionTimeout() {
	seed := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin

	rp.electionTimeout = time.Duration(seed) * time.Millisecond
	rp.lastHeartbeatTime = time.Now()
}

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

// TODO: you will need to define a struct that contains the parameters/variables that define
// and explain the current status of each Raft peer. it doesn't matter what you call this
// struct, and the test code doesn't really care what state it contains, so this part is up
// to you.

// the Controller calls NewRaftPeer in its own go routine to spawn a new Raft peer. the
// arguments contain everything needed for the new Raft peer to determine its own identity and
// callee addresses as well as the relevant callee address of all other Raft peers. examples
// of the RaftSetupInfo are provided in the lab description on canvas. the index parameter
// indicates the index in the slice of RaftSetupInfo structs corresponding to this new Raft
// peer, and the remaining info is for other peers.
//
// TODO: spawn a new raft peer (called in its own go routine by the Controller)
func NewRaftPeer(peerInfo []RaftSetupInfo, index int) {

	// when a new raft peer is created, its initial state should be populated into the
	// corresponding struct entries, it should create two Callee stubs and N-1 Caller stubs,
	// where N is the Raft group size. the Callee stub for the ControlInterface must be
	// started immediately, so the Raft peer can accept commands from the Controller, but
	// the Callee stub for the RemoteInterface should not be started until the Controller
	// issues the remote call telling the peer to start.
	//
	// the CalleeStubs using the RemoteInterface and ControlInterface should bind to the
	// addresses in the Addr and Caddr entry in peerInfo[index], respectively. each caller
	// stub created using remote.CallerStubCreator should be used to send Raft algorithm
	// commands to a different Raft peer in the group. the addresses provided by the
	// Controller are guaranteed to be unique (i.e., no peers will have the same ID or use
	// the same address).
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
		nextIndex:   make([]int, len(peerInfo)),
		matchIndex:  make([]int, len(peerInfo)),

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

// * Activate -- this remote method is used exclusively by the Controller whenever it needs
// to start the underlying server in the Raft peer and allow it to receive calls from other
// Raft peers. this is used to emulate connecting a new peer to the network or recovery of a
// previously failed peer. when this method is called, the Raft peer should do whatever is
// necessary to enable its remote.CallerStub interface to support remote calls from other Raft
// peers as soon as the method returns (i.e., if it takes time for the remote.CallerStub to
// start, this method should not return until that happens). the method should not otherwise
// block the Controller.
//
// TODO: implement the Activate remote method

func (rp *RaftPeer) Activate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.isActivate = true
	rp.raftCalleeStub.Start()

	return remote.RemoteError{}
}

// * Deactivate -- this remote method performs the "inverse" operation to Activate, namely to
// stop the underlying server in the Raft peer to emulate disconnection / failure of the Raft
// peer. when called, the Raft peer should disable only the stub serving the RaftInterface,
// causing any remote calls to this Raft peer to fail due to connection error. when
// deactivated, a Raft peer should not make or receive any remote calls on the stub using the
// RaftInterface, and any execution of the Raft protocol should effectively pause. however,
// local state should be maintained and the stub using the ControlInterface should continue to
// operate without disruption. if a Raft node was the leader when it was deactivated, it
// should still believe it is the leader when it reactivates.
//
// TODO: implement the Deactivate remote method

func (rp *RaftPeer) Deactivate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.isActivate = false

	rp.raftCalleeStub.Stop()
	return remote.RemoteError{}
}

// * Terminate -- this remote method is used exclusively by the Controller to permanently
// cease operation of the Raft peer. this is called at the end of each test when the Raft peer
// is no longer needed, and it allows the Raft peer to completely terminate all services and
// delete all relevant state.
//
// TODO: implement the Terminate remote method

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

// * GetStatus -- this remote method is used exclusively by the Controller. this method takes
// no arguments and is essentially a "getter" for the state of the Raft peer, including the
// Raft peer's current term, current last log index, role in the Raft algorithm,
// active/non-active state, and total number of remote calls handled since starting. the
// method returns a StatusReport as defined above.
//
// TODO: implement the GetStatus remote method

func (rp *RaftPeer) GetStatus() (StatusReport, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	callCount := rp.raftCalleeStub.GetCallCount()

	return StatusReport{
		Index:     0,
		Term:      rp.currentTerm,
		Leader:    rp.isLeader,
		Active:    rp.isActivate,
		CallCount: callCount,
	}, remote.RemoteError{}
}

// * GetCommittedCmd -- this remote method is used exclusively by the Controller. this method
// provides an input argument `index`. if the Raft peer has a log entry at the given `index`,
// and that log entry has been committed (per the Raft algorithm), then the command stored in
// the log entry should be returned to the Controller. otherwise, the Raft peer should return
// the nil value of the command type to indicate that no committed log entry exists at that
// index.
//
// TODO: implement the GetCommittedCmd remote method
func (rp *RaftPeer) GetCommittedCmd(index int) ([]byte, remote.RemoteError) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if index < len(rp.log) && index <= rp.commitIndex {
		return rp.log[index].Command, remote.RemoteError{}
	}

	return nil, remote.RemoteError{}
}

// * NewCommand -- this remote method is used exclusively by the Controller. this method
// emulates submission of a new command by a Raft client to this Raft peer, which should be
// handled and processed according to the rules of the Raft algorithm. in particular, the Raft
// peer should accept the command only if it is currently active and believes it is the leader.
// regardless of whether the command is accepted and processed, the Raft peer should return a
// StatusReport reflecting the status of the Raft peer after the new command message was
// received.
//
// TODO: implement the NewCommand remote method
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

//// method implementations for the RaftInterface

// * RequestVote -- this remote method is called (only) by other Raft peers and should operate
// according to the description in the Raft paper.
//
// TODO: implement the RequestVote remote method (which you can name/structure as desired)

// * AppendEntries -- this remote method is called (only) by other Raft peers and should
// operate according to the description in the Raft paper.
//
// TODO: implement the AppendEntries remote method (which you can name/structure as desired)
