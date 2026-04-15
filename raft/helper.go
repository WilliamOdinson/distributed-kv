package raft

import (
	"remote"
	"time"
)

// NewHKVCRaftPeer creates a Raft peer for use within an HKVC participant. Unlike NewRaftPeer
// (used by the controller in lab 2), this variant omits the ControlInterface callee since
// HKVC uses its own control interface.
func NewHKVCRaftPeer(id int, selfAddr string, peerAddrs []string) *RaftPeer {
	rp := &RaftPeer{
		id:           id,
		totalPeers:   len(peerAddrs) + 1,
		isActive:     false,
		isTerminated: false,
		isLeader:     false,
		isCandidate:  false,

		currentTerm:   0,
		votedFor:      -1,
		currentLeader: -1,
		log:           make([]LogEntry, 1),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peerAddrs)),
		matchIndex:    make([]int, len(peerAddrs)),

		ch: make(chan struct{}),
	}

	// create Callee stubs for RaftInterface, should not start until Activate is called
	raftStub, err := remote.NewCalleeStub(&RaftInterface{}, rp, selfAddr, false, false)
	if err != nil {
		panic(err)
	}
	rp.raftCalleeStub = raftStub

	// RPC caller stubs for sending to each peer
	for _, addr := range peerAddrs {
		stub := &RaftInterface{}
		remote.CallerStubCreator(stub, addr, false, false)
		rp.peerStubs = append(rp.peerStubs, stub)
	}

	go rp.run()
	return rp
}

// TerminateHKVC permanently shuts down this Raft peer so no further RPCs are accepted.
func (rp *RaftPeer) TerminateHKVC() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.isTerminated {
		return
	}
	rp.isTerminated = true
	rp.isActive = false
	rp.raftCalleeStub.Stop()
}

// SubmitCommand appends a command to the Raft log if this peer is the leader.
func (rp *RaftPeer) SubmitCommand(command []byte) (int, bool) {
	sr, _ := rp.NewCommand(command)
	return sr.Index, sr.IsLeader
}

// WaitForCommit blocks until the given log index is committed or until the peer loses leadership,
// or the peer is terminated, or the timeout expires.
func (rp *RaftPeer) WaitForCommit(index int, timeout time.Duration) (int, bool) {
	ddl := time.Now().Add(timeout)
	for {
		rp.mu.Lock()
		if rp.isTerminated {
			rp.mu.Unlock()
			return -1, false
		}
		if rp.commitIndex >= index {
			rp.mu.Unlock()
			return index, true
		}
		if !rp.isLeader || !rp.isActive {
			rp.mu.Unlock()
			return -1, false
		}
		rp.mu.Unlock()

		if time.Now().After(ddl) {
			return -1, false
		}
		time.Sleep(HeartbeatInterval)
	}
}

// GetLogEntry returns the raw command bytes at the given log index.
func (rp *RaftPeer) GetLogEntry(index int) []byte {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if index > 0 && index < len(rp.log) {
		return rp.log[index].Command
	}
	return nil
}
