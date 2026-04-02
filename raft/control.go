package raft

import "remote"

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
		isActive:     false,
		isTerminated: false,
		isLeader:     false,
		isCandidate:  false,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // log index starts at 1, so we must add a dummy entry at index 0
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

	rp.isActive = true
	rp.resetElectionTimeout()
	rp.raftCalleeStub.Start()

	return remote.RemoteError{}
}

// Deactivate stops the RaftInterface so that to emulate disconnection/failure of the Raft peer.
func (rp *RaftPeer) Deactivate() remote.RemoteError {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.isActive = false

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
	rp.isActive = false

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
		Index:       len(rp.log) - 1,
		CommitIndex: rp.commitIndex,
		Term:        rp.currentTerm,
		IsLeader:    rp.isLeader,
		IsActive:    rp.isActive,
		CallCount:   callCount,
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

	if !rp.isActive {
		return StatusReport{
			Index:     0,
			Term:      rp.currentTerm,
			IsLeader:  false,
			IsActive:  rp.isActive,
			CallCount: 0,
		}, remote.RemoteError{}
	}

	if rp.isActive && !rp.isTerminated && rp.isLeader {
		rp.log = append(rp.log, LogEntry{
			Term:    rp.currentTerm,
			Command: command,
		})
	}

	return StatusReport{
		Index:     len(rp.log) - 1,
		Term:      rp.currentTerm,
		IsLeader:  rp.isLeader,
		IsActive:  rp.isActive,
		CallCount: 0,
	}, remote.RemoteError{}
}
