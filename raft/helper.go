package raft

import "remote"

func NewHKVCRaftPeer(id int, selfAddr string, peerAddrs []string) *RaftPeer {
	rp := &RaftPeer{
		id:           id,
		totalPeers:   len(peerAddrs) + 1,
		isActivate:   false,
		isTerminated: false,
		isLeader:     false,
		isCandidate:  false,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peerAddrs)),
		matchIndex:  make([]int, len(peerAddrs)),

		ch: make(chan struct{}),
	}

	// create Callee stubs for RaftInterface, should not start until Activate is called
	raftStub, err := remote.NewCalleeStub(&RaftInterface{}, rp, selfAddr, false, false)
	if err != nil {
		panic(err)
	}
	rp.raftCalleeStub = raftStub

	for _, addr := range peerAddrs {
		stub := &RaftInterface{}
		remote.CallerStubCreator(stub, addr, false, false)
		rp.peerStubs = append(rp.peerStubs, stub)
	}

	go rp.run()
	return rp
}
