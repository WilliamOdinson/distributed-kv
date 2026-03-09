package raft

import (
	"remote"
	"sync"
	"time"
)

const (
	PollInterval       = 15 * time.Millisecond // how often run() checks for timeouts
	HeartbeatInterval  = 50 * time.Millisecond // leader sends heartbeats this often
	ElectionTimeoutMin = 150                   // minimum election timeout in ms
	ElectionTimeoutMax = 300                   // maximum election timeout in ms
)

// Controller sends to Raft peer at creation time. do not change.
type RaftSetupInfo struct {
	Id    int
	Addr  string
	Caddr string
}

// Raft peer must send to Controller on request. do not change.
type StatusReport struct {
	Index     int
	Term      int
	Leader    bool
	Active    bool
	CallCount int
}

type LogEntry struct {
	Term    int
	Command []byte
}

// empty template for the "service interface" that specifies remote calls between Raft peers.
// it must include the two remote methods needed for the Raft algorithm.
type RaftInterface struct {
	RequestVote   func(term int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool, remote.RemoteError)
	AppendEntries func(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []int, leaderCommit int) (int, bool, remote.RemoteError)
}

// complete template for the Control "service interface" that specifies remote calls from
// Controller to Raft peer. the ControlInterface is active from the moment the Raft peer is
// created until the Raft peer is no longer needed by the Controller. this interface specifies
// six remote methods that you must implement. these methods are described later in this file.
type ControlInterface struct {
	Activate        func() remote.RemoteError
	Deactivate      func() remote.RemoteError
	Terminate       func() remote.RemoteError
	GetStatus       func() (StatusReport, remote.RemoteError)
	GetCommittedCmd func(int) ([]byte, remote.RemoteError)
	NewCommand      func([]byte) (StatusReport, remote.RemoteError)
}

type RaftPeer struct {
	mu sync.Mutex

	id           int
	totalPeers   int
	isActivate   bool
	isTerminated bool
	isLeader     bool
	isCandidate  bool

	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	raftCalleeStub    remote.Callee
	controlCalleeStub remote.Callee
	peerStubs         []*RaftInterface
	ch                chan struct{}

	lastHeartbeatTime     time.Time
	electionTimeout       time.Duration
	lastHeartBeatSentTime time.Time
	randomTimeout         time.Duration
}
