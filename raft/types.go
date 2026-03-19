package raft

import (
	"remote"
	"sync"
	"time"
)

const (
	PollInterval       = 15 * time.Millisecond  // how often run() checks for timeouts
	HeartbeatInterval  = 100 * time.Millisecond // leader sends heartbeats this often
	ElectionTimeoutMin = 300                    // minimum election timeout in ms
	ElectionTimeoutMax = 500                    // maximum election timeout in ms
)

// Controller sends to Raft peer at creation time. DO NOT CHANGE.
type RaftSetupInfo struct {
	Id    int
	Addr  string
	Caddr string
}

// Raft peer must send to Controller on request. DO NOT CHANGE.
type StatusReport struct {
	Index     int
	Term      int
	Leader    bool
	Active    bool
	CallCount int
}

// LogEntry represents a single entry in the Raft log, containing the term
// when the entry was received by the leader and the client command.
type LogEntry struct {
	Term    int
	Command []byte
}

// RaftInterface defines the service RPC interface for peer-to-peer Raft RPCs.
type RaftInterface struct {
	RequestVote   func(term int, candidateId int, lastLogIndex int, lastLogTerm int) (int, bool, remote.RemoteError)
	AppendEntries func(term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) (int, bool, remote.RemoteError)
}

// ControlInterface defines the service RPC interface for Controller-to-peer RPCs.
type ControlInterface struct {
	Activate        func() remote.RemoteError
	Deactivate      func() remote.RemoteError
	Terminate       func() remote.RemoteError
	GetStatus       func() (StatusReport, remote.RemoteError)
	GetCommittedCmd func(int) ([]byte, remote.RemoteError)
	NewCommand      func([]byte) (StatusReport, remote.RemoteError)
}

// RaftPeer defines the state and RPC stubs for a single Raft peer.
type RaftPeer struct {
	// Persistent state on all servers, controlled by Controller
	id           int  // unique identifier
	totalPeers   int  // total number of peers in the Raft group
	isActivate   bool // whether this peer is active
	isTerminated bool // whether this peer has been terminated

	// Volatile state on all servers, controlled by Raft algorithm
	isLeader          bool          // whether this peer believes it is the leader
	isCandidate       bool          // whether this peer is currently a candidate
	currentTerm       int           // current term number
	votedFor          int           // candidateId that the peer voted for in current term, default to -1 if none
	log               []LogEntry    // log entries, indexed starting at 1 (index 0 is dummy)
	commitIndex       int           // index of highest log entry known to be committed
	lastApplied       int           // index of highest log entry applied to state machine
	nextIndex         []int         // each peer's index of the next log entry
	matchIndex        []int         // each peer's index of highest log entry
	lastHeartbeatTime time.Time     // last time a heartbeat was received or sent
	electionTimeout   time.Duration // randomized election timeout for this peer

	mu                sync.Mutex       // mutex to protect current peer's state
	raftCalleeStub    remote.Callee    // serves RaftInterface RPCs from other peers
	controlCalleeStub remote.Callee    // serves ControlInterface RPCs from Controller
	peerStubs         []*RaftInterface // caller stubs to communicate with other peers
	ch                chan struct{}    // signals NewRaftPeer to return on termination
}
