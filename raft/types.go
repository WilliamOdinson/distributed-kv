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
	Index         int
	CommitIndex   int
	Term          int
	IsLeader      bool
	IsActive      bool
	CurrentLeader int
	CallCount     int
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
	// InstallSnapshot ships a compacted prefix of the log to a follower that has
	// fallen behind the leader's snapshot boundary (§7 of the Raft paper). It is
	// sent instead of AppendEntries when the entries a follower needs have
	// already been discarded. The snapshot is transferred in a single message
	// (no chunking). Returns the follower's current term so the leader can step
	// down if it is stale.
	InstallSnapshot func(term int, leaderId int, lastIncludedIndex int, lastIncludedTerm int, data []byte) (int, remote.RemoteError)
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
	isActive     bool // whether this peer is active
	isTerminated bool // whether this peer has been terminated

	// Volatile state on all servers, controlled by Raft algorithm
	isLeader          bool          // whether this peer believes it is the leader
	isCandidate       bool          // whether this peer is currently a candidate
	currentTerm       int           // current term number
	votedFor          int           // candidateId that the peer voted for in current term, default to -1 if none
	currentLeader     int           // uid of the current leader, -1 if unknown
	log               []LogEntry    // log entries; log[0] is a sentinel holding {lastIncludedTerm}, and log[i] holds absolute index lastIncludedIndex+i
	commitIndex       int           // absolute index of highest log entry known to be committed
	lastApplied       int           // absolute index of highest log entry applied to state machine
	nextIndex         []int         // each peer's absolute index of the next log entry
	matchIndex        []int         // each peer's absolute index of highest log entry
	lastHeartbeatTime time.Time     // last time a heartbeat was received or sent
	electionTimeout   time.Duration // randomized election timeout for this peer

	// Snapshot state (log compaction, Raft paper §7). lastIncludedIndex is the
	// absolute index of the last entry captured by the most recent snapshot;
	// everything at or below it has been discarded from log[]. When no snapshot
	// has been taken, lastIncludedIndex is 0 and all index math reduces to the
	// original 1-based scheme, so pre-snapshot behavior is unchanged.
	lastIncludedIndex int    // absolute index of the last entry included in the snapshot
	lastIncludedTerm  int    // term of that entry
	snapshot          []byte // most recent state-machine snapshot bytes (nil if none)

	// snapshotHandlers lets the embedding application (HKVC) produce and consume
	// snapshots of its own state machine. Optional; nil for the plain controller.
	snapshotHandlers *SnapshotHandlers

	mu                sync.Mutex       // mutex to protect current peer's state
	raftCalleeStub    remote.Callee    // serves RaftInterface RPCs from other peers
	controlCalleeStub remote.Callee    // serves ControlInterface RPCs from Controller
	peerStubs         []*RaftInterface // caller stubs to communicate with other peers
	ch                chan struct{}    // signals NewRaftPeer to return on termination
}

// SnapshotHandlers are optional callbacks an application registers so raft can
// snapshot and restore the application's state machine during log compaction.
//
//   - OnInstallSnapshot is invoked (with the raft mutex NOT held) when a leader
//     ships a snapshot via InstallSnapshot, so the application can replace its
//     state machine with the snapshot's contents.
//
// Producing a snapshot is driven by the application itself via
// RaftPeer.Snapshot, so no "produce" callback is needed here.
type SnapshotHandlers struct {
	OnInstallSnapshot func(groupID int, lastIncludedIndex int, data []byte)
	// GroupID identifies which application state machine this peer backs, passed
	// back to OnInstallSnapshot so a multi-group application can route it.
	GroupID int
}
