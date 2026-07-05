// Package raft implements the Raft consensus algorithm on top of the remote
// RPC library. It maintains a replicated log across a fixed set of peers and
// provides leader election, log replication, and fault tolerance as long as a
// majority of peers are reachable.
//
// It deliberately does NOT implement persistence, log compaction, or dynamic
// cluster membership changes; the peer set is fixed at construction.
//
// # Two RPC surfaces
//
// Every peer exposes two remote service interfaces:
//
//   - [RaftInterface] carries the peer-to-peer RPCs RequestVote and
//     AppendEntries. Its callee is started and stopped via Activate/Deactivate
//     so tests can simulate a peer failing and recovering.
//   - [ControlInterface] carries the controller/client RPCs (Activate,
//     Deactivate, Terminate, GetStatus, GetCommittedCmd, NewCommand). Its callee
//     runs for the entire lifetime of the peer.
//
// # Two entry points
//
// There are two ways to construct a peer, depending on who is driving it:
//
//   - [NewRaftPeer] is used by a standalone controller. It
//     starts a ControlInterface callee and blocks until the peer is terminated,
//     so it is meant to be launched in its own goroutine.
//   - [NewHKVCRaftPeer] is used when the peer is embedded inside an HKVC
//     participant. It omits the ControlInterface (HKVC supplies its own control
//     plane) and returns the *RaftPeer for direct in-process use through
//     [RaftPeer.SubmitCommand], [RaftPeer.WaitForCommit], and
//     [RaftPeer.GetLogEntry].
//
// # State machine
//
// A peer is at any moment a follower, a candidate, or a leader. The run loop
// (started as a goroutine) wakes every PollInterval and:
//
//   - as leader, sends heartbeats / AppendEntries every HeartbeatInterval and
//     advances commitIndex to the median matchIndex of the current term;
//   - as follower or candidate, starts an election once a randomized timeout in
//     [ElectionTimeoutMin, ElectionTimeoutMax) elapses without contact.
//
// Election timeouts are randomized per peer to avoid split votes, and the
// timing constants are tuned so an election typically completes within ~1s.
//
// # Log indexing
//
// The log is 1-indexed: index 0 holds a dummy sentinel entry so that the first
// real command lands at index 1. commitIndex is the highest index known to be
// replicated on a majority; GetCommittedCmd only returns entries at or below it.
//
// # Guarantees and limits
//
//   - NewCommand is accepted only by an active leader and, once committed,
//     appears at the returned index on a majority of peers.
//   - The cluster tolerates the simultaneous failure of up to (n-1)/2 peers.
//   - Because there is no persistence, a peer that restarts loses its state; the
//     failure model is process pause/resume (Deactivate/Activate), not crash
//     recovery from disk.
package raft
