package raft

import "remote"

// snapshot.go implements log compaction (Raft paper §7). The log is stored with
// a base offset: log[0] is a sentinel whose Term is lastIncludedTerm, and the
// real entry at absolute index i lives at physical slice position
// i - lastIncludedIndex. When lastIncludedIndex == 0 (no snapshot yet) this
// reduces exactly to the original 1-based indexing, so the rest of the
// algorithm is written in terms of these helpers and behaves identically until
// a snapshot is actually taken.
//
// All helpers assume the caller holds rp.mu.

// absLastIndex returns the absolute index of the last entry in the log,
// including entries captured only by the snapshot.
func (rp *RaftPeer) absLastIndex() int {
	return rp.lastIncludedIndex + len(rp.log) - 1
}

// physIndex converts an absolute log index to a physical position in rp.log.
// The result is negative if absIdx has already been compacted into the
// snapshot, and >= len(rp.log) if it is beyond the current log.
func (rp *RaftPeer) physIndex(absIdx int) int {
	return absIdx - rp.lastIncludedIndex
}

// hasEntry reports whether absIdx refers to a live entry currently held in
// rp.log (i.e. not compacted away and not past the end). Physical position 0 is
// the snapshot sentinel, not a real entry, so a live entry is at position >= 1.
func (rp *RaftPeer) hasEntry(absIdx int) bool {
	p := rp.physIndex(absIdx)
	return p >= 1 && p < len(rp.log)
}

// termAt returns the term of the entry at absolute index absIdx. It knows about
// the snapshot boundary: the term at lastIncludedIndex is lastIncludedTerm.
// For an index that is neither in the log nor the snapshot boundary it returns
// -1, which never equals a real term and so safely fails consistency checks.
func (rp *RaftPeer) termAt(absIdx int) int {
	if absIdx == rp.lastIncludedIndex {
		return rp.lastIncludedTerm
	}
	p := rp.physIndex(absIdx)
	if p < 0 || p >= len(rp.log) {
		return -1
	}
	return rp.log[p].Term
}

// entryAt returns the command bytes at absolute index absIdx, or nil if absIdx
// is not a live entry.
func (rp *RaftPeer) entryAt(absIdx int) []byte {
	if !rp.hasEntry(absIdx) {
		return nil
	}
	return rp.log[rp.physIndex(absIdx)].Command
}

// Snapshot compacts the log up to and including absolute index index, recording
// data as the application's serialized state machine at that point. Everything
// at or below index is discarded from the in-memory log; log[0] becomes a
// sentinel carrying the term of the last included entry.
//
// It is a no-op if index is not a committed, live entry (you can only snapshot
// what has been applied and not yet compacted). This is safe to call from the
// application while the peer is running; it takes the mutex itself.
func (rp *RaftPeer) Snapshot(index int, data []byte) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Refuse to snapshot beyond what is committed, or to re-snapshot a prefix we
	// have already compacted, or an index we no longer hold.
	if index <= rp.lastIncludedIndex || index > rp.commitIndex || !rp.hasEntry(index) {
		return
	}

	newLastTerm := rp.termAt(index)
	p := rp.physIndex(index)

	// Rebuild the log with a fresh sentinel at position 0 and the surviving
	// suffix (entries strictly after index) after it.
	tail := rp.log[p+1:]
	newLog := make([]LogEntry, 1, len(tail)+1)
	newLog[0] = LogEntry{Term: newLastTerm} // sentinel
	newLog = append(newLog, tail...)

	rp.log = newLog
	rp.lastIncludedIndex = index
	rp.lastIncludedTerm = newLastTerm
	rp.snapshot = data

	if rp.lastApplied < index {
		rp.lastApplied = index
	}
	if rp.commitIndex < index {
		rp.commitIndex = index
	}
}

// InstallSnapshot handles a snapshot shipped by the leader for a follower that
// has fallen behind the leader's compaction boundary (Raft paper §7). It
// replaces the follower's log prefix with the snapshot, adopts the leader's
// term if higher, and invokes the application's restore handler so the state
// machine catches up.
func (rp *RaftPeer) InstallSnapshot(term int, leaderId int, lastIncludedIndex int, lastIncludedTerm int, data []byte) (int, remote.RemoteError) {
	rp.mu.Lock()

	if term < rp.currentTerm {
		reply := rp.currentTerm
		rp.mu.Unlock()
		return reply, remote.RemoteError{}
	}
	if term > rp.currentTerm {
		rp.currentTerm = term
		rp.votedFor = -1
	}
	rp.isLeader = false
	rp.isCandidate = false
	rp.currentLeader = leaderId
	rp.resetElectionTimeout()

	// A stale or redundant snapshot (we are already at least this caught up) is
	// acknowledged but not applied.
	if lastIncludedIndex <= rp.lastIncludedIndex || lastIncludedIndex <= rp.commitIndex {
		reply := rp.currentTerm
		rp.mu.Unlock()
		return reply, remote.RemoteError{}
	}

	// If we happen to still hold the entry at lastIncludedIndex with a matching
	// term, keep the entries after it (fast path); otherwise discard the whole
	// log and start fresh from the snapshot.
	if rp.hasEntry(lastIncludedIndex) && rp.termAt(lastIncludedIndex) == lastIncludedTerm {
		tail := rp.log[rp.physIndex(lastIncludedIndex)+1:]
		newLog := make([]LogEntry, 1, len(tail)+1)
		newLog[0] = LogEntry{Term: lastIncludedTerm}
		newLog = append(newLog, tail...)
		rp.log = newLog
	} else {
		rp.log = []LogEntry{{Term: lastIncludedTerm}}
	}

	rp.lastIncludedIndex = lastIncludedIndex
	rp.lastIncludedTerm = lastIncludedTerm
	rp.snapshot = data
	rp.lastApplied = lastIncludedIndex
	if rp.commitIndex < lastIncludedIndex {
		rp.commitIndex = lastIncludedIndex
	}

	handlers := rp.snapshotHandlers
	reply := rp.currentTerm
	rp.mu.Unlock()

	// Restore the application state machine outside the lock: the handler may be
	// slow and must not deadlock with other raft operations.
	if handlers != nil && handlers.OnInstallSnapshot != nil {
		handlers.OnInstallSnapshot(handlers.GroupID, lastIncludedIndex, data)
	}
	return reply, remote.RemoteError{}
}

// installSnapshotToPeer ships the current snapshot to follower idx and applies
// the reply. It runs on its own goroutine from the leader's heartbeat loop.
// term/leaderId are the leader's values captured under the lock by the caller.
// It must be called WITHOUT rp.mu held; it re-acquires the lock to apply the
// reply (step down on a higher term, or advance replication state).
func (rp *RaftPeer) installSnapshotToPeer(idx int, stub *RaftInterface, term, leaderId, lastIncludedIndex, lastIncludedTerm int, data []byte) {
	replyTerm, remoteErr := stub.InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, data)
	if remoteErr.Error() != "" {
		return
	}
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if replyTerm > rp.currentTerm {
		rp.currentTerm = replyTerm
		rp.isLeader = false
		rp.isCandidate = false
		rp.votedFor = -1
		return
	}
	if rp.isLeader && rp.currentTerm == term {
		// The follower is now caught up through lastIncludedIndex.
		if rp.matchIndex[idx] < lastIncludedIndex {
			rp.matchIndex[idx] = lastIncludedIndex
		}
		if rp.nextIndex[idx] < lastIncludedIndex+1 {
			rp.nextIndex[idx] = lastIncludedIndex + 1
		}
	}
}

// SnapshotInfo returns the current snapshot boundary and bytes, for the
// application to persist or inspect. Safe to call concurrently.
func (rp *RaftPeer) SnapshotInfo() (lastIncludedIndex int, lastIncludedTerm int, data []byte) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.lastIncludedIndex, rp.lastIncludedTerm, rp.snapshot
}

// SetSnapshotHandlers registers the application's snapshot restore callback.
func (rp *RaftPeer) SetSnapshotHandlers(h *SnapshotHandlers) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.snapshotHandlers = h
}

// LastIncludedIndex reports the current snapshot boundary (0 if none), letting
// the application decide when its log has grown enough to snapshot.
func (rp *RaftPeer) LastIncludedIndex() int {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.lastIncludedIndex
}

// remote is imported for the RemoteError return type of InstallSnapshot.
