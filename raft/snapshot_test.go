package raft

// Fast, in-memory unit tests for log compaction (snapshotting). They build a
// bare peer, drive Snapshot / InstallSnapshot / AppendEntries directly, and
// assert on the compacted log and the absolute-index bookkeeping, with no
// network or goroutines.

import (
	"testing"
)

// appendCmds appends n committed entries (all in term `term`) to a bare peer,
// returning the absolute index of the last one.
func appendCmds(rp *RaftPeer, term, n int) int {
	for i := 0; i < n; i++ {
		rp.log = append(rp.log, LogEntry{Term: term, Command: []byte{byte(i)}})
	}
	rp.currentTerm = term
	rp.commitIndex = rp.absLastIndex()
	rp.lastApplied = rp.commitIndex
	return rp.absLastIndex()
}

func TestSnapshot_CompactsLog(t *testing.T) {
	t.Log("step: building a peer with 10 committed entries (indices 1..10)")
	rp := newBarePeer(1, 3)
	last := appendCmds(rp, 1, 10)
	if last != 10 {
		t.Fatalf("absLastIndex = %d, want 10", last)
	}

	t.Log("step: snapshotting through index 6")
	rp.Snapshot(6, []byte("state@6"))

	if rp.lastIncludedIndex != 6 || rp.lastIncludedTerm != 1 {
		t.Fatalf("snapshot boundary = (%d, term %d), want (6, term 1)", rp.lastIncludedIndex, rp.lastIncludedTerm)
	}
	// Physical log should now be: sentinel + entries 7..10 = 5 elements.
	if len(rp.log) != 5 {
		t.Fatalf("physical log length = %d, want 5 (sentinel + 4 tail)", len(rp.log))
	}
	// Absolute last index must be unchanged by compaction.
	if rp.absLastIndex() != 10 {
		t.Fatalf("absLastIndex after snapshot = %d, want 10", rp.absLastIndex())
	}
	t.Log("ok: log compacted, absolute indexing preserved")

	t.Log("step: entries at/below the boundary are gone; entries above remain readable")
	if rp.hasEntry(6) {
		t.Fatal("index 6 should have been compacted away")
	}
	if !rp.hasEntry(7) || !rp.hasEntry(10) {
		t.Fatal("indices 7..10 should still be live")
	}
	if got, _ := rp.GetCommittedCmd(6); got != nil {
		t.Fatalf("GetCommittedCmd(6) = %v, want nil (compacted)", got)
	}
	if got := rp.entryAt(7); got == nil {
		t.Fatal("entryAt(7) should return the command bytes")
	}
	// termAt at the boundary returns lastIncludedTerm.
	if rp.termAt(6) != 1 {
		t.Fatalf("termAt(6) = %d, want lastIncludedTerm 1", rp.termAt(6))
	}
}

func TestSnapshot_NoOpCases(t *testing.T) {
	rp := newBarePeer(1, 3)
	appendCmds(rp, 1, 5) // indices 1..5, committed

	t.Log("step: snapshotting beyond commitIndex is a no-op")
	rp.Snapshot(99, []byte("x"))
	if rp.lastIncludedIndex != 0 {
		t.Fatalf("snapshot past commit changed boundary to %d, want 0", rp.lastIncludedIndex)
	}

	t.Log("step: first real snapshot at 3, then a stale re-snapshot at 2 is a no-op")
	rp.Snapshot(3, []byte("s3"))
	if rp.lastIncludedIndex != 3 {
		t.Fatalf("boundary = %d, want 3", rp.lastIncludedIndex)
	}
	rp.Snapshot(2, []byte("s2"))
	if rp.lastIncludedIndex != 3 {
		t.Fatalf("stale snapshot moved boundary to %d, want 3", rp.lastIncludedIndex)
	}
}

// After a snapshot, AppendEntries whose prevLogIndex lands inside the compacted
// region must still be accepted (the prefix is known-consistent) and must
// append only the entries beyond the boundary.
func TestAppendEntries_AfterSnapshot(t *testing.T) {
	rp := newBarePeer(1, 3)
	appendCmds(rp, 1, 5)
	rp.Snapshot(3, []byte("s3")) // boundary at 3; log holds 4,5

	t.Log("step: leader sends entries 4..6 with prevLogIndex=3 (at our boundary)")
	entries := []LogEntry{
		{Term: 1, Command: []byte("four")}, // index 4, matches
		{Term: 1, Command: []byte("five")}, // index 5, matches
		{Term: 2, Command: []byte("six")},  // index 6, new
	}
	term, ok, _ := rp.AppendEntries(2, 2, 3, 1, entries, 6)
	if !ok {
		t.Fatal("AppendEntries with prevLogIndex at the snapshot boundary was rejected")
	}
	_ = term
	if rp.absLastIndex() != 6 {
		t.Fatalf("absLastIndex = %d, want 6 after appending index 6", rp.absLastIndex())
	}
	if string(rp.entryAt(6)) != "six" {
		t.Fatalf("entry at 6 = %q, want six", rp.entryAt(6))
	}
	t.Log("ok: append across the snapshot boundary worked")
}

// AppendEntries whose prevLogIndex is entirely below the boundary (already
// compacted) must be accepted without a term check, since we cannot verify a
// term we've discarded but the prefix is guaranteed consistent.
func TestAppendEntries_PrevBelowSnapshotBoundary(t *testing.T) {
	rp := newBarePeer(1, 3)
	appendCmds(rp, 1, 5)
	rp.Snapshot(4, []byte("s4")) // boundary 4; log holds 5

	t.Log("step: prevLogIndex=2 is below our boundary; append should be accepted")
	entries := []LogEntry{{Term: 1, Command: []byte("five")}} // index 5, already have it
	_, ok, _ := rp.AppendEntries(1, 2, 2, 1, entries, 5)
	if !ok {
		t.Fatal("AppendEntries with prevLogIndex below the snapshot boundary was rejected")
	}
}

func TestInstallSnapshot_AdoptsSnapshot(t *testing.T) {
	rp := newBarePeer(2, 3)
	appendCmds(rp, 1, 2) // small, stale log

	var restored struct {
		called bool
		idx    int
		data   string
	}
	rp.SetSnapshotHandlers(&SnapshotHandlers{
		GroupID: 7,
		OnInstallSnapshot: func(gid, idx int, data []byte) {
			restored.called = true
			restored.idx = idx
			restored.data = string(data)
		},
	})

	t.Log("step: leader installs a snapshot covering through index 50 in term 4")
	replyTerm, re := rp.InstallSnapshot(4, 9, 50, 3, []byte("snap@50"))
	if re.Error() != "" {
		t.Fatalf("InstallSnapshot returned RemoteError: %q", re.Error())
	}
	if replyTerm != 4 {
		t.Fatalf("reply term = %d, want 4 (adopted leader term)", replyTerm)
	}
	if rp.lastIncludedIndex != 50 || rp.lastIncludedTerm != 3 {
		t.Fatalf("boundary = (%d, term %d), want (50, term 3)", rp.lastIncludedIndex, rp.lastIncludedTerm)
	}
	if rp.absLastIndex() != 50 {
		t.Fatalf("absLastIndex = %d, want 50 (log reset to snapshot)", rp.absLastIndex())
	}
	if rp.commitIndex != 50 || rp.lastApplied != 50 {
		t.Fatalf("commit/apply = (%d,%d), want (50,50)", rp.commitIndex, rp.lastApplied)
	}
	if !restored.called || restored.idx != 50 || restored.data != "snap@50" {
		t.Fatalf("restore handler got %+v, want called with (50, snap@50)", restored)
	}
	t.Log("ok: snapshot installed and application state restored")
}

func TestInstallSnapshot_RejectsStaleTerm(t *testing.T) {
	rp := newBarePeer(2, 3)
	rp.currentTerm = 9
	t.Log("step: a snapshot from an older-term leader must be rejected")
	replyTerm, _ := rp.InstallSnapshot(5, 3, 100, 4, []byte("x"))
	if replyTerm != 9 {
		t.Fatalf("reply term = %d, want 9", replyTerm)
	}
	if rp.lastIncludedIndex != 0 {
		t.Fatal("stale-term snapshot should not have moved the boundary")
	}
}

// A redundant snapshot (we are already caught up past it) is acknowledged but
// does not clobber our more-advanced state.
func TestInstallSnapshot_IgnoresRedundant(t *testing.T) {
	rp := newBarePeer(2, 3)
	appendCmds(rp, 2, 10) // committed through index 10
	t.Log("step: installing a snapshot at index 5 when we are already committed to 10")
	rp.InstallSnapshot(2, 3, 5, 2, []byte("old"))
	if rp.lastIncludedIndex != 0 {
		// We never snapshotted locally, and the redundant install must not apply.
		t.Fatalf("redundant snapshot changed boundary to %d, want 0", rp.lastIncludedIndex)
	}
	if rp.absLastIndex() != 10 {
		t.Fatalf("redundant snapshot truncated our log to %d, want 10", rp.absLastIndex())
	}
}

func TestSnapshotInfo_And_LastIncludedIndex(t *testing.T) {
	rp := newBarePeer(1, 3)
	appendCmds(rp, 1, 8)
	rp.Snapshot(5, []byte("state@5"))

	if got := rp.LastIncludedIndex(); got != 5 {
		t.Fatalf("LastIncludedIndex = %d, want 5", got)
	}
	idx, term, data := rp.SnapshotInfo()
	if idx != 5 || term != 1 || string(data) != "state@5" {
		t.Fatalf("SnapshotInfo = (%d, %d, %q), want (5, 1, state@5)", idx, term, data)
	}
}
