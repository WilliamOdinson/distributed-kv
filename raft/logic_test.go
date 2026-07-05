package raft

// Fast, deterministic unit tests for the pure state-machine logic of a raft
// peer. These build a RaftPeer in memory and call its methods directly, with no
// network, no goroutines, and no timing dependence, so they pin down the
// election-restriction and log-consistency rules precisely and run instantly.
//
// The networked election/commit behavior is covered separately by the
// Controller-driven integration tests.

import (
	"testing"
	"time"
)

// newBarePeer builds a RaftPeer with the given number of peers but without
// starting any RPC callees or the run loop. It is only safe to call the pure
// methods (RequestVote, AppendEntries, NewCommand, calculateCommitIndex,
// isUpToDate, GetCommittedCmd) on the result.
func newBarePeer(id, totalPeers int) *RaftPeer {
	return &RaftPeer{
		id:            id,
		totalPeers:    totalPeers,
		currentTerm:   0,
		votedFor:      -1,
		currentLeader: -1,
		log:           make([]LogEntry, 1), // dummy entry at index 0
		nextIndex:     make([]int, totalPeers-1),
		matchIndex:    make([]int, totalPeers-1),
	}
}

func TestRequestVote_RejectsLowerTerm(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.currentTerm = 5
	t.Logf("step: peer at term %d receives RequestVote from candidate at lower term 4", rp.currentTerm)
	term, granted, re := rp.RequestVote(4, 2, 0, 0)
	if re.Error() != "" {
		t.Fatalf("unexpected RemoteError: %q", re.Error())
	}
	if granted {
		t.Fatal("granted vote to a candidate with a lower term")
	}
	if term != 5 {
		t.Fatalf("returned term %d, want the peer's current term 5", term)
	}
	t.Log("ok: stale-term candidate refused, current term retained")
}

func TestRequestVote_GrantsWhenEligible(t *testing.T) {
	rp := newBarePeer(1, 3)
	t.Log("step: fresh peer receives RequestVote from eligible candidate 2 at term 1")
	term, granted, _ := rp.RequestVote(1, 2, 0, 0)
	if !granted {
		t.Fatal("did not grant vote to an eligible candidate")
	}
	if rp.votedFor != 2 {
		t.Fatalf("votedFor = %d, want 2", rp.votedFor)
	}
	if term != 1 {
		t.Fatalf("returned term %d, want 1 (adopted candidate term)", term)
	}
	t.Logf("ok: vote granted to candidate 2, votedFor=%d term=%d", rp.votedFor, term)
}

func TestRequestVote_OneVotePerTerm(t *testing.T) {
	rp := newBarePeer(1, 3)
	t.Log("step: candidate 2 requests a vote in term 1 (should be granted)")
	if _, granted, _ := rp.RequestVote(1, 2, 0, 0); !granted {
		t.Fatal("first eligible candidate was not granted a vote")
	}
	// A different candidate in the same term must be refused.
	t.Log("step: different candidate 3 requests a vote in the same term 1 (should be refused)")
	if _, granted, _ := rp.RequestVote(1, 3, 0, 0); granted {
		t.Fatal("granted a second vote to a different candidate in the same term")
	}
	// The same candidate asking again in the same term is fine (idempotent).
	t.Log("step: already-voted-for candidate 2 asks again in term 1 (should be idempotently granted)")
	if _, granted, _ := rp.RequestVote(1, 2, 0, 0); !granted {
		t.Fatal("refused a repeat vote request from the already-voted-for candidate")
	}
	t.Log("ok: at most one distinct candidate per term, repeats idempotent")
}

func TestRequestVote_HigherTermStepsDown(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.currentTerm = 2
	rp.isLeader = true
	rp.votedFor = 1
	t.Logf("step: leader at term %d receives RequestVote from candidate 2 at higher term 5", rp.currentTerm)
	if _, granted, _ := rp.RequestVote(5, 2, 0, 0); !granted {
		t.Fatal("did not grant vote after observing a higher term")
	}
	if rp.isLeader {
		t.Fatal("remained leader after observing a higher term")
	}
	if rp.currentTerm != 5 {
		t.Fatalf("currentTerm = %d, want 5", rp.currentTerm)
	}
	t.Logf("ok: stepped down to follower and adopted term %d", rp.currentTerm)
}

// The election restriction (§5.4): a candidate whose log is less up-to-date
// than the voter's must be refused even if otherwise eligible.
func TestRequestVote_RejectsStaleLog(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.currentTerm = 2
	rp.log = append(rp.log, LogEntry{Term: 2, Command: []byte("x")})
	// Candidate at term 2 but with an empty log (lastLogIndex 0, term 0).
	t.Log("step: voter has a term-2 entry; candidate 2 requests vote with an empty log (index 0, term 0)")
	if _, granted, _ := rp.RequestVote(2, 2, 0, 0); granted {
		t.Fatal("granted vote to a candidate with a less-up-to-date log")
	}
	t.Log("ok: election restriction refused the less-up-to-date candidate")
}

func TestIsUpToDate(t *testing.T) {
	rp := newBarePeer(1, 3)
	// Empty log: any candidate is at least as up-to-date.
	t.Log("step: empty log accepts any candidate (index 0, term 0)")
	if !rp.isUpToDate(0, 0) {
		t.Fatal("empty log should accept any candidate as up-to-date")
	}
	rp.log = append(rp.log,
		LogEntry{Term: 1, Command: []byte("a")},
		LogEntry{Term: 2, Command: []byte("b")},
	)
	t.Log("step: seeded voter log to lastIndex 2, lastTerm 2; running comparison cases")
	cases := []struct {
		name              string
		lastIdx, lastTerm int
		want              bool
	}{
		{"higher last term wins", 0, 3, true},
		{"lower last term loses", 5, 1, false},
		{"same term longer log wins", 2, 2, true},
		{"same term shorter log loses", 1, 2, false},
		{"same term equal length ties as up-to-date", 2, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("step: isUpToDate(lastIdx=%d, lastTerm=%d) want %v", tc.lastIdx, tc.lastTerm, tc.want)
			if got := rp.isUpToDate(tc.lastIdx, tc.lastTerm); got != tc.want {
				t.Fatalf("isUpToDate(%d,%d) = %v, want %v", tc.lastIdx, tc.lastTerm, got, tc.want)
			}
		})
	}
}

func TestAppendEntries_RejectsLowerTerm(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.currentTerm = 5
	t.Logf("step: peer at term %d receives AppendEntries from stale leader 2 at term 3", rp.currentTerm)
	term, ok, _ := rp.AppendEntries(3, 2, 0, 0, nil, 0)
	if ok {
		t.Fatal("accepted AppendEntries from a stale leader")
	}
	if term != 5 {
		t.Fatalf("returned term %d, want 5", term)
	}
	t.Log("ok: stale-leader AppendEntries rejected, current term retained")
}

func TestAppendEntries_HeartbeatAcceptsAndResetsRole(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isCandidate = true
	t.Log("step: candidate receives a valid heartbeat from leader 2 at term 1")
	term, ok, _ := rp.AppendEntries(1, 2, 0, 0, nil, 0)
	if !ok {
		t.Fatal("rejected a valid heartbeat")
	}
	if term != 1 {
		t.Fatalf("returned term %d, want 1", term)
	}
	if rp.isCandidate || rp.isLeader {
		t.Fatal("did not revert to follower on valid AppendEntries")
	}
	if rp.currentLeader != 2 {
		t.Fatalf("currentLeader = %d, want 2", rp.currentLeader)
	}
	t.Logf("ok: reverted to follower, currentLeader=%d", rp.currentLeader)
}

func TestAppendEntries_AppendsNewEntries(t *testing.T) {
	rp := newBarePeer(1, 3)
	entries := []LogEntry{
		{Term: 1, Command: []byte("one")},
		{Term: 1, Command: []byte("two")},
	}
	t.Logf("step: appending %d entries from leader 2 with leaderCommit 2", len(entries))
	if _, ok, _ := rp.AppendEntries(1, 2, 0, 0, entries, 2); !ok {
		t.Fatal("rejected valid entries")
	}
	if len(rp.log) != 3 {
		t.Fatalf("log length = %d, want 3 (dummy + 2 entries)", len(rp.log))
	}
	if rp.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2", rp.commitIndex)
	}
	t.Logf("ok: log length %d, commitIndex %d", len(rp.log), rp.commitIndex)
}

func TestAppendEntries_RejectsPrevLogMismatch(t *testing.T) {
	rp := newBarePeer(1, 3)
	// prevLogIndex 5 is far beyond our log, so this must be rejected.
	t.Log("step: AppendEntries with prevLogIndex 5 beyond our log (should be rejected)")
	if _, ok, _ := rp.AppendEntries(1, 2, 5, 1, nil, 0); ok {
		t.Fatal("accepted AppendEntries with an impossible prevLogIndex")
	}
	t.Log("ok: prevLogIndex mismatch rejected")
}

// A conflicting entry at an existing index must truncate the log and replace
// the tail, per §5.3.
func TestAppendEntries_TruncatesConflict(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.currentTerm = 2
	rp.log = append(rp.log,
		LogEntry{Term: 1, Command: []byte("old1")},
		LogEntry{Term: 1, Command: []byte("old2")},
	)
	// Leader says index 1 should have term 1 (matches), then a new index-2
	// entry with a different term, which must overwrite old2.
	entries := []LogEntry{{Term: 2, Command: []byte("new2")}}
	t.Log("step: appending a term-2 entry at index 2 that conflicts with existing old2 (should truncate+replace)")
	if _, ok, _ := rp.AppendEntries(2, 2, 1, 1, entries, 0); !ok {
		t.Fatal("rejected a valid conflict-resolving AppendEntries")
	}
	if len(rp.log) != 3 {
		t.Fatalf("log length = %d, want 3 after truncation+append", len(rp.log))
	}
	if rp.log[2].Term != 2 || string(rp.log[2].Command) != "new2" {
		t.Fatalf("index 2 = %+v, want term 2 / new2", rp.log[2])
	}
	t.Logf("ok: index 2 replaced with term %d / %q", rp.log[2].Term, rp.log[2].Command)
}

func TestNewCommand_OnlyLeaderAppends(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = true

	// Non-leader: must not append.
	t.Log("step: active non-leader receives NewCommand (should refuse to append)")
	sr, _ := rp.NewCommand([]byte("nope"))
	if sr.IsLeader {
		t.Fatal("non-leader reported IsLeader on NewCommand")
	}
	if len(rp.log) != 1 {
		t.Fatalf("non-leader appended to the log (len=%d)", len(rp.log))
	}

	// Leader: must append and return the new index.
	rp.isLeader = true
	rp.currentTerm = 3
	t.Logf("step: leader at term %d receives NewCommand (should append at index 1)", rp.currentTerm)
	sr, _ = rp.NewCommand([]byte("do it"))
	if !sr.IsLeader || sr.Index != 1 {
		t.Fatalf("leader NewCommand = {IsLeader:%v Index:%d}, want {true 1}", sr.IsLeader, sr.Index)
	}
	if rp.log[1].Term != 3 || string(rp.log[1].Command) != "do it" {
		t.Fatalf("appended entry = %+v, want term 3 / \"do it\"", rp.log[1])
	}
	t.Logf("ok: only the leader appended; entry at index %d has term %d", sr.Index, rp.log[1].Term)
}

func TestNewCommand_InactivePeerRejects(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = false
	rp.isLeader = true // even a "leader" that is inactive must refuse
	t.Log("step: inactive peer that still claims leadership receives NewCommand (should refuse)")
	sr, _ := rp.NewCommand([]byte("x"))
	if sr.IsActive {
		t.Fatal("inactive peer reported active on NewCommand")
	}
	if len(rp.log) != 1 {
		t.Fatal("inactive peer appended to the log")
	}
	t.Log("ok: inactive peer refused the command and left the log untouched")
}

func TestCalculateCommitIndex_MajorityMedian(t *testing.T) {
	rp := newBarePeer(1, 5) // 4 followers + self
	rp.currentTerm = 2
	rp.log = append(rp.log,
		LogEntry{Term: 2, Command: []byte("a")}, // index 1
		LogEntry{Term: 2, Command: []byte("b")}, // index 2
	)
	// Self is implicitly at index 2. Two followers reached index 2, so a
	// majority (self + 2) covers index 2.
	rp.matchIndex = []int{2, 2, 1, 0}
	t.Logf("step: 5-node leader at term %d, matchIndex %v (majority covers index 2)", rp.currentTerm, rp.matchIndex)
	if got := rp.calculateCommitIndex(); got != 2 {
		t.Fatalf("calculateCommitIndex = %d, want 2", got)
	}
	t.Log("ok: commit index advanced to the majority median 2")
}

func TestCalculateCommitIndex_IgnoresOtherTermEntries(t *testing.T) {
	rp := newBarePeer(1, 5)
	rp.currentTerm = 3
	// The replicated entry at index 1 is from an older term (2), so per §5.4 it
	// must NOT be committed by counting replicas alone.
	rp.log = append(rp.log, LogEntry{Term: 2, Command: []byte("old")})
	rp.matchIndex = []int{1, 1, 1, 1}
	t.Logf("step: leader at term %d, index 1 holds an older term-2 entry replicated to all (must not commit by count)", rp.currentTerm)
	if got := rp.calculateCommitIndex(); got != 0 {
		t.Fatalf("calculateCommitIndex = %d, want 0 (old-term entry not committable)", got)
	}
	t.Log("ok: old-term entry not committed, commit index stayed at 0")
}

func TestGetCommittedCmd_RespectsCommitIndex(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.log = append(rp.log,
		LogEntry{Term: 1, Command: []byte("c1")},
		LogEntry{Term: 1, Command: []byte("c2")},
	)
	rp.commitIndex = 1

	t.Logf("step: commitIndex is %d; reading committed index 1 (should return c1)", rp.commitIndex)
	if cmd, _ := rp.GetCommittedCmd(1); string(cmd) != "c1" {
		t.Fatalf("GetCommittedCmd(1) = %q, want c1", cmd)
	}
	// Index 2 exists but is not yet committed.
	t.Log("step: reading index 2 which exists but is uncommitted (should return nil)")
	if cmd, _ := rp.GetCommittedCmd(2); cmd != nil {
		t.Fatalf("GetCommittedCmd(2) = %q, want nil (uncommitted)", cmd)
	}
	// Out-of-range indices return nil.
	t.Log("step: reading out-of-range index 99 (should return nil)")
	if cmd, _ := rp.GetCommittedCmd(99); cmd != nil {
		t.Fatalf("GetCommittedCmd(99) = %q, want nil", cmd)
	}
	t.Log("ok: only committed in-range indices returned a command")
}

func TestGetLogEntry_Bounds(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.log = append(rp.log, LogEntry{Term: 1, Command: []byte("only")})
	t.Log("step: reading valid index 1 (should return only)")
	if cmd := rp.GetLogEntry(1); string(cmd) != "only" {
		t.Fatalf("GetLogEntry(1) = %q, want only", cmd)
	}
	t.Log("step: reading dummy sentinel index 0 (should return nil)")
	if cmd := rp.GetLogEntry(0); cmd != nil {
		t.Fatal("GetLogEntry(0) should be nil (dummy sentinel)")
	}
	t.Log("step: reading out-of-range index 5 (should return nil)")
	if cmd := rp.GetLogEntry(5); cmd != nil {
		t.Fatal("GetLogEntry(out of range) should be nil")
	}
	t.Log("ok: bounds honored, sentinel and out-of-range return nil")
}

func TestResetElectionTimeout_WithinConfiguredBounds(t *testing.T) {
	rp := newBarePeer(1, 3)
	minD := time.Duration(ElectionTimeoutMin) * time.Millisecond
	maxD := time.Duration(ElectionTimeoutMax) * time.Millisecond
	t.Logf("step: sampling resetElectionTimeout 200 times, expecting range [%v, %v)", minD, maxD)
	for i := 0; i < 200; i++ {
		rp.resetElectionTimeout()
		if rp.electionTimeout < minD || rp.electionTimeout >= maxD {
			t.Fatalf("electionTimeout %v out of range [%v, %v)", rp.electionTimeout, minD, maxD)
		}
	}
	t.Log("ok: all 200 sampled timeouts fell within the configured bounds")
}

func TestWaitForCommit_ReturnsWhenCommitted(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = true
	rp.isLeader = true
	rp.commitIndex = 4
	t.Logf("step: active leader with commitIndex %d waits for index 3 (already committed)", rp.commitIndex)
	idx, ok := rp.WaitForCommit(3, time.Second)
	if !ok || idx != 3 {
		t.Fatalf("WaitForCommit = (%d,%v), want (3,true)", idx, ok)
	}
	t.Logf("ok: WaitForCommit returned (%d, %v)", idx, ok)
}

func TestWaitForCommit_FailsWhenNotLeader(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = true
	rp.isLeader = false
	t.Log("step: active non-leader waits for index 1 (should fail immediately)")
	idx, ok := rp.WaitForCommit(1, time.Second)
	if ok || idx != -1 {
		t.Fatalf("WaitForCommit on non-leader = (%d,%v), want (-1,false)", idx, ok)
	}
	t.Log("ok: non-leader returned (-1, false)")
}

func TestWaitForCommit_FailsWhenTerminated(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = true
	rp.isLeader = true
	rp.isTerminated = true
	t.Log("step: terminated leader waits for index 1 (should fail immediately)")
	idx, ok := rp.WaitForCommit(1, time.Second)
	if ok || idx != -1 {
		t.Fatalf("WaitForCommit on terminated peer = (%d,%v), want (-1,false)", idx, ok)
	}
	t.Log("ok: terminated peer returned (-1, false)")
}

func TestSubmitCommand_DelegatesToNewCommand(t *testing.T) {
	rp := newBarePeer(1, 3)
	rp.isActive = true
	rp.isLeader = true
	rp.currentTerm = 1
	t.Logf("step: active leader at term %d submits a command (should append at index 1)", rp.currentTerm)
	idx, isLeader := rp.SubmitCommand([]byte("cmd"))
	if !isLeader || idx != 1 {
		t.Fatalf("SubmitCommand = (%d,%v), want (1,true)", idx, isLeader)
	}
	t.Logf("ok: SubmitCommand returned (%d, %v)", idx, isLeader)
}
