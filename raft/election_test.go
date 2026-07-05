package raft

// Integration tests for leader election under a live, networked cluster driven
// by the Controller harness. These are the slow, timing-dependent tests; the
// fast in-memory equivalents live in logic_test.go.

import (
	"testing"
	"time"
)

// A single-peer cluster must be able to stand up and answer status queries.
func TestElection_Setup(t *testing.T) {
	t.Log("step: creating a 1-node cluster")
	ctrl := NewController(t, 1)
	t.Log("step: querying GetStatus on the fresh peer")
	if _, re := ctrl.stubs[0].GetStatus(); re.Error() != "" {
		t.Fatalf("GetStatus on a fresh peer failed: %s", re.Error())
	}
	t.Log("ok: fresh peer answered GetStatus")
}

// A healthy cluster must elect exactly one leader, agree on a term >= 1, and
// still have a leader after several election windows elapse without failures.
func TestElection_ElectsStableLeader(t *testing.T) {
	t.Log("step: creating a 3-node cluster")
	ctrl := NewController(t, 3)

	t.Log("step: waiting for an initial leader to emerge")
	ctrl.findLeader()

	term1 := ctrl.getTerm()
	t.Logf("step: checking initial term %d is at least 1", term1)
	if term1 < 1 {
		t.Fatalf("term is %d, want at least 1", term1)
	}

	t.Logf("step: sleeping %v to let leadership settle without induced failures", 2*electionSettleTime)
	time.Sleep(2 * electionSettleTime)
	if term2 := ctrl.getTerm(); term1 != term2 {
		t.Logf("note: term changed %d -> %d with no induced failure", term1, term2)
	}

	t.Log("step: confirming exactly one leader still exists")
	ctrl.findLeader() // still exactly one leader
	t.Log("ok: cluster kept a single stable leader")
}

// Killing the leader must trigger a new election; rejoining the old leader must
// not disturb the new one; losing quorum must yield no leader; and restoring
// quorum must elect one again.
func TestElection_ReElectionAfterFailures(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	leader1 := ctrl.findLeader()
	t.Logf("step: disconnecting leader %d and waiting for a replacement", leader1)

	ctrl.disconnect(leader1)
	ctrl.findLeader() // a replacement must appear
	t.Log("ok: a replacement leader was elected")

	t.Logf("step: reconnecting old leader %d and confirming leadership stays intact", leader1)
	ctrl.connect(leader1)
	leader2 := ctrl.findLeader() // old leader rejoining doesn't break things
	t.Logf("ok: current leader is %d after old leader rejoined", leader2)

	// Drop a majority: no leader should exist.
	t.Logf("step: dropping majority (peers %d and %d) and expecting no leader", leader2, (leader2+1)%n)
	ctrl.disconnect(leader2)
	ctrl.disconnect((leader2 + 1) % n)
	ctrl.ensureNoLeader()
	t.Log("ok: no leader while quorum is lost")

	// Restore quorum: a leader must be elected again.
	t.Logf("step: restoring quorum by reconnecting peer %d", (leader2+1)%n)
	ctrl.connect((leader2 + 1) % n)
	ctrl.findLeader()
	t.Log("ok: a leader was elected once quorum returned")

	// Old leader rejoining still leaves exactly one leader.
	t.Logf("step: reconnecting old leader %d and confirming a single leader", leader2)
	ctrl.connect(leader2)
	ctrl.findLeader()
	t.Log("ok: exactly one leader after all peers rejoined")
}

// Across many rounds of minority failures, a leader must exist in every round.
func TestElection_SequentialElections(t *testing.T) {
	const n = 7
	t.Logf("step: creating a %d-node cluster and electing an initial leader", n)
	ctrl := NewController(t, n)
	ctrl.findLeader()

	for round := 0; round < 10; round++ {
		p1 := ctrl.randIntn(n)
		p2 := ctrl.randIntn(n)
		p3 := ctrl.randIntn(n)
		t.Logf("round %d: disconnecting peers %d, %d, %d then expecting a leader", round, p1, p2, p3)
		ctrl.disconnect(p1)
		ctrl.disconnect(p2)
		ctrl.disconnect(p3)

		ctrl.findLeader()

		ctrl.connect(p1)
		ctrl.connect(p2)
		ctrl.connect(p3)
	}
	t.Log("step: confirming a leader exists after all rounds")
	ctrl.findLeader()
	t.Log("ok: a leader existed in every round")
}
