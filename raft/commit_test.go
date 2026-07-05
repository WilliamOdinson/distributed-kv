package raft

// Integration tests for log replication and commitment under failures, driven
// by the Controller harness against a live networked cluster.

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

// With no failures, a sequence of commands must commit on every peer.
func TestCommit_SimpleAgreement(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)
	leader := ctrl.findLeader()
	t.Logf("ok: leader is peer %d", leader)

	for i := 0; i < 5; i++ {
		cmd := []byte("commit me " + strconv.Itoa(i+1))
		t.Logf("step: submitting command %d %q to leader %d", i+1, cmd, leader)
		sr, re := ctrl.stubs[leader].NewCommand(cmd)
		if re.Error() != "" || !sr.IsLeader {
			t.Fatalf("leader failed to accept command %d", i+1)
		}
		time.Sleep(250 * time.Millisecond)
		ct, cd := ctrl.countCommitsAtIndex(sr.Index)
		if !bytes.Equal(cd, cmd) || ct < n {
			t.Fatalf("commit %d: %d of %d peers committed", i+1, ct, n)
		}
		t.Logf("ok: command %d committed at index %d on all %d peers", i+1, sr.Index, n)
	}
}

// Agreement must survive a disconnected follower and resume fully once it
// reconnects.
func TestCommit_WithFailedFollower(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	t.Log("step: committing initial command on all peers")
	ctrl.startCommit([]byte("commit me 0"), n)

	leader := ctrl.findLeader()
	t.Logf("step: disconnecting follower %d (leader is %d)", (leader+1)%n, leader)
	ctrl.disconnect((leader + 1) % n)

	t.Logf("step: committing 4 commands on the %d-node majority", n-1)
	ctrl.startCommit([]byte("commit me 1"), n-1)
	ctrl.startCommit([]byte("commit me 2"), n-1)
	time.Sleep(electionSettleTime)
	ctrl.startCommit([]byte("commit me 3"), n-1)
	ctrl.startCommit([]byte("commit me 4"), n-1)
	t.Log("ok: majority kept committing while follower was down")

	t.Logf("step: reconnecting follower %d", (leader+1)%n)
	ctrl.connect((leader + 1) % n)
	t.Logf("step: committing on all %d peers after reconnect", n)
	ctrl.startCommit([]byte("commit me 5"), n)
	time.Sleep(electionSettleTime)
	ctrl.startCommit([]byte("commit me 6"), n)
	t.Log("ok: reconnected follower caught up and full agreement resumed")
}

// Agreement must survive repeated leader failures and recoveries.
func TestCommit_RepeatedLeaderFailures(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	t.Log("step: committing initial command on all peers")
	ctrl.startCommit([]byte("commit me 0"), n)

	leader := ctrl.findLeader()
	t.Logf("step: disconnecting original leader %d", leader)
	ctrl.disconnect(leader)
	time.Sleep(2 * electionSettleTime)
	t.Logf("step: committing on the surviving %d-node majority", n-1)
	ctrl.startCommit([]byte("commit me 1"), n-1)
	ctrl.startCommit([]byte("commit me 2"), n-1)

	leader2 := ctrl.findLeader()
	t.Logf("step: disconnecting new leader %d and reconnecting old leader %d", leader2, leader)
	ctrl.disconnect(leader2)
	ctrl.connect(leader)
	time.Sleep(2 * electionSettleTime)
	t.Logf("step: committing again on the %d-node majority", n-1)
	ctrl.startCommit([]byte("commit me 3"), n-1)
	ctrl.startCommit([]byte("commit me 4"), n-1)

	t.Logf("step: reconnecting leader %d so the whole cluster is healthy", leader2)
	ctrl.connect(leader2)
	time.Sleep(2 * electionSettleTime)
	t.Logf("step: committing on all %d peers after full recovery", n)
	ctrl.startCommit([]byte("commit me 5"), n)
	ctrl.startCommit([]byte("commit me 6"), n)
	t.Log("ok: agreement survived repeated leader failures")
}

// A minority partition must not commit; once healed, logs must become
// consistent and further commits must succeed.
func TestCommit_ConsistentRecovery(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	t.Log("step: committing initial command on all peers")
	ctrl.startCommit([]byte("commit me"), n)
	leader1 := ctrl.findLeader()

	t.Logf("step: partitioning leader %d away from a 3-peer majority", leader1)
	ctrl.disconnect((leader1 + 1) % n)
	ctrl.disconnect((leader1 + 2) % n)
	ctrl.disconnect((leader1 + 3) % n)

	t.Logf("step: submitting a command to isolated leader %d (must not commit)", leader1)
	sr1 := ctrl.issueCommand(leader1, []byte("commit me if you can"))
	if !sr1.IsActive || !sr1.IsLeader {
		t.Fatal("unexpected leadership change during minority partition")
	}
	if sr1.Index != 2 {
		t.Fatalf("got index %d, want 2", sr1.Index)
	}
	time.Sleep(2 * electionSettleTime)
	if ct, _ := ctrl.countCommitsAtIndex(sr1.Index); ct > 0 {
		t.Fatalf("%d peers committed without a majority", ct)
	}
	t.Logf("ok: index %d stayed uncommitted in the minority partition", sr1.Index)

	t.Log("step: healing the partition")
	ctrl.connect((leader1 + 1) % n)
	ctrl.connect((leader1 + 2) % n)
	ctrl.connect((leader1 + 3) % n)

	leader2 := ctrl.findLeader()
	t.Logf("step: submitting a command to leader %d after healing", leader2)
	sr2 := ctrl.issueCommand(leader2, []byte("commit me please"))
	if !sr2.IsActive || !sr2.IsLeader {
		t.Fatal("unexpected leadership change after healing")
	}
	if sr2.Index < 2 || sr2.Index > 3 {
		t.Fatalf("unexpected index %d, want 2 or 3", sr2.Index)
	}
	t.Log("step: committing a final command on all peers")
	ctrl.startCommit([]byte("commit me too"), n)
	t.Log("ok: logs reconciled and further commits succeeded")
}

// A partitioned leader that keeps receiving commands must have its uncommitted
// tail reconciled once it rejoins.
func TestCommit_PartitionAndMerge(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	leader1 := ctrl.findLeader()
	t.Logf("step: isolating leader %d and feeding it 3 uncommittable commands", leader1)
	ctrl.disconnect((leader1 + 1) % n)
	ctrl.disconnect((leader1 + 2) % n)
	ctrl.issueCommand(leader1, []byte("try to commit this 0"))
	ctrl.issueCommand(leader1, []byte("try to commit this 1"))
	ctrl.issueCommand(leader1, []byte("try to commit this 2"))

	t.Logf("step: disconnecting old leader %d and reconnecting the other two peers", leader1)
	ctrl.disconnect(leader1)
	ctrl.connect((leader1 + 1) % n)
	ctrl.connect((leader1 + 2) % n)
	time.Sleep(electionSettleTime)

	t.Logf("step: committing on the %d-node majority", n-1)
	ctrl.startCommit([]byte("try to commit this 3"), n-1)

	leader2 := ctrl.findLeader()
	t.Logf("step: disconnecting leader %d and reconnecting old leader %d", leader2, leader1)
	ctrl.disconnect(leader2)
	ctrl.connect(leader1)
	ctrl.startCommit([]byte("try to commit this 4"), n-1)

	t.Logf("step: reconnecting leader %d so the whole cluster is healthy", leader2)
	ctrl.connect(leader2)
	ctrl.startCommit([]byte("try to commit this 5"), n)
	t.Log("ok: uncommitted tail reconciled after rejoin")
}

// Logs full of uncommitted entries across shifting partitions must be purged
// and reconciled once everyone reconnects.
func TestCommit_PurgeUncommitted(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	t.Log("step: committing initial command on all peers")
	ctrl.startCommit([]byte("commit me first"), n)

	leader1 := ctrl.findLeader()
	t.Logf("step: isolating leader %d with peer %d as its only partner", leader1, (leader1+1)%n)
	ctrl.disconnect((leader1 + 2) % n)
	ctrl.disconnect((leader1 + 3) % n)
	ctrl.disconnect((leader1 + 4) % n)

	const numCmds = 25
	t.Logf("step: feeding %d uncommittable commands to isolated leader %d", numCmds, leader1)
	for i := 0; i < numCmds; i++ {
		ctrl.issueCommand(leader1, []byte("don't commit me "+strconv.Itoa(i)))
	}

	// Swap connectivity to the other partition.
	t.Logf("step: swapping connectivity to the other 3-peer partition")
	ctrl.disconnect(leader1)
	ctrl.disconnect((leader1 + 1) % n)
	ctrl.connect((leader1 + 2) % n)
	ctrl.connect((leader1 + 3) % n)
	ctrl.connect((leader1 + 4) % n)
	time.Sleep(electionSettleTime)

	t.Logf("step: committing %d commands on the new majority", numCmds)
	for i := 0; i < numCmds; i++ {
		ctrl.startCommit([]byte("do commit me "+strconv.Itoa(i)), n/2+1)
	}

	leader2 := ctrl.findLeader()
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	t.Logf("step: isolating leader %d (dropping peer %d) and feeding %d more uncommittable commands", leader2, other, numCmds)
	ctrl.disconnect(other)
	for i := 0; i < numCmds; i++ {
		ctrl.issueCommand(leader2, []byte("don't commit me "+strconv.Itoa(100+i)))
	}

	t.Logf("step: reforming the cluster from peers %d, %d and %d", leader1, (leader1+1)%n, other)
	for i := 0; i < n; i++ {
		ctrl.disconnect(i)
	}
	ctrl.connect(leader1)
	ctrl.connect((leader1 + 1) % n)
	ctrl.connect(other)
	time.Sleep(electionSettleTime)

	t.Logf("step: committing %d commands, which must purge the stale uncommitted tails", numCmds)
	for i := 0; i < numCmds; i++ {
		ctrl.startCommit([]byte("please commit me "+strconv.Itoa(i)), n/2+1)
	}

	t.Logf("step: reconnecting all %d peers and committing a final command", n)
	for i := 0; i < n; i++ {
		ctrl.connect(i)
	}
	ctrl.startCommit([]byte("commit me last"), n)
	t.Log("ok: all uncommitted entries purged and logs reconciled")
}

// The number of RPCs used for elections, commitment, and idle must stay within
// reasonable bounds (a proxy for "no busy-looping or RPC storms").
func TestCommit_CallCountReasonable(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewController(t, n)

	t.Log("step: measuring RPC calls used to elect a leader")
	ctrl.findLeader()
	electionCalls := 0
	for i := 0; i < n; i++ {
		electionCalls += ctrl.getCallCount(i)
	}
	if electionCalls < 7 || electionCalls > 75 {
		t.Fatalf("election used %d remote calls, want [7, 75]", electionCalls)
	}
	t.Logf("ok: election used %d remote calls (want [7, 75])", electionCalls)

	t.Log("step: measuring RPC calls used to commit a burst of commands")
	commitOK := false
	var afterCommit int
	for try := 0; try < 5 && !commitOK; try++ {
		if try > 0 {
			time.Sleep(electionSettleTime)
		}
		t.Logf("round %d: finding a stable leader for the commit measurement", try)
		leader := ctrl.findLeader()
		beforeCommit := 0
		for i := 0; i < n; i++ {
			beforeCommit += ctrl.getCallCount(i)
		}

		const iters = 10
		t.Logf("round %d: submitting %d commands to leader %d", try, iters, leader)
		sr1 := ctrl.issueCommand(leader, []byte("initial command"))
		if !sr1.IsLeader {
			continue
		}
		cmds := make([][]byte, 0, iters+1)
		termChanged := false
		for i := 0; i <= iters; i++ {
			x := []byte("random command " + strconv.Itoa(ctrl.randIntn(65536)))
			cmds = append(cmds, x)
			sr2 := ctrl.issueCommand(leader, x)
			if sr2.Term != sr1.Term || !sr2.IsLeader {
				termChanged = true
				break
			}
			if sr1.Index+i+1 != sr2.Index {
				t.Fatalf("issueCommand produced non-sequential index: %d then %d", sr1.Index, sr2.Index)
			}
		}
		if termChanged {
			continue
		}

		t.Logf("round %d: waiting for all %d peers to commit indices %d..%d", try, n, sr1.Index+1, sr1.Index+iters)
		retry := false
		for i := 0; i < iters; i++ {
			cmd := ctrl.waitForCommits(sr1.Index+i+1, n, sr1.Term)
			if !bytes.Equal(cmd, cmds[i]) {
				if bytes.Equal(cmd, nil) {
					retry = true
					break
				}
				t.Fatalf("index %d committed %q, expected %q", sr1.Index+i+1, cmd, cmds[i])
			}
		}
		if retry {
			continue
		}

		afterCommit = 0
		failed := false
		for i := 0; i < n; i++ {
			sr, re := ctrl.stubs[i].GetStatus()
			if re.Error() != "" {
				continue
			}
			if sr.Term != sr1.Term {
				failed = true
			}
			afterCommit += sr.CallCount
		}
		if failed {
			continue
		}
		if afterCommit-beforeCommit > 200 {
			t.Fatalf("commitment used %d remote calls, want <= 200", afterCommit-beforeCommit)
		}
		t.Logf("ok: committing %d commands used %d remote calls (want <= 200)", iters, afterCommit-beforeCommit)
		commitOK = true
	}
	if !commitOK {
		t.Fatal("commitment measurement failed (term changed too often)")
	}

	t.Log("step: measuring RPC calls used during 1s of idle")
	time.Sleep(electionSettleTime)
	idle := 0
	for i := 0; i < n; i++ {
		idle += ctrl.getCallCount(i)
	}
	if idle-afterCommit > 20 {
		t.Fatalf("1s idle used %d remote calls, want <= 20", idle-afterCommit)
	}
	t.Logf("ok: 1s idle used %d remote calls (want <= 20)", idle-afterCommit)
}
