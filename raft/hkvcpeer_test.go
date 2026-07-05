package raft

// Integration tests for the HKVC-embedded entry points (NewHKVCRaftPeer,
// TerminateHKVC, SubmitCommand, WaitForCommit, GetLogEntry). Unlike the
// Controller-driven tests, these use raft peers directly in-process, the way an
// HKVC participant does, with no ControlInterface callee.

import (
	"strconv"
	"testing"
	"time"
)

// newHKVCCluster builds n HKVC-style raft peers wired to each other, activates
// them, and registers cleanup. It returns the peers indexed 0..n-1.
func newHKVCCluster(t *testing.T, n int) []*RaftPeer {
	t.Helper()
	t.Logf("step: building a %d-node HKVC cluster", n)
	ports := freePorts(t, n)
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = "localhost:" + strconv.Itoa(ports[i])
	}
	t.Logf("ok: reserved %d ports %v", n, ports)

	peers := make([]*RaftPeer, n)
	for i := 0; i < n; i++ {
		var others []string
		for j := 0; j < n; j++ {
			if j != i {
				others = append(others, addrs[j])
			}
		}
		peers[i] = NewHKVCRaftPeer(i, addrs[i], others)
	}

	t.Cleanup(func() {
		for _, rp := range peers {
			rp.TerminateHKVC()
		}
	})

	// Activate the raft interface on each peer so they can talk to each other.
	t.Logf("step: activating %d peers", n)
	time.Sleep(500 * time.Millisecond)
	for _, rp := range peers {
		rp.Activate()
	}
	t.Log("ok: cluster activated")
	return peers
}

// waitForHKVCLeader returns the index of a leader among the peers, or fails.
func waitForHKVCLeader(t *testing.T, peers []*RaftPeer) int {
	t.Helper()
	t.Log("step: waiting for a leader to be elected (up to 10s)")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, rp := range peers {
			sr, _ := rp.GetStatus()
			if sr.IsLeader && sr.IsActive {
				t.Logf("ok: peer %d is leader (term %d)", i, sr.Term)
				return i
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no HKVC raft leader elected")
	return -1
}

func TestHKVCPeer_ElectsAndCommits(t *testing.T) {
	const n = 3
	peers := newHKVCCluster(t, n)

	leader := waitForHKVCLeader(t, peers)

	// Submit a command to the leader and wait for it to commit.
	t.Logf("step: submitting %q to leader %d and waiting for commit", "hello hkvc", leader)
	idx, isLeader := peers[leader].SubmitCommand([]byte("hello hkvc"))
	if !isLeader {
		t.Fatal("SubmitCommand to the leader reported non-leader")
	}
	if _, ok := peers[leader].WaitForCommit(idx, 5*time.Second); !ok {
		t.Fatalf("command at index %d did not commit within timeout", idx)
	}
	t.Logf("ok: command committed at index %d", idx)

	// The committed command must be readable from the leader's log.
	t.Logf("step: reading back log entry at index %d", idx)
	if got := peers[leader].GetLogEntry(idx); string(got) != "hello hkvc" {
		t.Fatalf("GetLogEntry(%d) = %q, want %q", idx, got, "hello hkvc")
	}
	t.Logf("ok: GetLogEntry(%d) == %q", idx, "hello hkvc")
}

func TestHKVCPeer_NonLeaderRejectsSubmit(t *testing.T) {
	const n = 3
	peers := newHKVCCluster(t, n)
	leader := waitForHKVCLeader(t, peers)

	// Find a follower and confirm it refuses to accept a command.
	follower := (leader + 1) % n
	t.Logf("step: submitting %q to follower %d, expecting rejection", "nope", follower)
	if _, isLeader := peers[follower].SubmitCommand([]byte("nope")); isLeader {
		// It is possible (though unlikely) that leadership moved; only fail if
		// this peer actually still claims leadership.
		sr, _ := peers[follower].GetStatus()
		if sr.IsLeader {
			t.Skip("leadership moved to the sampled follower; skipping")
		}
		t.Fatal("a follower accepted SubmitCommand as if it were leader")
	}
}

func TestHKVCPeer_TerminateIsIdempotent(t *testing.T) {
	peers := newHKVCCluster(t, 1)
	waitForHKVCLeader(t, peers)

	// Terminate twice; the second call must be a harmless no-op (the test
	// cleanup will also call it a third time).
	t.Log("step: calling TerminateHKVC twice, expecting the second to be a no-op")
	peers[0].TerminateHKVC()
	peers[0].TerminateHKVC()

	// Termination must actually take effect, not merely avoid panicking: the
	// peer must report inactive and must refuse to wait for new commits.
	t.Log("step: verifying the terminated peer is inactive and refuses WaitForCommit")
	sr, _ := peers[0].GetStatus()
	if sr.IsActive {
		t.Fatal("peer still reports active after TerminateHKVC")
	}
	if idx, ok := peers[0].WaitForCommit(1, 200*time.Millisecond); ok || idx != -1 {
		t.Fatalf("WaitForCommit on a terminated peer = (%d, %v), want (-1, false)", idx, ok)
	}
	t.Log("ok: terminated peer is inactive and WaitForCommit returned (-1, false)")
}
