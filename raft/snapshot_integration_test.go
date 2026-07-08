package raft

// Networked integration test for log compaction: a follower that falls behind
// the leader's snapshot boundary must catch up via the InstallSnapshot RPC
// rather than AppendEntries. This exercises the real RPC path end to end.

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// commitOnLeader submits cmd to the current leader and waits for it to commit,
// returning the absolute index. It re-finds the leader if needed.
func commitOnLeader(t *testing.T, peers []*RaftPeer, cmd []byte) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, rp := range peers {
			sr, _ := rp.GetStatus()
			if !sr.IsLeader || !sr.IsActive {
				continue
			}
			idx, isLeader := rp.SubmitCommand(cmd)
			if !isLeader {
				continue
			}
			if _, ok := rp.WaitForCommit(idx, 5*time.Second); ok {
				return idx
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("could not commit %q within timeout", cmd)
	return -1
}

// leaderIndex returns the index of the current active leader, or -1.
func leaderIndex(peers []*RaftPeer) int {
	for i, rp := range peers {
		if sr, _ := rp.GetStatus(); sr.IsLeader && sr.IsActive {
			return i
		}
	}
	return -1
}

// TestHKVCPeer_FollowerCatchesUpViaSnapshot verifies that a follower which was
// disconnected while the leader compacted its log is brought back into sync by
// an InstallSnapshot RPC, not by replaying individual entries it no longer can
// receive.
func TestHKVCPeer_FollowerCatchesUpViaSnapshot(t *testing.T) {
	const n = 3
	peers := newHKVCCluster(t, n)
	leader := waitForHKVCLeader(t, peers)

	// Register a restore handler on every peer so we can observe which follower
	// gets its state reinstalled from a snapshot.
	restored := make([]int, n) // per-peer: lastIncludedIndex last restored
	var restoreMu sync.Mutex
	for i := range peers {
		i := i
		peers[i].SetSnapshotHandlers(&SnapshotHandlers{
			GroupID: i,
			OnInstallSnapshot: func(gid, idx int, data []byte) {
				restoreMu.Lock()
				restored[i] = idx
				restoreMu.Unlock()
			},
		})
	}

	// Commit one entry with everyone present so all peers share a baseline.
	commitOnLeader(t, peers, []byte("baseline"))

	// Disconnect a follower so it stops receiving replication.
	follower := (leader + 1) % n
	if follower == leaderIndex(peers) {
		follower = (leader + 2) % n
	}
	t.Logf("step: deactivating follower %d so it falls behind", follower)
	peers[follower].Deactivate()

	// Commit a batch of entries while the follower is away, then snapshot the
	// leader's log well past where the follower stopped. Because commands only
	// commit with a majority (2 of 3), the two remaining peers keep progressing.
	t.Log("step: committing 20 entries and snapshotting on the majority")
	var lastIdx int
	for i := 0; i < 20; i++ {
		lastIdx = commitOnLeader(t, peers, []byte("cmd "+strconv.Itoa(i)))
	}
	// Snapshot on every currently-active peer up to a point beyond the
	// follower's last-known index, forcing catch-up by snapshot on reconnect.
	snapAt := lastIdx - 2
	for i, rp := range peers {
		if i == follower {
			continue
		}
		rp.Snapshot(snapAt, []byte("state@"+strconv.Itoa(snapAt)))
	}
	if li := leaderPeer(peers).LastIncludedIndex(); li < snapAt {
		// leadership may have moved; snapshot the new leader too
		leaderPeer(peers).Snapshot(snapAt, []byte("state@"+strconv.Itoa(snapAt)))
	}
	t.Logf("ok: compacted the log up to index %d (follower was far behind)", snapAt)

	// Reconnect the follower. It should be caught up via InstallSnapshot.
	t.Logf("step: reactivating follower %d; it must catch up via InstallSnapshot", follower)
	peers[follower].Activate()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		restoreMu.Lock()
		got := restored[follower]
		restoreMu.Unlock()
		if got >= snapAt {
			t.Logf("ok: follower %d restored from snapshot at index %d", follower, got)
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	restoreMu.Lock()
	got := restored[follower]
	restoreMu.Unlock()
	t.Fatalf("follower %d never caught up via snapshot (restored index %d, wanted >= %d)", follower, got, snapAt)
}

// leaderPeer returns the current leader peer or the first peer as a fallback.
func leaderPeer(peers []*RaftPeer) *RaftPeer {
	if i := leaderIndex(peers); i >= 0 {
		return peers[i]
	}
	return peers[0]
}
