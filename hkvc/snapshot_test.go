package hkvc

// Unit tests for HKVC's directory-tree snapshot serialization, plus an
// integration test that a follower which fell behind the leader's compaction
// boundary rebuilds correct state from a shipped snapshot.

import (
	"net/http"
	"testing"
	"time"
)

// TestTreeSnapshot_RoundTrip checks that serializing and deserializing the
// directory tree preserves keys, values, versions, nesting, and group IDs.
func TestTreeSnapshot_RoundTrip(t *testing.T) {
	p := newBareParticipant()
	// Build /a (group 3) with key k=v (version 2), and /a/b (group 3) with j=w.
	a := &directory{name: "a", groupID: 3, subDirs: map[string]*directory{}, kvPairs: map[string]*kvPair{}}
	a.kvPairs["k"] = &kvPair{key: "k", value: "v", version: 2}
	b := &directory{name: "b", groupID: 3, subDirs: map[string]*directory{}, kvPairs: map[string]*kvPair{}}
	b.kvPairs["j"] = &kvPair{key: "j", value: "w", version: 1}
	a.subDirs["b"] = b
	p.root.subDirs["a"] = a
	p.createCounter = 5

	t.Log("step: serialize the tree, then restore it into a fresh participant")
	data := p.serializeState()

	p2 := newBareParticipant()
	p2.restoreState(data)

	if p2.createCounter != 5 {
		t.Fatalf("createCounter = %d, want 5", p2.createCounter)
	}
	ra := p2.resolveDir("/a")
	if ra == nil || ra.groupID != 3 {
		t.Fatalf("/a not restored correctly: %+v", ra)
	}
	if kv := ra.kvPairs["k"]; kv == nil || kv.value != "v" || kv.version != 2 {
		t.Fatalf("/a/k not restored: %+v", kv)
	}
	rb := p2.resolveDir("/a/b")
	if rb == nil {
		t.Fatal("/a/b not restored")
	}
	if kv := rb.kvPairs["j"]; kv == nil || kv.value != "w" {
		t.Fatalf("/a/b/j not restored: %+v", kv)
	}
	t.Log("ok: tree round-tripped through gob with all fields intact")
}

// TestTreeSnapshot_RestoreReplacesTree confirms restoreState wholesale replaces
// prior state (a follower installing a snapshot must not merge with stale data).
func TestTreeSnapshot_RestoreReplacesTree(t *testing.T) {
	src := newBareParticipant()
	src.root.kvPairs["only"] = &kvPair{key: "only", value: "1", version: 1}
	data := src.serializeState()

	dst := newBareParticipant()
	dst.root.kvPairs["stale"] = &kvPair{key: "stale", value: "x", version: 9}
	dst.restoreState(data)

	if _, ok := dst.root.kvPairs["stale"]; ok {
		t.Fatal("restoreState left stale keys behind; it must replace the tree")
	}
	if kv := dst.root.kvPairs["only"]; kv == nil || kv.value != "1" {
		t.Fatalf("restored key missing: %+v", kv)
	}
}

// TestCluster_SnapshotCatchUp is an integration test: a single-group cluster
// writes enough keys to trigger log compaction while one follower is
// disconnected, then the follower reconnects and must serve consistent data,
// which it can only do by catching up through an InstallSnapshot.
func TestCluster_SnapshotCatchUp(t *testing.T) {
	const n = 3
	ctrl := NewHKVCController(t, n, 0, 0) // 0 additional groups => single-group participants
	leader, _ := ctrl.getGroupLeaderCommit(0)
	if leader < 0 {
		t.Fatal("no group-0 leader")
	}
	client := randSeq(12)

	// Disconnect a follower so it misses the coming writes and their compaction.
	follower := (leader + 1) % n
	t.Logf("step: disconnecting follower %d", follower)
	ctrl.disconnect(follower)

	// Write well past the snapshot threshold so the majority compacts its log.
	const writes = snapshotThreshold + 20
	t.Logf("step: writing %d keys on the majority to trigger log compaction", writes)
	for i := 0; i < writes; i++ {
		port := ctrl.clientPort(leader)
		kvm := KeyValueMessage{Directory: "/", Key: "k" + itoa(i), Value: "v" + itoa(i), SeqNumber: i, ClientID: client}
		rc, er := validateResponseToClient(t, kvm, port, "/set", &KeySuccessResponse{}, "", 0)
		if er != nil && er.ErrorType == NonLeaderError {
			// leadership may have moved; re-find and retry this index
			leader, _ = ctrl.getGroupLeaderCommit(0)
			i--
			continue
		}
		if rc != http.StatusCreated && rc != http.StatusOK {
			t.Fatalf("set k%d failed: code %d err %+v", i, rc, er)
		}
	}

	// Reconnect the follower and give it time to catch up (via snapshot).
	t.Logf("step: reconnecting follower %d; it must catch up via InstallSnapshot", follower)
	ctrl.connect(follower)
	time.Sleep(WaitForRaft)

	// The follower should now be able to become leader and serve a key written
	// before it reconnected. To check its state directly, read from whichever
	// participant is currently leader and confirm a late key resolves.
	deadline := time.Now().Add(10 * time.Second)
	lastKey := "k" + itoa(writes-1)
	for time.Now().Before(deadline) {
		curLeader, _ := ctrl.getGroupLeaderCommit(0)
		if curLeader < 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		kvm := KeyValueMessage{}
		rc, er := validateResponseToClient(t, KeyRequest{Directory: "/", Key: lastKey, SeqNumber: writes + 100, ClientID: client}, ctrl.clientPort(curLeader), "/get", &kvm, "", 0)
		if er == nil && rc == http.StatusOK && kvm.Value == "v"+itoa(writes-1) {
			t.Logf("ok: cluster serves %s=%s after follower snapshot catch-up", lastKey, kvm.Value)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("cluster did not serve %s consistently after follower reconnected", lastKey)
}
