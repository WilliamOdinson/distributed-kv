package hkvc

// Advanced integration tests exercising the full distributed behavior: client
// request sequencing/deduplication, hierarchical namespaces, linearizable
// commit ordering under concurrency, leader failover discovery, delete/rebuild
// freshness, and multi-group sharding.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// findDirLeaderViaList probes each port with a /list request and returns the
// index of the single one that answers as leader (or -1). It fails the test if
// more than one claims leadership.
func findDirLeaderViaList(t *testing.T, msg any, ports []int) int {
	t.Helper()
	t.Logf("step: probing %d ports with /list to find the directory leader", len(ports))
	leader := -1
	for i := range ports {
		jsonmsg, err := json.Marshal(msg)
		if err != nil {
			t.Fatalf("encoding list request failed: %v", err)
		}
		lr := ListResponse{}
		rc, er, callErr := getResponse(ports[i], "/list", jsonmsg, &lr)
		if callErr != nil || (er != nil && er.ErrorType == NonLeaderError) {
			continue
		}
		if er == nil && rc == http.StatusOK {
			if leader != -1 {
				t.Fatal("multiple participants claim to be the leader")
			}
			leader = i
		}
	}
	t.Logf("ok: directory leader is port index %d", leader)
	return leader
}

// Duplicate requests must replay the cached response; out-of-order (lower
// sequence) requests must be rejected with HKVCMsgOutOfSequenceError.
func TestCluster_ClientSequencing(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d on client port %d, client %q", leader, port, client)

	ksr := KeySuccessResponse{}
	keys := make([]string, 3)
	t.Log("step: setting 3 keys with increasing sequence numbers")
	for k := 0; k < 3; k++ {
		kvm := KeyValueMessage{Directory: "/", Key: "key" + itoa(k), Value: "value #" + itoa(k), SeqNumber: k, ClientID: client}
		keys[k] = kvm.Key
		validateResponseToClient(t, kvm, port, "/set", &ksr, "", http.StatusCreated)
	}

	t.Log("step: duplicate set (seq 2) must replay the cached 201; outdated set (seq 0) must be rejected")
	// Duplicate of the last set replays the same 201.
	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: keys[2], Value: "value #2", SeqNumber: 2, ClientID: client}, port, "/set", &ksr, "", http.StatusCreated)
	// Outdated set is rejected.
	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: keys[0], Value: "new value #0", SeqNumber: 0, ClientID: client}, port, "/set", &ksr, OutOfSequenceError, http.StatusNotAcceptable)

	kvm := KeyValueMessage{}
	t.Log("step: getting the 3 keys back with fresh sequence numbers")
	for k := 0; k < 3; k++ {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[k], SeqNumber: k + 3, ClientID: client}, port, "/get", &kvm, "", http.StatusOK)
	}
	t.Log("step: duplicate get (seq 5) must replay; outdated get (seq 4) must be rejected")
	// Duplicate get replays; outdated get is rejected.
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[2], SeqNumber: 5, ClientID: client}, port, "/get", &kvm, "", http.StatusOK)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[1], SeqNumber: 4, ClientID: client}, port, "/get", &kvm, OutOfSequenceError, http.StatusNotAcceptable)

	t.Log("step: exercising sequencing on /list, /get_metadata, /create, /delete")
	lr := ListResponse{}
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 7, ClientID: client}, port, "/list", &lr, "", http.StatusOK)
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 7, ClientID: client}, port, "/list", &lr, "", http.StatusOK)
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 3, ClientID: client}, port, "/list", &lr, OutOfSequenceError, http.StatusNotAcceptable)

	mr := MetadataResponse{}
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[1], SeqNumber: 11, ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[1], SeqNumber: 11, ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[1], SeqNumber: 8, ClientID: client}, port, "/get_metadata", &mr, OutOfSequenceError, http.StatusNotAcceptable)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 13, ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 13, ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 7, ClientID: client}, port, "/create", &ksr, OutOfSequenceError, http.StatusNotAcceptable)

	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 15, ClientID: client}, port, "/delete", &ksr, "", http.StatusOK)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 15, ClientID: client}, port, "/delete", &ksr, "", http.StatusOK)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: 7, ClientID: client}, port, "/delete", &ksr, OutOfSequenceError, http.StatusNotAcceptable)
}

// Build a directory hierarchy with /create and /set, verify same-named keys in
// different directories are independent, and confirm conflict handling.
func TestCluster_CreateHierarchy(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d on client port %d", leader, port)

	ksr := KeySuccessResponse{}
	seq := 0
	next := func() int { seq++; return seq - 1 }

	const numDirs, numKeys = 5, 4
	t.Logf("step: creating %d directories under /", numDirs)
	for d := 0; d < numDirs; d++ {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d" + itoa(d), SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
		if ksr.Directory != "/" || ksr.Key != "d"+itoa(d) || !ksr.Success {
			t.Fatal("create response contents unexpected")
		}
	}
	t.Log("step: repeating the creates; duplicates must return success=false at 200")
	// Duplicate creates now return success=false at 200.
	for d := 0; d < numDirs; d++ {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d" + itoa(d), SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusOK)
		if ksr.Success {
			t.Fatal("duplicate create reported success=true")
		}
	}

	t.Logf("step: setting %d keys in each of %d directories to check per-directory independence", numKeys, numDirs)
	// Same key name in different directories are distinct entries.
	for k := 0; k < numKeys; k++ {
		for d := 0; d < numDirs; d++ {
			kvm := KeyValueMessage{Directory: "/d" + itoa(d), Key: "key" + itoa(k), Value: fmt.Sprintf("value #%d in d%d", k, d), SeqNumber: next(), ClientID: client}
			validateResponseToClient(t, kvm, port, "/set", &ksr, "", http.StatusCreated)
			if ksr.Directory != "/d"+itoa(d) || ksr.Key != "key"+itoa(k) || !ksr.Success {
				t.Fatal("set response contents unexpected")
			}
		}
	}

	t.Log("step: overwriting key0 in each directory and confirming the new value is read back")
	// Overwriting an existing key returns 200 and reflects the new value.
	for d := 0; d < numDirs; d++ {
		kvm := KeyValueMessage{Directory: "/d" + itoa(d), Key: "key0", Value: "new value #0 in d" + itoa(d), SeqNumber: next(), ClientID: client}
		validateResponseToClient(t, kvm, port, "/set", &ksr, "", http.StatusOK)
		got := KeyValueMessage{}
		validateResponseToClient(t, KeyRequest{Directory: "/d" + itoa(d), Key: "key0", SeqNumber: next(), ClientID: client}, port, "/get", &got, "", http.StatusOK)
		if got.Value != "new value #0 in d"+itoa(d) {
			t.Fatalf("overwrite not reflected: got %q", got.Value)
		}
	}

	t.Log("step: creating 4 levels of nested subdirectories under /d0")
	// Nested directories.
	dir := "/d0"
	for l := 0; l < 4; l++ {
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir" + itoa(l), SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
		if ksr.Directory != dir || ksr.Key != "subdir"+itoa(l) || !ksr.Success {
			t.Fatal("nested create response contents unexpected")
		}
		dir += "/subdir" + itoa(l)
	}

	t.Log("step: exercising conflict handling (set/create over a key, dir over a subdir)")
	// Conflict handling.
	validateResponseToClient(t, KeyValueMessage{Directory: "/d0/key0", Key: "x", Value: "v", SeqNumber: next(), ClientID: client}, port, "/set", &ksr, ConflictKeyError, http.StatusConflict)
	validateResponseToClient(t, KeyRequest{Directory: "/d0/key0", Key: "x", SeqNumber: next(), ClientID: client}, port, "/create", &ksr, ConflictKeyError, http.StatusConflict)
	validateResponseToClient(t, KeyValueMessage{Directory: "/d0/subdir0", Key: "subdir1", Value: "v", SeqNumber: next(), ClientID: client}, port, "/set", &ksr, ConflictDirError, http.StatusConflict)
	validateResponseToClient(t, KeyRequest{Directory: "/d0", Key: "key0", SeqNumber: next(), ClientID: client}, port, "/create", &ksr, ConflictKeyError, http.StatusConflict)
}

// Under many concurrent set requests sharing a sequence counter, every accepted
// request must be committed before its response arrives (linearizable commit
// ordering). Goroutines report failures over a channel so all assertions run on
// the test goroutine (calling t.Fatal from a child goroutine is unreliable and
// is flagged by go vet).
func TestCluster_RespondAfterCommit(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d on client port %d", leader, port)

	const numKeys = 20
	var seqMu, successMu sync.Mutex
	var wg sync.WaitGroup
	seq := 0
	successes := 0
	failCh := make(chan string, numKeys)

	t.Logf("step: launching %d concurrent setters sharing a sequence counter; each verifies commit>=successes", numKeys)
	wg.Add(numKeys)
	for range numKeys {
		go func() {
			defer wg.Done()
			for {
				seqMu.Lock()
				k := seq
				seq++
				seqMu.Unlock()
				key := "key" + itoa(k)
				value := "value #" + itoa(k)

				jsonmsg, _ := json.Marshal(KeyValueMessage{Directory: "/", Key: key, Value: value, SeqNumber: k, ClientID: client})
				ksr := KeySuccessResponse{}
				rc, er, callErr := getResponse(port, "/set", jsonmsg, &ksr)
				if callErr != nil {
					failCh <- "set transport error: " + callErr.Error()
					return
				}
				if (er != nil && er.ErrorType == OutOfSequenceError) || rc == http.StatusNotAcceptable {
					continue // lost the seq race; retry with a fresh k
				}

				successMu.Lock()
				successes++
				s := successes
				successMu.Unlock()

				if er != nil || rc != http.StatusCreated {
					failCh <- fmt.Sprintf("set returned code %d err %+v", rc, er)
					return
				}
				if ksr.Directory != "/" || ksr.Key != key || !ksr.Success {
					failCh <- "set response contents unexpected"
					return
				}

				curLead, comIdx := ctrl.getGroupLeaderCommit(0)
				if curLead != leader {
					failCh <- "leader changed unexpectedly"
					return
				}
				if comIdx < s {
					failCh <- fmt.Sprintf("response before commit: commit=%d < successes=%d", comIdx, s)
					return
				}
				return
			}
		}()
	}
	t.Log("step: waiting for all setters to finish and draining the failure channel")
	wg.Wait()
	close(failCh)
	if msg, ok := <-failCh; ok {
		t.Fatal(msg)
	}
	t.Log("ok: every accepted set was committed before its response arrived")
}

// After each induced leader failure, a client must be able to rediscover the
// new leader (via metadata-provided addresses) with fully consistent state.
func TestCluster_FaultyLeaders(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: initial leader is participant %d on client port %d", leader, port)

	ksr := KeySuccessResponse{}
	seq := 0
	next := func() int { seq++; return seq - 1 }

	const numModifies = 8
	t.Logf("step: creating /dir and modifying /dir/modkey %d times", numModifies)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)

	validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "modkey", Value: "0", SeqNumber: next(), ClientID: client}, port, "/set", &ksr, "", http.StatusCreated)
	for m := 0; m < numModifies; m++ {
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "modkey", Value: itoa(m), SeqNumber: next(), ClientID: client}, port, "/set", &ksr, "", http.StatusOK)
	}

	addrList := make([]string, n)
	for i := 0; i < n; i++ {
		addrList[i] = ctrl.setupInfo[i].ClientAddr
	}

	t.Log("step: reading /dir/modkey metadata and checking version, member list, and leader index")
	mr := MetadataResponse{}
	validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	if mr.Directory != "/dir" || mr.Key != "modkey" || mr.IsDirectory {
		t.Fatal("metadata dir/key/is_directory unexpected")
	}
	if mr.Version < uint64(numModifies) {
		t.Fatalf("metadata version %d < %d", mr.Version, numModifies)
	}
	if !sameStringSlices(mr.PAddrList, addrList) {
		t.Fatal("metadata participant address list mismatch")
	}
	if mr.LeaderIdx != leader {
		t.Fatalf("metadata leader index %d, want %d", mr.LeaderIdx, leader)
	}

	const failureRounds = 4
	t.Logf("step: inducing %d leader failures and rediscovering the leader after each", failureRounds)
	for f := 0; f < failureRounds; f++ {
		leader, _ = ctrl.getGroupLeaderCommit(0)
		port = ctrl.clientPort(leader)
		t.Logf("round %d: disconnecting current leader participant %d (port %d)", f, leader, port)
		ctrl.disconnect(leader)
		time.Sleep(WaitForRaft)

		kvm := KeyValueMessage{}
		for range numModifies {
			jsonmsg, _ := json.Marshal(KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: next(), ClientID: client})
			if _, _, err := getResponse(port, "/get", jsonmsg, &kvm); err == nil {
				t.Fatal("failed leader accepted a get request")
			}
		}

		clientLeader := -1
		for i := 0; i < n; i++ {
			p := portOf(t, mr.PAddrList[i])
			jsonmsg, _ := json.Marshal(KeyRequest{Directory: "/dir", Key: "modkey", SeqNumber: next(), ClientID: client})
			rc, er, err := getResponse(p, "/get", jsonmsg, &kvm)
			if i == leader && err == nil {
				t.Fatal("failed leader accepted a get request")
			}
			if i == leader || (er != nil && er.ErrorType == NonLeaderError) {
				continue
			}
			if er == nil && rc == http.StatusOK {
				if clientLeader != -1 {
					t.Fatal("multiple participants claim leadership")
				}
				clientLeader = i
			}
		}
		if clientLeader == -1 {
			t.Fatal("no new leader found after failure")
		}

		newLeader, comIdx := ctrl.getGroupLeaderCommit(0)
		if newLeader == leader || newLeader == -1 || clientLeader != newLeader {
			t.Fatal("client-discovered leader inconsistent with controller state")
		}
		if comIdx < 4+f+numModifies {
			t.Fatalf("new leader commit index %d too low", comIdx)
		}
		t.Logf("round %d: client rediscovered new leader participant %d at commit %d", f, newLeader, comIdx)

		ctrl.connect(leader)
		time.Sleep(WaitForRaft)
	}
}

// Deleting keys and directories must reset their metadata; re-creating a
// deleted key must start fresh (version not carried across deletion).
func TestCluster_DeleteAndRebuild(t *testing.T) {
	const n = 5
	t.Logf("step: creating a %d-node cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d on client port %d", leader, port)

	ksr := KeySuccessResponse{}
	mr := MetadataResponse{}
	kvm := KeyValueMessage{}
	seq := 0
	next := func() int { seq++; return seq - 1 }
	const numModifies = 8

	t.Log("step: creating /d and confirming its directory metadata")
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	if mr.Directory != "/" || mr.Key != "d" || !mr.IsDirectory {
		t.Fatal("directory metadata unexpected")
	}

	t.Logf("step: creating /d/k and modifying it %d times to bump its version", numModifies)
	validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v", SeqNumber: next(), ClientID: client}, port, "/set", &ksr, "", http.StatusCreated)
	for m := 0; m < numModifies; m++ {
		validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v" + itoa(m), SeqNumber: next(), ClientID: client}, port, "/set", &ksr, "", http.StatusOK)
	}

	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	if mr.Version < uint64(numModifies) {
		t.Fatalf("version %d < %d before deletion", mr.Version, numModifies)
	}
	kVersion := mr.Version

	t.Logf("step: deleting /d/k (version %d) and confirming get/get_metadata reject it", kVersion)
	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/delete", &ksr, "", http.StatusOK)

	if rc, er := validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/get", &kvm, "", 0); er == nil || rc == http.StatusOK {
		t.Fatal("get accepted a deleted key")
	}
	if rc, er := validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", 0); er == nil || rc == http.StatusOK {
		t.Fatal("get_metadata accepted a deleted key")
	}

	t.Log("step: re-creating /d/k and confirming its version did not carry over from before deletion")
	// Re-create the deleted key: version must not carry over.
	validateResponseToClient(t, KeyValueMessage{Directory: "/d", Key: "k", Value: "v", SeqNumber: next(), ClientID: client}, port, "/set", &ksr, "", http.StatusCreated)
	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", http.StatusOK)
	if mr.Version > kVersion {
		t.Fatal("re-created key kept its pre-deletion version")
	}

	t.Log("step: building nested subdirs, deleting /d/subdir0, and confirming it leaves the listing")
	// Delete a directory and confirm it disappears from listings.
	dir := "/d"
	for l := 0; l < 3; l++ {
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: "subdir" + itoa(l), SeqNumber: next(), ClientID: client}, port, "/create", &ksr, "", http.StatusCreated)
		dir += "/subdir" + itoa(l)
	}
	validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "subdir0", SeqNumber: next(), ClientID: client}, port, "/delete", &ksr, "", http.StatusOK)
	lr := ListResponse{}
	validateResponseToClient(t, DirectoryRequest{Directory: "/d", SeqNumber: next(), ClientID: client}, port, "/list", &lr, "", http.StatusOK)
	if slices.Contains(lr.List, "subdir0") {
		t.Fatal("list still contains a deleted directory")
	}

	t.Log("step: deleting /d and confirming keys and metadata under it are gone")
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: next(), ClientID: client}, port, "/delete", &ksr, "", http.StatusOK)
	if rc, er := validateResponseToClient(t, KeyRequest{Directory: "/d", Key: "k", SeqNumber: next(), ClientID: client}, port, "/get", &kvm, "", 0); er == nil || rc == http.StatusOK {
		t.Fatal("get accepted a key under a deleted directory")
	}
	if rc, er := validateResponseToClient(t, KeyRequest{Directory: "/", Key: "d", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &mr, "", 0); er == nil || rc == http.StatusOK {
		t.Fatal("get_metadata accepted a deleted directory")
	}
}

// A cluster with additional raft groups must (a) have everyone agree that group
// 0 manages the root, and (b) actually distribute created directories across
// all groups so every group ends up managing content.
func TestCluster_MultipleGroups(t *testing.T) {
	const clusterSize, addlGroups, addlGroupSize = 5, 2, 3
	t.Logf("step: creating a %d-node cluster with %d additional raft groups of size %d", clusterSize, addlGroups, addlGroupSize)
	ctrl := NewHKVCController(t, clusterSize, addlGroups, addlGroupSize)

	t.Log("step: locating the leader of every raft group")
	leaders := make(map[int]int)
	for g := range ctrl.raftGroups {
		leaders[g], _ = ctrl.getGroupLeaderCommit(g)
		if leaders[g] < 0 {
			t.Fatalf("group %d has no leader", g)
		}
	}

	groupClientAddrs := make(map[int][]string)
	groupClientPorts := make(map[int][]int)
	for g, ids := range ctrl.raftGroups {
		for _, pidx := range ids {
			groupClientAddrs[g] = append(groupClientAddrs[g], ctrl.setupInfo[pidx].ClientAddr)
			groupClientPorts[g] = append(groupClientPorts[g], portOf(t, ctrl.setupInfo[pidx].ClientAddr))
		}
	}

	client := randSeq(12)
	mr := MetadataResponse{}
	ksr := KeySuccessResponse{}
	seq := 0
	next := func() int { seq++; return seq - 1 }

	t.Log("step: confirming all participants agree group 0 manages the root")
	// Everyone agrees group 0 manages the root.
	for i := 0; i < clusterSize; i++ {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: ".", SeqNumber: next(), ClientID: client}, groupClientPorts[0][i], "/get_metadata", &mr, "", http.StatusOK)
		if mr.Directory != "/" || mr.Key != "." || !mr.IsDirectory {
			t.Fatal("root metadata unexpected")
		}
		if !sameStringSlices(mr.PAddrList, groupClientAddrs[0]) {
			t.Fatalf("participant %d reports wrong group-0 members", i)
		}
		if mr.PAddrList[mr.LeaderIdx] != groupClientAddrs[0][leaders[0]] {
			t.Fatalf("participant %d reports wrong group-0 leader", i)
		}
	}

	t.Log("step: creating /dir, matching it to its managing group, and putting 8 keys via that group's leader")
	// Create a directory and confirm keys within it are all managed by one
	// group.
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: next(), ClientID: client}, groupClientPorts[0][leaders[0]], "/create", &ksr, "", http.StatusCreated)

	dirLeader := findDirLeaderViaList(t, DirectoryRequest{Directory: "/dir", SeqNumber: next(), ClientID: client}, groupClientPorts[0])
	if dirLeader == -1 {
		t.Fatal("no leader found for new directory")
	}
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: next(), ClientID: client}, groupClientPorts[0][dirLeader], "/get_metadata", &mr, "", http.StatusOK)

	dirGroup := matchGroup(ctrl, mr, groupClientAddrs, groupClientPorts, dirLeader, leaders)
	if dirGroup == -1 {
		t.Fatal("could not match new directory to a group")
	}
	t.Logf("ok: /dir is managed by group %d", dirGroup)
	for k := 0; k < 8; k++ {
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "k" + itoa(k), Value: "v" + itoa(k), SeqNumber: next(), ClientID: client}, groupClientPorts[0][leaders[dirGroup]], "/set", &ksr, "", http.StatusCreated)
	}
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "dir", SeqNumber: next(), ClientID: client}, groupClientPorts[0][leaders[0]], "/delete", &ksr, "", http.StatusOK)

	// Fan out a tree and count how many directories land in each group.
	groupHits := make(map[int]int)
	for g := range ctrl.raftGroups {
		groupHits[g] = 0
	}
	const fanout = 4
	t.Logf("step: fanning out a directory tree (fanout %d, depth 3) to distribute dirs across groups", fanout)
	seqPtr := &seq
	buildTree(t, ctrl, client, seqPtr, groupClientAddrs, groupClientPorts, leaders, groupHits, "/", 0, fanout, 3)

	t.Log("step: checking every group managed at least fanout directories")
	for g := range ctrl.raftGroups {
		if groupHits[g] < fanout {
			t.Fatalf("group %d managed only %d directories, want >= %d", g, groupHits[g], fanout)
		}
	}
	t.Logf("ok: directories distributed across all groups (hits=%v)", groupHits)
}

// matchGroup finds the group whose membership and leader match the metadata
// response for a directory whose leader is at dirLeader (an index into
// groupClientPorts[0]). Returns -1 if none matches.
func matchGroup(ctrl *HKVCController, mr MetadataResponse, addrs map[int][]string, ports map[int][]int, dirLeader int, leaders map[int]int) int {
	for g := range ctrl.raftGroups {
		if sameStringSlices(mr.PAddrList, addrs[g]) &&
			slices.Contains(ports[g], ports[0][dirLeader]) &&
			dirLeader == leaders[g] {
			return g
		}
	}
	return -1
}

// buildTree recursively creates subdirectories and keys, recording which group
// manages each created directory in groupHits. depth bounds the recursion.
func buildTree(t *testing.T, ctrl *HKVCController, client string, seq *int, addrs map[int][]string, ports map[int][]int, leaders map[int]int, groupHits map[int]int, dir string, parentGroup, fanout, depth int) {
	t.Helper()
	if depth == 0 {
		return
	}
	next := func() int { *seq++; return *seq - 1 }
	ksr := KeySuccessResponse{}
	mr := MetadataResponse{}

	for f := 0; f < fanout; f++ {
		childName := "sd" + itoa(depth) + itoa(f)
		childPath := strings.TrimRight(dir, "/") + "/" + childName

		validateResponseToClient(t, KeyRequest{Directory: dir, Key: childName, SeqNumber: next(), ClientID: client}, ports[0][leaders[parentGroup]], "/create", &ksr, "", http.StatusCreated)

		dirLeader := findDirLeaderViaList(t, DirectoryRequest{Directory: childPath, SeqNumber: next(), ClientID: client}, ports[0])
		if dirLeader == -1 {
			t.Fatalf("no leader found for %s", childPath)
		}
		validateResponseToClient(t, KeyRequest{Directory: dir, Key: childName, SeqNumber: next(), ClientID: client}, ports[0][dirLeader], "/get_metadata", &mr, "", http.StatusOK)

		childGroup := matchGroup(ctrl, mr, addrs, ports, dirLeader, leaders)
		if childGroup == -1 {
			t.Fatalf("could not match %s to a group", childPath)
		}
		t.Logf("iter: created %s (depth %d), managed by group %d", childPath, depth, childGroup)
		groupHits[childGroup]++

		// Put a couple of keys in the child directory.
		for k := 0; k < 2; k++ {
			validateResponseToClient(t, KeyValueMessage{Directory: childPath, Key: "k" + itoa(k), Value: "v" + itoa(k), SeqNumber: next(), ClientID: client}, ports[0][leaders[childGroup]], "/set", &ksr, "", http.StatusCreated)
		}

		buildTree(t, ctrl, client, seq, addrs, ports, leaders, groupHits, childPath, childGroup, fanout, depth-1)
	}
}
