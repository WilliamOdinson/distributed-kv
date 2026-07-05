package hkvc

// Integration tests for the client HTTP interface: input validation, cluster
// bring-up, non-leader rejection, and the basic set/list/get round trip.

import (
	"net/http"
	"testing"
)

// TestClient_InvalidRequests drives every endpoint with malformed input and
// checks the error type/status. It uses a single-participant cluster (which is
// always its own leader) so the only failures possible are validation errors.
func TestClient_InvalidRequests(t *testing.T) {
	t.Log("step: creating a single-participant cluster (its own leader)")
	ctrl := NewHKVCController(t, 1, 0, 0)
	port := ctrl.clientPort(0)
	client := randSeq(12)
	seq := 0
	next := func() int { seq++; return seq - 1 }
	t.Logf("ok: cluster up, client port %d", port)

	t.Run("list", func(t *testing.T) {
		t.Log("step: driving /list with bad directories, expecting validation errors")
		validateResponseToClient(t, DirectoryRequest{Directory: "", SeqNumber: next(), ClientID: client}, port, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, DirectoryRequest{Directory: "dir", SeqNumber: next(), ClientID: client}, port, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, DirectoryRequest{Directory: "/dir:name", SeqNumber: next(), ClientID: client}, port, "/list", &ListResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, DirectoryRequest{Directory: "/dir", SeqNumber: next(), ClientID: client}, port, "/list", &ListResponse{}, DirNotFoundError, http.StatusNotFound)
		t.Log("ok: /list rejected malformed and missing directories")
	})

	t.Run("get_metadata", func(t *testing.T) {
		t.Log("step: driving /get_metadata with bad dir/key, expecting validation and not-found errors")
		validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, DirNotFoundError, http.StatusNotFound)
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get_metadata", &MetadataResponse{}, KeyNotFoundError, http.StatusNotFound)
		t.Log("ok: /get_metadata rejected bad input and reported missing dir/key")
	})

	t.Run("get", func(t *testing.T) {
		t.Log("step: driving /get with bad dir, expecting validation and not-found errors")
		validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get", &KeyValueMessage{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get", &KeyValueMessage{}, DirNotFoundError, http.StatusNotFound)
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: next(), ClientID: client}, port, "/get", &KeyValueMessage{}, KeyNotFoundError, http.StatusNotFound)
		t.Log("ok: /get rejected bad input and reported missing dir/key")
	})

	t.Run("set", func(t *testing.T) {
		t.Log("step: driving /set with bad dir, expecting validation and not-found errors")
		validateResponseToClient(t, KeyValueMessage{Directory: "", Key: "key", Value: "abcd", SeqNumber: next(), ClientID: client}, port, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyValueMessage{Directory: "dir", Key: "key", Value: "abcd", SeqNumber: next(), ClientID: client}, port, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir:name", Key: "key", Value: "abcd", SeqNumber: next(), ClientID: client}, port, "/set", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyValueMessage{Directory: "/dir", Key: "key", Value: "abcd", SeqNumber: next(), ClientID: client}, port, "/set", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)
		t.Log("ok: /set rejected bad input and reported missing dir")
	})

	t.Run("create", func(t *testing.T) {
		t.Log("step: driving /create with bad dir, expecting validation and not-found errors")
		validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: next(), ClientID: client}, port, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: next(), ClientID: client}, port, "/create", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/create", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)
		t.Log("ok: /create rejected bad input and reported missing dir")
	})

	t.Run("delete", func(t *testing.T) {
		t.Log("step: driving /delete with bad dir/key, expecting validation and not-found errors")
		validateResponseToClient(t, KeyRequest{Directory: "", Key: "key", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir:name", Key: "key", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, InvalidError, http.StatusBadRequest)
		validateResponseToClient(t, KeyRequest{Directory: "/dir", Key: "key", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, DirNotFoundError, http.StatusNotFound)
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: next(), ClientID: client}, port, "/delete", &KeySuccessResponse{}, KeyNotFoundError, http.StatusNotFound)
		t.Log("ok: /delete rejected bad input and reported missing dir/key")
	})
}

// A three-participant cluster must elect a leader that answers client requests.
func TestClient_InitializeAndQueryLeader(t *testing.T) {
	t.Log("step: creating a 3-participant cluster")
	ctrl := NewHKVCController(t, 3, 0, 0)
	client := randSeq(12)

	t.Log("step: querying participant 0 status")
	if _, re := ctrl.stubs[0].GetStatus(); re.Error() != "" {
		t.Fatalf("GetStatus failed: %s", re.Error())
	}

	t.Log("step: locating the group-0 leader")
	leader, _ := ctrl.getGroupLeaderCommit(0)
	if leader < 0 {
		t.Fatal("no group-0 leader elected")
	}
	t.Logf("ok: group-0 leader is participant %d", leader)
	t.Logf("step: sending /list of %q to leader, expecting 200", "/")
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 0, ClientID: client}, ctrl.clientPort(leader), "/list", &ListResponse{}, "", http.StatusOK)
}

// Every mutating and reading endpoint on a non-leader must be rejected with
// HKVCNonRaftLeaderError.
func TestClient_NonLeaderRejects(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-participant cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	nonLeaderPort := ctrl.clientPort((leader + 1) % n)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d; targeting non-leader %d at port %d", leader, (leader+1)%n, nonLeaderPort)

	t.Log("step: hitting every endpoint on the non-leader, expecting 403 non-leader errors")
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: 0, ClientID: client}, nonLeaderPort, "/list", &ListResponse{}, NonLeaderError, http.StatusForbidden)
	validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: "key", Value: "value", SeqNumber: 1, ClientID: client}, nonLeaderPort, "/set", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 2, ClientID: client}, nonLeaderPort, "/get", &KeyValueMessage{}, NonLeaderError, http.StatusForbidden)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 3, ClientID: client}, nonLeaderPort, "/create", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)
	validateResponseToClient(t, KeyRequest{Directory: "/", Key: "key", SeqNumber: 4, ClientID: client}, nonLeaderPort, "/delete", &KeySuccessResponse{}, NonLeaderError, http.StatusForbidden)
	t.Log("ok: non-leader rejected list/set/get/create/delete")
}

// Populate a small key-value store with /set, then verify contents via /list
// and /get.
func TestClient_SetListGet(t *testing.T) {
	const n = 3
	t.Logf("step: creating a %d-participant cluster", n)
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	port := ctrl.clientPort(leader)
	client := randSeq(12)
	t.Logf("ok: leader is participant %d at port %d", leader, port)

	const numKeys = 10
	ksr := KeySuccessResponse{}
	keys := make([]string, numKeys)
	values := make([]string, numKeys)
	t.Logf("step: setting %d keys under %q, each expecting 201", numKeys, "/")
	for k := 0; k < numKeys; k++ {
		kvm := KeyValueMessage{Directory: "/", Key: "key" + itoa(k), Value: "value #" + itoa(k), SeqNumber: k, ClientID: client}
		keys[k] = kvm.Key
		values[k] = kvm.Value
		validateResponseToClient(t, kvm, port, "/set", &ksr, "", http.StatusCreated)
	}
	t.Logf("ok: %d keys created", numKeys)

	t.Logf("step: listing %q and checking it returns exactly the %d keys", "/", numKeys)
	lr := ListResponse{}
	validateResponseToClient(t, DirectoryRequest{Directory: "/", SeqNumber: numKeys, ClientID: client}, port, "/list", &lr, "", http.StatusOK)
	if !sameStringSlices(lr.List, keys) || lr.Directory != "/" {
		t.Fatalf("list = %v for dir %q, want keys %v under /", lr.List, lr.Directory, keys)
	}
	t.Log("ok: list matched the created keys")

	t.Logf("step: getting each of the %d keys and checking the value round-trips", numKeys)
	kvm := KeyValueMessage{}
	for k := 0; k < numKeys; k++ {
		validateResponseToClient(t, KeyRequest{Directory: "/", Key: keys[k], SeqNumber: numKeys + 1 + k, ClientID: client}, port, "/get", &kvm, "", http.StatusOK)
		if kvm.Value != values[k] {
			t.Fatalf("get %s = %q, want %q", keys[k], kvm.Value, values[k])
		}
	}
	t.Log("ok: every key returned its stored value")
}
