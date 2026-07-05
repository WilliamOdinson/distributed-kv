package hkvc

// Fast, deterministic unit tests for the pure helper logic of an HKVC
// participant: path normalization, client-sequence classification, directory
// resolution, and the apply* command handlers that mutate the in-memory tree.
// None of these touch the network or raft, so they run instantly and isolate
// the store's core behavior from the distributed machinery exercised by the
// integration tests.

import (
	"net/http"
	"testing"
)

// newBareParticipant builds a participant with an initialized root directory
// and the per-client bookkeeping maps, but no raft peers, HTTP server, or
// control callee. Only the pure helpers and apply* handlers are safe to call.
func newBareParticipant() *HKVCParticipant {
	return &HKVCParticipant{
		root: &directory{
			name:    "/",
			subDirs: make(map[string]*directory),
			kvPairs: make(map[string]*kvPair),
			groupID: 0,
		},
		clientSeq:  make(map[string]int),
		clientResp: make(map[string]*cachedResponse),
	}
}

func TestNormalizeDir(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"root", "/", "/", true},
		{"simple", "/a", "/a", true},
		{"nested", "/a/b/c", "/a/b/c", true},
		{"trailing slash trimmed", "/a/b/", "/a/b", true},
		{"collapses double slashes", "/a//b", "/a/b", true},
		{"empty rejected", "", "", false},
		{"relative rejected", "a/b", "", false},
		{"colon rejected", "/a:b", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("step: normalizing %q, want ok=%v want=%q", tc.in, tc.ok, tc.want)
			got, ok := normalizeDir(tc.in)
			if ok != tc.ok {
				t.Fatalf("normalizeDir(%q) ok = %v, want %v", tc.in, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("normalizeDir(%q) = %q, want %q", tc.in, got, tc.want)
			}
			t.Logf("ok: %q normalized as expected", tc.in)
		})
	}
}

func TestCheckSeq(t *testing.T) {
	p := newBareParticipant()
	const client = "c1"

	t.Logf("step: classifying first request from client %q (seq 0)", client)
	if got := p.checkSeq(client, 0); got != SEQ_FRESH {
		t.Fatalf("first request classified %d, want SEQ_FRESH", got)
	}
	t.Logf("step: setting last seen seq for %q to 5", client)
	p.clientSeq[client] = 5

	cases := []struct {
		name string
		seq  int
		want int
	}{
		{"greater is fresh", 6, SEQ_FRESH},
		{"equal is duplicate", 5, SEQ_DUPLICATE},
		{"smaller is outdated", 4, SEQ_OUTDATED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("step: classifying seq %d against last=5, want %d", tc.seq, tc.want)
			if got := p.checkSeq(client, tc.seq); got != tc.want {
				t.Fatalf("checkSeq(%d) with last=5 = %d, want %d", tc.seq, got, tc.want)
			}
		})
	}
}

func TestResolveDir(t *testing.T) {
	p := newBareParticipant()
	// Build /a/b by hand.
	t.Log("step: building the /a/b directory tree by hand")
	a := &directory{name: "a", subDirs: map[string]*directory{}, kvPairs: map[string]*kvPair{}}
	b := &directory{name: "b", subDirs: map[string]*directory{}, kvPairs: map[string]*kvPair{}}
	p.root.subDirs["a"] = a
	a.subDirs["b"] = b

	t.Log("step: resolving existing paths /, /a, /a/b")
	if p.resolveDir("/") != p.root {
		t.Fatal("resolveDir(/) did not return root")
	}
	if p.resolveDir("/a") != a {
		t.Fatal("resolveDir(/a) did not return a")
	}
	if p.resolveDir("/a/b") != b {
		t.Fatal("resolveDir(/a/b) did not return b")
	}
	t.Log("ok: existing paths resolved to the expected nodes")

	t.Log("step: resolving missing paths /a/missing and /x/y, want nil")
	if p.resolveDir("/a/missing") != nil {
		t.Fatal("resolveDir of a nonexistent path should be nil")
	}
	if p.resolveDir("/x/y") != nil {
		t.Fatal("resolveDir with a missing first segment should be nil")
	}
	t.Log("ok: missing paths resolved to nil")
}

func TestApplySetCmd_CreateThenOverwrite(t *testing.T) {
	p := newBareParticipant()

	// First set creates the key at version 1 with 201 Created.
	t.Log("step: setting /k=v1 (fresh key), want 201 at version 1")
	r := p.applySetCmd(0, &raftCommand{Op: "set", Directory: "/", Key: "k", Value: "v1"})
	if !r.success || r.status != http.StatusCreated {
		t.Fatalf("first set = {%v, %d}, want {true, 201}", r.success, r.status)
	}
	if kv := p.root.kvPairs["k"]; kv == nil || kv.value != "v1" || kv.version != 1 {
		t.Fatalf("after create: %+v, want value v1 / version 1", kv)
	}
	t.Log("ok: /k created at version 1")

	// Overwrite bumps the version and returns 200 OK.
	t.Log("step: overwriting /k=v2, want 200 at version 2")
	r = p.applySetCmd(0, &raftCommand{Op: "set", Directory: "/", Key: "k", Value: "v2"})
	if !r.success || r.status != http.StatusOK {
		t.Fatalf("overwrite set = {%v, %d}, want {true, 200}", r.success, r.status)
	}
	if kv := p.root.kvPairs["k"]; kv.value != "v2" || kv.version != 2 {
		t.Fatalf("after overwrite: %+v, want value v2 / version 2", kv)
	}
	t.Log("ok: /k overwritten to v2 at version 2")
}

func TestApplySetCmd_MissingDirectory(t *testing.T) {
	p := newBareParticipant()
	t.Log("step: setting into missing dir /nope, want {false, 404}")
	r := p.applySetCmd(0, &raftCommand{Op: "set", Directory: "/nope", Key: "k", Value: "v"})
	if r.success || r.status != http.StatusNotFound {
		t.Fatalf("set into missing dir = {%v, %d}, want {false, 404}", r.success, r.status)
	}
}

func TestApplyCreateCmd(t *testing.T) {
	p := newBareParticipant()

	t.Log("step: creating dir /d, want {true, 201}")
	r := p.applyCreateCmd(0, &raftCommand{Op: "create", Directory: "/", Key: "d"})
	if !r.success || r.status != http.StatusCreated {
		t.Fatalf("create = {%v, %d}, want {true, 201}", r.success, r.status)
	}
	if _, ok := p.root.subDirs["d"]; !ok {
		t.Fatal("create did not add the subdirectory")
	}
	t.Log("ok: subdirectory /d created")

	// Creating the same directory again is a no-op success=false at 200.
	t.Log("step: re-creating existing dir /d, want {false, 200}")
	r = p.applyCreateCmd(0, &raftCommand{Op: "create", Directory: "/", Key: "d"})
	if r.success || r.status != http.StatusOK {
		t.Fatalf("duplicate create = {%v, %d}, want {false, 200}", r.success, r.status)
	}

	// A name colliding with an existing key also yields success=false at 200.
	t.Log("step: creating dir over existing key /kk, want {false, 200}")
	p.root.kvPairs["kk"] = &kvPair{key: "kk", value: "x", version: 1}
	r = p.applyCreateCmd(0, &raftCommand{Op: "create", Directory: "/", Key: "kk"})
	if r.success || r.status != http.StatusOK {
		t.Fatalf("create over key = {%v, %d}, want {false, 200}", r.success, r.status)
	}
}

// A directory created directly under the root should be assigned a group by
// round-robin over the sorted group IDs; deeper directories inherit the parent.
func TestApplyCreateCmd_GroupAssignment(t *testing.T) {
	p := newBareParticipant()
	p.sortedGIDs = []int{0, 7, 9}
	p.createCounter = 0

	// Root-level children round-robin across sortedGIDs.
	t.Logf("step: creating root-level dirs r0, r1 with sortedGIDs %v (round-robin)", p.sortedGIDs)
	p.applyCreateCmd(0, &raftCommand{Op: "create", Directory: "/", Key: "r0"})
	p.applyCreateCmd(0, &raftCommand{Op: "create", Directory: "/", Key: "r1"})
	if g := p.root.subDirs["r0"].groupID; g != 0 {
		t.Fatalf("r0 group = %d, want 0", g)
	}
	if g := p.root.subDirs["r1"].groupID; g != 7 {
		t.Fatalf("r1 group = %d, want 7 (round-robin)", g)
	}
	t.Log("ok: r0 got group 0, r1 got group 7")

	// A child of a non-root directory inherits its parent's group.
	t.Log("step: creating /r1/child, want inherited group 7")
	parent := p.root.subDirs["r1"] // group 7
	p.applyCreateCmd(7, &raftCommand{Op: "create", Directory: "/r1", Key: "child"})
	if g := parent.subDirs["child"].groupID; g != 7 {
		t.Fatalf("child group = %d, want inherited 7", g)
	}
	t.Log("ok: child inherited parent group 7")
}

func TestApplyDeleteCmd(t *testing.T) {
	p := newBareParticipant()
	t.Log("step: seeding root with key /k and subdir /d")
	p.root.kvPairs["k"] = &kvPair{key: "k", value: "v", version: 3}
	p.root.subDirs["d"] = &directory{name: "d", subDirs: map[string]*directory{}, kvPairs: map[string]*kvPair{}}

	t.Log("step: deleting key /k, want {true, 200}")
	if r := p.applyDeleteCmd(0, &raftCommand{Op: "delete", Directory: "/", Key: "k"}); !r.success || r.status != http.StatusOK {
		t.Fatalf("delete key = {%v, %d}, want {true, 200}", r.success, r.status)
	}
	if _, ok := p.root.kvPairs["k"]; ok {
		t.Fatal("delete did not remove the key")
	}

	t.Log("step: deleting subdir /d, want {true, 200}")
	if r := p.applyDeleteCmd(0, &raftCommand{Op: "delete", Directory: "/", Key: "d"}); !r.success || r.status != http.StatusOK {
		t.Fatalf("delete dir = {%v, %d}, want {true, 200}", r.success, r.status)
	}
	if _, ok := p.root.subDirs["d"]; ok {
		t.Fatal("delete did not remove the subdirectory")
	}
	t.Log("ok: key and subdir removed")

	t.Log("step: deleting missing entry /missing, want {false, 404}")
	if r := p.applyDeleteCmd(0, &raftCommand{Op: "delete", Directory: "/", Key: "missing"}); r.success || r.status != http.StatusNotFound {
		t.Fatalf("delete missing = {%v, %d}, want {false, 404}", r.success, r.status)
	}
}

func TestApplyCommand_Dispatch(t *testing.T) {
	p := newBareParticipant()

	t.Log("step: dispatching no-op, want {true, 200}")
	if r := p.applyCommand(0, &raftCommand{Op: "no-op"}); !r.success || r.status != http.StatusOK {
		t.Fatalf("no-op = {%v, %d}, want {true, 200}", r.success, r.status)
	}
	// Unknown op falls through to an internal error.
	t.Log("step: dispatching unknown op \"bogus\", want status 500")
	if r := p.applyCommand(0, &raftCommand{Op: "bogus"}); r.status != http.StatusInternalServerError {
		t.Fatalf("unknown op status = %d, want 500", r.status)
	}
	// Dispatch reaches the set handler.
	t.Log("step: dispatching set /k=v, want it to reach the set handler")
	if r := p.applyCommand(0, &raftCommand{Op: "set", Directory: "/", Key: "k", Value: "v"}); !r.success {
		t.Fatal("dispatch to set failed")
	}
	t.Log("ok: dispatch routed no-op, unknown, and set correctly")
}
