package hkvc

// testkit_test.go holds the shared scaffolding for the HKVC test suite: random
// data helpers, the HTTP request/response plumbing used to talk to a
// participant's client interface, and the HKVCController that spins up a
// cluster and drives it through the control plane.
//
// Addresses are drawn from the kernel (freePort) rather than computed from a
// single random base port, and tests locate a participant's client port via
// clientPort() instead of reconstructing it with arithmetic. This removes both
// the cross-test port-collision flakiness and the brittle coupling between the
// tests and the exact port layout.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"remote"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// WaitForRaft is how long to wait for raft to elect leaders and settle.
const WaitForRaft = 2 * time.Second

// itoa is a short alias for strconv.Itoa, used pervasively when building the
// keys, values, and directory names that tests submit.
func itoa(i int) string { return strconv.Itoa(i) }

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// randSeq returns a random string of n letters, used for unique client IDs.
func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// randomSubset returns k distinct values chosen from {0, ..., N-1}.
func randomSubset(N, k int) []int {
	if k == N {
		s := make([]int, k)
		for i := range s {
			s[i] = i
		}
		return s
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	seen := make(map[int]bool)
	out := make([]int, 0, k)
	for len(out) < k {
		r := rng.Intn(N)
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out
}

// sameStringSlices reports whether two slices contain the same elements,
// ignoring order.
func sameStringSlices(one, two []string) bool {
	if len(one) != len(two) {
		return false
	}
	for _, s := range one {
		if !slices.Contains(two, s) {
			return false
		}
	}
	return true
}

// freeAddrs reserves n distinct localhost addresses at once. Because every
// listener is held open until all n are chosen, the kernel cannot hand out the
// same port twice within a batch; they are closed just before returning so the
// participants can bind them. This eliminates the intra-cluster port-collision
// flakiness that per-address reservation risks.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("could not reserve address %d/%d: %v", i+1, n, err)
		}
		listeners = append(listeners, ln)
		addrs = append(addrs, ln.Addr().String())
	}
	for _, ln := range listeners {
		ln.Close()
	}
	return addrs
}

// portOf extracts the integer port from an "ip:port" address, failing the test
// on a malformed input.
func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("malformed address %q: %v", addr, err)
	}
	p, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("non-numeric port in %q: %v", addr, err)
	}
	return p
}

// getResponse POSTs msg to http://localhost:port<endpoint> and decodes either a
// success value into v or an error response. endpoint must include the leading
// slash. It returns the status code, any error response, and a transport error.
func getResponse(port int, endpoint string, msg []byte, v any) (int, *HKVCErrorResponse, error) {
	url := "http://localhost:" + strconv.Itoa(port) + endpoint
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(msg))
	if err != nil {
		return -1, nil, errors.New("http request failed in getResponse: " + err.Error())
	}
	code := resp.StatusCode
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return -1, nil, errors.New("reading " + endpoint + " response failed: " + err.Error())
	}
	str := string(body)

	dec := json.NewDecoder(strings.NewReader(str))
	dec.DisallowUnknownFields()

	if strings.Contains(str, "error_type") {
		e := &HKVCErrorResponse{}
		if err := dec.Decode(e); err != nil {
			return code, nil, errors.New("decoding " + endpoint + " error response failed: " + err.Error())
		}
		return code, e, nil
	}
	if err := dec.Decode(v); err != nil {
		return code, nil, errors.New("decoding " + endpoint + " success response failed: " + err.Error())
	}
	return code, nil, nil
}

// validateResponseToClient marshals msg, sends it to endpoint on the given
// port, and checks the response against expectations:
//
//   - expStatusCode 200/201: expect a success response with that exact code
//   - expStatusCode > 0 (other): expect an error response with expError (if set)
//     and that exact code
//   - expStatusCode == 0: perform no assertions; just return the results so the
//     caller can validate them directly
//
// It returns the status code and any error response for further inspection.
func validateResponseToClient(t *testing.T, msg any, port int, endpoint string, resp any, expError string, expStatusCode int) (int, *HKVCErrorResponse) {
	t.Helper()
	jsonmsg, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("encoding %T failed: %v", msg, err)
	}
	rc, er, err := getResponse(port, endpoint, jsonmsg, resp)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case expStatusCode == http.StatusOK || expStatusCode == http.StatusCreated:
		if er != nil || rc != expStatusCode {
			t.Fatalf("endpoint %s returned error on valid %T (code %d, err %+v)", endpoint, msg, rc, er)
		}
	case expStatusCode > 0:
		if er == nil || (expError != "" && er.ErrorType != expError) || rc != expStatusCode {
			t.Fatalf("endpoint %s gave wrong error/code on %T (code %d, err %+v, wanted %s/%d)", endpoint, msg, rc, er, expError, expStatusCode)
		}
	}
	return rc, er
}

// HKVCController spins up an HKVC cluster and drives it through the control
// plane, mimicking a client for tests.
type HKVCController struct {
	t           *testing.T
	clusterSize int
	nAddlGroups int
	groupSize   int
	stubs       []*HKVCControlInterface
	raftGroups  map[int][]int
	setupInfo   []HKVCSetupInfo
}

// NewHKVCController creates a cluster of clSize participants, with addlGroups
// additional raft groups each of size grSize (beyond the base group 0 that
// contains everyone). It activates all participants and waits for raft to
// settle before returning.
func NewHKVCController(t *testing.T, clSize, addlGroups, grSize int) *HKVCController {
	t.Helper()
	t.Logf("step: creating a %d-node cluster with %d additional group(s) of size %d", clSize, addlGroups, grSize)
	ctrl := &HKVCController{
		t:           t,
		clusterSize: clSize,
		nAddlGroups: addlGroups,
		groupSize:   grSize,
		stubs:       make([]*HKVCControlInterface, clSize),
		setupInfo:   make([]HKVCSetupInfo, clSize),
		raftGroups:  make(map[int][]int),
	}

	// Base group 0 contains everyone; each additional group manages a shard.
	groupIDs := randomSubset(254, addlGroups) // shifted +1 below into [1, 255]
	ctrl.raftGroups[0] = randomSubset(clSize, clSize)
	for i := 0; i < addlGroups; i++ {
		ctrl.raftGroups[groupIDs[i]+1] = randomSubset(clSize, grSize)
	}

	// Assign a unique ID and freshly reserved addresses to each participant.
	// Each participant needs: client + control + one raft address per group it
	// could join (base group plus every additional group). Reserve them all in
	// one batch so no two collide.
	ids := randomSubset(65000, clSize)
	perParticipant := 2 + 1 + addlGroups // client, control, base-group raft, addl-group rafts
	t.Logf("step: reserving %d addresses (%d per participant)", clSize*perParticipant, perParticipant)
	addrs := freeAddrs(t, clSize*perParticipant)
	next := 0
	take := func() string { a := addrs[next]; next++; return a }
	for i := 0; i < clSize; i++ {
		ctrl.setupInfo[i] = HKVCSetupInfo{
			Id:          ids[i],
			ClientAddr:  take(),
			ControlAddr: take(),
			RaftAddrs:   map[int]string{0: take()},
		}
		for j := 0; j < addlGroups; j++ {
			ctrl.setupInfo[i].RaftAddrs[groupIDs[j]+1] = take()
		}
	}

	t.Logf("step: launching %d participants and creating control stubs", clSize)
	for i := range ctrl.setupInfo {
		go NewHKVCParticipant(ctrl.setupInfo, i, ctrl.raftGroups)
		ctrl.stubs[i] = &HKVCControlInterface{}
		if err := remote.CallerStubCreator(ctrl.stubs[i], ctrl.setupInfo[i].ControlAddr, false, false); err != nil {
			t.Fatalf("cannot create controller stub for participant %d: %v", i, err)
		}
	}

	t.Cleanup(func() {
		for i := range ctrl.stubs {
			ctrl.stubs[i].Terminate()
		}
	})

	time.Sleep(2 * time.Second) // let control callees come up
	t.Logf("step: activating %d participants", clSize)
	for i := range ctrl.stubs {
		ctrl.connect(i)
	}
	time.Sleep(WaitForRaft) // let raft stabilize
	t.Logf("ok: cluster of %d participants activated and raft settled", clSize)
	return ctrl
}

// clientPort returns the HTTP client port of participant i, decoupling tests
// from how addresses are laid out.
func (ctrl *HKVCController) clientPort(i int) int {
	return portOf(ctrl.t, ctrl.setupInfo[i].ClientAddr)
}

// controlRetryTimeout bounds how long connect/disconnect retry a control-plane
// RPC before giving up, so a participant that never came up fails cleanly
// instead of hanging the whole suite until the global test timeout.
const controlRetryTimeout = 20 * time.Second

// connect activates participant i, retrying briefly on transient RPC errors. It
// uses t.Errorf (not Fatalf) because it may be invoked from a non-test
// goroutine (e.g. the failover chaos loop), where FailNow/Fatal is unsafe.
func (ctrl *HKVCController) connect(i int) {
	deadline := time.Now().Add(controlRetryTimeout)
	for {
		re := ctrl.stubs[i].Activate()
		if re.Error() == "" {
			return
		}
		if time.Now().After(deadline) {
			ctrl.t.Errorf("participant %d never accepted Activate within %v: %s", i, controlRetryTimeout, re.Error())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// disconnect deactivates participant i, retrying briefly on transient RPC errors.
func (ctrl *HKVCController) disconnect(i int) {
	deadline := time.Now().Add(controlRetryTimeout)
	for {
		re := ctrl.stubs[i].Deactivate()
		if re.Error() == "" {
			return
		}
		if time.Now().After(deadline) {
			ctrl.t.Errorf("participant %d never accepted Deactivate within %v: %s", i, controlRetryTimeout, re.Error())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// getGroupLeaderCommit returns the participant index and commit index of the
// leader of groupID, or (-1, -1) if there is no leader.
func (ctrl *HKVCController) getGroupLeaderCommit(groupID int) (int, int) {
	group, ok := ctrl.raftGroups[groupID]
	if !ok {
		return -1, -1
	}
	for _, idx := range group {
		sr, re := ctrl.stubs[idx].GetStatus()
		if re.Error() != "" {
			continue
		}
		if sr.Active && sr.GroupLeader[groupID] {
			if commit, ok := sr.GroupCommit[groupID]; ok {
				return idx, commit
			}
			return idx, -1
		}
	}
	return -1, -1
}
