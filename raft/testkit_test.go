package raft

// testkit_test.go provides the Controller harness that the raft integration
// tests share. The Controller stands in for a client: it spawns a set of peers,
// drives them through their ControlInterface, and offers assertions about
// leadership, terms, and committed commands.
//
// Compared with a "pick a random base port" approach, addresses here come from
// freePorts, which asks the kernel for genuinely unused ports and so avoids the
// cross-test collisions that make networked suites flaky.

import (
	"bytes"
	"math/rand"
	"net"
	"remote"
	"strconv"
	"sync"
	"testing"
	"time"
)

// electionSettleTime is how long we allow between leadership observations. It
// must comfortably exceed the election timeout window so a stable leader can
// emerge before we sample.
const electionSettleTime = time.Second

// freePorts reserves n distinct localhost ports and returns them. Each listener
// is closed before returning, so the ports are immediately reusable by the peer
// under test. Reserving them together makes accidental reuse within one test
// vanishingly unlikely.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	t.Logf("step: reserving %d free localhost ports", n)
	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("could not reserve port %d/%d: %v", i+1, n, err)
		}
		listeners = append(listeners, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	for _, ln := range listeners {
		ln.Close()
	}
	t.Logf("ok: reserved ports %v", ports)
	return ports
}

// Controller drives a group of raft peers for testing.
type Controller struct {
	mu         sync.Mutex
	t          *testing.T
	totalPeers int
	stubs      []*ControlInterface
	rng        *rand.Rand
}

// NewController spawns totalPeers raft peers, wires up a ControlInterface stub
// to each, activates them all, and registers cleanup that terminates every
// peer when the test ends.
func NewController(t *testing.T, totalPeers int) *Controller {
	t.Helper()
	t.Logf("step: creating a %d-node cluster", totalPeers)
	ctrl := &Controller{
		t:          t,
		totalPeers: totalPeers,
		stubs:      make([]*ControlInterface, totalPeers),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// Two ports per peer: one for RaftInterface, one for ControlInterface.
	ports := freePorts(t, 2*totalPeers)

	// Unique peer IDs plus deterministic addresses derived from the ports.
	info := make([]RaftSetupInfo, totalPeers)
	usedIDs := make(map[int]bool)
	for i := 0; i < totalPeers; i++ {
		id := ctrl.rng.Intn(65500)
		for usedIDs[id] {
			id = ctrl.rng.Intn(65500)
		}
		usedIDs[id] = true
		info[i] = RaftSetupInfo{
			Id:    id,
			Addr:  "localhost:" + strconv.Itoa(ports[2*i]),
			Caddr: "localhost:" + strconv.Itoa(ports[2*i+1]),
		}
	}

	t.Logf("step: spawning %d peers and their control stubs", totalPeers)
	for i, pinfo := range info {
		go NewRaftPeer(info, i)
		ctrl.stubs[i] = &ControlInterface{}
		if err := remote.CallerStubCreator(ctrl.stubs[i], pinfo.Caddr, false, false); err != nil {
			t.Fatalf("cannot create controller stub for peer %d: %v", i, err)
		}
	}

	t.Cleanup(func() {
		for i := range ctrl.stubs {
			ctrl.stubs[i].Terminate()
		}
	})

	// Give the control callees time to come up, then activate everyone.
	t.Logf("step: waiting for control callees, then activating all %d peers", totalPeers)
	time.Sleep(3 * time.Second)
	for i := 0; i < totalPeers; i++ {
		ctrl.connect(i)
	}
	t.Logf("ok: %d-node cluster is up and activated", totalPeers)
	return ctrl
}

// controlRetryTimeout bounds how long connect/disconnect retry a control-plane
// RPC before giving up. It is generous (the control interface runs for the
// peer's whole lifetime, so calls normally succeed on the first try) but finite,
// so a peer that never came up produces a clean failure instead of hanging the
// whole suite until the global test timeout.
const controlRetryTimeout = 20 * time.Second

// connect (re)activates peer i, retrying briefly on transient RPC errors. It
// uses t.Errorf (not Fatalf) because it may be invoked from a non-test
// goroutine, where FailNow/Fatal is unsafe.
func (ctrl *Controller) connect(i int) {
	ctrl.t.Logf("step: connecting (activating) peer %d", i)
	deadline := time.Now().Add(controlRetryTimeout)
	for {
		re := ctrl.stubs[i].Activate()
		if re.Error() == "" {
			return
		}
		if time.Now().After(deadline) {
			ctrl.t.Errorf("peer %d never accepted Activate within %v: %s", i, controlRetryTimeout, re.Error())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// disconnect deactivates peer i, retrying briefly on transient RPC errors.
func (ctrl *Controller) disconnect(i int) {
	ctrl.t.Logf("step: disconnecting (deactivating) peer %d", i)
	deadline := time.Now().Add(controlRetryTimeout)
	for {
		re := ctrl.stubs[i].Deactivate()
		if re.Error() == "" {
			return
		}
		if time.Now().After(deadline) {
			ctrl.t.Errorf("peer %d never accepted Deactivate within %v: %s", i, controlRetryTimeout, re.Error())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// countCommitsAtIndex reports how many peers have committed the same command at
// idx, failing the test if two peers disagree on the committed value.
func (ctrl *Controller) countCommitsAtIndex(idx int) (int, []byte) {
	count := 0
	var cmd []byte
	for i := 0; i < ctrl.totalPeers; i++ {
		logcmd, re := ctrl.stubs[i].GetCommittedCmd(idx)
		if re.Error() != "" {
			continue
		}
		if !bytes.Equal(logcmd, nil) {
			if count > 0 && !bytes.Equal(cmd, logcmd) {
				ctrl.t.Fatalf("committed values disagree at index %d: %v vs %v", idx, cmd, logcmd)
			}
			count++
			cmd = bytes.Clone(logcmd)
		}
	}
	return count, cmd
}

// findLeader returns the single current leader, failing the test if none
// appears within a bounded number of election windows or if two leaders share a
// term.
func (ctrl *Controller) findLeader() int {
	ctrl.t.Logf("step: waiting for a single leader to emerge")
	for iter := 0; iter < 10; iter++ {
		time.Sleep(electionSettleTime)
		leaders := make(map[int][]int)
		for i := 0; i < ctrl.totalPeers; i++ {
			sr, re := ctrl.stubs[i].GetStatus()
			if re.Error() != "" {
				continue
			}
			if sr.IsActive && sr.IsLeader {
				leaders[sr.Term] = append(leaders[sr.Term], i)
			}
		}
		lastTerm := -1
		for term, leads := range leaders {
			if len(leads) > 1 {
				ctrl.t.Fatalf("found %d leaders in term %d", len(leads), term)
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm != -1 {
			ctrl.t.Logf("ok: peer %d is leader in term %d", leaders[lastTerm][0], lastTerm)
			return leaders[lastTerm][0]
		}
	}
	ctrl.t.Fatal("no leader found")
	return -1
}

// getTerm returns the term all responding active peers agree on, failing the
// test on disagreement.
func (ctrl *Controller) getTerm() int {
	ctrl.t.Logf("step: reading the term agreed on by active peers")
	term := -1
	for i := 0; i < ctrl.totalPeers; i++ {
		sr, re := ctrl.stubs[i].GetStatus()
		if re.Error() != "" {
			continue
		}
		if sr.IsActive && sr.Term > 0 {
			if term == -1 {
				term = sr.Term
			} else if term != sr.Term {
				ctrl.t.Fatal("peers disagree about term")
			}
		}
	}
	ctrl.t.Logf("ok: active peers agree on term %d", term)
	return term
}

// ensureNoLeader fails the test if any active peer claims leadership.
func (ctrl *Controller) ensureNoLeader() {
	ctrl.t.Logf("step: verifying no active peer claims leadership")
	time.Sleep(electionSettleTime)
	for i := 0; i < ctrl.totalPeers; i++ {
		sr, re := ctrl.stubs[i].GetStatus()
		if re.Error() != "" {
			continue
		}
		if sr.IsActive && sr.IsLeader {
			ctrl.t.Fatalf("expected no leader, but peer %d claims leadership", i)
		}
	}
	ctrl.t.Logf("ok: no leader present, as expected")
}

// issueCommand submits cmd to peer idx and returns its status report.
func (ctrl *Controller) issueCommand(idx int, cmd []byte) StatusReport {
	sr, re := ctrl.stubs[idx].NewCommand(cmd)
	if re.Error() != "" {
		return StatusReport{}
	}
	return sr
}

// startCommit repeatedly finds a leader and submits cmd until at least
// expectedPeers commit it at a shared index, giving up after ~20s. It returns
// the commit index.
func (ctrl *Controller) startCommit(cmd []byte, expectedPeers int) int {
	ctrl.t.Logf("step: submitting %q and waiting for %d peers to commit it", cmd, expectedPeers)
	start := time.Now()
	for time.Since(start).Seconds() < 20 {
		index := -1
		for i := 0; i < ctrl.totalPeers; i++ {
			sr := ctrl.issueCommand(i, cmd)
			if sr.Index > 0 && sr.IsActive && sr.IsLeader {
				index = sr.Index
				break
			}
		}
		if index != -1 {
			inner := time.Now()
			for time.Since(inner).Seconds() < 3 {
				ct, cd := ctrl.countCommitsAtIndex(index)
				if bytes.Equal(cd, cmd) && ct >= expectedPeers {
					ctrl.t.Logf("ok: %q committed at index %d by %d peers", cmd, index, ct)
					return index
				}
				time.Sleep(electionSettleTime)
			}
		} else {
			time.Sleep(electionSettleTime)
		}
	}
	ctrl.t.Fatalf("startCommit(%q, %d) failed to reach agreement", cmd, expectedPeers)
	return -1
}

// waitForCommits waits for at least n peers to commit index, bailing out early
// (returning an empty slice) if any peer advances beyond startTerm.
func (ctrl *Controller) waitForCommits(index, n, startTerm int) []byte {
	ctrl.t.Logf("step: waiting for %d peers to commit index %d (bail if term > %d)", n, index, startTerm)
	backoff := 10
	for iter := 0; iter < 30; iter++ {
		if ct, _ := ctrl.countCommitsAtIndex(index); ct >= n {
			break
		}
		time.Sleep(time.Duration(backoff) * time.Millisecond)
		if backoff < 1000 {
			backoff *= 2
		}
		if startTerm > -1 {
			for i := 0; i < ctrl.totalPeers; i++ {
				sr, re := ctrl.stubs[i].GetStatus()
				if re.Error() != "" {
					continue
				}
				if sr.Term > startTerm {
					ctrl.t.Logf("ok: term advanced past %d at peer %d; bailing out of wait for index %d", startTerm, i, index)
					return []byte{}
				}
			}
		}
	}
	ct, cd := ctrl.countCommitsAtIndex(index)
	if ct < n {
		ctrl.t.Fatalf("only %d peers committed index %d; wanted %d", ct, index, n)
	}
	ctrl.t.Logf("ok: index %d committed by %d peers", index, ct)
	return cd
}

// getCallCount returns the CallCount reported by peer idx.
func (ctrl *Controller) getCallCount(idx int) int {
	sr, re := ctrl.stubs[idx].GetStatus()
	if re.Error() != "" {
		return 0
	}
	return sr.CallCount
}

// randIntn is a small convenience wrapper for tests that need randomness.
func (ctrl *Controller) randIntn(n int) int { return ctrl.rng.Intn(n) }
