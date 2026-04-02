package raft

// this is a subset of the raft_test.go file that only includes tests for the Checkpoint

// Raft test suite, using Controller to mimic client interaction and facilitate testing
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.

import (
	"bytes"
	"fmt"
	"math/rand"
	"remote"
	"strconv"
	"sync"
	"testing"
	"time"
)

const ELECTION_TIMEOUT time.Duration = time.Second

type Controller struct {
	mu         sync.Mutex          // mutex control for Controller
	t          *testing.T          // allow Controller to affect test
	totalPeers int                 // total number of Raft peers in group
	basePort   int                 // starting CalleeStub port number, +1 for each subsequent interface
	stubs      []*ControlInterface // stubs for communicating with Raft peers
	rng        *rand.Rand          // time-seeded rng
}

// create a Controller to facilitate tests
// totalPeers -- number of Raft peers to spawn
func NewController(t *testing.T, totalPeers int) *Controller {
	ctrl := &Controller{}
	ctrl.t = t
	ctrl.totalPeers = totalPeers
	ctrl.stubs = make([]*ControlInterface, totalPeers)
	ctrl.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	ctrl.basePort = 20000 + ctrl.rng.Intn(10000)

	// generate unique IDs and sequential addresses for all Raft peers
	ids := make(map[int]bool)
	info := make([]RaftSetupInfo, totalPeers)
	var idx int = 0
	for idx < totalPeers {
		rnd := ctrl.rng.Intn(65500)
		if !ids[rnd] {
			ids[rnd] = true
			info[idx] = RaftSetupInfo{
				Id:    rnd,
				Addr:  "localhost:" + strconv.Itoa(ctrl.basePort+2*idx),
				Caddr: "localhost:" + strconv.Itoa(ctrl.basePort+2*idx+1),
			}
			idx++
		}
	}

	for i, pinfo := range info {
		go NewRaftPeer(info, i)
		ctrl.stubs[i] = &ControlInterface{}
		err := remote.CallerStubCreator(ctrl.stubs[i], pinfo.Caddr, false, false)
		if err != nil {
			ctrl.t.Fatalf("Cannot create Controller stub for Raft peer: %s", err.Error())
		}
	}

	// register a cleanup function to terminate all callee stubs
	t.Cleanup(func() {
		for i := range ctrl.totalPeers {
			ctrl.stubs[i].Terminate()
		}
	})

	// wait to spin up all peers, then activate them all
	time.Sleep(3 * time.Second)
	for i := range totalPeers {
		ctrl.connect(i)
	}

	return ctrl
}

// (re)connect a Raft peer by (re)starting its underlying CalleeStub
func (ctrl *Controller) connect(i int) {
	for {
		re := ctrl.stubs[i].Activate()
		if re.Error() == "" {
			break
		}
		fmt.Println("warning: remote call Activate failed -- " + re.Error())
	}
}

// disconnect a Raft peer by stopping its underlying CalleeStub
func (ctrl *Controller) disconnect(i int) {
	for {
		re := ctrl.stubs[i].Deactivate()
		if re.Error() == "" {
			break
		}
		fmt.Println("warning: remote call Deactivate failed -- " + re.Error())
	}
}

// count how many Raft peers have committed the same command at index idx
func (ctrl *Controller) countCommitsAtIndex(idx int) (int, []byte) {
	var count int = 0
	var cmd []byte

	for i := range ctrl.totalPeers {
		logcmd, re := ctrl.stubs[i].GetCommittedCmd(idx)
		if re.Error() != "" {
			fmt.Println("warning: remote call GetCommittedCmd failed -- " + re.Error())
			continue
		}
		if !bytes.Equal(logcmd, nil) {
			if count > 0 && !bytes.Equal(cmd, logcmd) {
				ctrl.t.Fatalf("Committed values do not match: index %d, cmds %v %v", idx, cmd, logcmd)
			}
			count++
			cmd = bytes.Clone(logcmd)
		}
	}
	return count, cmd
}

// find the leader, ensuring there is exactly one of them
func (ctrl *Controller) findLeader() int {
	lastTermWithLeader := -1

	maxIters := 10

	for range maxIters {
		time.Sleep(ELECTION_TIMEOUT)

		leaders := make(map[int][]int)
		for i := range ctrl.totalPeers {
			sr, re := ctrl.stubs[i].GetStatus()
			if re.Error() != "" {
				fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
				continue
			}
			if sr.IsActive && sr.IsLeader {
				leaders[sr.Term] = append(leaders[sr.Term], i)
			}
		}
		lastTermWithLeader = -1
		for trm, leads := range leaders {
			if len(leads) > 1 {
				ctrl.t.Fatalf("found %d leaders in term %d", len(leads), trm)
			}
			if trm > lastTermWithLeader {
				lastTermWithLeader = trm
			}
		}

		if lastTermWithLeader != -1 {
			return leaders[lastTermWithLeader][0]
		}
	}
	ctrl.t.Fatalf("no leader found!")
	return -1
}

// get the term that everyone who responds agrees on, or fail if there is disagreement
func (ctrl *Controller) getTerm() int {
	term := -1
	for i := range ctrl.totalPeers {
		sr, re := ctrl.stubs[i].GetStatus()
		if re.Error() != "" {
			fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
			continue
		}
		if sr.IsActive && sr.Term > 0 {
			if term == -1 {
				term = sr.Term
			} else if term != sr.Term {
				ctrl.t.Fatalf("Peers disagree about term!")
			}
		}
	}
	return term
}

// make sure there is no leader, or fail if one is found
func (ctrl *Controller) ensureNoLeader() {
	time.Sleep(ELECTION_TIMEOUT)
	for i := range ctrl.totalPeers {
		sr, re := ctrl.stubs[i].GetStatus()
		if re.Error() != "" {
			fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
			continue
		}
		if sr.IsActive && sr.IsLeader {
			ctrl.t.Fatalf("Expected no leader, but %d claims to be leader", i)
		}
	}
}

// ---------------- test cases begin here ---------------- //

// test initial setup and use of remote interfaces
func TestCheckpoint_Setup(t *testing.T) {
	totalPeers := 1

	fmt.Print("Checking controller creation ... ")
	ctrl := NewController(t, totalPeers)
	fmt.Println("ok")

	fmt.Print("Checking controller can get peer status ... ")
	_, re := ctrl.stubs[0].GetStatus()
	if re.Error() != "" {
		t.Fatalf("remote error getting status from Raft peer: %s", re.Error())
	}
	fmt.Println("ok")
}

// test initial election process for new Raft peers, specifically:
// -- is a leader elected?
// -- if no failure, does leadership change?
// -- after multiple timeout durations, is there still a leader?
func TestCheckpoint_Election(t *testing.T) {
	totalPeers := 3
	ctrl := NewController(t, totalPeers)

	// is a leader elected?
	fmt.Print("Checking for leader ... ")
	ctrl.findLeader()
	fmt.Println("ok")

	fmt.Print("Checking for term agreement ... ")
	term1 := ctrl.getTerm()
	if term1 < 1 {
		t.Fatalf("term is %d, but should be at least 1", term1)
	}
	fmt.Println("ok")

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * ELECTION_TIMEOUT)
	term2 := ctrl.getTerm()
	if term1 != term2 {
		fmt.Println("warning: term changed with no failures")
	}

	// there should still be a leader.
	fmt.Print("Checking there's still a leader ... ")
	ctrl.findLeader()
	fmt.Println("ok")
}

// test re-election process when failure is involved, specifically:
// -- is a leader elected?
// -- if it disconnects, is a new one elected?
// -- if the old leader rejoins, does it affect the new leader?
// -- if > 1/2 peers disconnect, is there no leader?
// -- if one reconnects, is there a leader?
// -- if another reconnects, is there still a leader?
func TestCheckpoint_FailElection(t *testing.T) {
	totalPeers := 3
	ctrl := NewController(t, totalPeers)

	fmt.Print("Checking for leader ... ")
	leader1 := ctrl.findLeader()
	fmt.Println("ok")

	// if the leader disconnects, a new one should be elected.
	fmt.Print("Checking for new leader after disconnecting previous leader ... ")
	ctrl.disconnect(leader1)
	ctrl.findLeader()
	fmt.Println("ok")

	// if the old leader rejoins, that shouldn't disturb the current leader.
	fmt.Print("Checking for leader after previous leader reconnects ... ")
	ctrl.connect(leader1)
	leader2 := ctrl.findLeader()
	fmt.Println("ok")

	// if there's no quorum, no leader should be elected.
	ctrl.disconnect(leader2)
	ctrl.disconnect((leader2 + 1) % totalPeers)
	fmt.Print("Checking for no leader after majority disconnects ... ")
	ctrl.ensureNoLeader()
	fmt.Println("ok")

	// if a quorum arises, it should elect a leader.
	fmt.Print("Checking for leader after follower reconnection ... ")
	ctrl.connect((leader2 + 1) % totalPeers)
	ctrl.findLeader()
	fmt.Println("ok")

	// re-join of previous leader shouldn't disturb the newly elected leader.
	fmt.Print("Checking for leader after previous leader reconnection ... ")
	ctrl.connect(leader2)
	ctrl.findLeader()
	fmt.Println("ok")
}

// test sequence of multiple rounds of elections and failures, making sure
// that a leader exists in every round, either the same leader as previous
// or a newly elected one due to failure of the previous leader
func TestCheckpoint_SequentialElections(t *testing.T) {
	totalPeers := 7
	iters := 10
	ctrl := NewController(t, totalPeers)

	ctrl.findLeader()

	fmt.Print("Checking multiple sequential elections ")
	for range iters {
		// disconnect minority of peer group
		p1 := ctrl.rng.Intn(totalPeers)
		ctrl.disconnect(p1)
		p2 := ctrl.rng.Intn(totalPeers)
		ctrl.disconnect(p2)
		p3 := ctrl.rng.Intn(totalPeers)
		ctrl.disconnect(p3)

		// if a new election was needed, did it succeed?
		ctrl.findLeader()

		fmt.Print(".")
		ctrl.connect(p1)
		ctrl.connect(p2)
		ctrl.connect(p3)
	}

	// do we have a final leader?
	ctrl.findLeader()
	fmt.Println(" ok")
}

// test simple case of agreement among Raft peers with no failure
func TestCheckpoint_Commit(t *testing.T) {
	totalPeers := 5
	maxIters := 5
	ctrl := NewController(t, totalPeers)
	ldr := ctrl.findLeader()

	for i := range maxIters {
		fmt.Printf("Checking for correct commit %d ... ", i+1)
		cmd := []byte("commit me " + strconv.Itoa(i+1))

		sr, re := ctrl.stubs[ldr].NewCommand(cmd)
		if re.Error() != "" || !sr.IsLeader {
			t.Fatalf("Leader failed to accept a command")
		}
		time.Sleep(250 * time.Millisecond)
		ct, cd := ctrl.countCommitsAtIndex(sr.Index)
		if !bytes.Equal(cd, cmd) || ct < totalPeers {
			t.Fatalf("Simple commit failed: %d of %d peers committed", ct, totalPeers)
		}

		fmt.Println("ok")
	}
}
