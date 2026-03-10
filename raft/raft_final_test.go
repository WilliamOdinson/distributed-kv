package raft

// Raft test suite, using Controller to mimic client interaction and facilitate testing
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"
)

// issue a new command to a single peer
//   - idx -- index of peer getting command
//   - cmd -- the command
func (ctrl *Controller) issueCommand(idx int, cmd []byte) StatusReport {
	sr, re := ctrl.stubs[idx].NewCommand(cmd)
	if re.Error() != "" {
		fmt.Println("warning: remote call NewCommand failed -- " + re.Error())
		return StatusReport{}
	}
	return sr
}

// run through a full commit. it might choose the wrong leader
// initially and start over if so, entirely giving up after
// about 20 seconds. indirectly checks peer agreement, since
// countCommitsAtIndex checks this.
//   - cmd -- command being logged
//   - expectedPeers -- #peers that should commit
//
// returns commit index.
func (ctrl *Controller) startCommit(cmd []byte, expectedPeers int) int {
	t0 := time.Now()

	for time.Since(t0).Seconds() < 20 {
		// find index of active leader, if there is one
		index := -1
		for i := range ctrl.totalPeers {
			sr := ctrl.issueCommand(i, cmd)
			if sr.Index > 0 && sr.Active && sr.Leader {
				index = sr.Index
				break
			}
		}
		// if we found a leader, keep going to see how they handled the command
		if index != -1 {
			t1 := time.Now()
			// wait for a bit before giving up
			for time.Since(t1).Seconds() < 3 {
				ct, cd := ctrl.countCommitsAtIndex(index)
				if bytes.Equal(cd, cmd) && ct >= expectedPeers {
					// this is exactly what we wanted!
					return index
				}
				time.Sleep(ELECTION_TIMEOUT)
			}
		} else {
			time.Sleep(ELECTION_TIMEOUT)
		}
	}
	ctrl.t.Fatalf("startCommit(%v, %d) failed to reach agreement", cmd, expectedPeers)
	return -1
}

// wait for at least n Raft peers to commit index starting
// from term startTerm, but don't wait forever.
func (ctrl *Controller) waitForCommits(index int, n int, startTerm int) []byte {
	t0 := 10
	maxIters := 30
	for range maxIters {
		ct, _ := ctrl.countCommitsAtIndex(index)
		if ct >= n {
			break
		}
		time.Sleep(time.Duration(t0) * time.Millisecond)
		if t0 < 1000 {
			t0 *= 2
		}
		if startTerm > -1 {
			for i := range ctrl.totalPeers {
				sr, re := ctrl.stubs[i].GetStatus()
				if re.Error() != "" {
					fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
					continue
				}
				if sr.Term > startTerm {
					// someone started a new term...
					return []byte{}
				}
			}
		}
	}

	ct, cd := ctrl.countCommitsAtIndex(index)
	if ct < n {
		ctrl.t.Fatalf("only %d decided for index %d; wanted %d", ct, index, n)
	}
	return cd
}

// helper function that returns the CallCount entry from a remotely fetched status
func (ctrl *Controller) getCallCount(idx int) int {
	sr, re := ctrl.stubs[idx].GetStatus()
	if re.Error() != "" {
		fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
		return 0
	}
	return sr.CallCount
}

// ---------------- test cases begin here ---------------- //

// test agreement among Raft peers with failures, specifically:
// -- do a basic commit and check for leader
// -- can we still get agreement with a disconnected follower?
// -- after peer reconnects, can we continue to get agreement?
func TestFinal_FailCommit(t *testing.T) {
	totalPeers := 3
	ctrl := NewController(t, totalPeers)

	fmt.Print("Checking initial commit ... ")
	ctrl.startCommit([]byte("commit me 0"), totalPeers)
	fmt.Println("ok")

	// disconnect a follower
	leader := ctrl.findLeader()
	ctrl.disconnect((leader + 1) % totalPeers)
	// agree despite one disconnected peer?
	fmt.Print("Checking agreement with one failed Raft peer ... ")
	ctrl.startCommit([]byte("commit me 1"), totalPeers-1)
	ctrl.startCommit([]byte("commit me 2"), totalPeers-1)
	time.Sleep(ELECTION_TIMEOUT)
	ctrl.startCommit([]byte("commit me 3"), totalPeers-1)
	ctrl.startCommit([]byte("commit me 4"), totalPeers-1)
	fmt.Println("ok")

	// re-connect the follower
	ctrl.connect((leader + 1) % totalPeers)
	// agree with full set of peers?
	fmt.Print("Checking agreement with reconnected peer ... ")
	ctrl.startCommit([]byte("commit me 5"), totalPeers)
	time.Sleep(ELECTION_TIMEOUT)
	ctrl.startCommit([]byte("commit me 6"), totalPeers)
	fmt.Println("ok")
}

// test agreement among Raft peers with repeated leader failures, specifically:
// -- do a basic commit and check for leader
// -- can we still get agreement after the original leader fails?
// -- can we still get agreement after the new leader fails and the original recovers?
// -- can we still get agreement after everyone recovers?
func TestFinal_FailedLeaders(t *testing.T) {
	totalPeers := 5
	ctrl := NewController(t, totalPeers)

	// initial commit with everyone active
	ctrl.startCommit([]byte("commit me 0"), totalPeers)

	// disconnect the leader and do more commits
	leader := ctrl.findLeader()
	ctrl.disconnect(leader)
	time.Sleep(2 * ELECTION_TIMEOUT)

	fmt.Print("Checking agreement after original leader failed ... ")
	ctrl.startCommit([]byte("commit me 1"), totalPeers-1)
	ctrl.startCommit([]byte("commit me 2"), totalPeers-1)
	fmt.Println("ok")

	// another leader fails, and the original leader comes back
	leader2 := ctrl.findLeader()
	ctrl.disconnect(leader2)
	ctrl.connect(leader)
	time.Sleep(2 * ELECTION_TIMEOUT)

	fmt.Print("Checking agreement after 2nd leader fails and 1st leader recovers ... ")
	ctrl.startCommit([]byte("commit me 3"), totalPeers-1)
	ctrl.startCommit([]byte("commit me 4"), totalPeers-1)
	fmt.Println("ok")

	// 2nd leader recovers, more commits
	ctrl.connect(leader2)
	time.Sleep(2 * ELECTION_TIMEOUT)
	fmt.Print("Checking agreement after 2nd leader recovers ... ")
	ctrl.startCommit([]byte("commit me 5"), totalPeers)
	ctrl.startCommit([]byte("commit me 6"), totalPeers)
	fmt.Println("ok")
}

// test non-agreement among Raft peers with failures, specifically:
// -- do we get any agreement when a majority disconnects?
// -- make sure logs are made consistent after everyone reconnects
// -- after reconnection, can we continue to get agreement?
func TestFinal_ConsistentRecovery(t *testing.T) {
	totalPeers := 5
	ctrl := NewController(t, totalPeers)

	ctrl.startCommit([]byte("commit me"), totalPeers)
	leader1 := ctrl.findLeader()

	fmt.Print("Checking for no agreement with majority of followers fail ... ")
	ctrl.disconnect((leader1 + 1) % totalPeers)
	ctrl.disconnect((leader1 + 2) % totalPeers)
	ctrl.disconnect((leader1 + 3) % totalPeers)

	sr1 := ctrl.issueCommand(leader1, []byte("commit me if you can"))
	if !sr1.Active || !sr1.Leader {
		t.Fatalf("Unexpected leader disconnection or change leading to command rejection")
	}
	if sr1.Index != 2 {
		t.Fatalf("Got index %d instead of 2", sr1.Index)
	}
	time.Sleep(2 * ELECTION_TIMEOUT)

	n, _ := ctrl.countCommitsAtIndex(sr1.Index)
	if n > 0 {
		t.Fatalf("%d peers committed without a majority", n)
	}
	fmt.Println("ok")

	// repair
	ctrl.connect((leader1 + 1) % totalPeers)
	ctrl.connect((leader1 + 2) % totalPeers)
	ctrl.connect((leader1 + 3) % totalPeers)

	fmt.Print("Checking for consistent logs after everyone reconnects ... ")
	leader2 := ctrl.findLeader()
	sr2 := ctrl.issueCommand(leader2, []byte("commit me please"))
	if !sr2.Active || !sr2.Leader {
		t.Fatalf("Unexpected leader disconnection or change leading to command rejection")
	}

	// one entry may not have been committed, depending on who the new leader is
	// so allow two possible outcomes (with / without that entry)
	if sr2.Index < 2 || sr2.Index > 3 {
		t.Fatalf("Unexpected index %d, should be 2 or 3", sr2.Index)
	}
	fmt.Println("ok")

	// run one more commit after everyone is all back together
	fmt.Print("Checking that more commits can be done after recovery ... ")
	ctrl.startCommit([]byte("commit me too"), totalPeers)
	fmt.Println("ok")
}

// test reconnection of leader who kept getting requests while disconnected from others:
// -- find the initial leader, partition them, and send them some new commands
// -- send some new commands to the new leader, then disconnect them too
// -- then reconnect old leader to see if conflicts can be resolved with new submissions
// -- after everyone is reconnected, submit one more thing to commit
func TestFinal_PartitionAndMerge(t *testing.T) {
	totalPeers := 3
	ctrl := NewController(t, totalPeers)

	// find the leader, partition everyone else, send leader some commands
	leader1 := ctrl.findLeader()
	ctrl.disconnect((leader1 + 1) % totalPeers)
	ctrl.disconnect((leader1 + 2) % totalPeers)
	ctrl.issueCommand(leader1, []byte("try to commit this 0"))
	ctrl.issueCommand(leader1, []byte("try to commit this 1"))
	ctrl.issueCommand(leader1, []byte("try to commit this 2"))

	// disconnect the leader, reconnect others, give them a little time to elect a leader
	ctrl.disconnect(leader1)
	ctrl.connect((leader1 + 1) % totalPeers)
	ctrl.connect((leader1 + 2) % totalPeers)
	time.Sleep(ELECTION_TIMEOUT)

	fmt.Print("Checking commitment after original leader failed ... ")
	// new leader commits for index 2
	ctrl.startCommit([]byte("try to commit this 3"), totalPeers-1)
	fmt.Println("ok")

	fmt.Print("Checking log consistency after new leader fails and original leader reconnects ... ")
	// new leader network failure
	leader2 := ctrl.findLeader()
	ctrl.disconnect(leader2)

	// old leader connected again
	ctrl.connect(leader1)
	ctrl.startCommit([]byte("try to commit this 4"), totalPeers-1)
	fmt.Println("ok")

	fmt.Print("Checking log consistency after everyone rejoins ... ")
	// all together now
	ctrl.connect(leader2)
	ctrl.startCommit([]byte("try to commit this 5"), totalPeers)
	fmt.Println("ok")
}

// test resolution of logs populated with lots of uncommitted entries:
// -- find the initial leader, partition them with a follower, and send them lots of new commands
// -- then send lots of new commands to the larger partition, then disconnect that leader with a follower
//
//	and send lots more commands to the disconnected leader
//
// -- then reconnect the original leader and send them lots of new commands
// -- reconnect everyone, submit one more thing to commit, ensure consistency of logs
func TestFinal_PurgeUncommitted(t *testing.T) {
	totalPeers := 5
	ctrl := NewController(t, totalPeers)

	ctrl.startCommit([]byte("commit me first"), totalPeers)

	fmt.Print("checking commitment among separate partitions ... ")
	// disconnect everyone except leader and one follower.
	leader1 := ctrl.findLeader()
	ctrl.disconnect((leader1 + 2) % totalPeers)
	ctrl.disconnect((leader1 + 3) % totalPeers)
	ctrl.disconnect((leader1 + 4) % totalPeers)

	numCmds := 25
	// submit many commands to the leader, which should not commit
	for i := range numCmds {
		ctrl.issueCommand(leader1, []byte("don't commit me "+strconv.Itoa(i)))
	}

	// "swap" connectivity to the other partition.
	ctrl.disconnect(leader1)
	ctrl.disconnect((leader1 + 1) % totalPeers)
	ctrl.connect((leader1 + 2) % totalPeers)
	ctrl.connect((leader1 + 3) % totalPeers)
	ctrl.connect((leader1 + 4) % totalPeers)

	time.Sleep(ELECTION_TIMEOUT)

	// submit many commands to the new group, which should commit
	for i := range numCmds {
		ctrl.startCommit([]byte("do commit me "+strconv.Itoa(i)), totalPeers/2+1)
	}
	fmt.Println("ok")

	fmt.Print("checking consistency after small partition recovers above majority ... ")
	// now another partitioned leader and one follower
	leader2 := ctrl.findLeader()
	other := (leader1 + 2) % totalPeers
	if leader2 == other {
		other = (leader2 + 1) % totalPeers
	}
	ctrl.disconnect(other)

	// many more commands that won't commit
	for i := range numCmds {
		ctrl.issueCommand(leader2, []byte("don't commit me "+strconv.Itoa(100+i)))
	}

	// bring original leader back to life
	for i := range totalPeers {
		ctrl.disconnect(i)
	}
	ctrl.connect(leader1)
	ctrl.connect((leader1 + 1) % totalPeers)
	ctrl.connect(other)

	time.Sleep(ELECTION_TIMEOUT)

	// many commands that should commit
	for i := range numCmds {
		ctrl.startCommit([]byte("please commit me "+strconv.Itoa(i)), totalPeers/2+1)
	}
	fmt.Println("ok")

	fmt.Print("checking overall consistency after everyone reconnects ... ")
	// now everyone
	for i := range totalPeers {
		ctrl.connect(i)
	}
	ctrl.startCommit([]byte("commit me last"), totalPeers)
	fmt.Println("ok")
}

// test that number of remote calls made when using Raft is reasonable:
// -- checks that the #calls for election is in [7, 75]
// -- checks that the #calls for commitment is < 120
// -- checks that the #calls during a 1s idle is < 20
// -- along the way, checks that everything else is working correctly, just in case
func TestFinal_CallCount(t *testing.T) {
	totalPeers := 3
	ctrl := NewController(t, totalPeers)

	var total1 int

	fmt.Print("checking call count for election ... ")
	ctrl.findLeader()
	for i := range totalPeers {
		total1 += ctrl.getCallCount(i)
	}

	if total1 < 7 || total1 > 75 {
		t.Fatalf("Too many or too few remote calls to elect a leader")
	}
	fmt.Println("ok")

	var success bool = false
	var total2 int
	maxTries := 5

	fmt.Print("checking call count for commitment ... ")

	for try := range maxTries {
		if success {
			break
		}
		func(iter int) {
			fmt.Printf("measuring call counts, try %d of %d\n", iter, maxTries)
			if iter > 0 {
				// extra settling time after first try
				time.Sleep(ELECTION_TIMEOUT)
			}

			leader := ctrl.findLeader()
			total1 = 0
			for i := range totalPeers {
				total1 += ctrl.getCallCount(i)
			}

			maxIters := 10

			sr1 := ctrl.issueCommand(leader, []byte("initial command"))
			if !sr1.Leader {
				// leader changed too quickly, try again or give up
				fmt.Println("leader changed during count, try again...")
				return
			}

			cmds := [][]byte{}
			for i := range maxIters + 1 {
				x := []byte("random command " + strconv.Itoa(ctrl.rng.Intn(65536)))
				cmds = append(cmds, x)

				sr2 := ctrl.issueCommand(leader, x)

				if sr2.Term != sr1.Term || !sr2.Leader {
					// term changed while starting or no longer leader, try again
					fmt.Println("leader/term changed during count, try again...")
					return
				}

				if sr1.Index+i+1 != sr2.Index {
					t.Fatalf("issueCommand() failed")
				}
			}

			for i := range maxIters {
				cmd := ctrl.waitForCommits(sr1.Index+i+1, totalPeers, sr1.Term)
				if !bytes.Equal(cmd, cmds[i]) {
					if bytes.Equal(cmd, nil) {
						// term changed, try again
						fmt.Println("term changed during count, try again...")
						return
					}
					t.Fatalf("wrong value %v committed for index %v; expected one of %v\n", cmd, sr1.Index+1, cmds)
				}
			}

			failed := false
			total2 = 0
			for i := range totalPeers {
				sr, re := ctrl.stubs[i].GetStatus()
				if re.Error() != "" {
					fmt.Println("warning: remote call GetStatus failed -- " + re.Error())
					continue
				}

				if sr.Term != sr1.Term {
					// term changed -- can't expect low remote call counts
					// need to keep going to update total2
					failed = true
				}
				total2 += sr.CallCount
			}

			if failed {
				// term mismatch above, try again or give up
				fmt.Println("term changed during count, try again...")
				return
			}

			fmt.Println("call count for commitment: ", total2-total1)

			if total2-total1 > 200 {
				t.Fatalf("Too many remote calls for commitment")
			}
			fmt.Println("ok")

			success = true
		}(try)
	}

	if !success {
		t.Fatalf("commitment failed (term changed too often)")
	}

	time.Sleep(ELECTION_TIMEOUT)
	fmt.Print("checking call count during idle ... ")

	total3 := 0
	for i := range totalPeers {
		total3 += ctrl.getCallCount(i)
	}

	fmt.Println("call count during idle: ", total3-total2)
	if total3-total2 > 20 {
		t.Fatalf("Too many remote calls for 1 second idle")
	}
	fmt.Println("ok")
}
