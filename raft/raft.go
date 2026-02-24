package raft

// Raft implementation
//
// general notes:
//
// - you are welcome to use additional helper functions to handle aspects of the Raft algorithm
//   logic within the scope of a single Raft peer. you should not need to create any additional
//   remote calls between Raft peers or the Controller. if there is a desire to create additional
//   remote calls, please talk with the course staff before doing so.
//
// - Raft peers are not able to share information with each other or with the Controller in any
//   way other than through remote calls, allowing peers the potential to operate on physically
//   distinct machines
//
// - please make sure to read the Raft paper (https://raft.github.io/raft.pdf) before attempting
//   any coding for this lab. you will most likely need to refer to it many times during your
//   implementation and testing tasks, so please consult the paper for algorithm details.
//
// - each Raft peer will accept a lot of concurrent remote calls from other Raft peers and the
//   Controller, so use of concurrency controls is essential. you are expected to use such
//   controls to prevent race conditions in your implementation. the Makefile supports testing
//   both without and with go's race condition detector, and the testing system will enable the
//   race condition detector, which will cause tests to fail if any race conditions are
//   encountered.
//
// - don't forget to ask for help!

import (
	"remote"
)

// Controller sends to Raft peer at creation time. do not change.
type RaftSetupInfo struct {
	Id    int
	Addr  string
	Caddr string
}

// Raft peer must send to Controller on request. do not change.
type StatusReport struct {
	Index     int
	Term      int
	Leader    bool
	Active    bool
	CallCount int
}

// empty template for the "service interface" that specifies remote calls between Raft peers.
// it must include the two remote methods needed for the Raft algorithm.
type RaftInterface struct {
	// TODO: define the request-vote remote method, which you can name and structure however you desire
	// TODO: define the append-entries remote method, which you can name and structure however you desire
}

// complete template for the Control "service interface" that specifies remote calls from
// Controller to Raft peer. the ControlInterface is active from the moment the Raft peer is
// created until the Raft peer is no longer needed by the Controller. this interface specifies
// six remote methods that you must implement. these methods are described later in this file.
type ControlInterface struct {
	Activate        func() remote.RemoteError
	Deactivate      func() remote.RemoteError
	Terminate       func() remote.RemoteError
	GetStatus       func() (StatusReport, remote.RemoteError)
	GetCommittedCmd func(int) ([]byte, remote.RemoteError)
	NewCommand      func([]byte) (StatusReport, remote.RemoteError)
}

// TODO: you will need to define a struct that contains the parameters/variables that define
// and explain the current status of each Raft peer. it doesn't matter what you call this
// struct, and the test code doesn't really care what state it contains, so this part is up
// to you.

// the Controller calls NewRaftPeer in its own go routine to spawn a new Raft peer. the
// arguments contain everything needed for the new Raft peer to determine its own identity and
// callee addresses as well as the relevant callee address of all other Raft peers. examples
// of the RaftSetupInfo are provided in the lab description on canvas. the index parameter
// indicates the index in the slice of RaftSetupInfo structs corresponding to this new Raft
// peer, and the remaining info is for other peers.
//
// TODO: spawn a new raft peer (called in its own go routine by the Controller)
func NewRaftPeer(peerInfo []RaftSetupInfo, index int) {

	// when a new raft peer is created, its initial state should be populated into the
	// corresponding struct entries, it should create two Callee stubs and N-1 Caller stubs,
	// where N is the Raft group size. the Callee stub for the ControlInterface must be
	// started immediately, so the Raft peer can accept commands from the Controller, but
	// the Callee stub for the RemoteInterface should not be started until the Controller
	// issues the remote call telling the peer to start.
	//
	// the CalleeStubs using the RemoteInterface and ControlInterface should bind to the
	// addresses in the Addr and Caddr entry in peerInfo[index], respectively. each caller
	// stub created using remote.CallerStubCreator should be used to send Raft algorithm
	// commands to a different Raft peer in the group. the addresses provided by the
	// Controller are guaranteed to be unique (i.e., no peers will have the same ID or use
	// the same address).

}

//// method implementations for the ControlInterface

// * Activate -- this remote method is used exclusively by the Controller whenever it needs
// to start the underlying server in the Raft peer and allow it to receive calls from other
// Raft peers. this is used to emulate connecting a new peer to the network or recovery of a
// previously failed peer. when this method is called, the Raft peer should do whatever is
// necessary to enable its remote.CallerStub interface to support remote calls from other Raft
// peers as soon as the method returns (i.e., if it takes time for the remote.CallerStub to
// start, this method should not return until that happens). the method should not otherwise
// block the Controller.
//
// TODO: implement the Activate remote method

// * Deactivate -- this remote method performs the "inverse" operation to Activate, namely to
// stop the underlying server in the Raft peer to emulate disconnection / failure of the Raft
// peer. when called, the Raft peer should disable only the stub serving the RaftInterface,
// causing any remote calls to this Raft peer to fail due to connection error. when
// deactivated, a Raft peer should not make or receive any remote calls on the stub using the
// RaftInterface, and any execution of the Raft protocol should effectively pause. however,
// local state should be maintained and the stub using the ControlInterface should continue to
// operate without disruption. if a Raft node was the leader when it was deactivated, it
// should still believe it is the leader when it reactivates.
//
// TODO: implement the Deactivate remote method

// * Terminate -- this remote method is used exclusively by the Controller to permanently
// cease operation of the Raft peer. this is called at the end of each test when the Raft peer
// is no longer needed, and it allows the Raft peer to completely terminate all services and
// delete all relevant state.
//
// TODO: implement the Terminate remote method

// * GetStatus -- this remote method is used exclusively by the Controller. this method takes
// no arguments and is essentially a "getter" for the state of the Raft peer, including the
// Raft peer's current term, current last log index, role in the Raft algorithm,
// active/non-active state, and total number of remote calls handled since starting. the
// method returns a StatusReport as defined above.
//
// TODO: implement the GetStatus remote method

// * GetCommittedCmd -- this remote method is used exclusively by the Controller. this method
// provides an input argument `index`. if the Raft peer has a log entry at the given `index`,
// and that log entry has been committed (per the Raft algorithm), then the command stored in
// the log entry should be returned to the Controller. otherwise, the Raft peer should return
// the nil value of the command type to indicate that no committed log entry exists at that
// index.
//
// TODO: implement the GetCommittedCmd remote method

// * NewCommand -- this remote method is used exclusively by the Controller. this method
// emulates submission of a new command by a Raft client to this Raft peer, which should be
// handled and processed according to the rules of the Raft algorithm. in particular, the Raft
// peer should accept the command only if it is currently active and believes it is the leader.
// regardless of whether the command is accepted and processed, the Raft peer should return a
// StatusReport reflecting the status of the Raft peer after the new command message was
// received.
//
// TODO: implement the NewCommand remote method

//// method implementations for the RaftInterface

// * RequestVote -- this remote method is called (only) by other Raft peers and should operate
// according to the description in the Raft paper.
//
// TODO: implement the RequestVote remote method (which you can name/structure as desired)

// * AppendEntries -- this remote method is called (only) by other Raft peers and should
// operate according to the description in the Raft paper.
//
// TODO: implement the AppendEntries remote method (which you can name/structure as desired)
