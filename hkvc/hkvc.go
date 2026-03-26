package hkvc

import (
	"net/http"
	"raft"
)

// Hierarchical Key-Value Cluster (HKVC)
//
// General notes:
//
// - More accurately --> a strongly consistent, in-memory, sharded, hierarchical key-value cluster
//
// - Each cluster participant maintains key-value data indexed by key but also arranged within
//   and indexed by a hierarchical directory structure, which can be interpreted as having a
//   separate key-value store embedded in each directory in the storage system.
//
// - The key-value store in each directory structure will be managed by a Raft group to ensure that
//   the key-value data is strongly consistent across the storage directories of the peers within
//   that group.  Each Raft group will collectively manage multiple directories, which can be
//   dynamically created and deleted by clients.
//
// - You will need to extend your Raft implementation with an `Apply` ability, which will be the
//   point of interaction between your Raft group and the key-value data.  Your implementation must
//   ensure that any response to a client command accounts for commitment and application of all
//   other client commands received previously.  There are a few important notes about this in the
//   Raft paper that apply even without persistence and snapshots (which you still don't need to
//   implement).
//
// - A single HKVC participant can be a member of multiple Raft groups, so it may host multiple
//   Raft instances running in parallel.  To guarantee that conflicts do not arise among Raft
//   instances, directories must be strictly partitioned across Raft groups.  All interactions
//   between Raft peers in the same group must use the Raft implementation and the corresponding
//   stubs that you created in the previous lab.
//
// - Each HKVC participant must also expose an HTTP interface with a collection of API endpoints.
//   These are exclusively for use by external clients, who interact with the HKVC as a key-value
//   data store.  Many internal details of the HKVC system functionality will be opaque to the
//   client, which means that the HTTP endpoint handlers are responsible for translating between
//   client commands and Raft group interactions used to manage the distributed storage system.
//
// - Our test implementation still uses a controller with its own remote interface, but it is
//   no longer interacting with a single Raft peer, so you can strip all Controller functionality
//   from your version of the Raft interface used in this lab (if desired).  The controller used
//   for the HKVC participant is slightly different (and has a different name, to avoid conflict),
//   since it is no longer mimicking client input or deeply querying the internals of the Raft
//   state. The details can be found in the accompanying test code and below.
//
// - As with previous labs, cluster members are not allowed to share information with each other,
//   with the HKVCController, or with external clients in any way other than through the corresponding
//   interfaces. Your implementation should be able to support a deployment of each HKVC participant
//   and each client on a physically distinct machine.
//
// - You are always welcome to use additional helper functions, to separate your implementation
//   into multiple files (or even multiple packages), as long as you are not violating the above
//   or any other requirement of the lab.
//
// - Don't forget to ask for help!

// TODO: define a struct for the HKVC participant state.

// The HKVCController calls NewHKVCParticipant in its own go routine, containing everything
// needed for the new HKVC participant to configure and launch itself.
func NewHKVCParticipant(pInfo []HKVCSetupInfo, index int, groups map[int][]int) {

	p := &HKVCParticipant{
		uid:          pInfo[index].Id,
		isActive:     false,
		isTerminated: false,

		root: &directory{
			name:    "/",
			subDirs: make(map[string]*directory),
			kvPairs: make(map[string]*kvPair),
		},

		mux: http.NewServeMux(),

		raftPeers: make(map[int]*raft.RaftPeer),
	}

	p.mux.HandleFunc("/list", p.handleList)
	p.mux.HandleFunc("/get_metadata", p.handleGetMetadata)
	p.mux.HandleFunc("/get", p.handleGet)
	p.mux.HandleFunc("/set", p.handleSet)
	p.mux.HandleFunc("/create", p.handleCreate)
	p.mux.HandleFunc("/delete", p.handleDelete)

	// * populate initial state
	// * create all needed Callee and Caller stubs for internal communication
	// * create client interface for external communication
	// * start stub for HKVCControlInterface immediately, wait on others

}

//// method implementations for the HKVCControlInterface

// * Activate -- this remote method is used exclusively by the HKVCController whenever it needs
// to start the Raft peer and client interface contained within an HKVC participant, allowing it to
// interact with other participants and external clients, respectively.  the purpose of this method
// is similar to the method of the same name from the previous lab.
//
// TODO: implement the Activate remote method

// * Deactivate -- this remote method performs the "inverse" operation to Activate, namely to stop
// the Raft peer and client interface contained within the HKVC participant and pausing its interaction
// with other participants and external clients. the purpose of this method is similar to the method
// of the same name from the previous lab.
//
// TODO: implement the Deactivate remote method

// * Terminate -- this remote method is used exclusively by the HKVCController to permanently cease operation
// of the HKVC participant, similar to the method of the same name in the previous lab.
//
// TODO: implement the Terminate remote method

// * GetStatus -- this remote method is used exclusively by the HKVCController to get the activation status and
// list of Raft roles for all of the Raft groups that this HKVC participant is in.  the method returns a
// HKVCStatusReport as defined above.
//
// TODO: implement the GetStatus remote method
