package hkvc

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

import (
	"raft"   // please do not change this, but delete if not needed
	"remote" // please do not change this
)

//
// message request/response types, including json tags
//
// these are used by the test code, so don't change them!
//

// DirectoryRequest with a directory path for `/list` request
type DirectoryRequest struct {
	Directory string `json:"directory"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyRequest with a directory path and key for `/get_metadata`, `/get`, `/create`, and `/delete` requests
type KeyRequest struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyValueMessage with a directory path, key, and value for `/get` response and `/set` request
type KeyValueMessage struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// ListResponse with a directory path and list of names (keys or subdirectories) for `/list` response
type ListResponse struct {
	Directory string   `json:"directory"`
	List      []string `json:"list"`
	ClientID  string   `json:"client_id"`
}

// KeySuccessResponse with a directory path, key, and boolean success for `/set`, `/create`, and `/delete` responses
type KeySuccessResponse struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Success   bool   `json:"success"`
	ClientID  string `json:"client_id"`
}

// MetadataResponse with a directory path, key, and collection of associated metadata for `/get_metadata` response
type MetadataResponse struct {
	Directory   string   `json:"directory"`
	Key         string   `json:"key"`
	IsDirectory bool     `json:"is_directory"`
	Size        int      `json:"size"`
	Version     uint64   `json:"version"`
	PAddrList   []string `json:"p_addr_list"`
	LeaderIdx   int      `json:"leader_index"`
	Tags        []string `json:"tags"`
	ClientID    string   `json:"client_id"`
}

// HKVCErrorResponse with one of several defined error types and a description
type HKVCErrorResponse struct {
	ErrorType string `json:"error_type"`
	ErrorInfo string `json:"error_info"`
	ClientID  string `json:"client_id"`
}

// constants representing different ErrorType values
const (
	InvalidError       string = "HKVCInvalidRequestError"
	DirNotFoundError   string = "HKVCDirectoryNotFoundError"
	KeyNotFoundError   string = "HKVCKeyNotFoundError"
	ConflictKeyError   string = "HKVCConflictExistingKeyError"
	ConflictDirError   string = "HKVCConflictExistingDirectoryError"
	NonLeaderError     string = "HKVCNonRaftLeaderError"
	OutOfSequenceError string = "HKVCMsgOutOfSequenceError"
)

//
// data structures used by the test controller, so don't change them!
//

// HKVCController sends to HKVC participant at creation time. Do not change.
type HKVCSetupInfo struct {
	Id          int            // unique ID of the cluster participant
	ControlAddr string         // ip:port address for the participant's control interface
	ClientAddr  string         // ip:port address for the participant's client interface
	RaftAddrs   map[int]string // map of groupID to ip:port address for participant's raft interface
	// specific to Raft group with ID groupID (note: only use if ID in groupID)
}

// HKVC participant must send to HKVCController on request. Do not change.
type HKVCStatusReport struct {
	Active      bool         // indicator of active/inactive status
	GroupLeader map[int]bool // GroupLeader[groupID] is true if this participant is leader of Raft group groupID
	GroupCommit map[int]int  // GroupCommit[groupID] is the leader's commit index for Raft group groupID
	//     this value should only be present in the map if GroupLeader[groupID] is true
}

// Complete template for the HKVC control "service interface" that specifies remote calls from controller
// to HKVC participants.  The HKVCControlInterface must be active from the moment the HKVC participant is
// created until the moment it is terminated by the controller or testing instance.  This interface
// specifies four remote methods that you must implement, as described later in this file.
type HKVCControlInterface struct {
	Activate   func() remote.RemoteError
	Deactivate func() remote.RemoteError
	Terminate  func() remote.RemoteError
	GetStatus  func() (HKVCStatusReport, remote.RemoteError)
}

// TODO: define a struct for the HKVC participant state.

// The HKVCController calls NewHKVCParticipant in its own go routine, containing everything needed for the
// new HKVC participant to configure and launch itself.
//
// TODO: spawn a new HKVC participant (called in its own go routine by the HKVCController)
func NewHKVCParticipant(pInfo []HKVCSetupInfo, index int, groups map[int][]int) {

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
