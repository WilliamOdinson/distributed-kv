package hkvc

import (
	"net"
	"net/http"
	"raft"
	"remote"
	"sync"
)

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

// DirectoryRequest with a directory path for `/list` request
// DO NOT CHANGE.
type DirectoryRequest struct {
	Directory string `json:"directory"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyRequest with a directory path and key for `/get_metadata`, `/get`, `/create`,
// and `/delete` requests. DO NOT CHANGE.
type KeyRequest struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// KeyValueMessage with a directory path, key, and value for `/get` response and `/set` request.
// DO NOT CHANGE.
type KeyValueMessage struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

// ListResponse with a directory path and list of names (keys or subdirectories)
// for `/list` response. DO NOT CHANGE.
type ListResponse struct {
	Directory string   `json:"directory"`
	List      []string `json:"list"`
	ClientID  string   `json:"client_id"`
}

// KeySuccessResponse with a directory path, key, and boolean success
// for `/set`, `/create`, and `/delete` responses. DO NOT CHANGE.
type KeySuccessResponse struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Success   bool   `json:"success"`
	ClientID  string `json:"client_id"`
}

// MetadataResponse with a directory path, key, and collection of associated metadata
// for `/get_metadata` response. DO NOT CHANGE.
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

// HKVCErrorResponse with one of several defined error types and a description. DO NOT CHANGE.
type HKVCErrorResponse struct {
	ErrorType string `json:"error_type"`
	ErrorInfo string `json:"error_info"`
	ClientID  string `json:"client_id"`
}

// HKVCController sends to HKVC participant at creation time. DO NOT CHANGE.
type HKVCSetupInfo struct {
	Id          int            // unique ID of the cluster participant
	ControlAddr string         // ip:port address for the participant's control interface
	ClientAddr  string         // ip:port address for the participant's client interface
	RaftAddrs   map[int]string // map of groupID to ip:port address for participant's raft interface
	// specific to Raft group with ID groupID (note: only use if ID in groupID)
}

// HKVC participant must send to HKVCController on request. DO NOT CHANGE.
type HKVCStatusReport struct {
	Active      bool         // indicator of active/inactive status
	GroupLeader map[int]bool // GroupLeader[groupID] is true if this participant is leader of Raft group groupID
	GroupCommit map[int]int  // GroupCommit[groupID] is the leader's commit index for Raft group groupID
	//   this value should only be present in the map if GroupLeader[groupID] is true
}

// Complete template for the HKVC control "service interface" that specifies remote calls
// from controller to HKVC participants. The HKVCControlInterface must be active from the
// moment the HKVC participant is created until the moment it is terminated by the controller
// or testing instance. This interface specifies four remote methods that you must implement,
// as described later in this file.
type HKVCControlInterface struct {
	Activate   func() remote.RemoteError
	Deactivate func() remote.RemoteError
	Terminate  func() remote.RemoteError
	GetStatus  func() (HKVCStatusReport, remote.RemoteError)
}

type HKVCParticipant struct {
	uid           int           // unique ID of the cluster participant
	mu            sync.Mutex    // mutex to protect shared state of the participant
	isActive      bool          // indicator of active/inactive status
	isTerminated  bool          // indicator of whether the participant has been terminated
	controlCallee remote.Callee // remote calleestub for the control interface

	listener   net.Listener   // http listener for the participant's client interface
	mux        *http.ServeMux // http mux for the participant's client interface
	ClientAddr string         // ip:port address for the participant's client interface

	root *directory // root directory of the participant's key-value store

	raftPeers map[int]*raft.RaftPeer // map of groupID to RaftPeer for the participant's raft interface specific to Raft group with ID groupID
}

type directory struct {
	name    string
	subDirs map[string]*directory
	kvPairs map[string]*kvPair
}

type kvPair struct {
	key     string
	value   string
	version uint64
}
