package hkvc

import (
	"net"
	"net/http"
	"raft"
	"remote"
	"sort"
)

// NewHKVCParticipant creates and launches a single HKVC participant.
// Called by the controller in its own goroutine. The participant:
//   - Builds a sorted list of group IDs for deterministic round-robin
//   - Initializes the root directory (always managed by group 0)
//   - Creates a Raft peer for each group this participant belongs to
//   - Starts the control interface RPC callee
func NewHKVCParticipant(pInfo []HKVCSetupInfo, index int, groups map[int][]int) {
	// Sort group IDs so all participants assign directories to groups
	// in the same deterministic order (critical for consistency).
	sortedGIDs := make([]int, 0, len(groups))
	for gid := range groups {
		sortedGIDs = append(sortedGIDs, gid)
	}
	sort.Ints(sortedGIDs)

	p := &HKVCParticipant{
		uid:          pInfo[index].Id,
		isActive:     false,
		isTerminated: false,

		root: &directory{
			name:    "/",
			subDirs: make(map[string]*directory),
			kvPairs: make(map[string]*kvPair),
			groupID: 0,
		},

		mux:        http.NewServeMux(),
		ClientAddr: pInfo[index].ClientAddr,

		raftPeers:     make(map[int]*raft.RaftPeer),
		lastApplied:   make(map[int]int),
		applyResults:  make(map[int]map[int]*applyResult),
		allSetupInfo:  pInfo,
		selfIndex:     index,
		allGroups:     groups,
		sortedGIDs:    sortedGIDs,
		createCounter: 0,

		clientSeq:  make(map[string]int),
		clientResp: make(map[string]*cachedResponse),

		log:     newParticipantLogger(pInfo[index].Id),
		metrics: newMetrics(),
	}

	// Wrap each client handler so every request is timed and counted for the
	// /metrics endpoint. /metrics itself is not instrumented (avoids recursion).
	p.mux.HandleFunc("/list", p.instrument("/list", p.handleList))
	p.mux.HandleFunc("/get_metadata", p.instrument("/get_metadata", p.handleGetMetadata))
	p.mux.HandleFunc("/get", p.instrument("/get", p.handleGet))
	p.mux.HandleFunc("/set", p.instrument("/set", p.handleSet))
	p.mux.HandleFunc("/create", p.instrument("/create", p.handleCreate))
	p.mux.HandleFunc("/delete", p.instrument("/delete", p.handleDelete))
	p.mux.HandleFunc("/metrics", p.handleMetrics)

	// Create a Raft peer for each group this participant belongs to.
	for gid, pids := range groups {
		found := false
		for _, pid := range pids {
			if pid == index {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		var peerAddrs []string
		for _, pid := range pids {
			if pid != index {
				peerAddrs = append(peerAddrs, pInfo[pid].RaftAddrs[gid])
			}
		}
		p.raftPeers[gid] = raft.NewHKVCRaftPeer(pInfo[index].Id, pInfo[index].RaftAddrs[gid], peerAddrs)
		p.lastApplied[gid] = 0
		p.applyResults[gid] = make(map[int]*applyResult)
	}

	// Register snapshot restore handlers so a lagging participant can rebuild
	// its tree from a leader-shipped snapshot (log compaction).
	p.installSnapshotHandlers()

	ctrlIfc := &HKVCControlInterface{}
	ctrlStub, err := remote.NewCalleeStub(ctrlIfc, p, pInfo[index].ControlAddr, false, false)
	if err != nil {
		panic(err)
	}
	p.controlCallee = ctrlStub
	ctrlStub.Start()
}

// Activate starts the HTTP server and all Raft peers.
func (p *HKVCParticipant) Activate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isTerminated || p.isActive {
		return remote.RemoteError{}
	}
	p.isActive = true

	ln, err := net.Listen("tcp", p.ClientAddr)
	if err != nil {
		return remote.RemoteError{}
	}
	srv := &http.Server{Handler: p.mux}
	// SetKeepAlivesEnabled(false) ensures each HTTP request uses a fresh TCP connection,
	// so httpServer.Close() in Deactivate immediately kills all in-flight connections
	// (the old leader can't accidentally serve requests).
	srv.SetKeepAlivesEnabled(false)
	p.httpServer = srv
	go srv.Serve(ln)

	for _, rp := range p.raftPeers {
		rp.Activate()
	}
	if p.log != nil {
		p.log.Info("participant activated", "clientAddr", p.ClientAddr, "groups", len(p.raftPeers))
	}

	return remote.RemoteError{}
}

// Deactivate stops this participant. Raft is deactivated before closing the HTTP server
// so that any in-flight handler goroutine that checks IsLeader will see false.
func (p *HKVCParticipant) Deactivate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isTerminated || !p.isActive {
		return remote.RemoteError{}
	}
	p.isActive = false

	// Raft off first: in-flight HTTP handlers will see IsLeader=false
	for _, rp := range p.raftPeers {
		rp.Deactivate()
	}

	// Then kill HTTP: Close() terminates all active connections immediately
	if p.httpServer != nil {
		p.httpServer.Close()
		p.httpServer = nil
	}

	return remote.RemoteError{}
}

// Terminate permanently shuts down this participant.
func (p *HKVCParticipant) Terminate() remote.RemoteError {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isTerminated {
		return remote.RemoteError{}
	}
	p.isTerminated = true
	p.isActive = false

	if p.httpServer != nil {
		p.httpServer.Close()
		p.httpServer = nil
	}

	for _, rp := range p.raftPeers {
		rp.TerminateHKVC()
	}

	p.controlCallee.Stop()

	return remote.RemoteError{}
}

// GetStatus reports activation status and per-group leadership/commit info.
func (p *HKVCParticipant) GetStatus() (HKVCStatusReport, remote.RemoteError) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sr := HKVCStatusReport{
		Active:      p.isActive,
		GroupLeader: make(map[int]bool),
		GroupCommit: make(map[int]int),
	}

	for gid, rp := range p.raftPeers {
		report, _ := rp.GetStatus()
		sr.GroupLeader[gid] = report.IsLeader
		if report.IsLeader {
			sr.GroupCommit[gid] = report.CommitIndex
		}
	}
	return sr, remote.RemoteError{}
}
