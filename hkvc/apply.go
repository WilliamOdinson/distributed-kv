package hkvc

import (
	"encoding/json"
	"net/http"
	"time"
)

const (
	applyTimeout = 3 * time.Second
)

// applyCommand dispatches a deserialized raft command to the appropriate handler.
// Called by ensureApplied for each newly committed log entry.
func (p *HKVCParticipant) applyCommand(groupID int, command *raftCommand) *applyResult {
	switch command.Op {
	case "set":
		return p.applySetCmd(groupID, command)
	case "create":
		return p.applyCreateCmd(groupID, command)
	case "delete":
		return p.applyDeleteCmd(groupID, command)
	case "no-op":
		// no-ops are submitted for linearizable reads; nothing to apply
		return &applyResult{success: true, status: http.StatusOK}
	}
	return &applyResult{status: http.StatusInternalServerError}
}

// applySetCmd creates or updates a key-value pair in the target directory.
// Returns 201 Created for new keys, 200 OK for updates (with version bump).
func (p *HKVCParticipant) applySetCmd(groupID int, command *raftCommand) *applyResult {
	node := p.resolveDir(command.Directory)
	if node == nil {
		return &applyResult{success: false, status: http.StatusNotFound}
	}
	if _, ok := node.kvPairs[command.Key]; ok {
		currentVersion := node.kvPairs[command.Key].version
		node.kvPairs[command.Key].value = command.Value
		node.kvPairs[command.Key].version = currentVersion + 1
		return &applyResult{success: true, status: http.StatusOK}
	}
	node.kvPairs[command.Key] = &kvPair{key: command.Key, value: command.Value, version: 1}
	return &applyResult{success: true, status: http.StatusCreated}
}

// applyCreateCmd creates a new subdirectory. Assigns it a Raft group:
//   - Root-level dirs (parent.groupID == 0): round-robin across all groups
//   - Deeper dirs: inherit parent's group (so the managing leader can resolve full paths)
//
// Returns 200 OK (success=false) if the name conflicts with an existing key or dir.
func (p *HKVCParticipant) applyCreateCmd(groupID int, command *raftCommand) *applyResult {
	node := p.resolveDir(command.Directory)
	if node == nil {
		return &applyResult{success: false, status: http.StatusNotFound}
	}
	if _, ok := node.kvPairs[command.Key]; ok {
		return &applyResult{success: false, status: http.StatusOK}
	}
	if _, ok := node.subDirs[command.Key]; ok {
		return &applyResult{success: false, status: http.StatusOK}
	}

	// Group assignment: root-level children get round-robin, others inherit.
	newGroupID := node.groupID
	if node.groupID == 0 && len(p.sortedGIDs) > 0 {
		newGroupID = p.sortedGIDs[p.createCounter%len(p.sortedGIDs)]
		p.createCounter++
	}
	node.subDirs[command.Key] = &directory{
		name:    command.Key,
		subDirs: make(map[string]*directory),
		kvPairs: make(map[string]*kvPair),
		groupID: newGroupID,
	}
	return &applyResult{success: true, status: http.StatusCreated}
}

// applyDeleteCmd removes a key or subdirectory.
func (p *HKVCParticipant) applyDeleteCmd(groupID int, command *raftCommand) *applyResult {
	node := p.resolveDir(command.Directory)
	if node == nil {
		return &applyResult{success: false, status: http.StatusNotFound}
	}
	if _, ok := node.kvPairs[command.Key]; ok {
		delete(node.kvPairs, command.Key)
		return &applyResult{success: true, status: http.StatusOK}
	}
	if _, ok := node.subDirs[command.Key]; ok {
		delete(node.subDirs, command.Key)
		return &applyResult{success: true, status: http.StatusOK}
	}
	return &applyResult{success: false, status: http.StatusNotFound}
}

// ensureApplied applies all committed but unapplied log entries for the given group.
func (p *HKVCParticipant) ensureApplied(groupID int) {
	rp := p.raftPeers[groupID]
	report, _ := rp.GetStatus()

	for p.lastApplied[groupID] < report.CommitIndex {
		p.lastApplied[groupID]++
		idx := p.lastApplied[groupID]
		cmdBytes := rp.GetLogEntry(idx)
		if cmdBytes == nil {
			continue
		}
		var command raftCommand
		if err := json.Unmarshal(cmdBytes, &command); err != nil {
			continue
		}
		result := p.applyCommand(groupID, &command)
		p.applyResults[groupID][idx] = result
	}
}

// submitAndWait appends a command to the given group's Raft log and blocks until it is committed.
// Returns the log index on success, -1 if not leader.
func (p *HKVCParticipant) submitAndWait(cmd *raftCommand, groupID int) int {
	cmdBytes, _ := json.Marshal(cmd)
	rp := p.raftPeers[groupID]
	logIndex, isLeader := rp.SubmitCommand(cmdBytes)
	if !isLeader {
		return -1
	}
	if _, ok := rp.WaitForCommit(logIndex, applyTimeout); !ok {
		return -1
	}
	return logIndex
}
