package hkvc

import (
	"encoding/json"
	"net/http"
	"time"
)

const (
	applyTimeout = 3 * time.Second
)

func (p *HKVCParticipant) applyCommand(groupID int, command *raftCommand) *applyResult {
	switch command.Op {
	case "set":
		return p.applySetCmd(groupID, command)
	case "create":
		return p.applyCreateCmd(groupID, command)
	case "delete":
		return p.applyDeleteCmd(groupID, command)
	}
	return &applyResult{status: http.StatusInternalServerError}
}

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

func (p *HKVCParticipant) applyCreateCmd(groupID int, command *raftCommand) *applyResult {
	node := p.resolveDir(command.Directory)
	if node == nil {
		return &applyResult{success: false, status: http.StatusNotFound}
	}
	if _, ok := node.kvPairs[command.Key]; ok {
		return &applyResult{success: false, status: http.StatusOK}
	}

	// directory already exists: safe to use, but not newly created
	if _, ok := node.subDirs[command.Key]; ok {
		return &applyResult{success: false, status: http.StatusOK}
	}

	node.subDirs[command.Key] = &directory{
		name:    command.Key,
		subDirs: make(map[string]*directory),
		kvPairs: make(map[string]*kvPair),
	}
	return &applyResult{success: true, status: http.StatusCreated}
}

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

func (p *HKVCParticipant) submitAndWait(cmd *raftCommand) int {
	cmdBytes, _ := json.Marshal(cmd)
	rp := p.raftPeers[0]
	logIndex, isLeader := rp.SubmitCommand(cmdBytes)
	if !isLeader {
		return -1
	}
	if _, ok := rp.WaitForCommit(logIndex, applyTimeout); !ok {
		return -1
	}
	return logIndex
}
