package hkvc

import (
	"encoding/json"
	"net/http"
)

func (p *HKVCParticipant) handleList(w http.ResponseWriter, r *http.Request) {
	var req DirectoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad dir", ClientID: req.ClientID})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad directory", ClientID: req.ClientID})
		return
	}
	gid, errType := p.checkLeadership(dir, 5)
	if errType == NonLeaderError {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber
	p.mu.Unlock()

	if p.submitAndWait(&raftCommand{Op: "no-op"}, gid) < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(gid)
	node := p.resolveDir(dir)
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	list := make([]string, 0)
	for k := range node.subDirs {
		list = append(list, k)
	}
	for k := range node.kvPairs {
		list = append(list, k)
	}

	p.cacheAndResponse(w, req.ClientID, http.StatusOK, ListResponse{Directory: dir, List: list, ClientID: req.ClientID})
}

func (p *HKVCParticipant) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad json", ClientID: ""})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok || req.Key == "" {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad request", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()

	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber

	// apply all local groups to get latest state
	for gid := range p.raftPeers {
		p.ensureApplied(gid)
	}

	node := p.resolveDir(dir)
	if node != nil {
		if rp, ok := p.raftPeers[node.groupID]; ok {
			sr, _ := rp.GetStatus()
			if sr.IsLeader && sr.IsActive {
				p.mu.Unlock()
				p.submitAndWait(&raftCommand{Op: "no-op"}, node.groupID)
				p.mu.Lock()
				for gid := range p.raftPeers {
					p.ensureApplied(gid)
				}
				node = p.resolveDir(dir) // re-resolve after applying
			}
		}
	}
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}

	// key "." = the directory itself
	if req.Key == "." {
		addrs, leaderIdx := p.buildGroupMetadata(node.groupID)
		p.cacheAndResponse(w, req.ClientID, http.StatusOK, MetadataResponse{
			Directory: dir, Key: req.Key, IsDirectory: true, Size: -1,
			PAddrList: addrs, LeaderIdx: leaderIdx, Tags: []string{}, ClientID: req.ClientID,
		})
		return
	}
	// key is a subdirectory
	if sub, ok := node.subDirs[req.Key]; ok {
		addrs, leaderIdx := p.buildGroupMetadata(sub.groupID)
		p.cacheAndResponse(w, req.ClientID, http.StatusOK, MetadataResponse{
			Directory: dir, Key: req.Key, IsDirectory: true, Size: -1,
			PAddrList: addrs, LeaderIdx: leaderIdx, Tags: []string{}, ClientID: req.ClientID,
		})
		return
	}
	// key is a kvPair
	if e, ok := node.kvPairs[req.Key]; ok {
		addrs, leaderIdx := p.buildGroupMetadata(node.groupID)
		p.cacheAndResponse(w, req.ClientID, http.StatusOK, MetadataResponse{
			Directory: dir, Key: req.Key, IsDirectory: false, Size: len(e.value), Version: e.version,
			PAddrList: addrs, LeaderIdx: leaderIdx, Tags: []string{}, ClientID: req.ClientID,
		})
		return
	}

	p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: KeyNotFoundError, ErrorInfo: "key not found", ClientID: req.ClientID})

}

func (p *HKVCParticipant) handleGet(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad json", ClientID: ""})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok || req.Key == "" {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad request", ClientID: req.ClientID})
		return
	}

	gid, errType := p.checkLeadership(dir, 3)
	if errType == NonLeaderError {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	p.mu.Lock()

	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber
	p.mu.Unlock()

	// submit no-op for linearizable read
	if p.submitAndWait(&raftCommand{Op: "no-op"}, gid) < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(gid)
	node := p.resolveDir(dir)
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	e, ok := node.kvPairs[req.Key]
	if !ok {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: KeyNotFoundError, ErrorInfo: "key not found", ClientID: req.ClientID})
		return
	}
	val := e.value
	p.cacheAndResponse(w, req.ClientID, http.StatusOK, KeyValueMessage{Directory: dir, Key: req.Key, Value: val, ClientID: req.ClientID})
}

func (p *HKVCParticipant) handleSet(w http.ResponseWriter, r *http.Request) {
	var req KeyValueMessage
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad json", ClientID: ""})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok || req.Key == "" {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad request", ClientID: req.ClientID})
		return
	}
	gid, errType := p.checkLeadership(dir, 3)
	if errType == NonLeaderError {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	if errType == ConflictKeyError {
		sendJSONResponse(w, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictKeyError, ErrorInfo: "path conflicts with existing key", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()

	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber

	p.ensureApplied(gid)
	node := p.resolveDir(dir)
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	// conflict: key matches an existing subdirectory
	if _, isDir := node.subDirs[req.Key]; isDir {
		p.cacheAndResponse(w, req.ClientID, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictDirError, ErrorInfo: "key conflicts with existing directory", ClientID: req.ClientID})
		return
	}
	p.mu.Unlock()

	// submit to raft, wait for commit
	logIndex := p.submitAndWait(&raftCommand{Op: "set", Directory: dir, Key: req.Key, Value: req.Value}, gid)
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}
	// apply and get result
	p.mu.Lock()
	p.ensureApplied(gid)
	result := p.applyResults[gid][logIndex]
	delete(p.applyResults[gid], logIndex)

	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}

func (p *HKVCParticipant) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad json", ClientID: ""})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok || req.Key == "" {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad request", ClientID: req.ClientID})
		return
	}
	gid, errType := p.checkLeadership(dir, 3)
	if errType == NonLeaderError {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	if errType == ConflictKeyError {
		sendJSONResponse(w, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictKeyError, ErrorInfo: "path conflicts with existing key", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()

	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber

	p.ensureApplied(gid)
	node := p.resolveDir(dir)
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	// conflict: key matches an existing kvPair
	if _, isKey := node.kvPairs[req.Key]; isKey {
		p.cacheAndResponse(w, req.ClientID, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictKeyError, ErrorInfo: "key conflicts with existing key-value entry", ClientID: req.ClientID})
		return
	}
	p.mu.Unlock()

	logIndex := p.submitAndWait(&raftCommand{Op: "create", Directory: dir, Key: req.Key}, gid)
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(gid)
	result := p.applyResults[gid][logIndex]
	delete(p.applyResults[gid], logIndex)
	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}

func (p *HKVCParticipant) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req KeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad json", ClientID: ""})
		return
	}
	dir, ok := normalizeDir(req.Directory)
	if !ok || req.Key == "" {
		sendJSONResponse(w, http.StatusBadRequest, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "bad request", ClientID: req.ClientID})
		return
	}
	gid, errType := p.checkLeadership(dir, 3)
	if errType == NonLeaderError {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()

	switch p.checkSeq(req.ClientID, req.SeqNumber) {
	case SEQ_FRESH:
		// proceed with processing
	case SEQ_DUPLICATE:
		// replay cached response
		cached := p.clientResp[req.ClientID]
		p.mu.Unlock()
		sendJSONResponse(w, cached.statusCode, cached.body)
		return
	case SEQ_OUTDATED:
		// reject with 406
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotAcceptable, HKVCErrorResponse{ErrorType: OutOfSequenceError, ErrorInfo: "out of sequence number", ClientID: req.ClientID})
		return
	}
	p.clientSeq[req.ClientID] = req.SeqNumber

	p.ensureApplied(gid)

	if errType != "" {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}

	node := p.resolveDir(dir)
	if node == nil {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	_, hasKey := node.kvPairs[req.Key]
	_, hasDir := node.subDirs[req.Key]
	if !hasKey && !hasDir {
		p.cacheAndResponse(w, req.ClientID, http.StatusNotFound, HKVCErrorResponse{ErrorType: KeyNotFoundError, ErrorInfo: "key not found", ClientID: req.ClientID})
		return
	}
	p.mu.Unlock()

	logIndex := p.submitAndWait(&raftCommand{Op: "delete", Directory: dir, Key: req.Key}, gid)
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(gid)
	result := p.applyResults[gid][logIndex]
	delete(p.applyResults[gid], logIndex)
	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}
