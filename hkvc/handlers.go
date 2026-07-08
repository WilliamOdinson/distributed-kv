package hkvc

import (
	"encoding/json"
	"net/http"
)

// handleList serves the /list endpoint. Returns the names of all subdirectories and keys
// immediately within the requested directory. Only the managing group's leader may respond.
//
// Request:  DirectoryRequest (directory, seq_number, client_id)
// Success:  200 OK with ListResponse containing names of subdirs and keys
// Errors:
//   - 400 Bad Request    (HKVCInvalidRequestError)        malformed JSON or invalid directory path
//   - 403 Forbidden      (HKVCNonRaftLeaderError)         not the managing group's leader
//   - 404 Not Found      (HKVCDirectoryNotFoundError)     directory does not exist
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)      seq_number < previous from this client
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

// handleGetMetadata serves the /get_metadata endpoint. Returns metadata for a key or subdirectory
// within a directory, including version, size, managing group members (p_addr_list), and current
// leader index. Key "." refers to the directory itself.
// Note that any participant with the data can respond (no leader check).
//
// Request:  KeyRequest (directory, key, seq_number, client_id)
// Success:  200 OK with MetadataResponse (is_directory, size, version, p_addr_list, leader_index, tags)
// Errors:
//   - 400 Bad Request    (HKVCInvalidRequestError)       malformed JSON, invalid path, or empty key
//   - 404 Not Found      (HKVCDirectoryNotFoundError)    directory does not exist
//   - 404 Not Found      (HKVCKeyNotFoundError)          key not found in directory
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)	  seq_number < previous from this client
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

// handleGet serves the /get endpoint. Returns the value associated with a key in a directory.
// Only the managing group's leader may respond.
//
// Request:  KeyRequest (directory, key, seq_number, client_id)
// Success:  200 OK with KeyValueMessage containing the stored value
// Errors:
//   - 400 Bad Request    (HKVCInvalidRequestError)      malformed JSON, invalid path, or empty key
//   - 403 Forbidden      (HKVCNonRaftLeaderError)       not the managing group's leader
//   - 404 Not Found      (HKVCDirectoryNotFoundError)   directory does not exist
//   - 404 Not Found      (HKVCKeyNotFoundError)         key not found in directory
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)    seq_number < previous from this client
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

// handleSet serves the /set endpoint. Stores a value for a key in a directory, creating the
// key if it doesn't exist or overwriting if it does (incrementing the version number).
//
// Request:  KeyValueMessage (directory, key, value, seq_number, client_id)
// Success:  201 Created with KeySuccessResponse if key is new; 200 OK with KeySuccessResponse
// if key already existed (overwrite)
//
// Errors:
//   - 400 Bad Request  (HKVCInvalidRequestError)            malformed JSON, invalid path, or empty key
//   - 403 Forbidden    (HKVCNonRaftLeaderError)              not the managing group's leader
//   - 404 Not Found    (HKVCDirectoryNotFoundError)          directory does not exist
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)         seq_number < previous from this client
//   - 409 Conflict     (HKVCConflictExistingKeyError)        directory path contains a segment that is a key
//   - 409 Conflict     (HKVCConflictExistingDirectoryError)  key name matches an existing subdir
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

// handleCreate serves the /create endpoint. Creates a new subdirectory within an existing
// parent directory. If the subdirectory already exists, returns success=false with 200 OK.
// The new directory's managing Raft group is assigned by round-robin for
// root-level children, or inherited from the parent for deeper directories.
//
// Request:  KeyRequest (directory, key, seq_number, client_id)
// Success:  201 Created with KeySuccessResponse (success=true) if directory was created;
// 200 OK with KeySuccessResponse (success=false) if directory already exists
//
// Errors:
//   - 400 Bad Request  (HKVCInvalidRequestError)       malformed JSON, invalid path, or empty key
//   - 403 Forbidden    (HKVCNonRaftLeaderError)         not the managing group's leader
//   - 404 Not Found    (HKVCDirectoryNotFoundError)     parent directory does not exist
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)    seq_number < previous from this client
//   - 409 Conflict     (HKVCConflictExistingKeyError)   path segment or key name is an existing key
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
	// A missing result should never happen on the leader that submitted this
	// command, but guard against it so we never dereference nil while holding
	// p.mu (which would recover in the HTTP server yet leave the lock held,
	// deadlocking the participant). Report a server error instead.
	if result == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusInternalServerError, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "result unavailable", ClientID: req.ClientID})
		return
	}
	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}

// handleDelete serves the /delete endpoint. Removes a key or subdirectory (and all its contents).
//
// Request:  KeyRequest (directory, key, seq_number, client_id)
// Success:  200 OK with KeySuccessResponse (success=true)
//
// Errors:
//   - 400 Bad Request  (HKVCInvalidRequestError)       malformed JSON, invalid path, or empty key
//   - 403 Forbidden    (HKVCNonRaftLeaderError)         not the managing group's leader
//   - 404 Not Found    (HKVCDirectoryNotFoundError)     directory does not exist
//   - 404 Not Found    (HKVCKeyNotFoundError)           key not found in directory
//   - 406 Not Acceptable (HKVCMsgOutOfSequenceError)    seq_number < previous from this client
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
	// A missing result should never happen on the leader that submitted this
	// command, but guard against it so we never dereference nil while holding
	// p.mu (which would recover in the HTTP server yet leave the lock held,
	// deadlocking the participant). Report a server error instead.
	if result == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusInternalServerError, HKVCErrorResponse{ErrorType: InvalidError, ErrorInfo: "result unavailable", ClientID: req.ClientID})
		return
	}
	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}
