package hkvc

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	SEQ_FRESH     = 1  // new request, proceed with processing
	SEQ_DUPLICATE = 0  // same seq as last request, replay cached response
	SEQ_OUTDATED  = -1 // seq older than last request, reject with 406
)

func sendJSONResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func normalizeDir(dir string) (string, bool) {
	if len(dir) == 0 || dir[0] != '/' {
		return "", false
	}
	if strings.Contains(dir, ":") {
		return "", false
	}
	parts := strings.Split(dir, "/")
	var clean []string
	for _, s := range parts {
		if s != "" {
			clean = append(clean, s)
		}
	}
	return "/" + strings.Join(clean, "/"), true
}

func (p *HKVCParticipant) resolveDir(path string) *directory {
	if path == "/" {
		return p.root
	}
	cur := p.root
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		child, ok := cur.subDirs[seg]
		if !ok {
			return nil
		}
		cur = child
	}
	return cur
}

// resolveDirChecked resolves a directory path, distinguishing between
// "segment not found" and "segment is a kvPair, not a directory".
// Returns (dir, "") on success, (nil, DirNotFoundError) if missing,
// or (nil, ConflictKeyError) if a path segment is a key.
func (p *HKVCParticipant) resolveDirChecked(path string) (*directory, string) {
	if path == "/" {
		return p.root, ""
	}
	cur := p.root
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if child, ok := cur.subDirs[seg]; ok {
			cur = child
			continue
		}
		if _, isKey := cur.kvPairs[seg]; isKey {
			return nil, ConflictKeyError
		}
		return nil, DirNotFoundError
	}
	return cur, ""
}

func (p *HKVCParticipant) checkSeq(clientID string, seqNum int) int {
	lastSeq, exists := p.clientSeq[clientID]
	if !exists {
		return SEQ_FRESH // first request from this client
	}
	if seqNum > lastSeq {
		return SEQ_FRESH // new request, update last seq
	}
	if seqNum == lastSeq {
		return SEQ_DUPLICATE // same seq as last request, replay cached response
	}
	return SEQ_OUTDATED // seq older than last request, reject with 406
}

func (p *HKVCParticipant) cacheAndResponse(w http.ResponseWriter, clientID string, code int, body any) {
	p.clientResp[clientID] = &cachedResponse{statusCode: code, body: body}
	p.mu.Unlock()
	sendJSONResponse(w, code, body)
}

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
	sr, _ := p.raftPeers[0].GetStatus()

	if !sr.IsLeader || !sr.IsActive {
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
	sr, _ := p.raftPeers[0].GetStatus()
	if !sr.IsLeader || !sr.IsActive {
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
	logIndex := p.submitAndWait(&raftCommand{Op: "no-op"})
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	// build PAddrList: client addresses of all participants in group 0
	addrList := make([]string, len(p.allSetupInfo))
	for i := range p.allSetupInfo {
		addrList[i] = p.allSetupInfo[i].ClientAddr
	}

	p.mu.Lock()
	p.ensureApplied(0)
	node := p.resolveDir(dir)
	if node == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	if _, ok := node.subDirs[req.Key]; ok {
		p.cacheAndResponse(w, req.ClientID, http.StatusOK, MetadataResponse{Directory: dir, Key: req.Key, IsDirectory: true, Size: -1, PAddrList: addrList, LeaderIdx: p.selfIndex, Tags: []string{}, ClientID: req.ClientID})
		return
	}
	if e, ok := node.kvPairs[req.Key]; ok {
		p.cacheAndResponse(w, req.ClientID, http.StatusOK, MetadataResponse{Directory: dir, Key: req.Key, IsDirectory: false, Size: len(e.value), Version: e.version, PAddrList: addrList, LeaderIdx: p.selfIndex, Tags: []string{}, ClientID: req.ClientID})
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
	sr, _ := p.raftPeers[0].GetStatus()
	if !sr.IsLeader || !sr.IsActive {
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
	logIndex := p.submitAndWait(&raftCommand{Op: "no-op"})
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(0)
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
	sr, _ := p.raftPeers[0].GetStatus()
	if !sr.IsLeader || !sr.IsActive {
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

	p.ensureApplied(0)
	// use resolveDirChecked to detect path-is-a-key conflicts
	node, resolveErr := p.resolveDirChecked(dir)
	if resolveErr == ConflictKeyError {
		p.cacheAndResponse(w, req.ClientID, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictKeyError, ErrorInfo: "path conflicts with existing key", ClientID: req.ClientID})
		return
	}
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
	logIndex := p.submitAndWait(&raftCommand{Op: "set", Directory: dir, Key: req.Key, Value: req.Value})
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	// apply and get result
	p.mu.Lock()
	p.ensureApplied(0)
	result := p.applyResults[0][logIndex]
	delete(p.applyResults[0], logIndex)

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
	sr, _ := p.raftPeers[0].GetStatus()
	if !sr.IsLeader || !sr.IsActive {
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

	p.ensureApplied(0)

	node, resolveErr := p.resolveDirChecked(dir)
	if resolveErr == ConflictKeyError {
		p.cacheAndResponse(w, req.ClientID, http.StatusConflict, HKVCErrorResponse{ErrorType: ConflictKeyError, ErrorInfo: "path conflicts with existing key", ClientID: req.ClientID})
		return
	}

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

	logIndex := p.submitAndWait(&raftCommand{Op: "create", Directory: dir, Key: req.Key})
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(0)
	result := p.applyResults[0][logIndex]
	delete(p.applyResults[0], logIndex)
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
	sr, _ := p.raftPeers[0].GetStatus()
	if !sr.IsLeader || !sr.IsActive {
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

	p.ensureApplied(0)
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

	logIndex := p.submitAndWait(&raftCommand{Op: "delete", Directory: dir, Key: req.Key})
	if logIndex < 0 {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "lost leadership", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	p.ensureApplied(0)
	result := p.applyResults[0][logIndex]
	delete(p.applyResults[0], logIndex)
	p.cacheAndResponse(w, req.ClientID, result.status, KeySuccessResponse{Directory: dir, Key: req.Key, Success: result.success, ClientID: req.ClientID})
}
