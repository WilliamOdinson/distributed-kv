package hkvc

import (
	"encoding/json"
	"net/http"
	"strings"
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

	if !sr.Leader || !sr.Active {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}

	p.mu.Lock()
	node := p.resolveDir(dir)
	if node == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	list := make([]string, 0)
	for k := range node.subDirs {
		list = append(list, k)
	}
	for k := range node.kvPairs {
		list = append(list, k)
	}
	p.mu.Unlock()
	sendJSONResponse(w, http.StatusOK, ListResponse{Directory: dir, List: list, ClientID: req.ClientID})
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
	if !sr.Leader || !sr.Active {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	p.mu.Lock()
	node := p.resolveDir(dir)
	if node == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	if _, ok := node.subDirs[req.Key]; ok {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusOK, MetadataResponse{Directory: dir, Key: req.Key, IsDirectory: true, Size: -1, PAddrList: []string{}, Tags: []string{}, ClientID: req.ClientID})
		return
	}
	if e, ok := node.kvPairs[req.Key]; ok {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusOK, MetadataResponse{Directory: dir, Key: req.Key, Size: len(e.value), Version: e.version, PAddrList: []string{}, Tags: []string{}, ClientID: req.ClientID})
		return
	}
	p.mu.Unlock()
	sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: KeyNotFoundError, ErrorInfo: "key not found", ClientID: req.ClientID})
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
	if !sr.Leader || !sr.Active {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	p.mu.Lock()
	node := p.resolveDir(dir)
	if node == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}
	e, ok := node.kvPairs[req.Key]
	if !ok {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: KeyNotFoundError, ErrorInfo: "key not found", ClientID: req.ClientID})
		return
	}
	val := e.value
	p.mu.Unlock()
	sendJSONResponse(w, http.StatusOK, KeyValueMessage{Directory: dir, Key: req.Key, Value: val, ClientID: req.ClientID})
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
	if !sr.Leader || !sr.Active {
		sendJSONResponse(w, http.StatusForbidden, HKVCErrorResponse{ErrorType: NonLeaderError, ErrorInfo: "not leader", ClientID: req.ClientID})
		return
	}
	p.mu.Lock()
	node := p.resolveDir(dir)
	if node == nil {
		p.mu.Unlock()
		sendJSONResponse(w, http.StatusNotFound, HKVCErrorResponse{ErrorType: DirNotFoundError, ErrorInfo: "dir not found", ClientID: req.ClientID})
		return
	}

	_, existed := node.kvPairs[req.Key]
	if existed {
		node.kvPairs[req.Key].value = req.Value
		node.kvPairs[req.Key].version++
	} else {
		node.kvPairs[req.Key] = &kvPair{key: req.Key, value: req.Value, version: 1}
	}
	p.mu.Unlock()
	sendJSONResponse(w, http.StatusOK, KeySuccessResponse{Directory: dir, Key: req.Key, Success: true, ClientID: req.ClientID})
}

func (p *HKVCParticipant) handleCreate(w http.ResponseWriter, r *http.Request) {

}

func (p *HKVCParticipant) handleDelete(w http.ResponseWriter, r *http.Request) {

}
