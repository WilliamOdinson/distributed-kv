package hkvc

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	SEQ_FRESH     = 1  // new request, proceed with processing
	SEQ_DUPLICATE = 0  // same seq as last request, replay cached response
	SEQ_OUTDATED  = -1 // seq older than last request, reject with 406
)

// sendJSONResponse is a helper to send a JSON response with the given status code and body.
func sendJSONResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// normalizeDir validates a directory path.
// it will reject empty paths, paths not starting with '/', and paths containing ':'.
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

// resolveDir walks the directory tree to find the node at the given path.
// Returns nil if any segment along the path doesn't exist as a subdirectory.
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

// resolveDirWithGroups is a group-aware path resolver. For each directory node along the path,
// it calls ensureApplied on that node's managing group to guarantee the local state is up-to-date.
//
// Returns one of:
//   - (dir, groupID, "")               success
//   - (nil, groupID, NonLeaderError)   we don't belong to the group managing this segment
//   - (nil, groupID, ConflictKeyError) a path segment is a kvPair, not a directory
//   - (nil, groupID, DirNotFoundError) a path segment doesn't exist
func (p *HKVCParticipant) resolveDirWithGroups(path string) (*directory, int, string) {
	// Before resolving the path, ensure we've applied all committed logs for the root's group
	if _, inGroup := p.raftPeers[p.root.groupID]; inGroup {
		p.ensureApplied(p.root.groupID)
	}
	if path == "/" {
		return p.root, p.root.groupID, ""
	}
	cur := p.root
	for _, seg := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		// If we're not in this directory's group, we can't guarantee our local state is current.
		if _, inGroup := p.raftPeers[cur.groupID]; !inGroup {
			return nil, cur.groupID, NonLeaderError
		}

		// Ensure we've applied all committed logs for the current directory's group
		p.ensureApplied(cur.groupID)

		if child, ok := cur.subDirs[seg]; ok {
			cur = child
			continue
		}

		// If the segment doesn't exist as a subdirectory, check if it exists as a kvPair instead.
		if _, isKey := cur.kvPairs[seg]; isKey {
			return nil, cur.groupID, ConflictKeyError
		}
		return nil, cur.groupID, DirNotFoundError
	}
	return cur, cur.groupID, ""
}

// checkSeq checks the client's request sequence number for ordering and deduplication.
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

// cacheAndResponse stores the response for dup detection, unlocks mu, then sends the response.
func (p *HKVCParticipant) cacheAndResponse(w http.ResponseWriter, clientID string, code int, body any) {
	p.clientResp[clientID] = &cachedResponse{statusCode: code, body: body}
	p.mu.Unlock()
	sendJSONResponse(w, code, body)
}

// checkLeadership resolves the directory path to find its managing group,
// retrying briefly if DirNotFoundError occurs, then checks if we're the leader.
func (p *HKVCParticipant) checkLeadership(dir string, attempts int) (int, string) {
	var gid int
	var errType string

	for i := 0; i < attempts; i++ {
		p.mu.Lock()
		_, gid, errType = p.resolveDirWithGroups(dir)
		p.mu.Unlock()
		if errType != DirNotFoundError {
			break // directory not found, but could be due to stale state; retry
		}
		time.Sleep(100 * time.Millisecond)
	}

	if errType == NonLeaderError {
		return gid, NonLeaderError // after retries, still not found - likely not leader
	}

	rp, inGroup := p.raftPeers[gid]
	if !inGroup {
		return gid, NonLeaderError // not in group, treat as non-leader
	}

	sr, _ := rp.GetStatus()
	if !sr.IsLeader || !sr.IsActive {
		return gid, NonLeaderError // not leader, treat as non-leader
	}
	return gid, errType
}

// buildGroupMetadata help get_metadata build the PAddrList and LeaderIdx for a given group.
func (p *HKVCParticipant) buildGroupMetadata(groupID int) ([]string, int) {
	members := p.allGroups[groupID]
	addrList := make([]string, len(members))

	for i, pidx := range members {
		addrList[i] = p.allSetupInfo[pidx].ClientAddr
	}
	leaderIdx := -1
	if rp, ok := p.raftPeers[groupID]; ok {
		sr, _ := rp.GetStatus()
		for i, pidx := range members {
			if p.allSetupInfo[pidx].Id == sr.CurrentLeader {
				leaderIdx = i
				break
			}
		}
	}
	return addrList, leaderIdx
}
