package hkvc

// snapshot.go integrates HKVC's directory-tree state machine with raft log
// compaction. When a participant's raft log for a group grows past a threshold,
// it serializes the current tree and hands it to raft as a snapshot, letting
// raft discard the applied log prefix. On the receiving side, a follower that
// is shipped a snapshot rebuilds its tree from the serialized form.
//
// Scope: snapshotting is enabled only for participants that belong to a single
// raft group (the common case, and the only case where "the tree" maps cleanly
// to "this group's state"). In multi-group participants the same tree is shared
// across groups, so per-group snapshotting cannot cleanly capture just one
// group's slice of state; there we simply skip compaction (the log keeps
// growing, exactly as before this feature). The raft-level machinery is fully
// general regardless.

import (
	"bytes"
	"encoding/gob"
	"raft"
)

// snapshotThreshold is how many applied entries beyond the last snapshot
// trigger a new one. Kept small so tests exercise compaction quickly.
const snapshotThreshold = 50

// dirSnapshot is a gob-serializable mirror of *directory. The live types use
// unexported fields (not gob-encodable), so we convert to/from these DTOs.
type dirSnapshot struct {
	Name    string
	GroupID int
	KVPairs map[string]kvSnapshot
	SubDirs map[string]*dirSnapshot
}

type kvSnapshot struct {
	Value   string
	Version uint64
}

// treeSnapshot is the full payload raft stores for a snapshot: the serialized
// tree plus the createCounter (part of the deterministic state machine).
type treeSnapshot struct {
	Root          *dirSnapshot
	CreateCounter int
}

// encodeTree converts the live directory tree rooted at d into a DTO.
func encodeTree(d *directory) *dirSnapshot {
	if d == nil {
		return nil
	}
	out := &dirSnapshot{
		Name:    d.name,
		GroupID: d.groupID,
		KVPairs: make(map[string]kvSnapshot, len(d.kvPairs)),
		SubDirs: make(map[string]*dirSnapshot, len(d.subDirs)),
	}
	for k, v := range d.kvPairs {
		out.KVPairs[k] = kvSnapshot{Value: v.value, Version: v.version}
	}
	for k, sub := range d.subDirs {
		out.SubDirs[k] = encodeTree(sub)
	}
	return out
}

// decodeTree rebuilds a live directory tree from a DTO.
func decodeTree(s *dirSnapshot) *directory {
	if s == nil {
		return nil
	}
	d := &directory{
		name:    s.Name,
		groupID: s.GroupID,
		kvPairs: make(map[string]*kvPair, len(s.KVPairs)),
		subDirs: make(map[string]*directory, len(s.SubDirs)),
	}
	for k, v := range s.KVPairs {
		d.kvPairs[k] = &kvPair{key: k, value: v.Value, version: v.Version}
	}
	for k, sub := range s.SubDirs {
		d.subDirs[k] = decodeTree(sub)
	}
	return d
}

// serializeState gob-encodes the current tree and createCounter. The caller
// must hold p.mu.
func (p *HKVCParticipant) serializeState() []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(treeSnapshot{
		Root:          encodeTree(p.root),
		CreateCounter: p.createCounter,
	})
	return buf.Bytes()
}

// restoreState replaces the tree and createCounter from a snapshot payload. The
// caller must hold p.mu.
func (p *HKVCParticipant) restoreState(data []byte) {
	var snap treeSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		return
	}
	if snap.Root != nil {
		p.root = decodeTree(snap.Root)
	}
	p.createCounter = snap.CreateCounter
}

// singleGroup reports whether this participant belongs to exactly one raft
// group, the only configuration in which per-group tree snapshotting is
// well-defined (see the file comment).
func (p *HKVCParticipant) singleGroup() bool {
	return len(p.raftPeers) == 1
}

// maybeSnapshot takes a raft snapshot for groupID if the applied log has grown
// enough since the last one. Caller must hold p.mu. Only active for
// single-group participants.
func (p *HKVCParticipant) maybeSnapshot(groupID int) {
	if !p.singleGroup() {
		return
	}
	rp := p.raftPeers[groupID]
	applied := p.lastApplied[groupID]
	if applied-rp.LastIncludedIndex() < snapshotThreshold {
		return
	}
	data := p.serializeState()
	// Snapshot returns quickly; it compacts the RAFT LOG up to `applied`. We do
	// NOT prune p.applyResults here: those entries are consumed (read then
	// deleted) by the specific handler that submitted each command, and an
	// in-flight handler may still be between ensureApplied and its read of its
	// own logIndex. Pruning them here would race that handler and nil out the
	// result it is about to read. Growth of applyResults is bounded instead by
	// not storing results for no-op reads (see ensureApplied).
	rp.Snapshot(applied, data)
	p.metrics.recordSnapshot()
	if p.log != nil {
		p.log.Debug("took raft snapshot", "group", groupID, "throughIndex", applied, "bytes", len(data))
	}
}

// installSnapshotHandlers registers, for each of this participant's raft peers,
// a callback that rebuilds the tree when raft installs a snapshot from the
// leader. Only meaningful for single-group participants; for multi-group ones
// no snapshots are ever produced, so the handler is never invoked.
func (p *HKVCParticipant) installSnapshotHandlers() {
	for gid, rp := range p.raftPeers {
		gid := gid
		rp.SetSnapshotHandlers(&raft.SnapshotHandlers{
			GroupID: gid,
			OnInstallSnapshot: func(groupID, lastIncludedIndex int, data []byte) {
				p.mu.Lock()
				p.restoreState(data)
				// The snapshot embodies every command up to lastIncludedIndex,
				// so those entries are now applied. (We do not touch
				// applyResults: an in-flight handler consumes its own entry, and
				// stale entries are harmless and few.)
				if p.lastApplied[groupID] < lastIncludedIndex {
					p.lastApplied[groupID] = lastIncludedIndex
				}
				p.mu.Unlock()
				p.metrics.recordInstallRecv()
				if p.log != nil {
					p.log.Info("installed snapshot from leader", "group", groupID, "throughIndex", lastIncludedIndex, "bytes", len(data))
				}
			},
		})
	}
}
