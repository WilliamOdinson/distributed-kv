// Package hkvc implements a Hierarchical Key-Value storage Cluster: a
// linearizable, sharded key-value store with a filesystem-like namespace, built
// on top of the raft and remote packages.
//
// # Participants and groups
//
// The cluster is a set of participants. Each participant runs an HTTP client
// interface, a control-plane RPC callee ([HKVCControlInterface]), and one raft
// peer per raft group it belongs to. Group 0 always contains every participant
// and manages the root directory; additional groups shard subtrees of the
// namespace so that different directories can be served by different leaders.
//
// # Namespace and sharding
//
// State is a tree of directories; each directory holds key-value pairs and
// child directories. Every directory is owned by exactly one raft group:
//
//   - The root ("/") is owned by group 0.
//   - A directory created directly under the root is assigned a group by
//     round-robin across all groups (spreading load).
//   - A directory created deeper inherits its parent's group, so one leader can
//     resolve an entire path it owns.
//
// Only the leader of the group owning a directory may serve requests for it;
// other participants answer with HKVCNonRaftLeaderError so clients can locate
// the right leader (for example via /get_metadata, which any holder may serve).
//
// # Request lifecycle
//
// Every mutating and most reading endpoints follow the same pipeline:
//
//	validate path/key         -> 400 HKVCInvalidRequestError on bad input
//	check leadership          -> 403 HKVCNonRaftLeaderError if not our group's leader
//	check client sequence     -> replay cached reply (dup) or 406 (outdated)
//	submit command to raft    -> block until committed on a majority
//	apply committed entries   -> mutate the in-memory tree in log order
//	respond and cache reply    -> for idempotent replay of duplicates
//
// Reads (/list, /get) submit a no-op through raft before answering, which gives
// linearizable reads: the response reflects everything committed up to the
// moment the leader confirmed it still holds leadership.
//
// # Consistency and deduplication
//
// Each client stamps requests with a monotonically increasing sequence number.
// A request equal to the last seen number replays the cached response (so a
// client retry is safe); a smaller number is rejected with
// HKVCMsgOutOfSequenceError. Commands are applied strictly in raft log order,
// so all participants converge on the same tree.
//
// # HTTP endpoints
//
//   - /list          names of children of a directory
//   - /get           value for a key
//   - /get_metadata  size/version/owning-group/leader for a key or directory
//   - /set           create or overwrite a key (version bumps on overwrite)
//   - /create        create a subdirectory
//   - /delete        remove a key or a subdirectory (and its contents)
//
// The wire types (DirectoryRequest, KeyRequest, KeyValueMessage, and the
// responses) are the fixed protocol contract and are documented in types.go.
package hkvc
