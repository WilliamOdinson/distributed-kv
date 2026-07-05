# hkvc

A Hierarchical Key-Value storage Cluster: a linearizable, sharded key-value store with a filesystem-like namespace, built on [`raft`](../raft/README.md) and [`remote`](../remote/README.md).

See [`../README.md`](../README.md) for the full system design and [`doc.go`](doc.go) for package-level godoc.

## Model

A cluster is a set of **participants**. Each participant serves an HTTP client interface, a control-plane RPC callee, and one raft peer per **group** it belongs to. Group 0 contains everyone and manages the root `/`; additional groups shard subtrees so different directories can be served by different leaders concurrently. Only the leader of a directory's owning group serves requests for it; others reply `HKVCNonRaftLeaderError` so clients can find the leader.

## HTTP endpoints

| Endpoint        | Request            | Success            |
|-----------------|--------------------|--------------------|
| `/list`         | `DirectoryRequest` | `ListResponse`     |
| `/get`          | `KeyRequest`       | `KeyValueMessage`  |
| `/get_metadata` | `KeyRequest`       | `MetadataResponse` |
| `/set`          | `KeyValueMessage`  | `KeySuccessResponse` (201 new / 200 overwrite) |
| `/create`       | `KeyRequest`       | `KeySuccessResponse` (201 new / 200 existed)   |
| `/delete`       | `KeyRequest`       | `KeySuccessResponse` |

Every request carries a `client_id` and a monotonic `seq_number`. A duplicate sequence replays the cached response; an older one is rejected with `HKVCMsgOutOfSequenceError` (406).

## Consistency

Writes go through raft and are applied in log order on every participant. Reads (`/list`, `/get`) first commit a no-op through raft, so a response reflects a point at which the responding participant was confirmed leader: reads and writes are linearizable.

## Tests

```bash
go test -v -timeout 600s -race -cover ./...
```

The suite has two layers:

- **Unit tests** (`logic_test.go`): path normalization, client-sequence classification, directory resolution, and the apply handlers, all without a network. They run in ~2s.
- **Integration tests** (`client_test.go`, `cluster_test.go`): real clusters over HTTP, covering validation, non-leader rejection, sequencing/dedup, hierarchy, commit ordering under concurrency, leader failover, delete/rebuild, and multi-group sharding. Ports come from the kernel (`:0`) to avoid collisions.

- **Linearizability tests** (`linearizability_test.go`): concurrent get/put clients against a real cluster, with and without leadership churn. Recorded histories are checked with [porcupine](https://github.com/anishathalye/porcupine) against a per-key register model.
