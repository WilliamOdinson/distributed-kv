# HKVC: A Multi-Raft KV Store with Hierarchical Namespace

A **H**ierarchical **K**ey-**V**alue storage **C**luster written in Go from scratch, organized across three self-contained modules.

Each module has its own README with API documentation and usage examples. See [`remote/`](remote/README.md), [`raft/`](raft/README.md), and [`ticketbox/`](ticketbox/README.md).

## Architecture

```mermaid
%%{init: {'look': 'handDrawn', 'theme': 'neutral'}}%%
graph LR
    Client((Client)) ==>|read/write Request| F4

    subgraph cluster [HKVC Cluster: linearizable reads/writes via Raft]
        F4 -->|forward request| Leader
        Leader --> |replicate| F1
        Leader --> |replicate| F2
        Leader --> |replicate| F3
        Leader --> |replicate| F4
    end

    Leader ==>|reply| Client

    style Leader fill:#f4a261,stroke:#e76f51,color:#000
```
