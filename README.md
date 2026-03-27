# tinycfs — Tiny Cluster File System

A distributed shared filesystem for small files, written entirely in Rust.

tinycfs combines a FUSE-based virtual filesystem with cluster membership and consensus into a single static binary with no external daemon dependencies.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        tinycfs node                              │
│                                                                  │
│   ┌─────────────┐    ┌────────────────────────────────────────┐ │
│   │ FUSE layer  │    │           Raft Consensus                │ │
│   │  (fuser)    │───►│  actor model, proposal batching,       │ │
│   │             │◄───│  parallel per-peer replication         │ │
│   └─────────────┘    └───────────────┬────────────────────────┘ │
│                                      │ committed entries         │
│   ┌─────────────────────────────────▼────────────────────────┐  │
│   │         In-memory File State Machine                      │  │
│   │   (inode tree — read replica, applied in log order)       │  │
│   └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ┌───────────────────────────────────────────────────────────┐  │
│   │         Cluster Transport (TCP, length-prefixed frames)   │  │
│   └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
          ▲                   ▲                   ▲
          │  TCP 7788         │  TCP 7788         │  TCP 7788
       node1               node2               node3
```

### Key design decisions

| Concern | Approach |
|---|---|
| **Consensus** | Raft (leader election + log replication) — strong consistency with majority quorum |
| **Throughput** | Actor-model Raft engine (no Mutex contention) + proposal batching (up to 256 ops per RPC) |
| **Scalability** | Per-peer parallel replication — slow nodes don't stall others; O(N) leader fan-out |
| **Quorum** | Majority quorum (⌊N/2⌋ + 1 nodes must acknowledge a write) |
| **Reads** | Served directly from local in-memory store — no network round-trip |
| **Filesystem** | FUSE via the `fuser` crate; inode-based in-memory tree |
| **Transport** | Direct TCP with 4-byte length-prefixed `bincode` frames; persistent connections |
| **Small files** | All file data kept in memory; optimised for config/state files |
| **Config** | JSON (`tinycfs.conf`); no XML, no INI |

### Throughput model

| Cluster size | Expected write throughput | Notes |
|---|---|---|
| 3–10 nodes | ~20 000 writes/s | 1 ms batch window, 1 Gbit LAN |
| 10–100 nodes | ~5 000–10 000 writes/s | O(N) fan-out, parallel senders |
| 100–500 nodes | ~1 000–5 000 writes/s | Hierarchical relay planned (not yet implemented) |

Reads are local and scale linearly with hardware; they add zero cluster load.

---

## Usage

### 1. Install dependencies (Linux)

```bash
# FUSE kernel module
sudo apt install fuse3 libfuse3-dev   # Debian/Ubuntu
sudo dnf install fuse3 fuse3-devel    # Fedora/RHEL
```

### 2. Build

```bash
cargo build --release
# Static binary output: target/release/tinycfs
```

### 3. Configure

Copy the example and edit `local_node` on each machine:

```bash
sudo mkdir -p /etc/tinycfs
sudo cp tinycfs.conf.example /etc/tinycfs/tinycfs.conf
sudo $EDITOR /etc/tinycfs/tinycfs.conf
```

`/etc/tinycfs/tinycfs.conf` on **node1**:
```json
{
  "cluster_name": "mycluster",
  "local_node": "node1",
  "nodes": [
    { "name": "node1", "ip": "192.168.1.10", "port": 7788 },
    { "name": "node2", "ip": "192.168.1.11", "port": 7788 },
    { "name": "node3", "ip": "192.168.1.12", "port": 7788 }
  ]
}
```

Change `"local_node": "node2"` on **node2**, `"node3"` on **node3** — the `nodes` list is identical on all machines.

### 4. Run

```bash
# Create the mount point
mkdir -p /mnt/tinycfs

# Start on each node (--allow-other lets non-root users access the mount)
sudo tinycfs /mnt/tinycfs --config /etc/tinycfs/tinycfs.conf --allow-other
```

Files written on any node appear on all nodes within one Raft round-trip (~1 ms on a LAN).

---

## Configuration reference

| Field | Required | Default | Description |
|---|---|---|---|
| `cluster_name` | ✓ | — | Shared cluster identifier (prevents cross-cluster connections) |
| `local_node` | ✓ | — | Name of **this** node (must match an entry in `nodes`) |
| `data_dir` | | `null` | Reserved for future persistent Raft log storage |
| `nodes[].name` | ✓ | — | Unique node name |
| `nodes[].ip` | ✓ | — | IP address this node listens on |
| `nodes[].port` | | `7788` | TCP port for cluster communication |

---

## How it works

### Write path

1. Application writes a file via FUSE.
2. The FUSE handler proposes a `FileOp` to the local Raft engine.
3. If this node is the **leader**: the op is appended to the Raft log and replicated to all followers (`AppendEntries` RPC).
4. Once a majority acknowledges, the entry is **committed** and applied to every node's in-memory inode tree.
5. The FUSE handler returns success.
6. If this node is a **follower**: the request is forwarded to the current leader and the result is returned asynchronously.

### Read path

Reads (lookup, getattr, read, readdir) go directly to the local in-memory store — no network round-trip. Because the store is a deterministic replay of the committed Raft log, all nodes have an eventually-consistent view within one heartbeat interval (50 ms default).

### Cluster membership

- Each node dials every peer listed in `tinycfs.conf` and maintains a persistent TCP connection with automatic reconnect.
- Incoming connections must present the same `cluster_name` during the Hello handshake.
- Node IDs are stable 64-bit values derived from the node name via SHA-256.

---

## Limitations (v0.1)

- **Small files only**: All file data is held in RAM. Not suitable for large files or high-throughput workloads.
- **Static membership**: The node list is read from config at startup. Online add/remove of nodes is not yet supported.
- **No persistence**: Raft log and filesystem state are lost on restart. A snapshot/journal mechanism is planned.
- **No encryption**: Cluster traffic is plaintext TCP. Use a VPN or private network.

---

## License

Apache-2.0
