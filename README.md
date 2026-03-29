# tinycfs — Tiny Cluster File System

A distributed shared filesystem for small files, written entirely in Rust.

tinycfs combines a FUSE-based virtual filesystem with cluster membership and consensus into a single static binary with no external daemon dependencies. It supports two consensus algorithms: **Raft** (default) and **Totem SRTP** (compatible with the pmxcfs/Corosync model).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        tinycfs node                              │
│                                                                  │
│   ┌─────────────┐    ┌────────────────────────────────────────┐ │
│   │ FUSE layer  │    │     Consensus (Raft or Totem SRTP)      │ │
│   │  (fuser)    │───►│  Raft: actor model, proposal batching,  │ │
│   │             │◄───│  parallel per-peer replication          │ │
│   └─────────────┘    │  Totem: token-ring total-order          │ │
│                       │  multicast, ARU-gated delivery          │ │
│                       └───────────────┬────────────────────────┘ │
│                                       │ committed entries         │
│   ┌──────────────────────────────────▼────────────────────────┐  │
│   │         In-memory File State Machine                       │  │
│   │   (inode tree — read replica, applied in log order)        │  │
│   └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ┌────────────────────────────────────────────────────────────┐  │
│   │   SQLite Persistence (raft.db) — optional, configurable    │  │
│   └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│   ┌────────────────────────────────────────────────────────────┐  │
│   │         Cluster Transport (TCP, length-prefixed frames)    │  │
│   └────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
          ▲                   ▲                   ▲
          │  TCP 7788         │  TCP 7788         │  TCP 7788
       node1               node2               node3
```

### Key design decisions

| Concern | Approach |
|---|---|
| **Consensus** | Raft (default) or Totem SRTP — selectable per cluster |
| **Availability** | Read-only mode (EROFS) when cluster loses quorum — reads always succeed |
| **Throughput** | Raft: actor model + proposal batching (up to 256 ops/RPC); Totem: up to 64 msgs/token turn |
| **Scalability** | Raft: per-peer parallel replication; Totem: token ring, scales to ~7 nodes optimally |
| **Quorum** | Majority quorum (⌊N/2⌋ + 1 voting nodes) |
| **Reads** | Served directly from local in-memory store — no network round-trip |
| **Observers** | Non-voting replica nodes for read scale-out or DR |
| **Persistence** | Optional SQLite (`data_dir`) — stores Raft metadata + FileStore snapshot |
| **Filesystem** | FUSE via the `fuser` crate; inode-based in-memory tree |
| **Transport** | Direct TCP with 4-byte length-prefixed `bincode` frames; persistent connections |
| **Small files** | All file data kept in memory; 8 MiB per-file limit (configurable) |
| **Config** | JSON5 (`tinycfs.conf`) — supports `//` and `/* */` comments |

### Consensus algorithm comparison

| Property | Raft | Totem SRTP |
|---|---|---|
| **Leader** | Single elected leader | No leader — token rotates the ring |
| **Write latency** | 2 × leader RTT (independent of N) | (N/2) × ring RTT (scales with ring size) |
| **Leader election** | 700–1 400 ms | Ring reform ~1.5 s |
| **Optimal cluster size** | Any (scales to 20+ nodes) | 3–7 nodes (latency grows linearly) |
| **Fairness** | Leader serialises all writes | Each node gets equal token turns |
| **pmxcfs compatible** | No | Yes (same protocol family as Corosync) |
| **Crash handling** | Leader failover, quorum maintained | Ring reform, snapshot sync |

---

## Availability under cluster problems

When the cluster loses quorum (network partition, majority unreachable, or leader election in progress), tinycfs automatically switches to **read-only mode**:

- Every mutating FUSE operation (`write`, `create`, `mkdir`, `unlink`, `rename`, `setattr`, `symlink`, `setlk`) returns `EROFS` immediately — no 30-second timeout.
- Read operations (`open`, `read`, `readdir`, `getattr`, `lookup`) always succeed from the local in-memory copy regardless of cluster state.
- **Writes resume automatically** as soon as quorum is restored.

Applications should treat `EROFS` as a transient error and retry. This matches the pmxcfs behaviour.

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

`/etc/tinycfs/tinycfs.conf` on **node1** (JSON5 format — `//` comments supported):

```json5
{
  cluster_name: "mycluster",
  local_node: "node1",          // change per machine
  data_dir: "/var/lib/tinycfs", // optional: omit for in-memory only
  algorithm: "raft",            // "raft" (default) or "totem"
  nodes: [
    { name: "node1", ip: "192.168.1.10", port: 7788 },
    { name: "node2", ip: "192.168.1.11", port: 7788 },
    { name: "node3", ip: "192.168.1.12", port: 7788 },
  ],
}
```

Change `local_node: "node2"` on **node2**, `"node3"` on **node3** — the `nodes` list is identical on all machines.

**Backward compatibility**: plain JSON files (no comments) are also accepted automatically.

### 4. Run

```bash
# Create the mount point
mkdir -p /mnt/tinycfs

# Start on each node (--allow-other lets non-root users access the mount)
sudo tinycfs /mnt/tinycfs --config /etc/tinycfs/tinycfs.conf --allow-other
```

Files written on any node appear on all nodes within one Raft round-trip (~2 ms on a LAN, or ~N × RTT for Totem).

---

## Configuration reference

| Field | Required | Default | Description |
|---|---|---|---|
| `cluster_name` | ✓ | — | Shared cluster identifier; nodes with different names refuse to connect |
| `local_node` | ✓ | — | Name of **this** node (must match one entry in `nodes`) |
| `data_dir` | | `null` | Directory for SQLite persistence (`raft.db`). Omit for in-memory-only mode |
| `algorithm` | | `"raft"` | Consensus algorithm: `"raft"` or `"totem"` |
| `max_file_size_bytes` | | `8388608` | Per-file size limit in bytes (8 MiB default). Writes exceeding this return `EFBIG`. |
| `max_fs_size_bytes` | | `1073741824` | Total filesystem size limit in bytes (1 GiB default). Writes that would exceed this return `ENOSPC`. |
| `mountpoint` | | `null` | Default FUSE mount path. Used when no path is passed on the CLI. |
| `noexec` | | `true` | Disallow execution of files on the mount. |
| `nosuid` | | `true` | Disallow setuid/setgid execution. |
| `nodev` | | `true` | Disallow device file interpretation. |
| `snapshot_every` | | `10000` | Raft log compaction interval in applied entries |
| `nodes[].name` | ✓ | — | Unique node name (NodeId derived as SHA-256(name)[0..8]) |
| `nodes[].ip` | ✓ | — | IP address this node listens on |
| `nodes[].port` | | `7788` | TCP port for cluster communication |
| `nodes[].observer` | | `false` | If true: receives replication but does NOT vote |

See `tinycfs.conf.example` for the fully annotated reference with all options, trade-offs, and recommendations.

### Observer nodes

Observer nodes receive full log replication and can serve local reads, but do not vote and do not count toward quorum:

```json5
nodes: [
  { name: "node1", ip: "192.168.1.10", port: 7788 },             // voting
  { name: "node2", ip: "192.168.1.11", port: 7788 },             // voting
  { name: "node3", ip: "192.168.1.12", port: 7788 },             // voting
  { name: "observer1", ip: "192.168.2.10", port: 7788, observer: true }, // non-voting
],
```

Use observers for read scale-out, off-site DR replicas, or monitoring nodes.

### Fault tolerance

| Voting nodes | Quorum | Tolerated failures |
|---|---|---|
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

---

## How it works

### Write path

**Raft:**
1. Application writes a file via FUSE.
2. The FUSE handler checks cluster quorum; returns `EROFS` immediately if quorum is lost.
3. The op is proposed to the local Raft engine.
4. If this node is the **leader**: op is appended to the Raft log and replicated to all followers (`AppendEntries` RPC).
5. Once a majority acknowledges, the entry is **committed** and applied to every node's inode tree.
6. FUSE handler returns success.
7. If this node is a **follower**: the request is forwarded to the leader and the result returned asynchronously.

**Totem SRTP:**
1. Application writes a file via FUSE; quorum check same as above.
2. Proposal enters the local send queue.
3. When this node holds the token, it assigns a sequence number and multicasts up to 64 proposals.
4. The token circulates the ring; each node reports its ARU (All-Received-Up-To).
5. After a full rotation the coordinator confirms the global minimum ARU (`confirmed_aru`).
6. All nodes deliver messages with seq ≤ `confirmed_aru` in order — identical delivery on every node.
7. Callbacks fire and FUSE returns success.

### Read path

Reads (`lookup`, `getattr`, `read`, `readdir`) go directly to the local in-memory store — no network round-trip. All nodes have a deterministic view of the filesystem based on their current `deliver_seq`.

### Persistence

With `data_dir` set, tinycfs stores a SQLite database (`raft.db`) containing:

- **Raft metadata** (`current_term`, `voted_for`): prevents split-brain double-voting across restarts.
- **FileStore snapshot**: the full filesystem state at the last applied log index. On restart the node loads this snapshot and syncs only new entries from the leader, avoiding a full replay.

Without `data_dir`, the node runs in memory only. On restart it receives a full snapshot from the leader (traffic proportional to filesystem size). Fine for test nodes; use `data_dir` in production.

### Cluster membership

- Each node dials every peer listed in `tinycfs.conf` and maintains a persistent TCP connection with automatic reconnect.
- Incoming connections must present the same `cluster_name` during the Hello handshake.
- Node IDs are stable 64-bit values derived from the node name via SHA-256.
- Membership is static — to add or remove a node, update the config on every machine and do a rolling restart.

---

## Simulation / pressure test

```bash
# Raft — 20 nodes, 5000 rps, 5–10 ms artificial delay, 30 s
cargo run --bin sim -- --nodes 20 --rps 5000 --delay-min-ms 5 --delay-max-ms 10 --duration-secs 30

# Totem — same parameters
cargo run --bin sim -- --nodes 20 --rps 5000 --delay-min-ms 5 --delay-max-ms 10 --duration-secs 30 --algorithm totem
```

Expected output (Raft):
```
Total ops:    150000
Succeeded:    150000  (100.00%)
Throughput:   5000 ops/s
Consistency:  PASS - all stores converge to identical state
```

Expected output (Totem, 20 nodes — latency grows linearly with ring size):
```
Total ops:    ~118000
Succeeded:    ~118000  (100.00%)
Throughput:   ~3950 ops/s
Consistency:  PASS - all stores converge to identical state
```

---

## Limitations (v0.1)

- **Small files only**: All file data is held in RAM. Not suitable for large binary files (default 8 MiB limit per file; configurable via `max_file_size_bytes`).
- **Static membership**: The node list is read from config at startup. Online add/remove of nodes is not supported.
- **No encryption**: Cluster traffic is plaintext TCP. Use a VPN or private network.
- **Totem latency scales with N**: Totem write latency = (N/2) × RTT. Recommended for clusters of 3–7 nodes; use Raft for larger clusters or WAN deployments.

---

## License

Apache-2.0
