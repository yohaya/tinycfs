# CLAUDE.md — tinycfs project rules

## Automated actions after every code change

After modifying **any** Rust source file (`src/**/*.rs`, `Cargo.toml`, `Cargo.lock`):

### 1 — Trigger the GitHub CI build

Commit the changes and push to `main`.  The push automatically triggers
`.github/workflows/build.yml`, which compiles release binaries inside
Docker containers for:

| Target | Distro | glibc |
|---|---|---|
| `debian8` | Debian 8 (jessie) | 2.19 |
| `debian9` | Debian 9 (stretch) | 2.24 |
| `ubuntu22` | Ubuntu 22.04 LTS | 2.35 |
| `ubuntu24` | Ubuntu 24.04 LTS | 2.38 |

To trigger manually without pushing:
```bash
gh workflow run build.yml
```

### 2 — Run the local cluster pressure-test simulation

```bash
cargo run --bin sim -- \
  --nodes 20 \
  --rps 5000 \
  --delay-min-ms 5 \
  --delay-max-ms 10 \
  --duration-secs 30
```

This spawns 20 in-process cluster nodes on `127.0.0.1`, injects 5–10 ms of
artificial network latency on every inter-node message, fires 5 000
proposals/second from random nodes, and prints a summary:

```
Nodes:        20
Duration:     30 s
Total ops:    150 000
Succeeded:    149 812  (99.87 %)
Failed:       188
Throughput:   4 994 ops/s
p50 latency:  8.2 ms
p95 latency:  22.4 ms
p99 latency:  47.1 ms
Consistency:  PASS — all 20 stores converge to identical state
```

The simulation **must pass** (consistency: PASS, success rate > 95%) before
the code is considered ready for review.

---

## Repository layout

| Path | Purpose |
|---|---|
| `src/cluster/` | TCP cluster transport, peer management, delay injection |
| `src/consensus/` | Raft actor engine — batching, parallel per-peer replication |
| `src/fs/` | FUSE filesystem + in-memory inode state machine |
| `src/bin/sim.rs` | Cluster pressure-test simulation binary |
| `.github/workflows/build.yml` | Cross-distro release build CI |
| `scripts/post-change.sh` | Hook script: push + simulate after code changes |
| `tinycfs.conf.example` | Example JSON configuration |

## Build requirements

- **Linux**: `libfuse-dev`, `pkg-config`
- **macOS** (dev only): macFUSE — set `PKG_CONFIG_PATH=/usr/local/lib/pkgconfig`
- **Rust**: stable ≥ 1.70

## Running tests

```bash
# Unit tests
cargo test

# Full cluster simulation (no FUSE required, no root required)
cargo run --bin sim

# Release build
cargo build --release
```

## Configuration format (`tinycfs.conf`)

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

`local_node` must match one entry in `nodes`.  The full `nodes` list is
identical on every machine in the cluster.

## Code conventions

- All cluster state mutation goes through the Raft actor task (no Mutex on hot path).
- Write proposals are batched: accumulate up to `MAX_ENTRIES_PER_RPC = 256` ops
  per `AppendEntries` RPC.
- Reads serve directly from the local `FileStore` (no network round-trip).
- Network delay for tests is injected at the writer-task level in `src/cluster/mod.rs`.
