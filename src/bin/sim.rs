/// tinycfs cluster pressure-test simulation.
///
/// Spawns N in-process cluster nodes on localhost with configurable artificial
/// network delay, packet loss, and optional network partition injection.
/// Fires write proposals at a target RPS rate, then reports throughput,
/// latency percentiles, leader election time, and a consistency check.
///
/// Benchmark mode (--pre-populate):
///   Pre-populates the store with N files, then runs the load test.  Use
///   --snapshot-every 1 to reproduce the old "persist on every write" behaviour
///   and compare against the default (--snapshot-every 10000).
///
/// Usage:
///   cargo run --bin sim -- --nodes 20 --rps 5000 --delay-min-ms 5 --delay-max-ms 10
///   cargo run --bin sim -- --nodes 5 --pre-populate 3000 --with-db   # large-store bench
///   cargo run --bin sim -- --nodes 5 --pre-populate 3000 --with-db --snapshot-every 1  # slow path
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use clap::Parser;
use parking_lot::RwLock;
use rand::Rng;
use tokio::time;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use tinycfs::cluster::{Cluster, ClusterOptions};
use tinycfs::cluster::message::{FileOp, NodeId};
use tinycfs::config::{Config, ConsensusAlgorithm, NodeConfig};
use tinycfs::consensus::Consensus;
use tinycfs::fs::store::FileStore;
use tinycfs::persistence::RaftDb;

// ─── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "sim", about = "tinycfs cluster simulation / pressure test")]
struct Args {
    /// Number of cluster nodes to spawn on localhost.
    #[arg(long, default_value = "10")]
    nodes: usize,

    /// Target aggregate write proposals per second across all nodes.
    #[arg(long, default_value = "2000")]
    rps: u64,

    /// Minimum simulated one-way network delay (milliseconds).
    #[arg(long, default_value = "5")]
    delay_min_ms: u64,

    /// Maximum simulated one-way network delay (milliseconds).
    #[arg(long, default_value = "10")]
    delay_max_ms: u64,

    /// How long to run the load test (seconds).
    #[arg(long, default_value = "30")]
    duration_secs: u64,

    /// Base TCP port; nodes bind to BASE_PORT + index.
    #[arg(long, default_value = "17800")]
    base_port: u16,

    /// Warm-up period before measuring (seconds).
    #[arg(long, default_value = "3")]
    warmup_secs: u64,

    /// Consensus algorithm: "raft" (default) or "totem".
    #[arg(long, default_value = "raft")]
    algorithm: String,

    /// Drop this percentage of outbound messages to simulate packet loss (0–100).
    #[arg(long, default_value = "0")]
    packet_loss_pct: f64,

    /// Inject a network partition at this many seconds after the load test starts.
    /// The first --partition-size nodes are isolated from the rest.
    #[arg(long)]
    partition_at_secs: Option<u64>,

    /// Heal the partition at this many seconds after the load test starts.
    #[arg(long)]
    heal_at_secs: Option<u64>,

    /// How many nodes to put in the minority partition (default: half).
    #[arg(long)]
    partition_size: Option<usize>,

    // ── Benchmark / persistence options ────────────────────────────────────

    /// Pre-populate the store with this many files before the load test.
    /// Used to benchmark performance with a large FileStore.
    #[arg(long, default_value = "0")]
    pre_populate: usize,

    /// Override the snapshot_every (log compaction interval) for all nodes.
    /// Set to 1 to reproduce the old "write full snapshot on every commit"
    /// behaviour — useful as a before/after benchmark comparison.
    #[arg(long, default_value = "10000")]
    snapshot_every: u64,

    /// Override the persist_every (SQLite snapshot write interval) for all nodes.
    /// Defaults to snapshot_every.  Set to 1 for maximum durability (slow for large stores).
    #[arg(long)]
    persist_every: Option<u64>,

    /// Enable per-node SQLite persistence using temporary directories.
    /// Without this flag nodes run purely in-memory (faster, no I/O overhead).
    #[arg(long)]
    with_db: bool,
}

// ─── Per-node handle ──────────────────────────────────────────────────────────

struct SimNode {
    consensus: Consensus,
    store: Arc<RwLock<FileStore>>,
    db_dir: Option<tempfile::TempDir>,
}

// ─── Latency histogram (lock-free, microsecond buckets) ───────────────────────

struct Histogram {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Histogram {
    fn new() -> Arc<Self> {
        let mut b = Vec::with_capacity(50_000);
        for _ in 0..50_000 {
            b.push(AtomicU64::new(0));
        }
        Arc::new(Histogram { buckets: b, count: AtomicU64::new(0), sum_us: AtomicU64::new(0) })
    }

    fn record(&self, elapsed: Duration) {
        let us = elapsed.as_micros() as u64;
        let idx = (us / 100).min(self.buckets.len() as u64 - 1) as usize;
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    fn percentile(&self, pct: f64) -> Duration {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 { return Duration::ZERO; }
        let target = ((total as f64) * pct / 100.0).ceil() as u64;
        let mut acc = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            acc += b.load(Ordering::Relaxed);
            if acc >= target {
                return Duration::from_micros((i as u64 + 1) * 100);
            }
        }
        Duration::from_millis(5000)
    }

    fn mean(&self) -> Duration {
        let c = self.count.load(Ordering::Relaxed);
        if c == 0 { return Duration::ZERO; }
        Duration::from_micros(self.sum_us.load(Ordering::Relaxed) / c)
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("warn,sim=info,tinycfs=warn"))
        .init();

    let args = Args::parse();

    let algorithm = match args.algorithm.to_lowercase().as_str() {
        "totem" => ConsensusAlgorithm::Totem,
        _ => ConsensusAlgorithm::Raft,
    };

    if args.nodes < 3 {
        eprintln!("ERROR: --nodes must be at least 3 (consensus needs an odd quorum >= 3)");
        std::process::exit(1);
    }

    let persist_every = args.persist_every.unwrap_or(args.snapshot_every);

    info!(
        "Simulation: {} nodes, algorithm={:?}, {} rps, delay {}-{} ms, \
         packet-loss {:.1}%, duration {} s (+{} s warmup), \
         snapshot_every={}, persist_every={}, db={}",
        args.nodes, algorithm, args.rps,
        args.delay_min_ms, args.delay_max_ms, args.packet_loss_pct,
        args.duration_secs, args.warmup_secs,
        args.snapshot_every, persist_every,
        if args.with_db { "enabled" } else { "disabled (in-memory)" },
    );

    // ── Build per-node configs ─────────────────────────────────────────────
    let node_configs: Vec<NodeConfig> = (0..args.nodes)
        .map(|i| NodeConfig {
            name: format!("sim-{}", i),
            ip: "127.0.0.1".to_string(),
            port: args.base_port + i as u16,
            observer: false,
        })
        .collect();

    // Pre-compute node IDs (used for partition injection).
    let node_ids: Vec<NodeId> = (0..args.nodes)
        .map(|i| tinycfs::config::Config::node_id(&format!("sim-{}", i)))
        .collect();

    // Each node gets its own ClusterOptions with a distinct blocked_peers Arc.
    let all_opts: Vec<ClusterOptions> = (0..args.nodes)
        .map(|_| ClusterOptions {
            network_delay: Some((
                Duration::from_millis(args.delay_min_ms),
                Duration::from_millis(args.delay_max_ms),
            )),
            packet_loss: args.packet_loss_pct / 100.0,
            blocked_peers: Arc::new(RwLock::new(HashSet::new())),
        })
        .collect();

    // ── Start all nodes ────────────────────────────────────────────────────
    let mut nodes: Vec<SimNode> = Vec::with_capacity(args.nodes);

    for i in 0..args.nodes {
        // Create temporary DB directory if persistence is enabled.
        let (db_dir, db) = if args.with_db {
            let dir = tempfile::TempDir::new().expect("failed to create temp dir for DB");
            let db_path = dir.path().join("raft.db");
            let db = RaftDb::open(&db_path).expect("failed to open raft.db");
            (Some(dir), Some(db))
        } else {
            (None, None)
        };

        let config = Config {
            cluster_name: "sim".to_string(),
            local_node: format!("sim-{}", i),
            data_dir: None,
            algorithm: algorithm.clone(),
            max_file_size_bytes: 1 * 1024 * 1024, // 1 MiB per file in sim
            max_fs_size_bytes: 4 * 1024 * 1024 * 1024,
            snapshot_every: args.snapshot_every as usize,
            persist_every: persist_every as usize,
            mountpoint: None,
            noexec: true,
            nosuid: true,
            nodev: true,
            nodes: node_configs.clone(),
        };

        let (cluster_handle, _cluster) =
            Cluster::start_with_opts(config.clone(), all_opts[i].clone()).await;

        let store = Arc::new(RwLock::new(FileStore::new()));
        let (consensus, msg_tx) = Consensus::new(
            algorithm.clone(),
            cluster_handle.clone(),
            db,
            store.clone(),
            config.snapshot_every as u64,
            config.persist_every as u64,
            0, // no per-file size limit in simulation
            0, // no total-fs size limit in simulation
        );

        // Forward inbound cluster messages to the consensus actor.
        let rx_in = cluster_handle.rx_in.clone();
        tokio::spawn(async move {
            loop {
                let env = rx_in.lock().await.recv().await;
                match env {
                    Some(e) => { if msg_tx.send(e).await.is_err() { break; } }
                    None => break,
                }
            }
        });

        nodes.push(SimNode { consensus, store, db_dir });
    }

    // ── Wait for leader election ───────────────────────────────────────────
    info!("Waiting up to {}s for cluster to stabilise…", args.warmup_secs);
    let election_timer = Instant::now();
    let warmup_deadline = time::Instant::now() + Duration::from_secs(args.warmup_secs);

    let election_time = loop {
        if nodes.iter().any(|n| n.consensus.is_leader()) {
            break election_timer.elapsed();
        }
        if time::Instant::now() >= warmup_deadline {
            eprintln!(
                "ERROR: No leader elected after {}s — check ports/network settings",
                args.warmup_secs
            );
            std::process::exit(1);
        }
        time::sleep(Duration::from_millis(10)).await;
    };
    info!("Leader elected in {:.0} ms", election_time.as_millis());

    // Finish out warmup.
    let elapsed = election_timer.elapsed();
    if elapsed < Duration::from_secs(args.warmup_secs) {
        time::sleep(Duration::from_secs(args.warmup_secs) - elapsed).await;
    }

    // ── Pre-population (benchmark mode) ───────────────────────────────────
    let pre_populate_time = if args.pre_populate > 0 {
        info!("Pre-populating store with {} files…", args.pre_populate);
        let t0 = Instant::now();
        // Use the first node that is (or knows) a leader.
        let writer = nodes.iter()
            .find(|n| n.consensus.is_leader() || n.consensus.has_quorum())
            .map(|n| n.consensus.clone())
            .unwrap_or_else(|| nodes[0].consensus.clone());

        for i in 0..args.pre_populate {
            let path = format!("/vm/{}/2024-01-01T00:00:00Z 10GiB", i);
            let op = FileOp::Write {
                path,
                offset: 0,
                data: format!("vm snapshot entry {} metadata text", i).into_bytes(),
            };
            if let Err(e) = writer.propose(op).await {
                warn!("Pre-populate write {}: {}", i, e);
            }
            if (i + 1) % 500 == 0 {
                info!("  pre-populated {}/{} files", i + 1, args.pre_populate);
            }
        }
        let t = t0.elapsed();
        let rate = args.pre_populate as f64 / t.as_secs_f64();
        info!(
            "Pre-populated {} files in {:.1}s ({:.0} writes/s)",
            args.pre_populate, t.as_secs_f64(), rate
        );
        Some((t, rate))
    } else {
        None
    };

    info!("Starting load test…");

    // ── Partition injection ────────────────────────────────────────────────
    let partition_size = args.partition_size.unwrap_or(args.nodes / 2);

    if let Some(at_secs) = args.partition_at_secs {
        let opts = all_opts.clone();
        let ids = node_ids.clone();
        let n = args.nodes;
        let ps = partition_size;
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(at_secs)).await;
            for i in 0..ps {
                for j in ps..n {
                    opts[i].blocked_peers.write().insert(ids[j]);
                    opts[j].blocked_peers.write().insert(ids[i]);
                }
            }
            warn!("═══ PARTITION at {}s: nodes 0–{} isolated from {}–{} ═══", at_secs, ps-1, ps, n-1);
        });
    }
    if let Some(heal_secs) = args.heal_at_secs {
        let opts = all_opts.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(heal_secs)).await;
            for opt in &opts { opt.blocked_peers.write().clear(); }
            warn!("═══ PARTITION HEALED at {}s ═══", heal_secs);
        });
    }

    // ── Load test ─────────────────────────────────────────────────────────
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let hist = Histogram::new();

    let consensuses: Arc<Vec<Consensus>> =
        Arc::new(nodes.iter().map(|n| n.consensus.clone()).collect());

    let duration = Duration::from_secs(args.duration_secs);
    let interval_ns = 1_000_000_000u64 / args.rps.max(1);
    let start = Instant::now();
    let end_time = start + duration;

    let mut seq: u64 = 0;
    let mut next_tick = Instant::now();

    while Instant::now() < end_time {
        if Instant::now() < next_tick {
            tokio::task::yield_now().await;
            continue;
        }
        next_tick += Duration::from_nanos(interval_ns);
        seq += 1;

        let node_idx = rand::thread_rng().gen_range(0..consensuses.len());
        let consensus = consensuses[node_idx].clone();
        let ok = ok_count.clone();
        let err = err_count.clone();
        let h = hist.clone();

        tokio::spawn(async move {
            let t = Instant::now();
            // Mix writes and creates to exercise different code paths.
            let path = format!("/file-{}.txt", seq % 1000);
            let op = FileOp::Write {
                path,
                offset: 0,
                data: format!("seq={}", seq).into_bytes(),
            };
            match consensus.propose(op).await {
                Ok(()) => { h.record(t.elapsed()); ok.fetch_add(1, Ordering::Relaxed); }
                Err(e) => { warn!("Proposal error: {}", e); err.fetch_add(1, Ordering::Relaxed); }
            }
        });
    }

    let total_elapsed = start.elapsed();
    info!("Load test done. Waiting 2s for final commits to flush…");
    time::sleep(Duration::from_secs(2)).await;

    // ── Consistency check ──────────────────────────────────────────────────
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let hashes: Vec<u64> = nodes.iter().map(|n| {
        let store = n.store.read();
        let mut h = DefaultHasher::new();
        let mut paths: Vec<(String, Vec<u8>)> = Vec::new();
        for key in 0u64..1000 {
            let path_str = format!("file-{}.txt", key);
            if let Some(ino) = store.lookup(1, &path_str) {
                if let Some(tinycfs::fs::inode::Inode::File(f)) = store.get(ino) {
                    paths.push((path_str, f.data.clone()));
                }
            }
        }
        paths.sort_by(|a, b| a.0.cmp(&b.0));
        paths.hash(&mut h);
        h.finish()
    }).collect();

    let all_same = hashes.windows(2).all(|w| w[0] == w[1]);

    // ── Results ───────────────────────────────────────────────────────────
    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let total = ok + err;
    let success_pct = if total > 0 { ok as f64 / total as f64 * 100.0 } else { 0.0 };
    let throughput = ok as f64 / total_elapsed.as_secs_f64();

    println!();
    println!("═══════════════════════════════════════════════════════");
    println!(" tinycfs Simulation Results");
    println!("═══════════════════════════════════════════════════════");
    println!(" Nodes:            {}", args.nodes);
    println!(" Algorithm:        {:?}", algorithm);
    println!(" Network delay:    {}–{} ms (one-way)", args.delay_min_ms, args.delay_max_ms);
    println!(" Packet loss:      {:.1}%", args.packet_loss_pct);
    println!(" Persistence:      {} (snapshot_every={}, persist_every={})",
        if args.with_db { "SQLite" } else { "in-memory" }, args.snapshot_every, persist_every);
    if let Some(at) = args.partition_at_secs {
        println!(" Partition:        at {}s (size {}), heal {}",
            at, partition_size,
            args.heal_at_secs.map(|h| format!("at {}s", h)).unwrap_or_else(|| "never".into()));
    }
    println!(" Target RPS:       {}", args.rps);
    println!(" Duration:         {:.1} s", total_elapsed.as_secs_f64());
    println!("───────────────────────────────────────────────────────");
    if let Some((t, rate)) = pre_populate_time {
        println!(" Pre-populate:     {} files in {:.1}s ({:.0} writes/s)",
            args.pre_populate, t.as_secs_f64(), rate);
    }
    println!(" Leader elected:   {:.0} ms", election_time.as_millis());
    println!(" Total ops:        {}", total);
    println!(" Succeeded:        {}  ({:.2}%)", ok, success_pct);
    println!(" Failed:           {}", err);
    println!(" Throughput:       {:.0} ops/s", throughput);
    println!("───────────────────────────────────────────────────────");
    println!(" Latency p50:      {:.1} ms", hist.percentile(50.0).as_secs_f64() * 1000.0);
    println!(" Latency p95:      {:.1} ms", hist.percentile(95.0).as_secs_f64() * 1000.0);
    println!(" Latency p99:      {:.1} ms", hist.percentile(99.0).as_secs_f64() * 1000.0);
    println!(" Latency mean:     {:.1} ms", hist.mean().as_secs_f64() * 1000.0);
    println!("───────────────────────────────────────────────────────");
    println!(" Consistency:      {}",
        if all_same { "PASS — all stores converge to identical state" }
        else { "FAIL — stores diverged! (possible consensus bug)" });
    println!("═══════════════════════════════════════════════════════");
    println!();

    // Keep DB dirs alive until after the test (drop here to force cleanup).
    drop(nodes);

    if !all_same || success_pct < 95.0 {
        std::process::exit(1);
    }
}
