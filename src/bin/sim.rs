/// tinycfs cluster pressure-test simulation.
///
/// Spawns N in-process cluster nodes on localhost with configurable artificial
/// network delay on all inter-node messages. Fires write proposals at a target
/// requests-per-second rate from random nodes, then reports:
///   - Throughput and success rate
///   - Latency percentiles (p50 / p95 / p99)
///   - Consistency check: all stores converge to identical state
///
/// Usage (see CLAUDE.md):
///   cargo run --bin sim -- --nodes 20 --rps 5000 --delay-min-ms 5 --delay-max-ms 10
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use clap::Parser;
use parking_lot::RwLock;
use rand::Rng;
use tokio::time;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

// Pull in the library modules from the main crate.
// `sim` lives in src/bin/sim.rs so it can access crate:: items the same way
// as main.rs does.
use tinycfs::cluster::{Cluster, ClusterOptions};
use tinycfs::config::{Config, NodeConfig};
use tinycfs::consensus::Consensus;
use tinycfs::cluster::message::FileOp;
use tinycfs::fs::store::FileStore;

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
}

// ─── Per-node handle ──────────────────────────────────────────────────────────

struct SimNode {
    name: String,
    consensus: Consensus,
    store: Arc<RwLock<FileStore>>,
}

// ─── Latency histogram (lock-free, microsecond buckets) ───────────────────────

struct Histogram {
    /// Buckets: index i covers [i*100 µs, (i+1)*100 µs)
    /// Up to 5 000 ms (50 000 * 100 µs).
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
        if total == 0 {
            return Duration::ZERO;
        }
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

    if args.nodes < 3 {
        eprintln!("ERROR: --nodes must be at least 3 (Raft needs an odd quorum ≥ 3)");
        std::process::exit(1);
    }

    info!(
        "Simulation: {} nodes, {} rps, delay {}-{} ms, {} s (+{} s warmup)",
        args.nodes, args.rps,
        args.delay_min_ms, args.delay_max_ms,
        args.duration_secs, args.warmup_secs,
    );

    // ── Build per-node configs ────────────────────────────────────────────
    let node_configs: Vec<NodeConfig> = (0..args.nodes)
        .map(|i| NodeConfig {
            name: format!("sim-{}", i),
            ip: "127.0.0.1".to_string(),
            port: args.base_port + i as u16,
        })
        .collect();

    let cluster_opts = ClusterOptions {
        network_delay: Some((
            Duration::from_millis(args.delay_min_ms),
            Duration::from_millis(args.delay_max_ms),
        )),
    };

    // ── Start all nodes ───────────────────────────────────────────────────
    let mut nodes: Vec<SimNode> = Vec::with_capacity(args.nodes);

    for i in 0..args.nodes {
        let config = Config {
            cluster_name: "sim".to_string(),
            local_node: format!("sim-{}", i),
            data_dir: None,
            nodes: node_configs.clone(),
        };

        let (cluster_handle, _cluster) =
            Cluster::start_with_opts(config.clone(), cluster_opts.clone()).await;

        let (consensus, mut apply_rx, msg_tx) = Consensus::new(cluster_handle.clone());

        // Forward inbound cluster messages to the Raft actor
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

        // Apply committed log entries to the local FileStore
        let store = Arc::new(RwLock::new(FileStore::new()));
        let store2 = store.clone();
        tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                let mut st = store2.write();
                if let Err(e) = st.apply(&entry.op) {
                    // AlreadyExists is expected on idempotent replayed creates
                    if !e.to_string().contains("Already exists") {
                        warn!("State machine: {}", e);
                    }
                }
            }
        });

        nodes.push(SimNode {
            name: format!("sim-{}", i),
            consensus,
            store,
        });
    }

    // ── Wait for leader election ──────────────────────────────────────────
    let warmup = Duration::from_secs(args.warmup_secs);
    info!("Waiting {}s for cluster to stabilise…", args.warmup_secs);
    time::sleep(warmup).await;

    // Verify leader elected
    let leader_found = nodes.iter().any(|n| n.consensus.is_leader());
    if !leader_found {
        error!("No leader elected after {}s — check ports and network settings", args.warmup_secs);
        std::process::exit(1);
    }
    info!("Leader elected. Starting load test…");

    // ── Load test ─────────────────────────────────────────────────────────
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let hist = Histogram::new();

    // Share node consensus handles
    let consensuses: Vec<Consensus> = nodes.iter().map(|n| n.consensus.clone()).collect();
    let consensuses = Arc::new(consensuses);

    let duration = Duration::from_secs(args.duration_secs);
    let interval_ns = 1_000_000_000u64 / args.rps;
    let start = Instant::now();
    let end_time = start + duration;

    let mut seq: u64 = 0;
    let mut next_tick = Instant::now();

    while Instant::now() < end_time {
        if Instant::now() < next_tick {
            // Yield to allow tokio tasks to run (heartbeats, etc.)
            tokio::task::yield_now().await;
            continue;
        }
        next_tick += Duration::from_nanos(interval_ns);
        seq += 1;

        // Pick a random node to submit through (tests follower forwarding too)
        let node_idx = rand::thread_rng().gen_range(0..consensuses.len());
        let consensus = consensuses[node_idx].clone();
        let ok = ok_count.clone();
        let err = err_count.clone();
        let h = hist.clone();

        tokio::spawn(async move {
            let t = Instant::now();
            let path = format!("/file-{}.txt", seq % 1000);
            let op = FileOp::Write {
                path,
                offset: 0,
                data: format!("seq={}", seq).into_bytes(),
            };
            match consensus.propose(op).await {
                Ok(()) => {
                    h.record(t.elapsed());
                    ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // NotLeader / NoQuorum during leader election are expected
                    let _ = e;
                    err.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    let total_elapsed = start.elapsed();

    // ── Wait for all proposals to flush ──────────────────────────────────
    info!("Load test done. Waiting 2s for final commits to flush…");
    time::sleep(Duration::from_secs(2)).await;

    // ── Consistency check ─────────────────────────────────────────────────
    // Hash each node's store and compare
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let hashes: Vec<u64> = nodes
        .iter()
        .map(|n| {
            let store = n.store.read();
            let mut h = DefaultHasher::new();
            // We hash a sorted representation of all file paths + data
            let mut paths: Vec<(String, Vec<u8>)> = Vec::new();
            for key in 1u64..1000 {
                let path = format!("/file-{}.txt", key % 1000);
                if let Some(ino) = store.lookup(1, &path.trim_start_matches('/')) {
                    if let Some(tinycfs::fs::inode::Inode::File(f)) = store.get(ino) {
                        paths.push((path, f.data.clone()));
                    }
                }
            }
            paths.sort_by(|a, b| a.0.cmp(&b.0));
            paths.hash(&mut h);
            h.finish()
        })
        .collect();

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
    println!(" Nodes:          {}", args.nodes);
    println!(" Network delay:  {}–{} ms (one-way)", args.delay_min_ms, args.delay_max_ms);
    println!(" Target RPS:     {}", args.rps);
    println!(" Duration:       {:.1} s", total_elapsed.as_secs_f64());
    println!("───────────────────────────────────────────────────────");
    println!(" Total ops:      {}", total);
    println!(" Succeeded:      {}  ({:.2}%)", ok, success_pct);
    println!(" Failed:         {}", err);
    println!(" Throughput:     {:.0} ops/s", throughput);
    println!("───────────────────────────────────────────────────────");
    println!(" Latency p50:    {:.1} ms", hist.percentile(50.0).as_secs_f64() * 1000.0);
    println!(" Latency p95:    {:.1} ms", hist.percentile(95.0).as_secs_f64() * 1000.0);
    println!(" Latency p99:    {:.1} ms", hist.percentile(99.0).as_secs_f64() * 1000.0);
    println!(" Latency mean:   {:.1} ms", hist.mean().as_secs_f64() * 1000.0);
    println!("───────────────────────────────────────────────────────");
    println!(
        " Consistency:    {}",
        if all_same { "PASS ✓ — all stores converge to identical state" }
        else        { "FAIL ✗ — stores diverged! (possible Raft bug)" }
    );
    println!("═══════════════════════════════════════════════════════");
    println!();

    // Exit with non-zero if consistency failed or success rate < 95%
    if !all_same || success_pct < 95.0 {
        std::process::exit(1);
    }
}
