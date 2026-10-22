//! Randomized fault injection + linearizability checking.
//!
//!   fuzz --iters 500 --nodes 5 --seed 1
//!
//! Every iteration is a fresh in-process cluster (real gRPC over loopback)
//! with crashes, restarts, partitions, message loss and delay. A failure
//! prints its seed; `--seed S --iters 1` reruns that configuration. (The seed
//! fixes the fault schedule, not the thread interleaving.)

use clap::Parser;
use raftkv::sim::{fuzz_once, FuzzOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 100)]
    iters: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    #[arg(long, default_value_t = 8)]
    clients: usize,
    /// Seconds of faults per iteration.
    #[arg(long, default_value_t = 3)]
    secs: u64,
    #[arg(long, default_value_t = 4)]
    parallel: usize,
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let opt = FuzzOptions {
        nodes: a.nodes,
        clients: a.clients,
        duration: Duration::from_secs(a.secs),
        ..Default::default()
    };
    let sem = Arc::new(Semaphore::new(a.parallel));
    let started = Instant::now();
    let mut runs = vec![];
    for seed in a.seed..a.seed + a.iters {
        let (sem, opt) = (sem.clone(), opt.clone());
        runs.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            fuzz_once(seed, &opt).await
        }));
    }
    let (mut passed, mut ops) = (0, 0);
    for r in runs {
        let r = r.await.unwrap();
        ops += r.ops;
        match &r.outcome {
            Ok(()) => {
                passed += 1;
                println!("seed {:>5}: ok   ({} ops, {} faults)", r.seed, r.ops, r.faults.len());
            }
            Err(e) => {
                println!("seed {:>5}: FAIL {e}\n  faults: {:#?}", r.seed, r.faults);
            }
        }
    }
    println!(
        "\n{passed}/{} iterations linearizable ({} nodes, {ops} ops checked, {:.0?})",
        a.iters,
        a.nodes,
        started.elapsed()
    );
    if passed != a.iters {
        std::process::exit(1);
    }
}
