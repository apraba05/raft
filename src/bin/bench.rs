//! Closed-loop benchmark: N clients, each issuing one request at a time.
//!
//!   bench --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 --clients 64

use clap::Parser;
use hdrhistogram::Histogram;
use raftkv::client::Clerk;
use rand::Rng;
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    /// Node addresses in id order (node 1 first).
    #[arg(long, value_delimiter = ',')]
    addrs: Vec<String>,
    #[arg(long, default_value_t = 64)]
    clients: usize,
    /// Measured seconds (after warmup).
    #[arg(long, default_value_t = 10)]
    duration: u64,
    #[arg(long, default_value_t = 2)]
    warmup: u64,
    #[arg(long, default_value_t = 128)]
    value_size: usize,
    #[arg(long, default_value_t = 1000)]
    keys: usize,
    /// Fraction of operations that are Gets.
    #[arg(long, default_value_t = 0.0)]
    read_ratio: f64,
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let start = Instant::now();
    let (measure_from, end) =
        (start + Duration::from_secs(a.warmup), start + Duration::from_secs(a.warmup + a.duration));
    let mut tasks = vec![];
    for _ in 0..a.clients {
        let (addrs, value, keys, read_ratio) =
            (a.addrs.clone(), vec![b'x'; a.value_size], a.keys, a.read_ratio);
        tasks.push(tokio::spawn(async move {
            let mut clerk = Clerk::new(&addrs);
            let mut hist = Histogram::<u64>::new(3).unwrap();
            loop {
                let key = format!("key{}", rand::thread_rng().gen_range(0..keys));
                let read = rand::thread_rng().gen_bool(read_ratio);
                let t = Instant::now();
                if t >= end {
                    return hist;
                }
                if read {
                    clerk.get(&key).await;
                } else {
                    clerk.put(&key, value.clone()).await;
                }
                if t >= measure_from && Instant::now() <= end {
                    hist.record(t.elapsed().as_micros() as u64).unwrap();
                }
            }
        }));
    }
    let mut hist = Histogram::<u64>::new(3).unwrap();
    for t in tasks {
        hist.add(t.await.unwrap()).unwrap();
    }
    let ms = |q: f64| hist.value_at_quantile(q) as f64 / 1000.0;
    println!(
        "nodes={} clients={} value={}B read_ratio={} duration={}s",
        a.addrs.len(),
        a.clients,
        a.value_size,
        a.read_ratio,
        a.duration
    );
    println!(
        "ops/sec={:.0}  p50={:.2}ms  p90={:.2}ms  p99={:.2}ms  p99.9={:.2}ms  max={:.2}ms",
        hist.len() as f64 / a.duration as f64,
        ms(0.5),
        ms(0.9),
        ms(0.99),
        ms(0.999),
        hist.max() as f64 / 1000.0
    );
}
