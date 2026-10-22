//! A raftkv server process.
//!
//!   node --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --dir run/1

use clap::Parser;
use raftkv::net::{self, Slot};
use raftkv::{Node, NodeOptions};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// This node's id (must appear in --peers).
    #[arg(long)]
    id: u64,
    /// Every node in the cluster as id=host:port, comma separated.
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,
    /// Data directory.
    #[arg(long)]
    dir: PathBuf,
    /// Take a snapshot once the log holds this many entries (0 = never).
    #[arg(long, default_value_t = 10_000)]
    snapshot_every: u64,
    /// Skip fsync (unsafe; for comparison only).
    #[arg(long)]
    no_fsync: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let addrs: HashMap<u64, String> = args
        .peers
        .iter()
        .map(|p| {
            let (id, addr) = p.split_once('=').expect("peer must be id=host:port");
            (id.parse().expect("bad peer id"), addr.to_string())
        })
        .collect();
    let mut opts = NodeOptions::new(args.id, addrs.keys().copied().collect());
    opts.fsync = !args.no_fsync;
    opts.snapshot_every = args.snapshot_every;

    let listener = tokio::net::TcpListener::bind(&addrs[&args.id]).await.expect("bind");
    let node = Node::start(opts, &args.dir, &addrs, None).expect("start node");
    let slot = Slot::default();
    slot.set(Some(node));
    eprintln!("node {} listening on {}", args.id, addrs[&args.id]);
    net::serve(listener, slot, None).await;
}
