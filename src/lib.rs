//! raftkv: a fault-tolerant, linearizable key-value store on Raft, over gRPC.
//!
//! * [`storage`] — WAL + snapshot file
//! * [`raft`]    — consensus: election, replication, snapshots
//! * [`kv`]      — the replicated map, duplicate detection
//! * [`net`]     — gRPC services, peer client, fault injection
//! * [`client`]  — `Clerk`, the client library
//! * [`check`]   — linearizability checker
//! * [`sim`]     — in-process cluster + randomized fault injection (tests, fuzz)

pub mod check;
pub mod client;
pub mod kv;
pub mod net;
pub mod raft;
pub mod sim;
pub mod storage;

pub mod pb {
    tonic::include_proto!("raftkv");
}

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// One running server: Raft plus the KV state machine on top of it.
#[derive(Clone)]
pub struct Node {
    pub raft: Arc<raft::Raft>,
    pub kv: Arc<kv::KvStore>,
}

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub raft: raft::Config,
    pub fsync: bool,
    pub snapshot_every: u64,
    pub request_timeout: Duration,
}

impl NodeOptions {
    pub fn new(id: u64, nodes: Vec<u64>) -> NodeOptions {
        NodeOptions {
            raft: raft::Config::new(id, nodes),
            fsync: true,
            snapshot_every: 10_000,
            request_timeout: Duration::from_secs(1),
        }
    }
}

impl Node {
    /// Recovers state from `dir` and starts the node. `addrs` maps every node
    /// id to its address. Must be called inside a tokio runtime.
    pub fn start(
        opts: NodeOptions,
        dir: &Path,
        addrs: &HashMap<u64, String>,
        faults: Option<Arc<net::Faults>>,
    ) -> std::io::Result<Node> {
        let (storage, recovered) = storage::Storage::open(dir, opts.fsync)?;
        let peers = net::Peers::new(opts.raft.id, addrs, faults);
        let (raft, applies) = raft::Raft::start(opts.raft, storage, recovered, peers);
        let kv = kv::KvStore::start(raft.clone(), applies, opts.snapshot_every, opts.request_timeout);
        Ok(Node { raft, kv })
    }

    /// Simulated crash (see `Raft::kill`).
    pub fn kill(&self) {
        self.raft.kill();
    }
}
