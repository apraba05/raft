//! In-process clusters over real gRPC sockets, and the randomized
//! fault-injection run used by the tests and the `fuzz` binary.

use crate::check::{self, Kind, Op, Outcome};
use crate::client::Clerk;
use crate::net::{self, Faults, Slot};
use crate::raft::Role;
use crate::{Node, NodeOptions};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct Cluster {
    pub n: usize,
    pub faults: Arc<Faults>,
    pub addrs: Vec<String>,
    pub snapshot_every: u64,
    dir: tempfile::TempDir,
    slots: Vec<Slot>,
    servers: Vec<JoinHandle<()>>,
}

impl Cluster {
    /// Starts `n` nodes (ids 1..=n) on loopback ports.
    pub async fn new(n: usize, seed: u64, snapshot_every: u64) -> Cluster {
        let faults = Arc::new(Faults::new(seed));
        let (mut addrs, mut slots, mut servers) = (vec![], vec![], vec![]);
        for _ in 0..n {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(listener.local_addr().unwrap().to_string());
            let slot = Slot::default();
            servers.push(tokio::spawn(net::serve(listener, slot.clone(), Some(faults.clone()))));
            slots.push(slot);
        }
        let c =
            Cluster { n, faults, addrs, snapshot_every, dir: tempfile::tempdir().unwrap(), slots, servers };
        for i in 1..=n as u64 {
            c.start(i);
        }
        c
    }

    /// Starts (or restarts, from its files) node `id`.
    pub fn start(&self, id: u64) {
        let nodes = (1..=self.n as u64).collect();
        let mut opts = NodeOptions::new(id, nodes);
        opts.snapshot_every = self.snapshot_every;
        opts.request_timeout = Duration::from_millis(500);
        let addrs: HashMap<u64, String> = (1..).zip(self.addrs.iter().cloned()).collect();
        let dir = self.dir.path().join(id.to_string());
        self.crash(id); // never let two instances share the directory
        let node = Node::start(opts, &dir, &addrs, Some(self.faults.clone())).unwrap();
        self.slots[id as usize - 1].set(Some(node));
    }

    /// Crashes node `id`: un-fsynced writes are lost and it stops responding.
    pub fn crash(&self, id: u64) {
        if let Some(node) = self.slots[id as usize - 1].set(None) {
            node.kill();
        }
    }

    pub fn is_up(&self, id: u64) -> bool {
        self.node(id).is_some()
    }

    pub fn node(&self, id: u64) -> Option<Node> {
        self.slots[id as usize - 1].get()
    }

    /// Ids of running nodes that currently believe they are leader, with terms.
    pub fn leaders(&self) -> Vec<(u64, u64)> {
        (1..=self.n as u64)
            .filter_map(|id| {
                let (term, role, _) = self.node(id)?.raft.status();
                (role == Role::Leader).then_some((id, term))
            })
            .collect()
    }

    /// Waits until exactly one leader exists in the highest term among
    /// `among`, and returns it.
    pub async fn wait_leader(&self, among: &[u64]) -> Option<u64> {
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut ls: Vec<_> = self.leaders().into_iter().filter(|(id, _)| among.contains(id)).collect();
            ls.sort_by_key(|&(_, t)| t);
            if let Some(&(id, term)) = ls.last() {
                if ls.iter().filter(|&&(_, t)| t == term).count() == 1 {
                    return Some(id);
                }
            }
        }
        None
    }

    pub fn clerk(&self) -> Clerk {
        let mut c = Clerk::new(&self.addrs);
        c.attempt_timeout = Duration::from_millis(600);
        c
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in 1..=self.n as u64 {
            self.crash(id);
        }
        for s in &self.servers {
            s.abort();
        }
    }
}

// ---------------------------------------------------------------- fuzzing

#[derive(Clone, Debug)]
pub struct FuzzOptions {
    pub nodes: usize,
    pub clients: usize,
    pub keys: usize,
    pub duration: Duration,
    pub snapshot_every: u64,
}

impl Default for FuzzOptions {
    fn default() -> Self {
        FuzzOptions { nodes: 3, clients: 8, keys: 4, duration: Duration::from_secs(3), snapshot_every: 200 }
    }
}

#[derive(Debug)]
pub struct FuzzReport {
    pub seed: u64,
    pub ops: usize,
    pub faults: Vec<String>,
    pub outcome: Result<(), String>,
}

/// One randomized run: concurrent clients over an unreliable network while
/// nodes crash, restart, and get partitioned; then heal, drain, and check
/// that the recorded history is linearizable.
pub async fn fuzz_once(seed: u64, opt: &FuzzOptions) -> FuzzReport {
    let mut rng = StdRng::seed_from_u64(seed);
    let cluster = Arc::new(Cluster::new(opt.nodes, seed, opt.snapshot_every).await);
    cluster.faults.set_unreliable(0.05, Duration::from_millis(10));

    let start = Instant::now();
    let now = move || start.elapsed().as_nanos() as u64;
    let stop = Arc::new(AtomicBool::new(false));
    let history = Arc::new(Mutex::new(Vec::new()));
    let mut clients = vec![];
    for c in 0..opt.clients {
        let (stop, history, cluster) = (stop.clone(), history.clone(), cluster.clone());
        let mut rng = StdRng::seed_from_u64(seed ^ (c as u64 + 1) << 32);
        let keys = opt.keys;
        clients.push(tokio::spawn(async move {
            let mut clerk = cluster.clerk();
            let mut n = 0;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let key = format!("k{}", rng.gen_range(0..keys));
                let (kind, input) = match rng.gen_range(0..10) {
                    0..=3 => (Kind::Get, vec![]),
                    4..=5 => (Kind::Put, format!("{c}.{n};").into_bytes()),
                    _ => (Kind::Append, format!("{c}.{n};").into_bytes()),
                };
                let call = now();
                let out = match kind {
                    Kind::Get => clerk.get(&key).await,
                    Kind::Put => {
                        clerk.put(&key, input.clone()).await;
                        input
                    }
                    Kind::Append => {
                        clerk.append(&key, input.clone()).await;
                        input
                    }
                };
                let ret = now();
                history.lock().unwrap().push(Op { key, kind, value: out, call, ret });
            }
        }));
    }

    // The nemesis.
    let mut log = vec![];
    let ids: Vec<u64> = (1..=opt.nodes as u64).collect();
    let deadline = Instant::now() + opt.duration;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(50..400))).await;
        let t = start.elapsed().as_millis();
        let id = *ids.choose(&mut rng).unwrap();
        match rng.gen_range(0..10) {
            0..=2 => {
                if cluster.is_up(id) {
                    cluster.crash(id);
                    log.push(format!("{t}ms crash {id}"));
                } else {
                    cluster.start(id);
                    log.push(format!("{t}ms restart {id}"));
                }
            }
            3..=5 => {
                let mut shuffled = ids.clone();
                shuffled.shuffle(&mut rng);
                let cut = rng.gen_range(1..opt.nodes);
                let groups = vec![shuffled[..cut].to_vec(), shuffled[cut..].to_vec()];
                log.push(format!("{t}ms partition {groups:?}"));
                cluster.faults.partition(&groups);
            }
            _ => {
                log.push(format!("{t}ms heal"));
                cluster.faults.heal();
            }
        }
    }

    // Repair everything and let every in-flight operation finish.
    cluster.faults.heal();
    cluster.faults.set_unreliable(0.0, Duration::ZERO);
    for &id in &ids {
        if !cluster.is_up(id) {
            cluster.start(id);
        }
    }
    stop.store(true, Ordering::Relaxed);
    let drained = tokio::time::timeout(Duration::from_secs(20), async {
        for c in clients.iter_mut() {
            c.await.unwrap();
        }
    })
    .await;
    let history = std::mem::take(&mut *history.lock().unwrap());
    let ops = history.len();
    let outcome = if drained.is_err() {
        for c in &clients {
            c.abort();
        }
        Err("liveness: operations did not complete within 20s of healing".to_string())
    } else {
        let h = tokio::task::spawn_blocking(move || check::check(&history, 5_000_000)).await.unwrap();
        match h {
            Outcome::Ok => Ok(()),
            Outcome::Violation(k) => Err(format!("NOT LINEARIZABLE (key {k})")),
            Outcome::Unknown(k) => Err(format!("checker gave up (key {k})")),
        }
    };
    FuzzReport { seed, ops, faults: log, outcome }
}
