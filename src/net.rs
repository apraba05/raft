//! gRPC plumbing: the outgoing peer client, the server side of both services,
//! and optional in-process fault injection used by tests and the fuzzer.

use crate::pb::kv_server::{Kv, KvServer};
use crate::pb::raft_client::RaftClient;
use crate::pb::raft_server::{Raft as RaftRpc, RaftServer};
use crate::pb::{
    AppendReply, AppendRequest, Command, KvReply, SnapshotReply, SnapshotRequest, VoteReply, VoteRequest,
};
use crate::Node;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

const MAX_MSG: usize = 256 << 20;

/// Outgoing Raft RPCs to the other nodes.
pub struct Peers {
    me: u64,
    clients: HashMap<u64, RaftClient<Channel>>,
    faults: Option<Arc<Faults>>,
}

impl Peers {
    /// `addrs` maps node id to "host:port". Connections are made lazily.
    pub fn new(me: u64, addrs: &HashMap<u64, String>, faults: Option<Arc<Faults>>) -> Peers {
        let clients = addrs
            .iter()
            .filter(|(&id, _)| id != me)
            .map(|(&id, addr)| {
                let ch = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
                let c =
                    RaftClient::new(ch).max_decoding_message_size(MAX_MSG).max_encoding_message_size(MAX_MSG);
                (id, c)
            })
            .collect();
        Peers { me, clients, faults }
    }

    pub async fn vote(&self, to: u64, req: VoteRequest) -> Result<VoteReply, Status> {
        self.call(to, |mut c| async move { c.request_vote(req).await }).await
    }

    pub async fn append(&self, to: u64, req: AppendRequest) -> Result<AppendReply, Status> {
        self.call(to, |mut c| async move { c.append_entries(req).await }).await
    }

    pub async fn snapshot(&self, to: u64, req: SnapshotRequest) -> Result<SnapshotReply, Status> {
        self.call(to, |mut c| async move { c.install_snapshot(req).await }).await
    }

    async fn call<T, F, Fut>(&self, to: u64, f: F) -> Result<T, Status>
    where
        F: FnOnce(RaftClient<Channel>) -> Fut,
        Fut: Future<Output = Result<Response<T>, Status>>,
    {
        let lost = || Status::unavailable("message lost");
        if self.faults.as_ref().is_some_and(|f| !f.deliver(self.me, to)) {
            return Err(lost());
        }
        let reply = f(self.clients[&to].clone()).await?.into_inner();
        if self.faults.as_ref().is_some_and(|f| !f.deliver(to, self.me)) {
            return Err(lost());
        }
        Ok(reply)
    }
}

/// A simulated unreliable network for in-process clusters: partitions,
/// random message loss, and random delivery delay (which also reorders).
pub struct Faults {
    inner: Mutex<FaultState>,
}

struct FaultState {
    group: HashMap<u64, u32>, // partition id per node; same id = can talk
    drop_rate: f64,
    max_delay: Duration,
    rng: StdRng,
}

impl Faults {
    pub fn new(seed: u64) -> Faults {
        let st = FaultState {
            group: HashMap::new(),
            drop_rate: 0.0,
            max_delay: Duration::ZERO,
            rng: StdRng::seed_from_u64(seed),
        };
        Faults { inner: Mutex::new(st) }
    }

    pub fn set_unreliable(&self, drop_rate: f64, max_delay: Duration) {
        let mut st = self.inner.lock().unwrap();
        st.drop_rate = drop_rate;
        st.max_delay = max_delay;
    }

    /// Splits nodes into groups that can only talk within themselves.
    pub fn partition(&self, groups: &[Vec<u64>]) {
        let mut st = self.inner.lock().unwrap();
        st.group.clear();
        for (g, nodes) in groups.iter().enumerate() {
            for &n in nodes {
                st.group.insert(n, g as u32 + 1);
            }
        }
    }

    pub fn heal(&self) {
        self.inner.lock().unwrap().group.clear();
    }

    /// Whether a message from `from` to `to` gets through.
    pub fn deliver(&self, from: u64, to: u64) -> bool {
        let mut st = self.inner.lock().unwrap();
        let g = |n| st.group.get(&n).copied().unwrap_or(0);
        if g(from) != g(to) {
            return false;
        }
        let p = st.drop_rate;
        !st.rng.gen_bool(p)
    }

    fn delay(&self) -> Duration {
        let mut st = self.inner.lock().unwrap();
        let max = st.max_delay;
        if max.is_zero() {
            return max;
        }
        st.rng.gen_range(Duration::ZERO..max)
    }

    fn drop_reply(&self) -> bool {
        let mut st = self.inner.lock().unwrap();
        let p = st.drop_rate;
        st.rng.gen_bool(p)
    }
}

/// The node currently served on an address. Empty while the node is "crashed",
/// so tests can kill and restart a node behind a listener that stays up.
#[derive(Clone, Default)]
pub struct Slot(Arc<RwLock<Option<Node>>>);

impl Slot {
    pub fn set(&self, node: Option<Node>) -> Option<Node> {
        std::mem::replace(&mut *self.0.write().unwrap(), node)
    }
    pub fn get(&self) -> Option<Node> {
        self.0.read().unwrap().clone()
    }
    fn node(&self) -> Result<Node, Status> {
        self.get().ok_or_else(|| Status::unavailable("node is down"))
    }
}

/// Serves the Raft and Kv services for `slot` on `listener` until the task
/// is dropped.
pub async fn serve(listener: TcpListener, slot: Slot, faults: Option<Arc<Faults>>) {
    let svc = Service { slot, faults };
    let raft =
        RaftServer::new(svc.clone()).max_decoding_message_size(MAX_MSG).max_encoding_message_size(MAX_MSG);
    tonic::transport::Server::builder()
        .add_service(raft)
        .add_service(KvServer::new(svc))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await
        .unwrap();
}

#[derive(Clone)]
struct Service {
    slot: Slot,
    faults: Option<Arc<Faults>>,
}

impl Service {
    /// Runs a Raft handler, after a random delay when faults are enabled. The
    /// handler runs in its own task so it still executes (late, possibly out
    /// of order) even if the sender gave up waiting — like a slow network.
    async fn run<T, F, Fut>(&self, f: F) -> Result<Response<T>, Status>
    where
        T: Send + 'static,
        F: FnOnce(Node) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        let node = self.slot.node()?;
        let delay = self.faults.as_ref().map_or(Duration::ZERO, |f| f.delay());
        let task = tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            f(node).await
        });
        let reply = task.await.map_err(|_| Status::internal("handler panicked"))??;
        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl RaftRpc for Service {
    async fn request_vote(&self, req: Request<VoteRequest>) -> Result<Response<VoteReply>, Status> {
        let req = req.into_inner();
        self.run(move |n| async move { n.raft.handle_vote(req).await }).await
    }

    async fn append_entries(&self, req: Request<AppendRequest>) -> Result<Response<AppendReply>, Status> {
        let req = req.into_inner();
        self.run(move |n| async move { n.raft.handle_append(req).await }).await
    }

    async fn install_snapshot(
        &self,
        req: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotReply>, Status> {
        let req = req.into_inner();
        self.run(move |n| async move {
            tokio::task::spawn_blocking(move || n.raft.handle_snapshot(req))
                .await
                .map_err(|_| Status::internal("snapshot install panicked"))?
        })
        .await
    }
}

#[tonic::async_trait]
impl Kv for Service {
    async fn execute(&self, req: Request<Command>) -> Result<Response<KvReply>, Status> {
        let node = self.slot.node()?;
        let reply = node.kv.execute(req.into_inner()).await;
        // Lose some replies after the command ran, so clients must retry
        // and duplicate detection gets exercised.
        if self.faults.as_ref().is_some_and(|f| f.drop_reply()) {
            return Err(Status::unavailable("reply lost"));
        }
        Ok(Response::new(reply))
    }
}
