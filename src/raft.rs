//! The Raft consensus module (Figure 2 of the extended Raft paper, plus
//! snapshots from §7).
//!
//! All state lives in one `Mutex<State>`. Background work:
//!
//! * `ticker`: starts an election when the election deadline passes.
//! * `replicator`: one per peer; sends AppendEntries / InstallSnapshot when
//!   the leader has something to send or a heartbeat is due.
//! * `persister`: (leader only) group commit. Writes and fsyncs newly proposed
//!   entries in batches; the leader's own entries count toward the commit
//!   quorum only once they are durable.
//! * `applier`: delivers committed entries / snapshots, in order, on a channel.
//!
//! Durability rule: every change to the log or hard state is written to the
//! WAL while the lock is held (`persist`), so the WAL sees changes in the order
//! they happened. The fsync happens after the lock is released, and nothing
//! that depends on the change (an RPC reply, a vote request) leaves the node
//! before the fsync returns.

use crate::net::Peers;
use crate::pb::{
    AppendReply, AppendRequest, Entry, SnapshotFile, SnapshotReply, SnapshotRequest, VoteReply, VoteRequest,
    WalRecord,
};
use crate::storage::{Recovered, Storage};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};
use tokio::time::{sleep, sleep_until, timeout, Instant};
use tonic::Status;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: u64,
    /// Every node id in the cluster, including `id`.
    pub nodes: Vec<u64>,
    pub election_timeout: Duration, // randomised in [t, 2t)
    pub heartbeat: Duration,
    pub rpc_timeout: Duration,
    pub max_batch: usize, // entries per AppendEntries
}

impl Config {
    pub fn new(id: u64, nodes: Vec<u64>) -> Config {
        Config {
            id,
            nodes,
            election_timeout: Duration::from_millis(150),
            heartbeat: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(500),
            max_batch: 1024,
        }
    }
}

/// What the state machine receives, in log order.
#[derive(Debug)]
pub enum Apply {
    Entry(Entry),
    Snapshot { index: u64, data: Arc<Vec<u8>> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Raft {
    cfg: Config,
    st: Mutex<State>,
    storage: Storage,
    peers: Peers,
    /// Serialises snapshot writes (local and installed) so an older snapshot
    /// can never overwrite a newer one on disk. Always taken before `st`.
    snap_lock: Mutex<()>,
    apply_notify: Notify,
    peer_notify: HashMap<u64, Notify>,
    persist_signal: Signal,
}

struct State {
    term: u64,
    vote: u64, // 0 = none
    role: Role,
    leader: u64, // 0 = unknown
    /// `log[0]` is a sentinel holding the snapshot's last index and term;
    /// real entries follow, so `log[i - log[0].index]` is entry `i`.
    log: Vec<Entry>,
    commit: u64,
    applied: u64,
    /// The latest snapshot, sent to followers that are too far behind.
    snapshot: Arc<Vec<u8>>,
    snapshot_pending: bool, // installed but not yet handed to the state machine
    unwritten: u64,         // first log index not yet written to the WAL
    dirty: bool,            // term/vote changed since the last WAL write
    deadline: Instant,      // election deadline
    votes: HashSet<u64>,
    next: HashMap<u64, u64>,
    matched: HashMap<u64, u64>, // includes our own durable index
    killed: bool,
}

impl State {
    fn base(&self) -> u64 {
        self.log[0].index
    }
    fn last_index(&self) -> u64 {
        self.log.last().unwrap().index
    }
    fn last_term(&self) -> u64 {
        self.log.last().unwrap().term
    }
    fn term_at(&self, i: u64) -> Option<u64> {
        let pos = i.checked_sub(self.base())? as usize;
        self.log.get(pos).map(|e| e.term)
    }
    fn entries(&self, from: u64, to: u64) -> Vec<Entry> {
        let b = self.base();
        self.log[(from - b) as usize..=(to - b) as usize].to_vec()
    }
    fn step_down(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.vote = 0;
            self.dirty = true;
        }
        self.role = Role::Follower;
    }
}

impl Raft {
    /// Starts a Raft node from recovered storage. Returns the node and the
    /// channel on which committed entries are delivered.
    pub fn start(
        cfg: Config,
        storage: Storage,
        rec: Recovered,
        peers: Peers,
    ) -> (Arc<Raft>, mpsc::UnboundedReceiver<Apply>) {
        let (snap, snap_term, data) = match rec.snapshot {
            Some(s) => (s.index, s.term, s.data),
            None => (0, 0, Vec::new()),
        };
        let mut log = vec![Entry { index: snap, term: snap_term, data: vec![] }];
        log.extend(rec.entries);
        let unwritten = log.last().unwrap().index + 1;
        let st = State {
            term: rec.term,
            vote: rec.vote,
            role: Role::Follower,
            leader: 0,
            log,
            commit: snap,
            applied: snap,
            snapshot: Arc::new(data),
            snapshot_pending: snap > 0,
            unwritten,
            dirty: false,
            deadline: Instant::now(),
            votes: HashSet::new(),
            next: HashMap::new(),
            matched: HashMap::new(),
            killed: false,
        };
        let others: Vec<u64> = cfg.nodes.iter().copied().filter(|&n| n != cfg.id).collect();
        let raft = Arc::new(Raft {
            peer_notify: others.iter().map(|&p| (p, Notify::new())).collect(),
            cfg,
            st: Mutex::new(st),
            storage,
            peers,
            snap_lock: Mutex::new(()),
            apply_notify: Notify::new(),
            persist_signal: Signal::default(),
        });
        raft.reset_deadline(&mut raft.lock());

        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(raft.clone().ticker());
        tokio::spawn(raft.clone().applier(tx));
        for p in others {
            tokio::spawn(raft.clone().replicator(p));
        }
        let r = raft.clone();
        std::thread::spawn(move || r.persister());
        (raft, rx)
    }

    // ------------------------------------------------------------ public API

    /// Appends a command to the log if this node is the leader. Returns the
    /// entry's (index, term), or the id of the node believed to be leader.
    pub fn propose(&self, data: Vec<u8>) -> Result<(u64, u64), u64> {
        let mut st = self.lock();
        if st.role != Role::Leader || st.killed {
            return Err(st.leader);
        }
        let (index, term) = (st.last_index() + 1, st.term);
        st.log.push(Entry { index, term, data });
        drop(st);
        self.persist_signal.notify();
        self.notify_peers();
        Ok((index, term))
    }

    /// (term, role, leader id)
    pub fn status(&self) -> (u64, Role, u64) {
        let st = self.lock();
        (st.term, st.role, st.leader)
    }

    pub fn commit_index(&self) -> u64 {
        self.lock().commit
    }

    /// Number of entries in the log since the last snapshot.
    pub fn log_len(&self) -> u64 {
        let st = self.lock();
        st.last_index() - st.base()
    }

    /// Compacts the log up to `index`, whose state is `data`. Blocking (disk
    /// I/O); call from a blocking thread.
    pub fn snapshot(&self, index: u64, data: Vec<u8>) {
        let _g = self.snap_lock.lock().unwrap();
        let term = {
            let st = self.lock();
            if st.killed || index <= st.base() || index > st.applied {
                return;
            }
            st.term_at(index).unwrap()
        };
        // The base cannot move while we hold snap_lock, so writing the file
        // outside the state lock is safe and keeps the node responsive.
        let file = SnapshotFile { index, term, data };
        self.storage.save_snapshot(&file).expect("save snapshot");
        let mut st = self.lock();
        if st.killed {
            return;
        }
        let drop_to = (index - st.base()) as usize;
        st.log.drain(..drop_to);
        st.log[0].data.clear();
        st.snapshot = Arc::new(file.data);
        drop(st);
        self.storage.compact(index).expect("WAL compaction");
    }

    /// Simulates a crash: stops all background work, makes sure this instance
    /// never touches its files again, and discards un-fsynced WAL bytes.
    pub fn kill(&self) {
        let _g = self.snap_lock.lock().unwrap();
        let mut st = self.lock();
        st.killed = true;
        self.storage.crash();
        drop(st);
        self.persist_signal.notify();
        self.apply_notify.notify_one();
        self.notify_peers();
    }

    // ------------------------------------------------------------ RPC handlers

    pub async fn handle_vote(self: &Arc<Self>, req: VoteRequest) -> Result<VoteReply, Status> {
        let reply = {
            let mut st = self.lock_alive()?;
            if req.term > st.term {
                st.step_down(req.term);
            }
            // §5.4.1: only vote for a candidate whose log is at least as up to date.
            let up_to_date = (req.last_log_term, req.last_log_index) >= (st.last_term(), st.last_index());
            let granted = req.term == st.term && (st.vote == 0 || st.vote == req.candidate) && up_to_date;
            if granted {
                st.vote = req.candidate;
                st.dirty = true;
                self.reset_deadline(&mut st);
            }
            self.persist(&mut st);
            VoteReply { term: st.term, granted }
        };
        self.sync().await?;
        Ok(reply)
    }

    pub async fn handle_append(self: &Arc<Self>, req: AppendRequest) -> Result<AppendReply, Status> {
        let reply = {
            let mut st = self.lock_alive()?;
            let reply = self.append_entries(&mut st, req);
            self.persist(&mut st);
            reply
        };
        self.sync().await?;
        Ok(reply)
    }

    fn append_entries(&self, st: &mut State, req: AppendRequest) -> AppendReply {
        let fail = |st: &State, conflict_index| AppendReply { term: st.term, success: false, conflict_index };
        if req.term < st.term {
            return fail(st, 0);
        }
        st.step_down(req.term);
        st.leader = req.leader;
        self.reset_deadline(st);

        // Consistency check. Entries at or below our snapshot are committed,
        // so they match by definition.
        let prev = req.prev_log_index;
        if prev > st.last_index() {
            return fail(st, st.last_index() + 1);
        }
        if let Some(t) = st.term_at(prev).filter(|_| prev > st.base()) {
            if t != req.prev_log_term {
                // Skip back over the whole conflicting term in one round trip.
                let mut i = prev;
                while i - 1 > st.base() && st.term_at(i - 1) == Some(t) {
                    i -= 1;
                }
                return fail(st, i);
            }
        }

        let last_new = prev + req.entries.len() as u64;
        for e in req.entries {
            if e.index <= st.base() {
                continue;
            }
            match st.term_at(e.index) {
                Some(t) if t == e.term => continue, // already have it (maybe a stale RPC)
                Some(_) => {
                    // Conflict: drop it and everything after it.
                    let keep = (e.index - st.base()) as usize;
                    st.log.truncate(keep);
                    st.unwritten = st.unwritten.min(e.index);
                }
                None => {}
            }
            st.log.push(e);
        }
        if req.leader_commit > st.commit {
            st.commit = st.commit.max(req.leader_commit.min(last_new));
            self.apply_notify.notify_one();
        }
        AppendReply { term: st.term, success: true, conflict_index: 0 }
    }

    /// Blocking (disk I/O); the gRPC layer runs it on a blocking thread.
    pub fn handle_snapshot(&self, req: SnapshotRequest) -> Result<SnapshotReply, Status> {
        let _g = self.snap_lock.lock().unwrap();
        let mut st = self.lock_alive()?;
        if req.term < st.term {
            return Ok(SnapshotReply { term: st.term });
        }
        st.step_down(req.term);
        st.leader = req.leader;
        self.reset_deadline(&mut st);

        if req.last_index > st.commit {
            let (index, term) = (req.last_index, req.last_term);
            let file = SnapshotFile { index, term, data: req.data };
            self.storage.save_snapshot(&file).expect("save snapshot");
            // §7: keep the log suffix if it extends the snapshot, else discard it.
            if st.term_at(index) == Some(term) {
                let drop_to = (index - st.base()) as usize;
                st.log.drain(..drop_to);
                st.log[0].data.clear();
            } else {
                // Log the snapshot's last entry as an anchor, so the discarded
                // entries are not resurrected from the WAL after a restart.
                st.log = vec![Entry { index, term, data: vec![] }];
                st.unwritten = index + 1;
                let rec = WalRecord { term: st.term, vote: st.vote, entries: st.log.clone() };
                self.storage.append(&rec).expect("WAL write");
                st.dirty = false;
            }
            st.commit = index;
            st.snapshot = Arc::new(file.data);
            st.snapshot_pending = true;
            self.apply_notify.notify_one();
        }
        self.persist(&mut st);
        let (reply, base) = (SnapshotReply { term: st.term }, st.base());
        drop(st);
        // Still holding snap_lock, so this instance cannot be killed meanwhile.
        self.storage.sync().expect("fsync");
        self.storage.compact(base).expect("WAL compaction");
        Ok(reply)
    }

    // ------------------------------------------------------------ election

    async fn ticker(self: Arc<Self>) {
        loop {
            sleep(Duration::from_millis(10)).await;
            let mut st = self.lock();
            if st.killed {
                return;
            }
            if st.role == Role::Leader {
                continue;
            }
            if self.storage.busy() {
                self.reset_deadline(&mut st); // time stuck on our own disk does not count
            } else if Instant::now() >= st.deadline {
                self.start_election(&mut st);
            }
        }
    }

    fn start_election(self: &Arc<Self>, st: &mut State) {
        st.term += 1;
        st.role = Role::Candidate;
        st.vote = self.cfg.id;
        st.leader = 0;
        st.dirty = true;
        st.votes.clear();
        self.reset_deadline(st);
        self.persist(st);
        let req = VoteRequest {
            term: st.term,
            candidate: self.cfg.id,
            last_log_index: st.last_index(),
            last_log_term: st.last_term(),
        };
        let this = self.clone();
        tokio::spawn(async move {
            // Our own vote must be durable before we ask for anyone else's.
            if this.sync().await.is_err() {
                return;
            }
            this.on_vote(req.term, this.cfg.id, VoteReply { term: req.term, granted: true });
            for &p in this.peer_notify.keys() {
                let this = this.clone();
                tokio::spawn(async move {
                    let call = this.peers.vote(p, req);
                    if let Ok(Ok(reply)) = timeout(this.cfg.rpc_timeout, call).await {
                        this.on_vote(req.term, p, reply);
                    }
                });
            }
        });
    }

    fn on_vote(&self, term: u64, from: u64, reply: VoteReply) {
        let mut st = self.lock();
        if st.killed {
            return;
        }
        if reply.term > st.term {
            st.step_down(reply.term);
            return;
        }
        if st.role != Role::Candidate || st.term != term || !reply.granted {
            return;
        }
        st.votes.insert(from);
        if st.votes.len() * 2 > self.cfg.nodes.len() {
            self.become_leader(&mut st);
        }
    }

    fn become_leader(&self, st: &mut State) {
        st.role = Role::Leader;
        st.leader = self.cfg.id;
        eprintln!("node {}: leader for term {}", self.cfg.id, st.term);
        let next = st.last_index() + 1;
        for &n in &self.cfg.nodes {
            st.next.insert(n, next);
            st.matched.insert(n, 0);
        }
        // A no-op in the new term lets entries from earlier terms commit (§5.4.2).
        let term = st.term;
        st.log.push(Entry { index: next, term, data: vec![] });
        self.persist_signal.notify();
        self.notify_peers();
    }

    // ------------------------------------------------------------ replication

    async fn replicator(self: Arc<Self>, peer: u64) {
        let hb = self.cfg.heartbeat;
        let mut last_sent = Instant::now() - hb;
        let mut sent_commit = 0;
        loop {
            let work = {
                let st = self.lock();
                if st.killed {
                    return;
                }
                self.next_message(&st, peer, last_sent, sent_commit)
            };
            match work {
                None => {
                    // Leader: sleep until the next heartbeat is due. Otherwise
                    // just check again later (or when we become leader).
                    let now = Instant::now();
                    let until = if last_sent + hb > now { last_sent + hb } else { now + hb };
                    tokio::select! {
                        _ = self.peer_notify[&peer].notified() => {}
                        _ = sleep_until(until) => {}
                    }
                }
                Some(Msg::Append(req)) => {
                    last_sent = Instant::now();
                    sent_commit = req.leader_commit;
                    let (term, matched) = (req.term, req.prev_log_index + req.entries.len() as u64);
                    match timeout(self.cfg.rpc_timeout, self.peers.append(peer, req)).await {
                        Ok(Ok(reply)) => self.on_append_reply(peer, term, matched, reply),
                        _ => sleep(hb).await, // unreachable; retry on the next heartbeat
                    }
                }
                Some(Msg::Snapshot(req)) => {
                    last_sent = Instant::now();
                    let (term, matched) = (req.term, req.last_index);
                    match timeout(self.cfg.rpc_timeout * 4, self.peers.snapshot(peer, req)).await {
                        Ok(Ok(reply)) => self.on_snapshot_reply(peer, term, matched, reply),
                        _ => sleep(hb).await,
                    }
                }
            }
        }
    }

    /// What, if anything, the leader should send to `peer` right now.
    fn next_message(&self, st: &State, peer: u64, last_sent: Instant, sent_commit: u64) -> Option<Msg> {
        if st.role != Role::Leader {
            return None;
        }
        let next = st.next[&peer];
        if next <= st.base() {
            return Some(Msg::Snapshot(SnapshotRequest {
                term: st.term,
                leader: self.cfg.id,
                last_index: st.base(),
                last_term: st.log[0].term,
                data: st.snapshot.to_vec(),
            }));
        }
        let heartbeat_due = last_sent.elapsed() >= self.cfg.heartbeat;
        if next > st.last_index() && st.commit <= sent_commit && !heartbeat_due {
            return None;
        }
        let last = st.last_index().min(next + self.cfg.max_batch as u64 - 1);
        let entries = if next <= last { st.entries(next, last) } else { vec![] };
        Some(Msg::Append(AppendRequest {
            term: st.term,
            leader: self.cfg.id,
            prev_log_index: next - 1,
            prev_log_term: st.term_at(next - 1).unwrap(),
            entries,
            leader_commit: st.commit,
        }))
    }

    fn on_append_reply(&self, peer: u64, term: u64, matched: u64, reply: AppendReply) {
        let mut st = self.lock();
        if st.killed || self.stale_reply(&mut st, term, reply.term) {
            return;
        }
        if reply.success {
            if matched > st.matched[&peer] {
                st.matched.insert(peer, matched);
                self.advance_commit(&mut st);
            }
            st.next.insert(peer, matched + 1);
        } else {
            st.next.insert(peer, reply.conflict_index.max(1));
        }
    }

    fn on_snapshot_reply(&self, peer: u64, term: u64, matched: u64, reply: SnapshotReply) {
        let mut st = self.lock();
        if st.killed || self.stale_reply(&mut st, term, reply.term) {
            return;
        }
        let matched = matched.max(st.matched[&peer]);
        st.matched.insert(peer, matched);
        st.next.insert(peer, matched + 1);
    }

    /// Steps down on a newer term; true if the reply should be ignored.
    fn stale_reply(&self, st: &mut State, sent_term: u64, reply_term: u64) -> bool {
        if reply_term > st.term {
            st.step_down(reply_term);
            return true;
        }
        st.role != Role::Leader || st.term != sent_term
    }

    /// Commits the highest index stored on a majority, if it is from the
    /// current term (Figure 8: older entries commit only indirectly).
    fn advance_commit(&self, st: &mut State) {
        let mut m: Vec<u64> = self.cfg.nodes.iter().map(|n| st.matched[n]).collect();
        m.sort_unstable();
        let n = m[(m.len() - 1) / 2];
        if n > st.commit && st.term_at(n) == Some(st.term) {
            st.commit = n;
            self.apply_notify.notify_one();
        }
    }

    // ------------------------------------------------------------ persistence

    /// Group commit for the leader's own log: write and fsync everything new,
    /// then count it toward the commit quorum.
    fn persister(self: Arc<Self>) {
        loop {
            self.persist_signal.wait();
            let (term, last) = {
                let mut st = self.lock();
                if st.killed {
                    return;
                }
                self.persist(&mut st);
                (st.term, st.last_index())
            };
            self.storage.sync().expect("fsync");
            let mut st = self.lock();
            if !st.killed && st.role == Role::Leader && st.term == term {
                let me = self.cfg.id;
                if last > st.matched[&me] {
                    st.matched.insert(me, last);
                    self.advance_commit(&mut st);
                }
            }
        }
    }

    /// Writes un-written entries and changed hard state to the WAL (no fsync).
    fn persist(&self, st: &mut State) {
        let from = st.unwritten.max(st.base() + 1);
        let last = st.last_index();
        if st.killed || (!st.dirty && from > last) {
            return;
        }
        let entries = if from <= last { st.entries(from, last) } else { vec![] };
        let rec = WalRecord { term: st.term, vote: st.vote, entries };
        self.storage.append(&rec).expect("WAL write");
        st.unwritten = last + 1;
        st.dirty = false;
    }

    /// fsyncs the WAL off the async runtime. Fails if the node was killed in
    /// the meantime, so a dead node never acknowledges anything.
    async fn sync(self: &Arc<Self>) -> Result<(), Status> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.storage.sync().expect("fsync"))
            .await
            .map_err(|_| Status::unavailable("shutdown"))?;
        self.lock_alive().map(|_| ())
    }

    // ------------------------------------------------------------ apply

    async fn applier(self: Arc<Self>, tx: mpsc::UnboundedSender<Apply>) {
        loop {
            let batch = {
                let mut st = self.lock();
                if st.killed {
                    return;
                }
                if st.snapshot_pending {
                    st.snapshot_pending = false;
                    st.applied = st.base();
                    vec![Apply::Snapshot { index: st.base(), data: st.snapshot.clone() }]
                } else if st.applied < st.commit {
                    let (from, to) = (st.applied + 1, st.commit);
                    st.applied = to;
                    st.entries(from, to).into_iter().map(Apply::Entry).collect()
                } else {
                    vec![]
                }
            };
            if batch.is_empty() {
                self.apply_notify.notified().await;
            }
            for m in batch {
                if tx.send(m).is_err() {
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------ helpers

    fn lock(&self) -> MutexGuard<'_, State> {
        self.st.lock().unwrap()
    }

    fn lock_alive(&self) -> Result<MutexGuard<'_, State>, Status> {
        let st = self.lock();
        if st.killed {
            return Err(Status::unavailable("node is down"));
        }
        Ok(st)
    }

    fn reset_deadline(&self, st: &mut State) {
        let t = self.cfg.election_timeout;
        st.deadline = Instant::now() + rand::thread_rng().gen_range(t..t * 2);
    }

    fn notify_peers(&self) {
        for n in self.peer_notify.values() {
            n.notify_one();
        }
    }
}

enum Msg {
    Append(AppendRequest),
    Snapshot(SnapshotRequest),
}

/// A sticky wake-up flag for the persister thread.
#[derive(Default)]
struct Signal {
    flag: Mutex<bool>,
    cv: Condvar,
}

impl Signal {
    fn notify(&self) {
        *self.flag.lock().unwrap() = true;
        self.cv.notify_one();
    }
    fn wait(&self) {
        let mut f = self.flag.lock().unwrap();
        while !*f {
            f = self.cv.wait(f).unwrap();
        }
        *f = false;
    }
}
