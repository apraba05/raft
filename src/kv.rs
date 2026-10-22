//! The replicated key-value state machine.
//!
//! Every command (reads included) goes through the Raft log, which makes all
//! operations linearizable. Each client tags commands with (client_id, seq);
//! the state machine remembers the last seq and result per client so a
//! retried command is applied at most once.

use crate::pb::{Command, KvReply, KvSnapshot, Op, Session};
use crate::raft::{Apply, Raft};
use prost::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

type Waiter = (u64, oneshot::Sender<Option<Vec<u8>>>); // (term, reply)

pub struct KvStore {
    raft: Arc<Raft>,
    /// Clients waiting for the entry at a log index to be applied.
    waiters: Mutex<HashMap<u64, Waiter>>,
    timeout: Duration,
}

impl KvStore {
    /// Starts the apply loop. `snapshot_every` = take a snapshot once the log
    /// holds this many entries (0 = never).
    pub fn start(
        raft: Arc<Raft>,
        applies: mpsc::UnboundedReceiver<Apply>,
        snapshot_every: u64,
        timeout: Duration,
    ) -> Arc<KvStore> {
        let kv = Arc::new(KvStore { raft, waiters: Mutex::new(HashMap::new()), timeout });
        tokio::spawn(kv.clone().apply_loop(applies, snapshot_every));
        kv
    }

    pub async fn execute(&self, cmd: Command) -> KvReply {
        let not_ok = |leader| KvReply { ok: false, value: vec![], leader };
        let rx = {
            // Register the waiter under the same lock the apply loop uses to
            // find it, so the entry cannot be applied before we are listening.
            let mut waiters = self.waiters.lock().unwrap();
            match self.raft.propose(cmd.encode_to_vec()) {
                Err(leader) => return not_ok(leader),
                Ok((index, term)) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.insert(index, (term, tx));
                    rx
                }
            }
        };
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(Some(value))) => KvReply { ok: true, value, leader: 0 },
            _ => not_ok(self.raft.status().2),
        }
    }

    async fn apply_loop(self: Arc<Self>, mut applies: mpsc::UnboundedReceiver<Apply>, snapshot_every: u64) {
        let mut sm = KvSnapshot::default();
        let snapshotting = Arc::new(AtomicBool::new(false));
        while let Some(msg) = applies.recv().await {
            let applied = match msg {
                Apply::Snapshot { index, data } => {
                    sm = KvSnapshot::decode(&data[..]).expect("corrupt snapshot");
                    // Anyone waiting on an index the snapshot covers lost track
                    // of their entry; let them retry (dedup makes that safe).
                    self.waiters.lock().unwrap().retain(|&i, _| i > index);
                    index
                }
                Apply::Entry(e) => {
                    let result = (!e.data.is_empty())
                        .then(|| apply(&mut sm, Command::decode(&e.data[..]).expect("corrupt command")));
                    if let Some((term, tx)) = self.waiters.lock().unwrap().remove(&e.index) {
                        // A different term means another leader overwrote our entry.
                        let _ = tx.send(result.filter(|_| term == e.term));
                    }
                    e.index
                }
            };
            // Check the log size now and then (each check takes the Raft lock).
            let check = applies.is_empty() || applied % 256 == 0;
            if check
                && snapshot_every > 0
                && !snapshotting.load(Ordering::Acquire)
                && self.raft.log_len() >= snapshot_every
            {
                snapshotting.store(true, Ordering::Release);
                let (raft, flag, data) = (self.raft.clone(), snapshotting.clone(), sm.encode_to_vec());
                tokio::task::spawn_blocking(move || {
                    raft.snapshot(applied, data);
                    flag.store(false, Ordering::Release);
                });
            }
        }
    }
}

/// Applies one command, at most once per (client_id, seq).
fn apply(sm: &mut KvSnapshot, cmd: Command) -> Vec<u8> {
    if let Some(s) = sm.sessions.get(&cmd.client_id) {
        if cmd.seq <= s.seq {
            return s.value.clone(); // duplicate: return the original result
        }
    }
    let value = match cmd.op() {
        Op::Get => sm.data.get(&cmd.key).cloned().unwrap_or_default(),
        Op::Put => {
            sm.data.insert(cmd.key, cmd.value);
            vec![]
        }
        Op::Append => {
            sm.data.entry(cmd.key).or_default().extend_from_slice(&cmd.value);
            vec![]
        }
    };
    sm.sessions.insert(cmd.client_id, Session { seq: cmd.seq, value: value.clone() });
    value
}
