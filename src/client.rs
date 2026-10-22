//! Client library: finds the leader, retries, and tags every command with
//! (client_id, seq) so retries are applied at most once.

use crate::pb::kv_client::KvClient;
use crate::pb::{Command, Op};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

pub struct Clerk {
    servers: Vec<KvClient<Channel>>, // index i = node id i + 1
    leader: usize,
    id: u64,
    seq: u64,
    pub attempt_timeout: Duration,
}

impl Clerk {
    pub fn new(addrs: &[String]) -> Clerk {
        let servers = addrs
            .iter()
            .map(|a| KvClient::new(Endpoint::from_shared(format!("http://{a}")).unwrap().connect_lazy()))
            .collect();
        Clerk { servers, leader: 0, id: rand::random(), seq: 0, attempt_timeout: Duration::from_secs(2) }
    }

    pub async fn get(&mut self, key: &str) -> Vec<u8> {
        self.call(Op::Get, key, vec![]).await
    }

    pub async fn put(&mut self, key: &str, value: Vec<u8>) {
        self.call(Op::Put, key, value).await;
    }

    pub async fn append(&mut self, key: &str, value: Vec<u8>) {
        self.call(Op::Append, key, value).await;
    }

    /// Retries until some leader executes the command.
    pub async fn call(&mut self, op: Op, key: &str, value: Vec<u8>) -> Vec<u8> {
        self.seq += 1;
        let cmd = Command { client_id: self.id, seq: self.seq, op: op as i32, key: key.to_string(), value };
        loop {
            let mut server = self.servers[self.leader].clone();
            let reply = tokio::time::timeout(self.attempt_timeout, server.execute(cmd.clone())).await;
            match reply.map(|r| r.map(|r| r.into_inner())) {
                Ok(Ok(r)) if r.ok => return r.value,
                // Follow a leader hint, unless it points back at this server.
                Ok(Ok(r)) if r.leader != 0 && (r.leader - 1) as usize != self.leader => {
                    self.leader = (r.leader - 1) as usize;
                    continue;
                }
                _ => self.leader = (self.leader + 1) % self.servers.len(),
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
