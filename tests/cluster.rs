//! Integration tests: real nodes talking gRPC over loopback, in one process.

use raftkv::sim::{fuzz_once, Cluster, FuzzOptions};
use std::time::Duration;

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_one_leader() {
    let c = Cluster::new(3, 1, 0).await;
    let leader = c.wait_leader(&[1, 2, 3]).await.expect("no leader");
    sleep_ms(500).await; // stable: still exactly one leader, same node
    assert_eq!(c.leaders().into_iter().map(|(id, _)| id).collect::<Vec<_>>(), vec![leader]);
    let terms: Vec<u64> = (1..=3).map(|id| c.node(id).unwrap().raft.status().0).collect();
    assert!(terms.iter().all(|&t| t == terms[0]), "terms differ: {terms:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reelects_after_leader_crash_and_keeps_data() {
    let c = Cluster::new(5, 2, 0).await;
    let mut ck = c.clerk();
    ck.put("a", b"1".to_vec()).await;
    let old = c.wait_leader(&[1, 2, 3, 4, 5]).await.unwrap();
    c.crash(old);
    let rest: Vec<u64> = (1..=5).filter(|&i| i != old).collect();
    let new = c.wait_leader(&rest).await.expect("no new leader");
    assert_ne!(new, old);
    assert_eq!(ck.get("a").await, b"1");
    ck.append("a", b"2".to_vec()).await;
    assert_eq!(ck.get("a").await, b"12");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_progress_without_a_majority() {
    let c = Cluster::new(3, 3, 0).await;
    let mut ck = c.clerk();
    ck.put("k", b"v".to_vec()).await;
    let leader = c.wait_leader(&[1, 2, 3]).await.unwrap();
    for id in (1..=3).filter(|&i| i != leader) {
        c.crash(id);
    }
    let attempt = tokio::time::timeout(Duration::from_secs(2), ck.put("k", b"w".to_vec())).await;
    assert!(attempt.is_err(), "a lone node committed a write");
    for id in (1..=3).filter(|&i| i != leader) {
        c.start(id);
    }
    // The retried put eventually lands once a majority is back.
    ck.put("k", b"w".to_vec()).await;
    assert_eq!(ck.get("k").await, b"w");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicates_to_every_node() {
    let c = Cluster::new(3, 4, 0).await;
    let mut ck = c.clerk();
    for i in 0..50 {
        ck.put(&format!("k{i}"), vec![i]).await;
    }
    let leader_commit = c.node(c.wait_leader(&[1, 2, 3]).await.unwrap()).unwrap().raft.commit_index();
    sleep_ms(300).await; // followers learn the commit index on the next heartbeat
    for id in 1..=3 {
        assert!(c.node(id).unwrap().raft.commit_index() >= leader_commit, "node {id} behind");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_leader_steps_down_and_minority_writes_are_lost() {
    let c = Cluster::new(5, 5, 0).await;
    let mut ck = c.clerk();
    ck.put("k", b"before".to_vec()).await;
    let old = c.wait_leader(&[1, 2, 3, 4, 5]).await.unwrap();
    let majority: Vec<u64> = (1..=5).filter(|&i| i != old).collect();
    c.faults.partition(&[vec![old], majority.clone()]);

    // A write sent to the isolated leader cannot commit.
    let stuck = c.node(old).unwrap().raft.propose(b"never".to_vec());
    assert!(stuck.is_ok(), "isolated leader should still accept proposals");
    c.wait_leader(&majority).await.expect("majority elected no leader");
    ck.put("k", b"after".to_vec()).await;

    c.faults.heal();
    sleep_ms(500).await;
    assert_eq!(c.leaders().len(), 1, "two leaders after healing");
    assert_ne!(c.node(old).unwrap().raft.status().1, raftkv::raft::Role::Leader);
    assert_eq!(ck.get("k").await, b"after");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survives_full_cluster_restart() {
    let c = Cluster::new(3, 6, 0).await;
    let mut ck = c.clerk();
    for i in 0..20u8 {
        ck.append("log", vec![b'a' + i]).await;
    }
    for id in 1..=3 {
        c.crash(id);
    }
    for id in 1..=3 {
        c.start(id);
    }
    assert_eq!(ck.get("log").await, b"abcdefghijklmnopqrst");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_from_a_snapshot() {
    let c = Cluster::new(3, 7, 50).await; // snapshot every 50 entries
    let mut ck = c.clerk();
    ck.put("x", b"0".to_vec()).await;
    let leader = c.wait_leader(&[1, 2, 3]).await.unwrap();
    let lagger = (1..=3).find(|&i| i != leader).unwrap();
    c.crash(lagger);
    for i in 0..300 {
        ck.put(&format!("k{}", i % 10), i.to_string().into_bytes()).await;
    }
    let ln = c.node(leader).unwrap();
    assert!(ln.raft.log_len() < 300, "leader never compacted its log");

    c.start(lagger);
    let target = ln.raft.commit_index();
    let mut caught_up = false;
    for _ in 0..50 {
        sleep_ms(100).await;
        if c.node(lagger).unwrap().raft.commit_index() >= target {
            caught_up = true;
            break;
        }
    }
    assert!(caught_up, "follower never caught up");

    // Kill the other follower: the restarted node is now needed for every
    // quorum, so reads succeed only if it really holds the data.
    let other = (1..=3).find(|&i| i != leader && i != lagger).unwrap();
    c.crash(other);
    for i in 290..300 {
        assert_eq!(ck.get(&format!("k{}", i % 10)).await, i.to_string().into_bytes());
    }
    // And it survives its own restart (snapshot + WAL on disk).
    c.crash(lagger);
    c.start(lagger);
    ck.put("x", b"1".to_vec()).await;
    assert_eq!(ck.get("k9").await, b"299");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn randomized_faults_stay_linearizable() {
    for (seed, nodes) in [(100, 3), (101, 5)] {
        let opt = FuzzOptions { nodes, duration: Duration::from_secs(2), ..Default::default() };
        let r = fuzz_once(seed, &opt).await;
        assert!(r.ops > 50, "seed {seed}: only {} ops completed", r.ops);
        assert_eq!(r.outcome, Ok(()), "seed {seed}: faults {:#?}", r.faults);
    }
}
