# raftkv

A small, fault-tolerant key-value store in Rust. It runs on Raft (leader election,
log replication, snapshots) and uses gRPC for every network call.

* **Linearizable** `Get` / `Put` / `Append`. Every operation goes through the
  Raft log, and duplicate detection means a retried request is applied at most once.
* **Durable.** Nothing is acknowledged before it has been fsynced on a majority.
* **Small.** About 2,500 lines of Rust in total, including the tests, the
  checker and the fault-injection harness. It is one crate with no framework
  beyond `tokio` and `tonic`.

## Results

All numbers below were measured on one Apple M2 Pro laptop (12 cores, 16 GB,
macOS 27, internal SSD).

**Throughput on 5 nodes.** Five separate node processes ran on loopback over
gRPC, each with fsync on and snapshots every 10,000 entries. The load was 100%
writes: 128-byte `Put`s across 1,000 keys. 32 closed-loop clients ran for 10 s
after a 2 s warm-up. The three runs went back to back on one cluster:

| run | ops/sec | p50 | p90 | p99 | p99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 27,829 | 1.10 ms | 1.36 ms | 2.15 ms | 7.38 ms | 11 ms |
| 2 | 27,220 | 1.10 ms | 1.34 ms | 1.90 ms | 4.04 ms | 311 ms |
| 3 | 25,824 | 1.10 ms | 1.37 ms | 2.47 ms | 6.77 ms | 384 ms |

Over all 36 seconds of load there was one leader election, the initial one.
The 300–400 ms maxima are APFS fsync stalls that hit every replica at once,
because all five share one SSD; see the design notes below. The disk-stall
guard keeps these stalls from turning into elections.

**Fault injection: 1,000 randomized runs, all linearizable.**

| cluster | iterations | linearizable | operations checked | wall time |
| --- | ---: | ---: | ---: | ---: |
| 3 nodes (seeds 1–500) | 500 | 500 | 249,505 | 408 s |
| 5 nodes (seeds 1001–1500) | 500 | 500 | 282,812 | 403 s |

On average each iteration injects about 14 crash, restart, partition or heal
events, on top of the background message loss and delay described below. The
median iteration completes about 500 operations. A handful (9 of 1,000) spend
most of their 3 seconds without a majority and complete fewer than 50.

```bash
./target/release/fuzz --iters 500 --nodes 3 --seed 1
./target/release/fuzz --iters 500 --nodes 5 --seed 1001
```

---

## Layout

```
proto/raftkv.proto   Raft + Kv gRPC services, WAL / snapshot record formats
src/raft.rs          consensus: election, replication, commit, snapshots, apply
src/storage.rs       segmented write-ahead log + snapshot file
src/kv.rs            the replicated map, duplicate detection, snapshot trigger
src/net.rs           gRPC server + peer client, in-process fault injection
src/client.rs        Clerk: leader discovery, retries, (client_id, seq)
src/check.rs         linearizability checker (Wing–Gong / Lowe search)
src/sim.rs           in-process clusters on real sockets + the randomized fuzz run
src/bin/node.rs      a server process
src/bin/bench.rs     closed-loop load generator with an HDR histogram
src/bin/fuzz.rs      runs N randomized fault-injection iterations
tests/cluster.rs     integration tests
scripts/cluster.sh   start / stop a local multi-process cluster
```

## How a write works

```
client ──Execute(Command)──▶ leader kv.rs: register waiter for log index i (under lock)
                                       raft.propose → append to in-memory log
                             leader persister thread: WAL write + fsync (group commit)
                             leader replicator (one per peer) ──AppendEntries──▶ follower
                                       follower: consistency check, append, WAL write,
                                       fsync, reply
                             leader: entry i stored on a majority and from the
                                     current term → commit
                             applier → kv.rs applies Command, dedups, wakes waiter
client ◀────────── KvReply ──┘
```

`raft.rs` keeps all of its state behind one mutex. Four kinds of background
tasks do the work: a **ticker** (election timeouts), one **replicator** per
peer (it sends AppendEntries or InstallSnapshot when there is something to send
or a heartbeat is due), a **persister** thread (the leader's group commit), and
an **applier** (it delivers committed entries in order).

### Design decisions worth knowing

* **Write under the lock, fsync outside it.** Every change to the log or to
  term/vote is appended to the WAL while the mutex is held, so the WAL records
  changes in the order they happened. The fsync runs after the lock is
  released, and nothing that depends on a change (an RPC reply, a vote
  request) leaves the node until the fsync returns. The leader counts its own
  entries toward the commit quorum only after they are durable.
* **Plain `fsync()`.** On macOS, Rust's `File::sync_all` issues `F_FULLFSYNC`,
  which also flushes the drive's write cache and costs about 4–20 ms on this
  machine. raftkv calls POSIX `fsync()`. That hands the data to the drive,
  which is what `fsync` does on Linux. It survives a process crash or kernel
  panic. It may not survive a power cut on a drive with a volatile write cache.
* **The WAL is append-only and segmented.** Each record holds the current
  (term, vote) plus any new entries. On replay, an entry at index *i* replaces
  everything from *i* onwards, so a follower's log truncation is logged without
  rewriting anything. A snapshot is written atomically (temp file, fsync,
  rename, directory fsync), and then whole 64 MB segments it covers are
  deleted. An earlier version rewrote the WAL after every snapshot. On APFS
  that caused volume-wide fsync stalls of 100–800 ms, which showed up as
  leader elections in the middle of benchmarks.
* **A stalled disk does not call an election.** All five replicas share one
  laptop SSD, so a disk stall hits every node at once. While a node's own
  fsync is in flight, its election timer is held off. It could not vote for
  itself until the disk answered anyway, and a late election would only depose
  a healthy leader.
* **No lost wake-ups.** The KV server registers its waiter while holding the
  same lock the apply loop uses to find it. Otherwise an entry could be
  applied before anyone was listening for it.
* **Crash simulation drops un-fsynced bytes.** In tests, killing a node
  truncates its WAL to the last fsynced byte. The fuzzer therefore checks the
  fsync discipline itself, and also torn-tail recovery. A node that has been
  killed never replies, even if an RPC handler was already running.
* **Unit tests cover the crash points the fuzzer cannot reach.** A simulated
  crash never lands in the middle of snapshot I/O, and a 3-second run never
  fills a 64 MB segment. So each crash point between the steps of a snapshot
  install and of a compaction has its own storage unit test (`src/storage.rs`).

---

## Running it

You need a Rust toolchain. `protoc` is vendored through `protoc-bin-vendored`,
so nothing else has to be installed.

```bash
cargo test --release                    # unit + integration tests

scripts/cluster.sh start 5              # nodes on 127.0.0.1:7001..7005, data in run/
./target/release/bench \
    --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005 \
    --clients 32 --duration 10 --warmup 2
scripts/cluster.sh clean                # stop and delete run/

./target/release/fuzz --iters 500 --nodes 5 --seed 1001
```

Node flags: `--snapshot-every N` (log entries between snapshots, default
10000, 0 = never) and `--no-fsync`. Bench flags: `--clients`, `--duration`,
`--warmup`, `--value-size`, `--keys`, `--read-ratio`.

### What the fuzzer does

Each iteration builds a fresh 3- or 5-node cluster in one process. The nodes
talk real gRPC over loopback. For 3 seconds, 8 clients issue random
`Get`/`Put`/`Append` operations on 4 keys. Meanwhile a nemesis acts every
50–400 ms: it crashes a node (losing un-fsynced data), restarts a crashed
node, splits the cluster into two random partitions, or heals it.
Throughout the run, 5% of Raft messages and 5% of client replies are dropped,
and every Raft RPC is delayed by 0–10 ms, which also reorders messages.
Snapshots are taken every ~200 entries, so restarted nodes often catch up
through `InstallSnapshot`. At the end the cluster is healed, every client
finishes its outstanding operation, and the full history is checked for
linearizability.

To confirm that the checker can actually catch bugs, three were planted on
purpose:

| Planted bug | Iterations failed |
| --- | ---: |
| duplicate detection disabled | 8 / 8 |
| commit without a majority (leader's own log only) | 8 / 12 |
| follower acknowledges AppendEntries before its fsync | 2 / 40 |

---

## Limitations

* **One machine.** Every number above comes from processes on one laptop
  talking over loopback, with five replicas sharing one SSD. That is not a
  datacenter, and these numbers say nothing about cross-machine latency.
* **No membership changes.** The set of nodes is fixed at startup.
* **Reads go through the log.** This is simple and linearizable, but each read
  costs a replication round. There are no ReadIndex or lease reads.
* **The duplicate-detection table grows without bound.** It keeps one entry per
  client id that was ever seen. A real system would expire client sessions.
* **`InstallSnapshot` sends the whole snapshot in one message** (up to 256 MB),
  not in chunks.
* **A follower writes an installed snapshot while holding the Raft lock.**
  This is safe but can delay that node's other work. Snapshot installs are rare.
* **A seed reproduces the fault schedule, not the exact thread
  interleaving.**
* **The benchmark is closed loop.** Each client waits for its reply before it
  sends the next request, so a stall delays the requests behind it instead of
  showing up as many slow requests. `max` and `p99.9` show the stalls.
