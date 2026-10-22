//! Durable Raft state: a segmented write-ahead log (WAL) and a snapshot file.
//!
//! The WAL is a sequence of segment files `wal-<seq>`, each a sequence of
//! frames `[len: u32][crc32: u32][WalRecord]`. Every record carries the hard
//! state (term, vote) at the time it was written plus any new entries. On
//! replay the last record's hard state wins, and an entry at index `i`
//! replaces everything from `i` onwards — that is how a follower's log
//! truncation is recorded without rewriting anything in place. A torn tail
//! (a crash mid-write) is cut off when the WAL is opened.
//!
//! The snapshot is a single framed `SnapshotFile`, replaced atomically (write
//! a temp file, fsync, rename, fsync the directory). After a snapshot, whole
//! WAL segments whose entries it covers are deleted. WAL files are only ever
//! appended to, created, or deleted — never rewritten — which keeps fsync
//! latency flat (replacing the live WAL caused 100+ ms fsync stalls on APFS).
//!
//! `append` only writes; `sync` makes everything written so far durable. The
//! split lets Raft write under its lock and fsync outside it.

use crate::pb::{Entry, SnapshotFile, WalRecord};
use prost::Message;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SEGMENT_BYTES: u64 = 64 << 20;

pub struct Storage {
    dir: PathBuf,
    fsync: bool,
    segment_bytes: u64,
    wal: Mutex<Wal>,
    fsyncs_running: AtomicUsize,
}

struct Wal {
    segs: Vec<Segment>, // oldest first; the last one is being appended to
    written: u64,       // bytes written, counted across all segments
    synced: u64,        // bytes known to be durable, same counting
}

struct Segment {
    seq: u64,
    file: Arc<File>,
    start: u64,     // value of `written` when the segment was created
    max_index: u64, // highest entry index written to it
}

/// Everything read back from disk on startup.
#[derive(Debug, Default)]
pub struct Recovered {
    pub term: u64,
    pub vote: u64,
    pub snapshot: Option<SnapshotFile>,
    /// Contiguous entries starting right after the snapshot (or at index 1).
    pub entries: Vec<Entry>,
}

impl Storage {
    pub fn open(dir: &Path, fsync: bool) -> io::Result<(Storage, Recovered)> {
        fs::create_dir_all(dir)?;
        let mut rec = Recovered::default();

        let snap_path = dir.join("snapshot");
        if snap_path.exists() {
            let buf = fs::read(&snap_path)?;
            let (body, _) = unframe(&buf).ok_or_else(|| corrupt("snapshot file"))?;
            rec.snapshot = Some(SnapshotFile::decode(body).map_err(|_| corrupt("snapshot"))?);
        }

        let mut seqs: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|e| e.ok()?.file_name().to_str()?.strip_prefix("wal-")?.parse().ok())
            .collect();
        seqs.sort_unstable();

        // Replay every segment in order. Only the last may have a torn tail.
        let mut log: Vec<Entry> = Vec::new();
        let (mut segs, mut offset) = (Vec::new(), 0);
        for (k, &seq) in seqs.iter().enumerate() {
            let path = segment_path(dir, seq);
            let buf = fs::read(&path)?;
            let (mut pos, mut max_index) = (0, 0);
            while let Some((body, used)) = unframe(&buf[pos..]) {
                let Ok(r) = WalRecord::decode(body) else {
                    break;
                };
                pos += used;
                rec.term = r.term;
                rec.vote = r.vote;
                for e in r.entries {
                    max_index = max_index.max(e.index);
                    replay(&mut log, e);
                }
            }
            let file = OpenOptions::new().append(true).open(&path)?;
            if pos < buf.len() {
                if k + 1 < seqs.len() {
                    return Err(corrupt("WAL segment"));
                }
                file.set_len(pos as u64)?;
            }
            segs.push(Segment { seq, file: Arc::new(file), start: offset, max_index });
            offset += pos as u64;
        }

        // Keep only the entries that extend the snapshot. If the WAL holds the
        // snapshot's last entry with a different term, the log was superseded
        // by an installed snapshot and is dropped (Raft §7).
        let (s_index, s_term) = rec.snapshot.as_ref().map_or((0, 0), |s| (s.index, s.term));
        let mut superseded = false;
        rec.entries = match log.iter().position(|e| e.index == s_index) {
            Some(p) if log[p].term == s_term => log.split_off(p + 1),
            Some(_) => {
                superseded = true;
                Vec::new()
            }
            // Segments up to the snapshot were deleted.
            None if log.first().is_none_or(|e| e.index == s_index + 1) => log,
            // Everything left is older than the snapshot.
            None if log.last().is_some_and(|e| e.index < s_index) => Vec::new(),
            None => return Err(corrupt("WAL does not continue the snapshot")),
        };

        let wal = Mutex::new(Wal { segs, written: offset, synced: offset });
        let s = Storage {
            dir: dir.to_path_buf(),
            fsync,
            segment_bytes: SEGMENT_BYTES,
            wal,
            fsyncs_running: AtomicUsize::new(0),
        };
        {
            let mut w = s.wal.lock().unwrap();
            if w.segs.is_empty() {
                s.new_segment(&mut w, 1)?;
            }
        }
        if superseded {
            // We crashed after installing the snapshot but before logging its
            // anchor. Log it now, or entries appended later would be replayed
            // on top of the stale log and dropped again on the next restart.
            let anchor = Entry { index: s_index, term: s_term, data: vec![] };
            s.append(&WalRecord { term: rec.term, vote: rec.vote, entries: vec![anchor] })?;
            s.sync()?;
        }
        Ok((s, rec))
    }

    /// Appends a record. Not durable until `sync` returns.
    pub fn append(&self, rec: &WalRecord) -> io::Result<()> {
        let buf = frame(rec);
        let mut w = self.wal.lock().unwrap();
        let active = w.segs.last().unwrap();
        if w.written > active.start && w.written - active.start + buf.len() as u64 > self.segment_bytes {
            // Roll over: make the full segment durable, then start a new one.
            self.fsync(&active.file)?;
            let seq = active.seq + 1;
            w.synced = w.written;
            self.new_segment(&mut w, seq)?;
        }
        let active = w.segs.last_mut().unwrap();
        (&*active.file).write_all(&buf)?;
        if let Some(e) = rec.entries.last() {
            active.max_index = active.max_index.max(e.index);
        }
        w.written += buf.len() as u64;
        Ok(())
    }

    /// Makes every record appended before this call durable. Concurrent calls
    /// are fine; a call with nothing new to sync returns immediately.
    pub fn sync(&self) -> io::Result<()> {
        let (file, target) = {
            let w = self.wal.lock().unwrap();
            if w.synced >= w.written {
                return Ok(());
            }
            (w.segs.last().unwrap().file.clone(), w.written)
        };
        // If a roll-over happens meanwhile, it fsyncs this file itself.
        self.fsync(&file)?;
        let mut w = self.wal.lock().unwrap();
        w.synced = w.synced.max(target);
        Ok(())
    }

    /// Atomically replaces the snapshot file.
    pub fn save_snapshot(&self, snap: &SnapshotFile) -> io::Result<()> {
        let tmp = self.dir.join("snapshot.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&frame(snap))?;
        self.fsync(&f)?;
        fs::rename(&tmp, self.dir.join("snapshot"))?;
        self.sync_dir()
    }

    /// Deletes WAL segments that only hold entries at or below `index`. Call
    /// only after a snapshot covering `index` has been saved.
    pub fn compact(&self, index: u64) -> io::Result<()> {
        self.sync()?; // the newest hard state must be durable before old copies go
        let mut w = self.wal.lock().unwrap();
        // Keep the oldest segment unless a later one holds a durable record:
        // the newest durable (term, vote) must survive the deletion.
        while w.segs.len() > 1 && w.segs[0].max_index <= index && w.synced > w.segs[1].start {
            let seg = w.segs.remove(0);
            fs::remove_file(segment_path(&self.dir, seg.seq))?;
        }
        Ok(())
    }

    /// Test hook that simulates a power cut: everything not yet fsynced is lost.
    pub fn crash(&self) {
        let w = self.wal.lock().unwrap();
        let active = w.segs.last().unwrap();
        let _ = active.file.set_len(w.synced - active.start);
    }

    fn new_segment(&self, w: &mut Wal, seq: u64) -> io::Result<()> {
        let path = segment_path(&self.dir, seq);
        let file = OpenOptions::new().create_new(true).append(true).open(path)?;
        self.sync_dir()?;
        w.segs.push(Segment { seq, file: Arc::new(file), start: w.written, max_index: 0 });
        Ok(())
    }

    /// True while an fsync is in progress. Raft uses this to avoid calling
    /// an election because its own disk stalled: it could not vote for itself
    /// until the disk responds anyway, and the attempt would only depose a
    /// healthy leader. (5 replicas sharing one laptop SSD see 100+ ms stalls.)
    pub fn busy(&self) -> bool {
        self.fsyncs_running.load(Ordering::Relaxed) > 0
    }

    fn sync_dir(&self) -> io::Result<()> {
        self.fsync(&File::open(&self.dir)?)
    }

    /// Plain POSIX fsync. Rust's `File::sync_all` uses `F_FULLFSYNC` on
    /// macOS, which also flushes the drive's write cache and costs ~4-20 ms;
    /// `fsync` hands the data to the drive, as Linux's fsync does.
    fn fsync(&self, f: &File) -> io::Result<()> {
        if !self.fsync {
            return Ok(());
        }
        self.fsyncs_running.fetch_add(1, Ordering::Relaxed);
        let r = unsafe { libc::fsync(f.as_raw_fd()) };
        self.fsyncs_running.fetch_sub(1, Ordering::Relaxed);
        if r == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// Applies one logged entry to the log being rebuilt: it replaces whatever
/// was at its index and everything after. An entry beyond the end (the anchor
/// Raft logs when it installs a snapshot) starts the log over.
fn replay(log: &mut Vec<Entry>, e: Entry) {
    if let (Some(first), Some(last)) = (log.first(), log.last()) {
        if e.index > last.index + 1 {
            log.clear();
        } else {
            log.truncate(e.index.saturating_sub(first.index) as usize);
        }
    }
    log.push(e);
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{seq:016}"))
}

fn frame(msg: &impl Message) -> Vec<u8> {
    let body = msg.encode_to_vec();
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Returns the frame's body and the number of bytes consumed, or `None` if the
/// buffer does not start with a complete, intact frame.
fn unframe(buf: &[u8]) -> Option<(&[u8], usize)> {
    let len = u32::from_le_bytes(buf.get(0..4)?.try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf.get(4..8)?.try_into().unwrap());
    let body = buf.get(8..8 + len)?;
    (crc32fast::hash(body) == crc).then_some((body, 8 + len))
}

fn corrupt(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("corrupt {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(index: u64, term: u64) -> Entry {
        Entry { index, term, data: vec![index as u8; 10] }
    }
    fn rec(term: u64, vote: u64, entries: Vec<Entry>) -> WalRecord {
        WalRecord { term, vote, entries }
    }
    fn indexes(r: &Recovered) -> Vec<(u64, u64)> {
        r.entries.iter().map(|e| (e.index, e.term)).collect()
    }
    fn segments(dir: &Path) -> usize {
        let names = fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap());
        names.filter(|n| n.starts_with("wal-")).count()
    }

    #[test]
    fn replay_applies_truncation_and_last_hard_state() {
        let dir = tempfile::tempdir().unwrap();
        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!((r.term, r.vote, r.entries.len()), (0, 0, 0));
        s.append(&rec(1, 1, vec![e(1, 1), e(2, 1), e(3, 1)])).unwrap();
        s.append(&rec(2, 3, vec![e(2, 2)])).unwrap(); // conflict at 2: drop 2..
        s.append(&rec(2, 3, vec![e(3, 2)])).unwrap();
        s.sync().unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!((r.term, r.vote), (2, 3));
        assert_eq!(indexes(&r), vec![(1, 1), (2, 2), (3, 2)]);
    }

    #[test]
    fn torn_tail_is_dropped_and_writes_continue() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(1, 0, vec![e(1, 1)])).unwrap();
        s.append(&rec(1, 0, vec![e(2, 1)])).unwrap();
        drop(s);
        let path = segment_path(dir.path(), 1);
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new().write(true).open(&path).unwrap().set_len(len - 3).unwrap();

        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(1, 1)]);
        s.append(&rec(1, 0, vec![e(2, 1)])).unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(1, 1), (2, 1)]);
    }

    #[test]
    fn crash_loses_only_unsynced_records() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(1, 0, vec![e(1, 1)])).unwrap();
        s.sync().unwrap();
        s.append(&rec(1, 0, vec![e(2, 1)])).unwrap();
        s.crash();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(1, 1)]);
    }

    #[test]
    fn segments_roll_over_and_compact_after_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut s, _) = Storage::open(dir.path(), true).unwrap();
        s.segment_bytes = 100; // a few entries per segment
        for i in 1..=20 {
            s.append(&rec(1, 1, vec![e(i, 1)])).unwrap();
        }
        assert!(segments(dir.path()) > 4);
        s.save_snapshot(&SnapshotFile { index: 12, term: 1, data: b"snap".to_vec() }).unwrap();
        s.compact(12).unwrap();
        let left = segments(dir.path());
        assert!(left < 4, "{left} segments left");
        drop(s);

        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(r.snapshot.as_ref().unwrap().data, b"snap");
        assert_eq!(indexes(&r), (13..=20).map(|i| (i, 1)).collect::<Vec<_>>());
        s.append(&rec(2, 0, vec![e(21, 2)])).unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!((r.term, r.vote), (2, 0));
        assert_eq!(r.entries.last().map(|e| (e.index, e.term)), Some((21, 2)));
    }

    #[test]
    fn crash_between_snapshot_and_compaction_keeps_the_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(1, 1, vec![e(1, 1), e(2, 1), e(3, 1), e(4, 1)])).unwrap();
        s.save_snapshot(&SnapshotFile { index: 2, term: 1, data: vec![] }).unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(3, 1), (4, 1)]);
    }

    #[test]
    fn conflicting_log_is_dropped_after_installed_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(1, 1, vec![e(1, 1), e(2, 1), e(3, 1)])).unwrap();
        // A leader's snapshot whose entry at index 3 has a different term.
        s.save_snapshot(&SnapshotFile { index: 3, term: 2, data: vec![] }).unwrap();
        drop(s);
        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert!(r.entries.is_empty());

        // Raft logs the snapshot's last entry as an anchor; new entries follow it.
        s.append(&rec(2, 0, vec![Entry { index: 3, term: 2, data: vec![] }])).unwrap();
        s.append(&rec(2, 0, vec![e(4, 2)])).unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(4, 2)]);
    }

    #[test]
    fn crash_before_the_anchor_does_not_lose_later_entries() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(2, 1, vec![e(1, 1), e(2, 2), e(3, 2)])).unwrap();
        // Snapshot at 2 from a leader whose entry 2 has term 3; crash before
        // the anchor is logged.
        s.save_snapshot(&SnapshotFile { index: 2, term: 3, data: vec![] }).unwrap();
        drop(s);
        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert!(r.entries.is_empty());
        s.append(&rec(3, 0, vec![e(3, 3), e(4, 3)])).unwrap();
        s.sync().unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(3, 3), (4, 3)]);
    }

    #[test]
    fn compaction_keeps_the_only_durable_hard_state() {
        let dir = tempfile::tempdir().unwrap();
        let (mut s, _) = Storage::open(dir.path(), true).unwrap();
        s.segment_bytes = 100;
        for i in 1..=6 {
            s.append(&rec(7, 3, vec![e(i, 1)])).unwrap();
        }
        s.sync().unwrap();
        // Force a roll-over whose first record is not yet durable.
        let mut w = s.wal.lock().unwrap();
        let seq = w.segs.last().unwrap().seq + 1;
        s.new_segment(&mut w, seq).unwrap();
        drop(w);
        s.append(&rec(8, 0, vec![])).unwrap();
        s.save_snapshot(&SnapshotFile { index: 6, term: 1, data: vec![] }).unwrap();
        // compact syncs first, so test the case where nothing new is written
        // after a crash leaves the newest segment empty.
        s.crash();
        drop(s);
        let (s, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!((r.term, r.vote), (7, 3));
        s.compact(6).unwrap();
        s.crash();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!((r.term, r.vote), (7, 3), "compaction lost the hard state");
    }

    #[test]
    fn anchor_beyond_the_log_end_starts_the_log_over() {
        let dir = tempfile::tempdir().unwrap();
        let (s, _) = Storage::open(dir.path(), true).unwrap();
        s.append(&rec(1, 1, vec![e(1, 1), e(2, 1)])).unwrap();
        // A lagging follower installs a snapshot at 50 (term 3) and goes on.
        s.save_snapshot(&SnapshotFile { index: 50, term: 3, data: vec![] }).unwrap();
        s.append(&rec(3, 0, vec![Entry { index: 50, term: 3, data: vec![] }])).unwrap();
        s.append(&rec(3, 0, vec![e(51, 3), e(52, 3)])).unwrap();
        drop(s);
        let (_, r) = Storage::open(dir.path(), true).unwrap();
        assert_eq!(indexes(&r), vec![(51, 3), (52, 3)]);
    }
}
