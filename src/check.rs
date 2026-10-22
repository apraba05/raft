//! Linearizability checker for key-value histories.
//!
//! Operations on different keys are independent, so the history is split by
//! key and each key is checked on its own. For one key this is the
//! Wing–Gong / Lowe search: repeatedly pick an operation that could take
//! effect next (it was invoked before every other pending operation
//! returned), apply it to the model, and backtrack when the model rejects it.
//! Visited (set-of-linearized-ops, state) pairs are memoised so each
//! configuration is explored once.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Get,
    Put,
    Append,
}

#[derive(Clone, Debug)]
pub struct Op {
    pub key: String,
    pub kind: Kind,
    /// Input for Put/Append; the observed output for Get.
    pub value: Vec<u8>,
    pub call: u64, // invocation time (any monotonic unit)
    pub ret: u64,  // response time
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// The key whose history has no valid linearization.
    Violation(String),
    /// Gave up after the step budget; neither proven nor refuted.
    Unknown(String),
}

/// Checks a complete history (every operation returned).
pub fn check(history: &[Op], max_steps: usize) -> Outcome {
    let mut by_key: HashMap<&str, Vec<&Op>> = HashMap::new();
    for op in history {
        by_key.entry(&op.key).or_default().push(op);
    }
    for (key, ops) in by_key {
        match check_key(ops, max_steps) {
            Some(true) => {}
            Some(false) => return Outcome::Violation(key.to_string()),
            None => return Outcome::Unknown(key.to_string()),
        }
    }
    Outcome::Ok
}

/// The model: the key's value ("" if never written).
fn step(state: &[u8], op: &Op) -> Option<Vec<u8>> {
    match op.kind {
        Kind::Get => (op.value == state).then(|| state.to_vec()),
        Kind::Put => Some(op.value.clone()),
        Kind::Append => Some([state, &op.value].concat()),
    }
}

fn check_key(mut ops: Vec<&Op>, max_steps: usize) -> Option<bool> {
    ops.sort_by_key(|o| o.call);
    let n = ops.len();
    let mut done = vec![false; n];
    let mut state: Vec<u8> = Vec::new();
    // Each frame: the op we linearized and the state before it.
    let mut stack: Vec<(usize, Vec<u8>)> = Vec::new();
    let mut seen: HashSet<(Vec<bool>, Vec<u8>)> = HashSet::new();
    let mut from = 0; // where to resume the candidate scan after backtracking
    for _ in 0..max_steps {
        if stack.len() == n {
            return Some(true);
        }
        // An op can go next only if it was invoked before every pending op returned.
        let min_ret = (0..n).filter(|&i| !done[i]).map(|i| ops[i].ret).min().unwrap();
        let mut picked = None;
        for i in from..n {
            if ops[i].call >= min_ret {
                break;
            }
            if done[i] {
                continue;
            }
            if let Some(next) = step(&state, ops[i]) {
                done[i] = true;
                if seen.insert((done.clone(), next.clone())) {
                    picked = Some((i, next));
                    break;
                }
                done[i] = false;
            }
        }
        match picked {
            Some((i, next)) => {
                stack.push((i, std::mem::replace(&mut state, next)));
                from = 0;
            }
            None => {
                let Some((i, prev)) = stack.pop() else {
                    return Some(false);
                };
                done[i] = false;
                state = prev;
                from = i + 1;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(kind: Kind, value: &str, call: u64, ret: u64) -> Op {
        Op { key: "k".into(), kind, value: value.into(), call, ret }
    }
    use Kind::*;

    #[test]
    fn sequential_history_is_linearizable() {
        let h = [op(Put, "a", 0, 1), op(Append, "b", 2, 3), op(Get, "ab", 4, 5)];
        assert_eq!(check(&h, 1_000_000), Outcome::Ok);
    }

    #[test]
    fn stale_read_is_a_violation() {
        let h = [op(Put, "a", 0, 1), op(Put, "b", 2, 3), op(Get, "a", 4, 5)];
        assert_eq!(check(&h, 1_000_000), Outcome::Violation("k".into()));
    }

    #[test]
    fn concurrent_ops_may_take_effect_in_either_order() {
        // The Get overlaps both Puts, so it may see either value.
        let h = [op(Put, "a", 0, 10), op(Put, "b", 1, 10), op(Get, "a", 2, 11), op(Get, "b", 12, 13)];
        assert_eq!(check(&h, 1_000_000), Outcome::Ok);
    }

    #[test]
    fn duplicated_append_is_a_violation() {
        let h = [op(Append, "x", 0, 1), op(Get, "xx", 2, 3)];
        assert_eq!(check(&h, 1_000_000), Outcome::Violation("k".into()));
    }

    #[test]
    fn read_must_not_go_backwards() {
        // Concurrent with the Put, the first Get sees "b"; a later Get then
        // seeing the old value is impossible.
        let h = [op(Put, "a", 0, 1), op(Put, "b", 2, 20), op(Get, "b", 3, 4), op(Get, "a", 5, 6)];
        assert_eq!(check(&h, 1_000_000), Outcome::Violation("k".into()));
    }
}
