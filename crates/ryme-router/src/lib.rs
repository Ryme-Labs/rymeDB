use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub type RangeLoadHookFn = Arc<dyn Fn(&[u8], u64) + Send + Sync>;

#[derive(Clone, Default)]
pub struct RangeLoadHook {
    inner: Option<RangeLoadHookFn>,
}

impl std::fmt::Debug for RangeLoadHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.inner.is_some() {
            "RangeLoadHook(armed)"
        } else {
            "RangeLoadHook(off)"
        })
    }
}

impl RangeLoadHook {
    pub fn armed(hook: RangeLoadHookFn) -> Self {
        Self { inner: Some(hook) }
    }

    pub fn is_armed(&self) -> bool {
        self.inner.is_some()
    }

    pub fn note(&self, key: &[u8], count: u64) {
        if let Some(hook) = self.inner.as_ref() {
            hook(key, count);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Range {
    pub id: String,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub leader: String,
    pub epoch: u64,
    #[serde(skip, default = "fresh_load")]
    pub load: Arc<AtomicU64>,
}

fn fresh_load() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(0))
}

impl Range {
    pub fn new(id: String, start: Vec<u8>, end: Vec<u8>, leader: String, epoch: u64) -> Self {
        Self { id, start, end, leader, epoch, load: fresh_load() }
    }

    pub fn writes(&self) -> u64 {
        self.load.load(Ordering::Relaxed)
    }
}

impl PartialEq for Range {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.start == other.start
            && self.end == other.end
            && self.leader == other.leader
            && self.epoch == other.epoch
    }
}

impl Eq for Range {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeLoad {
    pub id: String,
    pub epoch: u64,
    pub writes: u64,
}

#[derive(Debug, Default, Clone)]
pub struct Router {
    ranges: BTreeMap<Vec<u8>, Range>,
}

impl Router {
    pub fn new() -> Self {
        Self { ranges: BTreeMap::new() }
    }

    pub fn insert(&mut self, range: Range) {
        self.ranges.insert(range.start.clone(), range);
    }

    pub fn route(&self, key: &[u8]) -> Result<Range> {
        let candidate = self.ranges.range(..=key.to_vec()).next_back().map(|(_, range)| range);
        match candidate {
            Some(range) if key < range.end.as_slice() || range.end.is_empty() => Ok(range.clone()),
            _ => Err(RymeError::NotFound(String::from("range"))),
        }
    }

    pub fn get(&self, id: &str) -> Result<Range> {
        self.ranges
            .values()
            .find(|range| range.id == id)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("range")))
    }

    pub fn loads(&self) -> Vec<RangeLoad> {
        self.ranges
            .values()
            .map(|range| RangeLoad {
                id: range.id.clone(),
                epoch: range.epoch,
                writes: range.load.load(Ordering::Relaxed),
            })
            .collect()
    }

    pub fn note_write(&self, key: &[u8], count: u64) -> Result<Range> {
        let routed = self.route(key)?;
        routed.load.fetch_add(count, Ordering::Relaxed);
        Ok(routed)
    }

    pub fn list(&self) -> Vec<Range> {
        self.ranges.values().cloned().collect()
    }

    pub fn remove(&mut self, id: &str) -> Result<Range> {
        let target = self.get(id)?;
        self.ranges.remove(&target.start);
        Ok(target)
    }

    pub fn split(
        &mut self,
        id: &str,
        mid: Vec<u8>,
        left_id: String,
        right_id: String,
    ) -> Result<()> {
        let original = self
            .ranges
            .values()
            .find(|r| r.id == id)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("range")))?;
        if mid <= original.start || (!original.end.is_empty() && mid >= original.end) {
            return Err(RymeError::InvalidArgument(String::from("split point")));
        }
        if self.ranges.values().any(|range| range.id == left_id || range.id == right_id) {
            return Err(RymeError::Conflict(String::from("range id exists")));
        }
        self.ranges.remove(&original.start);
        self.ranges.insert(
            original.start.clone(),
            Range {
                id: left_id,
                start: original.start.clone(),
                end: mid.clone(),
                leader: original.leader.clone(),
                epoch: original.epoch + 1,
                load: fresh_load(),
            },
        );
        self.ranges.insert(
            mid.clone(),
            Range {
                id: right_id,
                start: mid,
                end: original.end,
                leader: original.leader,
                epoch: original.epoch + 1,
                load: fresh_load(),
            },
        );
        Ok(())
    }

    pub fn merge(&mut self, left_id: &str, right_id: &str, merged_id: String) -> Result<()> {
        let left = self.get(left_id)?;
        let right = self.get(right_id)?;
        if left.id == right.id {
            return Err(RymeError::InvalidArgument(String::from("self merge")));
        }
        if left.end != right.start {
            return Err(RymeError::InvalidArgument(String::from("not adjacent")));
        }
        if left.leader != right.leader {
            return Err(RymeError::InvalidArgument(String::from("leader mismatch")));
        }
        if self.ranges.values().any(|range| range.id == merged_id) {
            return Err(RymeError::Conflict(String::from("range id exists")));
        }
        self.ranges.remove(&left.start);
        self.ranges.remove(&right.start);
        let epoch = left.epoch.max(right.epoch) + 1;
        self.ranges.insert(
            left.start.clone(),
            Range {
                id: merged_id,
                start: left.start,
                end: right.end,
                leader: left.leader,
                epoch,
                load: fresh_load(),
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeded() -> Router {
        let mut router = Router::new();
        router.insert(Range {
            id: String::from("r0"),
            start: Vec::new(),
            end: Vec::new(),
            leader: String::from("n1"),
            epoch: 0,
            load: fresh_load(),
        });
        router
    }

    #[test]
    fn split_then_route() {
        let mut router = seeded();
        router.split("r0", b"m".to_vec(), String::from("rl"), String::from("rr")).unwrap();
        assert_eq!(router.route(b"a").unwrap().id, "rl");
        assert_eq!(router.route(b"z").unwrap().id, "rr");
        assert_eq!(router.route(b"a").unwrap().epoch, 1);
    }

    #[test]
    fn split_rejects_bad_point_and_dup_id() {
        let mut router = seeded();
        assert!(router.split("r0", Vec::new(), String::from("a"), String::from("b")).is_err());
        assert!(router.split("r0", b"m".to_vec(), String::from("r0"), String::from("b")).is_err());
        assert!(router
            .split("missing", b"m".to_vec(), String::from("a"), String::from("b"))
            .is_err());
    }

    #[test]
    fn merge_roundtrip() {
        let mut router = seeded();
        router.split("r0", b"m".to_vec(), String::from("rl"), String::from("rr")).unwrap();
        router.merge("rl", "rr", String::from("r1")).unwrap();
        let merged = router.get("r1").unwrap();
        assert!(merged.start.is_empty() && merged.end.is_empty());
        assert_eq!(merged.epoch, 2);
        assert_eq!(router.list().len(), 1);
    }

    #[test]
    fn merge_rejects_non_adjacent_and_leader_mismatch() {
        let mut router = Router::new();
        router.insert(Range {
            id: String::from("a"),
            start: Vec::new(),
            end: b"m".to_vec(),
            leader: String::from("n1"),
            epoch: 0,
            load: fresh_load(),
        });
        router.insert(Range {
            id: String::from("b"),
            start: b"z".to_vec(),
            end: Vec::new(),
            leader: String::from("n1"),
            epoch: 0,
            load: fresh_load(),
        });
        assert!(router.merge("a", "b", String::from("c")).is_err());
        router.insert(Range {
            id: String::from("c"),
            start: b"m".to_vec(),
            end: b"z".to_vec(),
            leader: String::from("n2"),
            epoch: 0,
            load: fresh_load(),
        });
        assert!(router.merge("a", "c", String::from("d")).is_err());
    }

    #[test]
    fn note_write_counts_only_routed_keys() {
        let router = seeded();
        router.note_write(b"anything", 3).unwrap();
        router.note_write(b"more", 2).unwrap();
        let loads = router.loads();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].id, "r0");
        assert_eq!(loads[0].writes, 5);
        let empty = Router::new();
        assert!(empty.note_write(b"anything", 1).is_err());
        assert!(empty.loads().is_empty());
    }

    #[test]
    fn split_resets_child_load() {
        let router = seeded();
        router.note_write(b"hot", 9).unwrap();
        let mut router = router;
        router.split("r0", b"m".to_vec(), String::from("rl"), String::from("rr")).unwrap();
        for load in router.loads() {
            assert_eq!(load.writes, 0);
        }
        router.note_write(b"a", 1).unwrap();
        let left = router.loads().into_iter().find(|load| load.id == "rl").unwrap();
        let right = router.loads().into_iter().find(|load| load.id == "rr").unwrap();
        assert_eq!(left.writes, 1);
        assert_eq!(right.writes, 0);
    }

    #[test]
    fn range_serde_omits_load_counter() {
        let range = seeded().get("r0").unwrap();
        let json = serde_json::to_string(&range).unwrap();
        assert!(!json.contains("load"));
        let back: Range = serde_json::from_str(&json).unwrap();
        assert_eq!(back, range);
    }
}
