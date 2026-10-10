use ryme_backup::BackupLog;
use ryme_branch::BranchManager;
use ryme_error::{Result, RymeError};
use ryme_router::{Range, Router};
use serde::{Deserialize, Serialize};

fn split_midpoint(start: &[u8], end: &[u8]) -> Result<Vec<u8>> {
    if start.is_empty() && end.is_empty() {
        return Ok(vec![0x80]);
    }
    if end.is_empty() {
        let mut mid = start.to_vec();
        mid.push(0x80);
        return Ok(mid);
    }
    if start.is_empty() {
        let mut trimmed = end.to_vec();
        while trimmed.last() == Some(&0) {
            trimmed.pop();
        }
        let Some(last) = trimmed.pop() else {
            return Err(RymeError::InvalidArgument(String::from("unsplittable range")));
        };
        trimmed.push(last / 2);
        if trimmed.is_empty() || trimmed.as_slice() >= end {
            return Err(RymeError::InvalidArgument(String::from("unsplittable range")));
        }
        return Ok(trimmed);
    }
    let common = start.iter().zip(end.iter()).take_while(|(a, b)| a == b).count();
    if common == end.len() {
        return Err(RymeError::InvalidArgument(String::from("unsplittable range")));
    }
    if common == start.len() {
        let next = end[common];
        let mut mid = start.to_vec();
        mid.push(next / 2);
        return Ok(mid);
    }
    let (low, high) = (start[common] as u16, end[common] as u16);
    if low >= high {
        return Err(RymeError::InvalidArgument(String::from("unsplittable range")));
    }
    if high - low >= 2 {
        let mut mid = start[..common].to_vec();
        mid.push(((low + high) / 2) as u8);
        Ok(mid)
    } else {
        let mut mid = start.to_vec();
        mid.push(0x80);
        Ok(mid)
    }
}

#[derive(Debug, Default)]
pub struct ControlPlane {
    pub branches: BranchManager,
    pub backups: BackupLog,
    pub router: Router,
    pub migrations: ryme_migrate::Ledger,
}

pub const AUTO_SPLIT_MAX_RANGES: usize = 256;

impl ControlPlane {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_range(&mut self, range: Range) {
        self.router.insert(range);
    }

    pub fn restore_ranges(&mut self, mut ranges: Vec<Range>) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        ranges.sort_by(|left, right| left.start.cmp(&right.start));
        if !ranges[0].start.is_empty()
            || ranges.last().map(|range| !range.end.is_empty()).unwrap_or(true)
        {
            return Err(RymeError::Corrupt(String::from("range snapshot does not cover keyspace")));
        }
        let mut router = Router::new();
        let mut ids = std::collections::HashSet::new();
        for (index, range) in ranges.iter().enumerate() {
            if !ids.insert(range.id.clone()) {
                return Err(RymeError::Corrupt(String::from("duplicate range id")));
            }
            if !range.end.is_empty() && range.start >= range.end {
                return Err(RymeError::Corrupt(String::from("invalid range bounds")));
            }
            if let Some(next) = ranges.get(index + 1) {
                if range.end.is_empty() || range.end != next.start {
                    return Err(RymeError::Corrupt(String::from("range snapshot has a gap")));
                }
            }
            router.insert(range.clone());
        }
        self.router = router;
        Ok(())
    }

    pub fn route(&self, key: &[u8]) -> Result<Range> {
        self.router.route(key)
    }

    pub fn ranges(&self) -> Vec<Range> {
        self.router.list()
    }

    pub fn router_get(&self, id: &str) -> Result<Range> {
        self.router.get(id)
    }

    pub fn split_range(
        &mut self,
        id: &str,
        mid: Vec<u8>,
        left_id: String,
        right_id: String,
        expected_epoch: u64,
    ) -> Result<()> {
        let current = self.router.get(id)?;
        if current.epoch != expected_epoch {
            return Err(RymeError::Conflict(format!(
                "stale epoch for range {id}: expected {expected_epoch}, current {}",
                current.epoch
            )));
        }
        self.router.split(id, mid, left_id, right_id)
    }

    pub fn merge_ranges(
        &mut self,
        left_id: &str,
        right_id: &str,
        merged_id: String,
        expected_left_epoch: u64,
        expected_right_epoch: u64,
    ) -> Result<()> {
        let left = self.router.get(left_id)?;
        if left.epoch != expected_left_epoch {
            return Err(RymeError::Conflict(format!(
                "stale epoch for range {left_id}: expected {expected_left_epoch}, current {}",
                left.epoch
            )));
        }
        let right = self.router.get(right_id)?;
        if right.epoch != expected_right_epoch {
            return Err(RymeError::Conflict(format!(
                "stale epoch for range {right_id}: expected {expected_right_epoch}, current {}",
                right.epoch
            )));
        }
        self.router.merge(left_id, right_id, merged_id)
    }

    pub fn move_range(&mut self, id: &str, leader: String, expected_epoch: u64) -> Result<Range> {
        let current = self.router.get(id)?;
        if current.epoch != expected_epoch {
            return Err(RymeError::Conflict(format!(
                "stale epoch for range {id}: expected {expected_epoch}, current {}",
                current.epoch
            )));
        }
        self.router.move_leader(id, leader)
    }

    pub fn range_loads(&self) -> Vec<ryme_router::RangeLoad> {
        self.router.loads()
    }

    pub fn split_candidates(&self, min_writes: u64) -> Vec<(String, u64)> {
        self.router
            .loads()
            .into_iter()
            .filter(|load| load.writes >= min_writes)
            .map(|load| (load.id, load.epoch))
            .collect()
    }

    pub fn range_midpoint(&self, id: &str) -> Result<Vec<u8>> {
        let range = self.router.get(id)?;
        split_midpoint(&range.start, &range.end)
    }

    pub fn auto_split_once(&mut self, min_writes: u64) -> Result<Vec<String>> {
        let mut created = Vec::new();
        if self.router.list().len() >= AUTO_SPLIT_MAX_RANGES {
            return Ok(created);
        }
        for (id, epoch) in self.split_candidates(min_writes) {
            let mid = match self.range_midpoint(&id) {
                Ok(mid) => mid,
                Err(_) => continue,
            };
            let left_id = format!("{id}-a-{epoch}");
            let right_id = format!("{id}-b-{epoch}");
            match self.split_range(&id, mid, left_id.clone(), right_id.clone(), epoch) {
                Ok(()) => {
                    created.push(left_id);
                    created.push(right_id);
                }
                Err(_) => continue,
            }
        }
        Ok(created)
    }

    pub fn create_branch(&mut self, id: String, parent: &str, base_commit_ts: u64) -> Result<()> {
        self.branches.create_child(id, parent, base_commit_ts)
    }

    pub fn delete_branch(&mut self, id: &str) -> Result<Vec<String>> {
        let garbage = self.branches.delete(id)?;
        Ok(garbage)
    }

    pub fn list_branches(&self) -> Vec<ryme_branch::Branch> {
        self.branches.list()
    }

    pub fn reset_branch(&mut self, id: &str, base_commit_ts: u64) -> Result<ryme_branch::Branch> {
        self.branches.reset(id, base_commit_ts)
    }

    pub fn promote_branch(&mut self, id: &str) -> Result<ryme_branch::Branch> {
        self.branches.promote(id)
    }

    pub fn diff_branches(&self, left: &str, right: &str) -> Result<(Vec<String>, Vec<String>)> {
        self.branches.diff(left, right)
    }

    pub fn require_branch(&self, id: &str) -> Result<()> {
        self.branches.get(id).map(|_| ()).map_err(|e| match e {
            RymeError::NotFound(_) => RymeError::NotFound(String::from("branch")),
            other => other,
        })
    }

    pub fn advise(&self, input: &AutoscaleInput) -> AutoscaleAdvice {
        AutoscaleAdvice::from_input(input)
    }

    pub fn health(&self, input: &HealthInput) -> HealthReport {
        HealthReport::from_input(input)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pressure {
    Cpu,
    Connections,
    ShardQps,
    Storage,
    ReadFanout,
    RealtimeSockets,
    CompactionDebt,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoscaleInput {
    pub cpu_pct: u8,
    pub active_connections: u64,
    pub connection_limit: u64,
    pub shard_qps: u64,
    pub shard_qps_limit: u64,
    pub disk_used_pct: u8,
    pub follower_lag_ms: u64,
    pub realtime_sockets: u64,
    pub realtime_lag_ms: u64,
    pub compaction_debt_mb: u64,
    pub p99_queue_ms: u64,
}

impl AutoscaleInput {
    pub fn healthy() -> Self {
        Self {
            cpu_pct: 20,
            active_connections: 10,
            connection_limit: 10000,
            shard_qps: 100,
            shard_qps_limit: 200000,
            disk_used_pct: 30,
            follower_lag_ms: 5,
            realtime_sockets: 100,
            realtime_lag_ms: 5,
            compaction_debt_mb: 8,
            p99_queue_ms: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoscaleAdvice {
    pub pressure: Pressure,
    pub action: String,
    pub urgent: bool,
}

impl AutoscaleAdvice {
    pub fn from_input(input: &AutoscaleInput) -> Self {
        if input.disk_used_pct >= 85 || input.compaction_debt_mb >= 4096 {
            return Self {
                pressure: if input.disk_used_pct >= 85 {
                    Pressure::Storage
                } else {
                    Pressure::CompactionDebt
                },
                action: String::from("add_io_capacity_or_throttle_writes"),
                urgent: true,
            };
        }
        if input.realtime_lag_ms >= 500 || input.realtime_sockets >= 800000 {
            return Self {
                pressure: Pressure::RealtimeSockets,
                action: String::from("add_realtime_partition"),
                urgent: input.realtime_lag_ms >= 2000,
            };
        }
        if input.follower_lag_ms >= 1000 {
            return Self {
                pressure: Pressure::ReadFanout,
                action: String::from("add_follower"),
                urgent: input.follower_lag_ms >= 5000,
            };
        }
        let conn_ratio =
            input.active_connections.saturating_mul(100) / input.connection_limit.max(1);
        if input.cpu_pct >= 75 || conn_ratio >= 80 || input.p99_queue_ms >= 25 {
            return Self {
                pressure: if input.cpu_pct >= 75 { Pressure::Cpu } else { Pressure::Connections },
                action: String::from("add_gateways_or_compute"),
                urgent: input.cpu_pct >= 90 || conn_ratio >= 95,
            };
        }
        if input.shard_qps >= input.shard_qps_limit {
            return Self {
                pressure: Pressure::ShardQps,
                action: String::from("split_hot_range"),
                urgent: false,
            };
        }
        Self { pressure: Pressure::None, action: String::from("hold"), urgent: false }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInput {
    pub quorum_healthy: bool,
    pub p99_within_slo: bool,
    pub archive_caught_up: bool,
    pub realtime_within_slo: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub ready: bool,
    pub reason: String,
}

impl HealthReport {
    pub fn from_input(input: &HealthInput) -> Self {
        if !input.quorum_healthy {
            return Self { ready: false, reason: String::from("quorum_degraded") };
        }
        if !input.archive_caught_up {
            return Self { ready: true, reason: String::from("archive_lagging") };
        }
        if !input.p99_within_slo {
            return Self { ready: true, reason: String::from("latency_slo_breach") };
        }
        if !input.realtime_within_slo {
            return Self { ready: true, reason: String::from("realtime_lagging") };
        }
        Self { ready: true, reason: String::from("ok") }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calm_input_holds() {
        let plane = ControlPlane::new();
        let advice = plane.advise(&AutoscaleInput::healthy());
        assert_eq!(advice.pressure, Pressure::None);
        assert!(!advice.urgent);
    }

    #[test]
    fn disk_pressure_is_urgent() {
        let plane = ControlPlane::new();
        let mut input = AutoscaleInput::healthy();
        input.disk_used_pct = 92;
        let advice = plane.advise(&input);
        assert_eq!(advice.pressure, Pressure::Storage);
        assert!(advice.urgent);
    }

    #[test]
    fn quorum_failure_not_ready() {
        let plane = ControlPlane::new();
        let report = plane.health(&HealthInput {
            quorum_healthy: false,
            p99_within_slo: true,
            archive_caught_up: true,
            realtime_within_slo: true,
        });
        assert!(!report.ready);
    }

    fn ranged(id: &str, start: &[u8], end: &[u8]) -> Range {
        Range::new(String::from(id), start.to_vec(), end.to_vec(), String::from("n1"), 0)
    }

    #[test]
    fn midpoint_vectors() {
        assert_eq!(split_midpoint(&[], &[]).unwrap(), vec![0x80]);
        assert_eq!(split_midpoint(b"abc", &[]).unwrap(), b"abc\x80".to_vec());
        assert_eq!(split_midpoint(&[], &[0x80]).unwrap(), vec![0x40]);
        assert_eq!(split_midpoint(&[], &[0x01]).unwrap(), vec![0x00]);
        assert_eq!(split_midpoint(b"a", b"c").unwrap(), b"b".to_vec());
        assert_eq!(split_midpoint(b"a", b"b").unwrap(), b"a\x80".to_vec());
        assert_eq!(split_midpoint(b"a", b"az").unwrap(), b"a=".to_vec());
        assert!(split_midpoint(&[], &[0x00]).is_err());
        assert!(split_midpoint(b"z", b"a").is_err());
    }

    #[test]
    fn auto_split_fires_only_on_hot_ranges() {
        let mut plane = ControlPlane::new();
        plane.add_range(ranged("hot", &[], &[]));
        plane.router.note_write(b"k1", 5).unwrap();
        plane.router.note_write(b"k2", 5).unwrap();
        assert!(plane.split_candidates(11).is_empty());
        assert_eq!(plane.split_candidates(10).len(), 1);
        let created = plane.auto_split_once(10).unwrap();
        assert_eq!(created.len(), 2);
        assert_eq!(plane.ranges().len(), 2);
        assert!(plane.router.loads().iter().all(|load| load.writes == 0));
        assert!(plane.auto_split_once(10).unwrap().is_empty());
        assert!(plane.range_midpoint(&created[0]).is_ok());
    }

    #[test]
    fn auto_split_stops_at_max_ranges() {
        let mut plane = ControlPlane::new();
        for index in 0..AUTO_SPLIT_MAX_RANGES {
            plane.add_range(ranged(&format!("r-{index}"), &[index as u8], &[]));
        }
        assert_eq!(plane.ranges().len(), AUTO_SPLIT_MAX_RANGES);
        plane.router.note_write(b"k1", 100).unwrap();
        assert!(plane.auto_split_once(10).unwrap().is_empty());
        assert_eq!(plane.ranges().len(), AUTO_SPLIT_MAX_RANGES);
    }

    #[test]
    fn auto_split_skips_unsplittable_ranges() {
        let mut plane = ControlPlane::new();
        plane.add_range(ranged("tiny", &[], &[0x00]));
        plane.router.note_write(&[0x00], 100).unwrap_err();
        plane.add_range(ranged("hot", &[0x00], &[]));
        plane.router.note_write(&[0x05], 50).unwrap();
        let created = plane.auto_split_once(10).unwrap();
        assert_eq!(created.len(), 2);
        assert!(plane.router.get("tiny-a-0").is_err());
    }

    #[test]
    fn restore_ranges_preserves_topology_and_rejects_gaps() {
        let mut plane = ControlPlane::new();
        plane.restore_ranges(vec![ranged("right", b"m", &[]), ranged("left", &[], b"m")]).unwrap();
        assert_eq!(plane.route(b"a").unwrap().id, "left");
        assert_eq!(plane.route(b"z").unwrap().id, "right");

        let error = plane
            .restore_ranges(vec![ranged("gap", &[], b"m"), ranged("tail", b"z", &[])])
            .unwrap_err();
        assert!(matches!(error, RymeError::Corrupt(_)));
    }
}
