use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: String,
    pub commit_ts: u64,
    pub manifest_id: String,
    pub created_unix: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackupLog {
    #[serde(default)]
    checkpoints: Vec<Checkpoint>,
}

pub const BACKUP_LOG_KEEP: usize = 128;

impl BackupLog {
    pub fn new() -> Self {
        Self { checkpoints: Vec::new() }
    }

    pub fn record(&mut self, checkpoint: Checkpoint) {
        self.checkpoints.push(checkpoint);
        self.checkpoints.sort_by_key(|c| c.commit_ts);
        if self.checkpoints.len() > BACKUP_LOG_KEEP {
            let excess = self.checkpoints.len() - BACKUP_LOG_KEEP;
            self.checkpoints.drain(0..excess);
        }
    }

    pub fn select_pitr(&self, target_ts: u64) -> Result<Checkpoint> {
        self.checkpoints
            .iter()
            .rev()
            .find(|c| c.commit_ts <= target_ts)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("checkpoint")))
    }

    pub fn latest(&self) -> Option<Checkpoint> {
        self.checkpoints.last().cloned()
    }

    pub fn from_checkpoints(checkpoints: Vec<Checkpoint>) -> Self {
        let mut log = Self::new();
        for checkpoint in checkpoints {
            log.record(checkpoint);
        }
        log
    }

    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    pub fn retain(&mut self, keep: usize) -> Vec<Checkpoint> {
        if self.checkpoints.len() <= keep {
            return Vec::new();
        }
        let remove = self.checkpoints.len() - keep;
        self.checkpoints.drain(0..remove).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint(commit_ts: u64) -> Checkpoint {
        Checkpoint {
            id: format!("ckpt-{commit_ts}"),
            commit_ts,
            manifest_id: String::from("genesis"),
            created_unix: commit_ts,
        }
    }

    #[test]
    fn record_caps_log_and_keeps_newest() {
        let mut log = BackupLog::new();
        for commit in 0..(BACKUP_LOG_KEEP + 10) as u64 {
            log.record(checkpoint(commit));
        }
        assert_eq!(log.checkpoints.len(), BACKUP_LOG_KEEP);
        assert_eq!(log.latest().unwrap().commit_ts, (BACKUP_LOG_KEEP + 10) as u64 - 1);
        assert_eq!(log.select_pitr(10).unwrap().commit_ts, 10);
        assert!(log.select_pitr(9).is_err());
        assert_eq!(log.select_pitr(u64::MAX).unwrap().commit_ts, (BACKUP_LOG_KEEP + 10) as u64 - 1);
    }
}
