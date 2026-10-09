use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub op: Operation,
    pub pk: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub commit_ts: u64,
    pub tx_id: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRow {
    pub pk: Vec<u8>,
    pub value: Vec<u8>,
}

pub const PRESENCE_MAX_MEMBERS: usize = 1000;
pub const PRESENCE_MAX_TTL_SECS: u64 = 86400;
const BROADCAST_SHARDS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryUpdate {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub commit_ts: u64,
    pub sequence: u64,
    pub rows: Vec<QueryRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct Realtime {
    inner: Arc<Mutex<RealtimeInner>>,
    broadcast_topics: Arc<Vec<Mutex<HashMap<String, broadcast::Sender<BroadcastMsg>>>>>,
    broadcast_capacity: usize,
    sequence: Arc<AtomicU64>,
}

#[derive(Debug)]
struct QueryTopic {
    sender: broadcast::Sender<QueryUpdate>,
    max_limit: usize,
}

#[derive(Debug)]
struct RealtimeInner {
    topics: HashMap<String, broadcast::Sender<ChangeRecord>>,
    history: HashMap<String, std::collections::VecDeque<ChangeRecord>>,
    queries: HashMap<String, QueryTopic>,
    presence: HashMap<String, HashMap<String, PresenceMember>>,
    durable: HashMap<String, DurableTopic>,
    capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceMember {
    pub member: String,
    pub state: serde_json::Value,
    pub expires_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMsg {
    pub channel: String,
    pub from: String,
    pub payload: serde_json::Value,
    pub commit_ts: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableMsg {
    pub partition: String,
    pub cursor: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub commit_ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableTopicSnapshot {
    pub tenant: String,
    pub partition: String,
    pub messages: Vec<DurableMsg>,
    pub next_cursor: u64,
    pub retention: usize,
}

#[derive(Debug)]
struct DurableTopic {
    messages: VecDeque<DurableMsg>,
    next_cursor: u64,
    retention: usize,
}

#[derive(Debug, Clone)]
pub struct NewChange {
    pub tenant: String,
    pub database: String,
    pub branch: String,
    pub table: String,
    pub op: Operation,
    pub pk: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub commit_ts: u64,
    pub tx_id: u64,
}

impl Realtime {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.clamp(16, 100000);
        Self {
            inner: Arc::new(Mutex::new(RealtimeInner {
                topics: HashMap::new(),
                history: HashMap::new(),
                queries: HashMap::new(),
                presence: HashMap::new(),
                durable: HashMap::new(),
                capacity,
            })),
            broadcast_topics: Arc::new(
                (0..BROADCAST_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            ),
            broadcast_capacity: capacity,
            sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn broadcast_shard(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.broadcast_topics.len()
    }

    pub fn publish(&self, event: NewChange) -> Result<u64> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let sequence = self.next_sequence();
        let record = ChangeRecord {
            tenant: event.tenant.clone(),
            database: event.database.clone(),
            branch: event.branch.clone(),
            table: event.table.clone(),
            op: event.op,
            pk: event.pk.clone(),
            before: event.before.clone(),
            after: event.after.clone(),
            commit_ts: event.commit_ts,
            tx_id: event.tx_id,
            sequence,
        };
        let key = branch_topic_key(&event.tenant, &event.database, &event.branch, &event.table);
        let capacity = inner.capacity;
        let sender =
            inner.topics.entry(key.clone()).or_insert_with(|| broadcast::channel(capacity).0);
        let _ = sender.send(record.clone());
        let log = inner.history.entry(key).or_default();
        while log.len() >= capacity {
            log.pop_front();
        }
        log.push_back(record);
        Ok(sequence)
    }

    pub fn has_subscribers(&self, tenant: &str, database: &str, table: &str) -> bool {
        self.has_subscribers_branch(tenant, database, "main", table)
    }

    pub fn has_subscribers_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> bool {
        let key = branch_topic_key(tenant, database, branch, table);
        self.inner
            .lock()
            .ok()
            .and_then(|inner| inner.topics.get(&key).map(|sender| sender.receiver_count() > 0))
            .unwrap_or(false)
    }

    pub fn subscribe(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> broadcast::Receiver<ChangeRecord> {
        self.subscribe_branch(tenant, database, "main", table)
    }

    pub fn subscribe_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> broadcast::Receiver<ChangeRecord> {
        let key = branch_topic_key(tenant, database, branch, table);
        let Ok(mut inner) = self.inner.lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        let capacity = inner.capacity;
        inner.topics.entry(key).or_insert_with(|| broadcast::channel(capacity).0).subscribe()
    }

    pub fn replay(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        since_commit_ts: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        self.replay_branch(tenant, database, "main", table, since_commit_ts, limit)
    }

    pub fn replay_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        since_commit_ts: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        let limit = limit.clamp(1, 100000);
        let key = branch_topic_key(tenant, database, branch, table);
        let Ok(inner) = self.inner.lock() else { return Vec::new() };
        let Some(log) = inner.history.get(&key) else { return Vec::new() };
        log.iter()
            .filter(|record| record.commit_ts > since_commit_ts)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Replay retained changes after an exact per-topic sequence watermark.
    /// Unlike commit timestamps, sequences distinguish multiple writes in the
    /// same transaction and are therefore safe for reconnecting consumers.
    pub fn replay_after_sequence(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        self.replay_after_sequence_branch(tenant, database, "main", table, after_sequence, limit)
    }

    pub fn replay_after_sequence_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Vec<ChangeRecord> {
        let limit = limit.clamp(1, 100000);
        let key = branch_topic_key(tenant, database, branch, table);
        let Ok(inner) = self.inner.lock() else { return Vec::new() };
        let Some(log) = inner.history.get(&key) else { return Vec::new() };
        log.iter().filter(|record| record.sequence > after_sequence).take(limit).cloned().collect()
    }

    pub fn history_capacity(&self) -> usize {
        self.inner.lock().map(|inner| inner.capacity).unwrap_or(0)
    }

    pub fn query_limit(&self, tenant: &str, database: &str, table: &str) -> Option<usize> {
        self.query_limit_branch(tenant, database, "main", table)
    }

    pub fn query_limit_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
    ) -> Option<usize> {
        let key = branch_topic_key(tenant, database, branch, table);
        self.inner.lock().ok()?.queries.get(&key).map(|topic| topic.max_limit)
    }

    pub fn query_subscribe(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        limit: usize,
    ) -> broadcast::Receiver<QueryUpdate> {
        self.query_subscribe_branch(tenant, database, "main", table, limit)
    }

    pub fn query_subscribe_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        limit: usize,
    ) -> broadcast::Receiver<QueryUpdate> {
        let key = branch_topic_key(tenant, database, branch, table);
        let limit = limit.clamp(1, 1000);
        let Ok(mut inner) = self.inner.lock() else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        let capacity = inner.capacity;
        let topic = inner.queries.entry(key).or_insert_with(|| QueryTopic {
            sender: broadcast::channel(capacity).0,
            max_limit: limit,
        });
        topic.max_limit = topic.max_limit.max(limit);
        topic.sender.subscribe()
    }

    pub fn publish_query(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        commit_ts: u64,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        limit: usize,
    ) -> Option<u64> {
        self.publish_query_branch(tenant, database, "main", table, commit_ts, rows, limit)
    }

    pub fn publish_query_branch(
        &self,
        tenant: &str,
        database: &str,
        branch: &str,
        table: &str,
        commit_ts: u64,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        limit: usize,
    ) -> Option<u64> {
        let inner = self.inner.lock().ok()?;
        let key = branch_topic_key(tenant, database, branch, table);
        if !inner.queries.contains_key(&key) {
            return None;
        }
        let sequence = self.next_sequence();
        let topic = inner.queries.get(&key)?;
        let truncated = rows.len() >= limit;
        let rows = rows.into_iter().take(limit).map(|(pk, value)| QueryRow { pk, value }).collect();
        let _ = topic.sender.send(QueryUpdate {
            tenant: tenant.to_string(),
            database: database.to_string(),
            branch: branch.to_string(),
            table: table.to_string(),
            commit_ts,
            sequence,
            rows,
            truncated,
        });
        Some(sequence)
    }

    pub fn presence_join(
        &self,
        tenant: &str,
        channel: &str,
        member: String,
        state: serde_json::Value,
        ttl_secs: u64,
        now_unix: u64,
    ) -> Result<usize> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let key = scope_key(tenant, channel);
        let members = inner.presence.entry(key).or_insert_with(HashMap::new);
        members.retain(|_, member| member.expires_unix > now_unix);
        if !members.contains_key(&member) && members.len() >= PRESENCE_MAX_MEMBERS {
            return Err(RymeError::Overload(String::from("presence room full")));
        }
        let ttl = ttl_secs.clamp(1, PRESENCE_MAX_TTL_SECS);
        members.insert(
            member.clone(),
            PresenceMember { member, state, expires_unix: now_unix.saturating_add(ttl) },
        );
        Ok(inner.presence.get(&scope_key(tenant, channel)).map(|m| m.len()).unwrap_or(0))
    }

    pub fn prune_presence(&self, now_unix: u64) -> usize {
        let Ok(mut inner) = self.inner.lock() else { return 0 };
        let mut removed = 0;
        inner.presence.retain(|_, members| {
            let before = members.len();
            members.retain(|_, member| member.expires_unix > now_unix);
            removed += before - members.len();
            !members.is_empty()
        });
        removed
    }

    pub fn prune_idle(&self) -> usize {
        let Ok(mut inner) = self.inner.lock() else { return 0 };
        let mut removed = 0;
        let idle_topics: Vec<String> = inner
            .topics
            .iter()
            .filter(|(key, sender)| {
                sender.receiver_count() == 0
                    && inner.history.get(*key).map(|log| log.is_empty()).unwrap_or(true)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in idle_topics {
            inner.topics.remove(&key);
            inner.history.remove(&key);
            removed += 1;
        }
        for shard in self.broadcast_topics.iter() {
            if let Ok(mut topics) = shard.lock() {
                let idle_broadcast: Vec<String> = topics
                    .iter()
                    .filter(|(_, sender)| sender.receiver_count() == 0)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in idle_broadcast {
                    topics.remove(&key);
                    removed += 1;
                }
            }
        }
        let idle_queries: Vec<String> = inner
            .queries
            .iter()
            .filter(|(_, topic)| topic.sender.receiver_count() == 0)
            .map(|(key, _)| key.clone())
            .collect();
        for key in idle_queries {
            inner.queries.remove(&key);
            removed += 1;
        }
        removed
    }

    pub fn presence_leave(&self, tenant: &str, channel: &str, member: &str) -> Result<bool> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        Ok(inner
            .presence
            .get_mut(&scope_key(tenant, channel))
            .map(|members| members.remove(member).is_some())
            .unwrap_or(false))
    }

    pub fn presence_list(&self, tenant: &str, channel: &str, now_unix: u64) -> Vec<PresenceMember> {
        let mut out = Vec::new();
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(members) = inner.presence.get_mut(&scope_key(tenant, channel)) {
                members.retain(|_, member| member.expires_unix > now_unix);
                let mut names: Vec<String> = members.keys().cloned().collect();
                names.sort();
                for name in names {
                    if let Some(member) = members.get(&name) {
                        out.push(PresenceMember {
                            member: name,
                            state: member.state.clone(),
                            expires_unix: member.expires_unix,
                        });
                    }
                }
            }
        }
        out
    }

    pub fn broadcast(
        &self,
        tenant: &str,
        channel: &str,
        from: String,
        payload: serde_json::Value,
        commit_ts: u64,
    ) -> Result<u64> {
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let mut topics = self
            .broadcast_topics
            .get(shard)
            .expect("broadcast shard")
            .lock()
            .map_err(|_| RymeError::Internal(String::from("broadcast lock")))?;
        let sequence = self.next_sequence();
        let sender =
            topics.entry(key).or_insert_with(|| broadcast::channel(self.broadcast_capacity).0);
        let _ = sender.send(BroadcastMsg {
            channel: channel.to_string(),
            from,
            payload,
            commit_ts,
            sequence,
        });
        Ok(sequence)
    }

    pub fn broadcast_subscribe(
        &self,
        tenant: &str,
        channel: &str,
    ) -> broadcast::Receiver<BroadcastMsg> {
        let key = scope_key(tenant, channel);
        let shard = self.broadcast_shard(&key);
        let Ok(mut topics) = self.broadcast_topics.get(shard).expect("broadcast shard").lock()
        else {
            let (_, receiver) = broadcast::channel(16);
            return receiver;
        };
        topics
            .entry(key)
            .or_insert_with(|| broadcast::channel(self.broadcast_capacity).0)
            .subscribe()
    }

    pub fn durable_append(
        &self,
        tenant: &str,
        partition: &str,
        key: Vec<u8>,
        value: Vec<u8>,
        commit_ts: u64,
        retention: usize,
    ) -> Result<u64> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let topic =
            inner.durable.entry(durable_key(tenant, partition)).or_insert_with(|| DurableTopic {
                messages: VecDeque::new(),
                next_cursor: 0,
                retention: retention.clamp(16, 100000),
            });
        topic.retention = retention.clamp(16, 100000);
        let cursor = topic.next_cursor;
        topic.next_cursor += 1;
        topic.messages.push_back(DurableMsg {
            partition: partition.to_string(),
            cursor,
            key,
            value,
            commit_ts,
        });
        while topic.messages.len() > topic.retention {
            topic.messages.pop_front();
        }
        Ok(cursor)
    }

    pub fn durable_read(
        &self,
        tenant: &str,
        partition: &str,
        from_cursor: u64,
        limit: usize,
    ) -> Vec<DurableMsg> {
        let limit = limit.clamp(1, 1000);
        let Ok(inner) = self.inner.lock() else {
            return Vec::new();
        };
        let Some(topic) = inner.durable.get(&durable_key(tenant, partition)) else {
            return Vec::new();
        };
        topic.messages.iter().filter(|msg| msg.cursor >= from_cursor).take(limit).cloned().collect()
    }

    pub fn durable_cursor(&self, tenant: &str, partition: &str) -> u64 {
        self.inner
            .lock()
            .ok()
            .and_then(|inner| {
                inner.durable.get(&durable_key(tenant, partition)).map(|t| t.next_cursor)
            })
            .unwrap_or(0)
    }

    pub fn durable_snapshot(&self) -> Result<Vec<DurableTopicSnapshot>> {
        let inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        let mut snapshots = Vec::with_capacity(inner.durable.len());
        for (scope, topic) in &inner.durable {
            let Some((tenant, partition)) = scope.split_once('\0') else {
                return Err(RymeError::Corrupt(String::from("durable topic scope")));
            };
            snapshots.push(DurableTopicSnapshot {
                tenant: tenant.to_string(),
                partition: partition.to_string(),
                messages: topic.messages.iter().cloned().collect(),
                next_cursor: topic.next_cursor,
                retention: topic.retention,
            });
        }
        snapshots.sort_by(|left, right| {
            left.tenant.cmp(&right.tenant).then(left.partition.cmp(&right.partition))
        });
        Ok(snapshots)
    }

    pub fn restore_durable_snapshot(&self, snapshots: Vec<DurableTopicSnapshot>) -> Result<()> {
        let mut inner =
            self.inner.lock().map_err(|_| RymeError::Internal(String::from("realtime lock")))?;
        for snapshot in snapshots {
            if snapshot.tenant.is_empty()
                || snapshot.partition.is_empty()
                || snapshot.tenant.contains('\0')
                || snapshot.partition.contains('\0')
            {
                return Err(RymeError::Corrupt(String::from("durable topic identity")));
            }
            if snapshot.messages.iter().any(|message| message.partition != snapshot.partition) {
                return Err(RymeError::Corrupt(String::from("durable topic partition")));
            }
            let retention = snapshot.retention.clamp(16, 100000);
            let mut messages = snapshot.messages;
            messages.sort_by_key(|message| message.cursor);
            while messages.len() > retention {
                messages.remove(0);
            }
            let next_cursor = snapshot
                .next_cursor
                .max(messages.last().map(|message| message.cursor.saturating_add(1)).unwrap_or(0));
            let key = durable_key(&snapshot.tenant, &snapshot.partition);
            if inner.durable.contains_key(&key) {
                return Err(RymeError::Corrupt(String::from("duplicate durable topic")));
            }
            inner.durable.insert(
                key,
                DurableTopic { messages: messages.into_iter().collect(), next_cursor, retention },
            );
        }
        Ok(())
    }
}

impl Default for Realtime {
    fn default() -> Self {
        Self::new(4096)
    }
}

fn branch_topic_key(tenant: &str, database: &str, branch: &str, table: &str) -> String {
    format!("{tenant}/{database}/{branch}/{table}")
}

fn scope_key(tenant: &str, name: &str) -> String {
    format!("{tenant}/{name}")
}

fn durable_key(tenant: &str, partition: &str) -> String {
    format!("{tenant}\0{partition}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_returns_changes_after_commit_oldest_first() {
        let realtime = Realtime::new(64);
        for commit in [10u64, 20, 30] {
            realtime
                .publish(NewChange {
                    tenant: String::from("t"),
                    database: String::from("d"),
                    branch: String::from("main"),
                    table: String::from("docs"),
                    op: crate::Operation::Insert,
                    pk: commit.to_be_bytes().to_vec(),
                    before: None,
                    after: None,
                    commit_ts: commit,
                    tx_id: commit,
                })
                .unwrap();
        }
        assert!(realtime.replay("t", "d", "missing", 0, 10).is_empty());
        let all = realtime.replay("t", "d", "docs", 0, 10);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].commit_ts, 10);
        assert_eq!(all[2].commit_ts, 30);
        assert!(all.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
        let tail = realtime.replay("t", "d", "docs", 10, 10);
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].commit_ts, 20);
        assert!(realtime.replay("t", "d", "docs", 30, 10).is_empty());
        assert_eq!(realtime.replay("t", "d", "docs", 0, 2).len(), 2);
        assert_eq!(realtime.replay("t", "d", "docs", 0, 5000).len(), 3);
        let by_sequence = realtime.replay_after_sequence("t", "d", "docs", all[0].sequence, 10);
        assert_eq!(by_sequence.len(), 2);
        assert_eq!(by_sequence[0].commit_ts, 20);
    }

    #[test]
    fn query_refresh_flow() {
        let realtime = Realtime::new(64);
        assert_eq!(realtime.query_limit("t", "d", "docs"), None);
        assert!(realtime.publish_query("t", "d", "docs", 5, Vec::new(), 100).is_none());
        let mut first = realtime.query_subscribe("t", "d", "docs", 10);
        assert_eq!(realtime.query_limit("t", "d", "docs"), Some(10));
        let mut second = realtime.query_subscribe("t", "d", "docs", 50);
        assert_eq!(realtime.query_limit("t", "d", "docs"), Some(50));
        let rows = vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec())];
        let sequence = realtime.publish_query("t", "d", "docs", 7, rows, 50).unwrap();
        assert!(sequence > 0);
        for receiver in [&mut first, &mut second] {
            let update = receiver.try_recv().unwrap();
            assert_eq!(update.commit_ts, 7);
            assert_eq!(update.rows.len(), 2);
            assert!(!update.truncated);
        }
    }

    #[test]
    fn query_truncation_flag() {
        let realtime = Realtime::new(64);
        let mut receiver = realtime.query_subscribe("t", "d", "docs", 2);
        let rows = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];
        realtime.publish_query("t", "d", "docs", 9, rows, 2).unwrap();
        let update = receiver.try_recv().unwrap();
        assert_eq!(update.rows.len(), 2);
        assert!(update.truncated);
    }

    #[test]
    fn change_and_query_topics_are_scoped_by_branch() {
        let realtime = Realtime::new(64);
        let mut main_changes = realtime.subscribe_branch("t", "d", "main", "docs");
        let mut preview_changes = realtime.subscribe_branch("t", "d", "preview", "docs");
        realtime
            .publish(NewChange {
                tenant: String::from("t"),
                database: String::from("d"),
                branch: String::from("main"),
                table: String::from("docs"),
                op: Operation::Insert,
                pk: b"main".to_vec(),
                before: None,
                after: Some(b"one".to_vec()),
                commit_ts: 1,
                tx_id: 1,
            })
            .unwrap();
        realtime
            .publish(NewChange {
                tenant: String::from("t"),
                database: String::from("d"),
                branch: String::from("preview"),
                table: String::from("docs"),
                op: Operation::Insert,
                pk: b"preview".to_vec(),
                before: None,
                after: Some(b"two".to_vec()),
                commit_ts: 2,
                tx_id: 2,
            })
            .unwrap();
        assert_eq!(main_changes.try_recv().unwrap().branch, "main");
        assert!(main_changes.try_recv().is_err());
        assert_eq!(preview_changes.try_recv().unwrap().branch, "preview");
        assert!(preview_changes.try_recv().is_err());

        let mut main_queries = realtime.query_subscribe_branch("t", "d", "main", "docs", 10);
        let mut preview_queries = realtime.query_subscribe_branch("t", "d", "preview", "docs", 10);
        realtime
            .publish_query_branch(
                "t",
                "d",
                "preview",
                "docs",
                3,
                vec![(b"preview".to_vec(), b"two".to_vec())],
                10,
            )
            .unwrap();
        assert_eq!(preview_queries.try_recv().unwrap().rows.len(), 1);
        assert!(main_queries.try_recv().is_err());
    }

    #[test]
    fn presence_join_leave_expire() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join(
                "t",
                "room:1",
                String::from("ada"),
                serde_json::json!({"x": 1}),
                60,
                1000,
            )
            .unwrap();
        realtime
            .presence_join(
                "t",
                "room:1",
                String::from("grace"),
                serde_json::json!({"x": 2}),
                60,
                1000,
            )
            .unwrap();
        let members = realtime.presence_list("t", "room:1", 1001);
        assert_eq!(members.len(), 2);
        assert!(realtime.presence_leave("t", "room:1", "ada").unwrap());
        assert_eq!(realtime.presence_list("t", "room:1", 1001).len(), 1);
        assert_eq!(realtime.presence_list("t", "room:1", 2000).len(), 0);
    }

    #[test]
    fn presence_room_cap_and_ttl_clamp() {
        let realtime = Realtime::new(64);
        for index in 0..PRESENCE_MAX_MEMBERS {
            realtime
                .presence_join("t", "full", format!("m-{index}"), serde_json::json!(null), 60, 1000)
                .unwrap();
        }
        assert!(realtime
            .presence_join("t", "full", String::from("extra"), serde_json::json!(null), 60, 1000)
            .is_err());
        realtime
            .presence_join("t", "full", String::from("m-0"), serde_json::json!(null), 60, 1000)
            .unwrap();
        realtime
            .presence_join("t", "far", String::from("a"), serde_json::json!(null), u64::MAX, 1000)
            .unwrap();
        assert!(realtime.presence_list("t", "far", 1000 + PRESENCE_MAX_TTL_SECS + 1).is_empty());
    }

    #[test]
    fn prune_idle_drops_subscriberless_empty_scopes() {
        let realtime = Realtime::new(64);
        let idle_change = realtime.subscribe("t", "d", "ghost");
        drop(idle_change);
        let idle_cast = realtime.broadcast_subscribe("t", "nowhere");
        drop(idle_cast);
        let idle_query = realtime.query_subscribe("t", "d", "ghost", 10);
        drop(idle_query);
        assert_eq!(realtime.prune_idle(), 3);
        assert_eq!(realtime.prune_idle(), 0);
        realtime
            .presence_join("t", "room", String::from("a"), serde_json::json!(null), 60, 1000)
            .unwrap();
        let live = realtime.subscribe("t", "d", "kept");
        realtime
            .broadcast("t", "kept-cast", String::from("a"), serde_json::json!(true), 1)
            .unwrap();
        let kept_cast = realtime.broadcast_subscribe("t", "kept-cast");
        assert_eq!(realtime.prune_idle(), 0);
        drop(live);
        drop(kept_cast);
    }

    #[test]
    fn broadcast_fanout() {
        let realtime = Realtime::new(64);
        let mut first = realtime.broadcast_subscribe("t", "lobby");
        let mut second = realtime.broadcast_subscribe("t", "lobby");
        realtime
            .broadcast("t", "lobby", String::from("ada"), serde_json::json!({"hello": true}), 3)
            .unwrap();
        for receiver in [&mut first, &mut second] {
            let msg = receiver.try_recv().unwrap();
            assert_eq!(msg.channel, "lobby");
            assert_eq!(msg.commit_ts, 3);
        }
    }

    #[test]
    fn broadcast_sequences_are_unique_under_concurrent_publishers() {
        let realtime = Realtime::new(512);
        let mut receiver = realtime.broadcast_subscribe("t", "lobby");
        let mut workers = Vec::new();
        for worker in 0..4u64 {
            let realtime = realtime.clone();
            workers.push(std::thread::spawn(move || {
                (0..100u64)
                    .map(|message| {
                        realtime
                            .broadcast(
                                "t",
                                "lobby",
                                format!("{worker}-{message}"),
                                serde_json::Value::Null,
                                message,
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let mut sequences =
            workers.into_iter().flat_map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=400).collect::<Vec<_>>());
        for expected in 1..=400 {
            assert_eq!(receiver.try_recv().unwrap().sequence, expected);
        }
    }

    #[test]
    fn durable_cursor_resume() {
        let realtime = Realtime::new(64);
        let first =
            realtime.durable_append("t", "orders", b"k1".to_vec(), b"v1".to_vec(), 5, 16).unwrap();
        let second =
            realtime.durable_append("t", "orders", b"k2".to_vec(), b"v2".to_vec(), 6, 16).unwrap();
        assert_eq!(second, first + 1);
        assert_eq!(realtime.durable_cursor("t", "orders"), second + 1);
        let tail = realtime.durable_read("t", "orders", second, 10);
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].key, b"k2".to_vec());
        let all = realtime.durable_read("t", "orders", 0, 10);
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn durable_topics_round_trip_through_snapshot() {
        let realtime = Realtime::new(64);
        realtime.durable_append("tenant", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16).unwrap();
        let snapshots = realtime.durable_snapshot().unwrap();
        let restored = Realtime::new(64);
        restored.restore_durable_snapshot(snapshots).unwrap();
        assert_eq!(restored.durable_cursor("tenant", "orders"), 1);
        let messages = restored.durable_read("tenant", "orders", 0, 10);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].key, b"k");
    }

    #[test]
    fn tenants_share_no_rooms_channels_or_partitions() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join("t", "room", String::from("ada"), serde_json::json!(null), 60, 1000)
            .unwrap();
        assert_eq!(realtime.presence_list("t", "room", 1001).len(), 1);
        assert!(realtime.presence_list("u", "room", 1001).is_empty());
        assert!(!realtime.presence_leave("u", "room", "ada").unwrap());
        assert_eq!(realtime.presence_list("t", "room", 1001).len(), 1);
        let mut subscriber = realtime.broadcast_subscribe("u", "lobby");
        realtime.broadcast("t", "lobby", String::from("ada"), serde_json::json!(true), 3).unwrap();
        assert!(subscriber.try_recv().is_err());
        realtime.durable_append("t", "orders", b"k".to_vec(), b"v".to_vec(), 5, 16).unwrap();
        assert_eq!(realtime.durable_cursor("u", "orders"), 0);
        assert!(realtime.durable_read("u", "orders", 0, 10).is_empty());
        assert_eq!(realtime.durable_read("t", "orders", 0, 10).len(), 1);
    }

    #[test]
    fn prune_presence_drops_expired_and_empty_rooms() {
        let realtime = Realtime::new(64);
        realtime
            .presence_join("t", "gone", String::from("a"), serde_json::json!(null), 10, 1000)
            .unwrap();
        realtime
            .presence_join("t", "gone", String::from("b"), serde_json::json!(null), 10, 1000)
            .unwrap();
        realtime
            .presence_join("t", "stays", String::from("c"), serde_json::json!(null), 5000, 1000)
            .unwrap();
        assert_eq!(realtime.prune_presence(1005), 0);
        assert_eq!(realtime.prune_presence(2000), 2);
        assert!(realtime.presence_list("t", "gone", 2000).is_empty());
        assert_eq!(realtime.presence_list("t", "stays", 2000).len(), 1);
        let count = realtime
            .presence_join("t", "stays", String::from("d"), serde_json::json!(null), 60, 3000)
            .unwrap();
        assert_eq!(count, 2);
        let revived = realtime
            .presence_join("t", "gone", String::from("e"), serde_json::json!(null), 60, 3000)
            .unwrap();
        assert_eq!(revived, 1);
    }
}
