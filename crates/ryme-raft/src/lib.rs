use ryme_error::{Result, RymeError};
use ryme_storage::RecordKey;
use ryme_txn::{decode_writes, encode_writes, TxnManager, WriteOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::net::ConfChange;

pub mod net;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct RaftGroup {
    pub id: String,
    pub role: Role,
    pub term: u64,
    pub commit_index: u64,
    pub log: Vec<LogEntry>,
    pub members: Vec<String>,
    pub quorum: usize,
    pub acks: Vec<u64>,
}

impl RaftGroup {
    pub fn new(id: String, members: Vec<String>) -> Result<Self> {
        if members.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("members")));
        }
        let quorum = members.len() / 2 + 1;
        let count = members.len();
        Ok(Self {
            id,
            role: Role::Follower,
            term: 0,
            commit_index: 0,
            log: Vec::new(),
            members,
            quorum,
            acks: vec![0; count],
        })
    }

    pub fn become_leader(&mut self, term: u64) {
        self.role = Role::Leader;
        self.term = term;
    }

    pub fn propose(&mut self, payload: Vec<u8>) -> Result<u64> {
        if self.role != Role::Leader {
            return Err(RymeError::Unavailable(String::from("not leader")));
        }
        let index = self.log.last().map(|e| e.index + 1).unwrap_or(1);
        self.log.push(LogEntry { index, term: self.term, payload });
        Ok(index)
    }

    pub fn ack(&mut self, member: usize, match_index: u64) {
        if let Some(slot) = self.acks.get_mut(member) {
            *slot = (*slot).max(match_index);
        }
        let mut sorted = self.acks.clone();
        sorted.sort_unstable();
        if let Some(quorum_index) = sorted.get(sorted.len().saturating_sub(self.quorum)) {
            if *quorum_index > self.commit_index {
                self.commit_index = *quorum_index;
            }
        }
    }

    pub fn committed_since(&self, from: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|e| e.index > from && e.index <= self.commit_index)
            .cloned()
            .collect()
    }

    pub fn last_position(&self) -> (u64, u64) {
        self.log.last().map(|e| (e.term, e.index)).unwrap_or((0, 0))
    }

    pub fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn request_vote(
        &mut self,
        term: u64,
        candidate_term: u64,
        candidate_index: u64,
        voted_term: u64,
    ) -> (bool, u64) {
        if term < self.term {
            return (false, voted_term);
        }
        if voted_term >= term {
            return (false, voted_term);
        }
        let (last_term, last_index) = self.last_position();
        if candidate_term < last_term
            || (candidate_term == last_term && candidate_index < last_index)
        {
            return (false, voted_term);
        }
        (true, term)
    }

    pub fn append_entries(
        &mut self,
        term: u64,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<u64> {
        if term < self.term {
            return Err(RymeError::Unavailable(String::from("stale term")));
        }
        self.term = term;
        self.role = Role::Follower;
        if prev_index > 0 {
            let Some(existing) = self.log.iter().find(|e| e.index == prev_index) else {
                return Err(RymeError::Unavailable(String::from("missing prefix")));
            };
            if existing.term != prev_term {
                self.log.retain(|e| e.index < prev_index);
                return Err(RymeError::Unavailable(String::from("conflict")));
            }
            self.log.retain(|e| e.index <= prev_index);
        }
        for entry in entries {
            if self.log.iter().any(|e| e.index == entry.index) {
                continue;
            }
            self.log.push(entry);
        }
        self.log.sort_by_key(|e| e.index);
        let last = self.last_index();
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(last);
        }
        Ok(self.last_index())
    }
}

pub fn encode_applied(commit_ts: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&commit_ts.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn decode_applied(input: &[u8]) -> Result<(u64, Vec<u8>)> {
    if input.len() < 8 {
        return Err(RymeError::Corrupt(String::from("applied")));
    }
    let commit_ts = u64::from_be_bytes([
        input[0], input[1], input[2], input[3], input[4], input[5], input[6], input[7],
    ]);
    Ok((commit_ts, input[8..].to_vec()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyPayload {
    Data { commit_ts: u64, writes: Vec<u8> },
    Conf { change: ConfChange },
    Metadata { payload: Vec<u8> },
}

pub fn encode_conf(members: &[crate::net::Member]) -> Result<Vec<u8>> {
    encode_conf_change(&ConfChange::single(members.to_vec()))
}

pub fn encode_conf_change(change: &ConfChange) -> Result<Vec<u8>> {
    let mut out = vec![1u8];
    let raw = serde_json::to_vec(change).map_err(|e| RymeError::Internal(e.to_string()))?;
    out.extend_from_slice(&raw);
    Ok(out)
}

pub fn encode_metadata(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("metadata")));
    }
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(2u8);
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_apply(input: &[u8]) -> Result<ApplyPayload> {
    if input.first() == Some(&1u8) {
        let body = &input[1..];
        if body.first() == Some(&b'[') {
            let members: Vec<crate::net::Member> = serde_json::from_slice(body)
                .map_err(|_| RymeError::Corrupt(String::from("conf")))?;
            return Ok(ApplyPayload::Conf { change: ConfChange::single(members) });
        }
        let change: ConfChange =
            serde_json::from_slice(body).map_err(|_| RymeError::Corrupt(String::from("conf")))?;
        return Ok(ApplyPayload::Conf { change });
    }
    if input.first() == Some(&2u8) {
        if input.len() == 1 {
            return Err(RymeError::Corrupt(String::from("metadata")));
        }
        return Ok(ApplyPayload::Metadata { payload: input[1..].to_vec() });
    }
    let (commit_ts, writes) = decode_applied(input)?;
    Ok(ApplyPayload::Data { commit_ts, writes })
}

#[derive(Debug)]
struct Node {
    group: RaftGroup,
    manager: TxnManager,
    applied: u64,
    alive: bool,
    voted_term: u64,
}

#[derive(Debug)]
pub struct Cluster {
    nodes: Vec<Node>,
    cells: Vec<Vec<usize>>,
}

impl Cluster {
    pub fn new(count: usize) -> Result<Self> {
        if count == 0 {
            return Err(RymeError::InvalidArgument(String::from("nodes")));
        }
        let members: Vec<String> = (0..count).map(|i| format!("n{i}")).collect();
        let mut nodes = Vec::new();
        for _ in 0..count {
            nodes.push(Node {
                group: RaftGroup::new(String::from("g"), members.clone())?,
                manager: TxnManager::new(),
                applied: 0,
                alive: true,
                voted_term: 0,
            });
        }
        let cells = vec![(0..count).collect()];
        Ok(Self { nodes, cells })
    }

    pub fn kill(&mut self, id: usize) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.alive = false;
        }
    }

    pub fn revive(&mut self, id: usize) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.alive = true;
        }
    }

    pub fn partition(&mut self, cells: Vec<Vec<usize>>) {
        self.cells = cells;
    }

    pub fn heal(&mut self) {
        self.cells = vec![(0..self.nodes.len()).collect()];
    }

    fn reachable(&self, id: usize) -> Vec<usize> {
        if !self.nodes.get(id).map(|n| n.alive).unwrap_or(false) {
            return Vec::new();
        }
        for cell in &self.cells {
            if cell.contains(&id) {
                return cell.iter().copied().filter(|m| self.nodes[*m].alive).collect();
            }
        }
        Vec::new()
    }

    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| n.alive && n.group.role == Role::Leader)
    }

    pub fn elect(&mut self, candidate: usize) -> bool {
        if !self.nodes.get(candidate).map(|n| n.alive).unwrap_or(false) {
            return false;
        }
        let term = self.nodes[candidate].group.term + 1;
        let (candidate_term, candidate_index) = self.nodes[candidate].group.last_position();
        let peers = self.reachable(candidate);
        if peers.is_empty() {
            return false;
        }
        let total = self.nodes.len();
        let quorum = total / 2 + 1;
        let mut votes = 1;
        self.nodes[candidate].voted_term = term;
        self.nodes[candidate].group.term = term;
        for peer in peers {
            if peer == candidate {
                continue;
            }
            let node = &mut self.nodes[peer];
            let (grant, voted) =
                node.group.request_vote(term, candidate_term, candidate_index, node.voted_term);
            node.voted_term = voted;
            if grant {
                node.group.term = term;
                node.group.role = Role::Follower;
                votes += 1;
            }
        }
        if votes >= quorum {
            self.nodes[candidate].group.become_leader(term);
            true
        } else {
            false
        }
    }

    pub fn client_write(
        &mut self,
        leader: usize,
        writes: BTreeMap<RecordKey, WriteOp>,
    ) -> Result<u64> {
        if !self.nodes.get(leader).map(|n| n.alive).unwrap_or(false) {
            return Err(RymeError::Unavailable(String::from("leader down")));
        }
        if self.nodes[leader].group.role != Role::Leader {
            return Err(RymeError::Unavailable(String::from("not leader")));
        }
        let reachable = self.reachable(leader);
        if reachable.len() < self.nodes[leader].group.quorum {
            return Err(RymeError::Unavailable(String::from("no quorum")));
        }
        let mut txn = self.nodes[leader].manager.begin();
        for (key, op) in writes.iter() {
            match op.value.clone() {
                Some(bytes) => self.nodes[leader].manager.put_with_ttl(
                    &mut txn,
                    key.clone(),
                    bytes,
                    op.expires_at,
                ),
                None => self.nodes[leader].manager.delete(&mut txn, key.clone()),
            }
        }
        let commit_ts = self.nodes[leader].manager.reserve(&txn)?;
        let payload = encode_applied(commit_ts, &encode_writes(&writes)?);
        let index = self.nodes[leader].group.propose(payload)?;
        self.nodes[leader].group.acks[leader] = index;
        self.replicate_from(leader)?;
        Ok(commit_ts)
    }

    pub fn replicate_from(&mut self, leader: usize) -> Result<()> {
        let term = self.nodes[leader].group.term;
        let commit = self.nodes[leader].group.commit_index;
        let log = self.nodes[leader].group.log.clone();
        let peers = self.reachable(leader);
        for peer in peers {
            if peer == leader {
                continue;
            }
            let next = self.nodes[peer].group.last_index() + 1;
            let prev = log.iter().rev().find(|e| e.index < next);
            let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
            let entries: Vec<LogEntry> = log.iter().filter(|e| e.index >= next).cloned().collect();
            let node = &mut self.nodes[peer];
            match node.group.append_entries(term, prev_index, prev_term, entries, commit) {
                Ok(matched) => {
                    self.nodes[leader].group.ack(peer, matched);
                }
                Err(_) => {
                    let retry_next = next.saturating_sub(1).max(1);
                    let prev = log.iter().rev().find(|e| e.index < retry_next);
                    let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
                    let entries: Vec<LogEntry> =
                        log.iter().filter(|e| e.index >= retry_next).cloned().collect();
                    if node
                        .group
                        .append_entries(term, prev_index, prev_term, entries, commit)
                        .is_ok()
                    {
                        let matched = node.group.last_index();
                        self.nodes[leader].group.ack(peer, matched);
                    }
                }
            }
        }
        let commit = self.nodes[leader].group.commit_index;
        for peer in self.reachable(leader) {
            if peer == leader {
                continue;
            }
            let entries: Vec<LogEntry> = Vec::new();
            let last = self.nodes[peer].group.last_index();
            let prev = log.iter().rev().find(|e| e.index <= last);
            let (prev_index, prev_term) = prev.map(|e| (e.index, e.term)).unwrap_or((0, 0));
            let _ =
                self.nodes[peer].group.append_entries(term, prev_index, prev_term, entries, commit);
        }
        self.apply_reachable(leader)
    }

    fn apply_committed(&mut self, node: usize) -> Result<()> {
        let entries = self.nodes[node].group.committed_since(self.nodes[node].applied);
        for entry in entries {
            let (commit_ts, payload) = decode_applied(&entry.payload)?;
            let writes = decode_writes(&payload)?;
            self.nodes[node].manager.replay_at(commit_ts, &writes)?;
            self.nodes[node].applied = entry.index;
        }
        Ok(())
    }

    fn apply_reachable(&mut self, from: usize) -> Result<()> {
        let peers = self.reachable(from);
        for peer in peers {
            self.apply_committed(peer)?;
        }
        Ok(())
    }

    pub fn read_latest(&self, node: usize, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        let manager = &self.nodes[node].manager;
        let mut txn = manager.begin();
        manager.get(&mut txn, key)
    }

    pub fn commit_index(&self, node: usize) -> u64 {
        self.nodes[node].group.commit_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writes(pairs: Vec<(&str, &str)>) -> BTreeMap<RecordKey, WriteOp> {
        let mut out = BTreeMap::new();
        for (key, value) in pairs {
            out.insert(
                RecordKey::new("t", "d", "s", key.as_bytes()),
                WriteOp::put(value.as_bytes().to_vec()),
            );
        }
        out
    }

    #[test]
    fn replication_and_failover() {
        let mut cluster = Cluster::new(3).unwrap();
        assert!(cluster.elect(0));
        cluster.client_write(0, writes(vec![("k", "v1")])).unwrap();
        let key = RecordKey::new("t", "d", "s", b"k");
        for node in 0..3 {
            assert_eq!(cluster.read_latest(node, &key).unwrap(), Some(b"v1".to_vec()));
        }
        cluster.kill(0);
        assert!(cluster.elect(1));
        cluster.client_write(1, writes(vec![("k", "v2")])).unwrap();
        cluster.revive(0);
        cluster.replicate_from(1).unwrap();
        assert_eq!(cluster.read_latest(0, &key).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(cluster.commit_index(0), cluster.commit_index(1));
    }

    #[test]
    fn minority_cannot_commit() {
        let mut cluster = Cluster::new(3).unwrap();
        assert!(cluster.elect(0));
        cluster.partition(vec![vec![0], vec![1, 2]]);
        assert!(!cluster.elect(0));
        assert!(cluster.elect(1));
        cluster.client_write(1, writes(vec![("k", "a")])).unwrap();
        let failed = cluster.client_write(0, writes(vec![("k", "b")]));
        assert!(matches!(failed, Err(RymeError::Unavailable(_))));
        cluster.heal();
        cluster.replicate_from(1).unwrap();
        let key = RecordKey::new("t", "d", "s", b"k");
        assert_eq!(cluster.read_latest(0, &key).unwrap(), Some(b"a".to_vec()));
    }

    #[test]
    fn conflicting_suffix_truncated() {
        let mut follower = RaftGroup::new(String::from("g"), vec![String::from("a")]).unwrap();
        follower.term = 1;
        follower.log.push(LogEntry { index: 1, term: 1, payload: b"x".to_vec() });
        follower.log.push(LogEntry { index: 2, term: 1, payload: b"bad".to_vec() });
        let fixed = follower.append_entries(
            2,
            1,
            1,
            vec![LogEntry { index: 2, term: 2, payload: b"good".to_vec() }],
            2,
        );
        assert!(fixed.is_ok());
        assert_eq!(follower.log.len(), 2);
        assert_eq!(follower.log[1].payload, b"good".to_vec());
        assert_eq!(follower.commit_index, 2);
    }

    #[test]
    fn malformed_committed_payload_fails_closed() {
        let mut cluster = Cluster::new(3).unwrap();
        assert!(cluster.elect(0));
        cluster.nodes[0].group.propose(b"corrupt".to_vec()).unwrap();
        cluster.nodes[0].group.acks[0] = 1;

        let result = cluster.replicate_from(0);
        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        assert_eq!(cluster.nodes[0].applied, 0);
    }

    #[test]
    fn metadata_payload_round_trips() {
        let encoded = encode_metadata(br#"[{"id":"r0"}]"#).unwrap();
        assert_eq!(
            decode_apply(&encoded).unwrap(),
            ApplyPayload::Metadata { payload: br#"[{"id":"r0"}]"#.to_vec() }
        );
        assert!(matches!(encode_metadata(&[]), Err(RymeError::InvalidArgument(_))));
    }
}
