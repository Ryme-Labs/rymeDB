use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const MAX_DIM: usize = 4096;
pub const MAX_TOP_K: usize = 100;
pub const MAX_VECTORS_PER_SPACE: usize = 100_000;
pub const MAX_DOCS_PER_SPACE: usize = 100_000;
pub const HNSW_M: usize = 16;
pub const HNSW_EF_BUILD: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorHit {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextHit {
    pub id: String,
    pub score: f64,
}

#[derive(Debug, Default)]
struct VectorSpace {
    dim: usize,
    vectors: HashMap<String, Vec<f32>>,
    ann: HnswIndex,
}

fn normalize(vector: &[f32]) -> Result<Vec<f32>> {
    let norm = (vector.iter().map(|v| (*v as f64) * (*v as f64)).sum::<f64>()).sqrt();
    if norm == 0.0 {
        return Err(RymeError::InvalidArgument(String::from("zero vector")));
    }
    Ok(vector.iter().map(|v| *v / norm as f32).collect())
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| (*x as f64) * (*y as f64)).sum()
}

#[derive(Debug, Clone)]
struct HnswNode {
    id: String,
    vector: Vec<f32>,
    neighbors: Vec<Vec<usize>>,
    tombstoned: bool,
}

#[derive(Debug, Default)]
struct HnswIndex {
    dim: usize,
    nodes: Vec<HnswNode>,
    id_to_pos: HashMap<String, usize>,
    entry: Option<usize>,
    rng: u64,
}

impl HnswIndex {
    fn next_rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e3779b97f4a7c15);
        let mut state = self.rng;
        state ^= state >> 30;
        state = state.wrapping_mul(0xbf58476d1ce4e5b9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94d049bb133111eb);
        state ^= state >> 31;
        state
    }

    fn sample_level(&mut self) -> usize {
        let uniform = ((self.next_rand() >> 11) as f64) / ((1u64 << 53) as f64);
        let uniform = uniform.clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
        ((-uniform.ln() / (HNSW_M as f64).ln()).floor() as usize).min(8)
    }

    fn upsert(&mut self, dim: usize, id: String, vector: Vec<f32>) -> Result<()> {
        if self.nodes.is_empty() {
            self.dim = dim;
        }
        if dim != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if let Some(pos) = self.id_to_pos.get(&id).copied() {
            if let Some(node) = self.nodes.get_mut(pos) {
                node.vector = vector;
                node.tombstoned = false;
            }
            return Ok(());
        }
        let level = self.sample_level();
        let pos = self.nodes.len();
        self.nodes.push(HnswNode {
            id: id.clone(),
            vector,
            neighbors: vec![Vec::new(); level + 1],
            tombstoned: false,
        });
        self.id_to_pos.insert(id, pos);
        let Some(entry) = self.entry else {
            self.entry = Some(pos);
            return Ok(());
        };
        let query = self.nodes[pos].vector.clone();
        let mut current = entry;
        let entry_level = self.nodes[entry].neighbors.len() - 1;
        for layer in ((level + 1)..=entry_level).rev() {
            current = self.greedy(&query, current, layer);
        }
        for layer in (0..=level.min(entry_level)).rev() {
            let candidates = self.beam(&query, vec![current], HNSW_EF_BUILD, layer);
            current = candidates.first().map(|(_, p)| *p).unwrap_or(current);
            let selected = select_neighbors(&candidates, HNSW_M, &self.nodes);
            self.nodes[pos].neighbors[layer] = selected.clone();
            for neighbor in selected {
                self.nodes[neighbor].neighbors[layer].push(pos);
                if self.nodes[neighbor].neighbors[layer].len() > HNSW_M * 2 {
                    let pruned = prune_neighbors(neighbor, layer, &self.nodes);
                    self.nodes[neighbor].neighbors[layer] = pruned;
                }
            }
        }
        if level > entry_level {
            self.entry = Some(pos);
        }
        Ok(())
    }

    fn greedy(&self, query: &[f32], start: usize, layer: usize) -> usize {
        let mut current = start;
        let mut best = dot(query, &self.nodes[start].vector);
        loop {
            let mut moved = false;
            let neighbors = self.nodes[current].neighbors.get(layer).cloned().unwrap_or_default();
            for neighbor in neighbors {
                if self.nodes[neighbor].tombstoned {
                    continue;
                }
                let score = dot(query, &self.nodes[neighbor].vector);
                if score > best {
                    best = score;
                    current = neighbor;
                    moved = true;
                }
            }
            if !moved {
                break;
            }
        }
        current
    }

    fn beam(
        &self,
        query: &[f32],
        starts: Vec<usize>,
        ef: usize,
        layer: usize,
    ) -> Vec<(f64, usize)> {
        use std::cmp::Ordering;
        use std::collections::{BinaryHeap, HashSet};
        #[derive(Clone, Copy)]
        struct Scored(f64, usize);
        impl PartialEq for Scored {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0 && self.1 == other.1
            }
        }
        impl Eq for Scored {}
        impl PartialOrd for Scored {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for Scored {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal).then(self.1.cmp(&other.1))
            }
        }
        let mut visited: HashSet<usize> = starts.iter().copied().collect();
        let mut candidates: BinaryHeap<Scored> = BinaryHeap::new();
        let mut results: BinaryHeap<std::cmp::Reverse<Scored>> = BinaryHeap::new();
        for start in starts {
            if self.nodes[start].tombstoned {
                continue;
            }
            let score = dot(query, &self.nodes[start].vector);
            candidates.push(Scored(score, start));
            results.push(std::cmp::Reverse(Scored(score, start)));
        }
        while let Some(Scored(score, pos)) = candidates.pop() {
            let worst = results.peek().map(|s| (s.0).0).unwrap_or(f64::NEG_INFINITY);
            if score < worst && results.len() >= ef {
                break;
            }
            let neighbors = self.nodes[pos].neighbors.get(layer).cloned().unwrap_or_default();
            for neighbor in neighbors {
                if !visited.insert(neighbor) || self.nodes[neighbor].tombstoned {
                    continue;
                }
                let neighbor_score = dot(query, &self.nodes[neighbor].vector);
                let worst = results.peek().map(|s| (s.0).0).unwrap_or(f64::NEG_INFINITY);
                if neighbor_score > worst || results.len() < ef {
                    candidates.push(Scored(neighbor_score, neighbor));
                    results.push(std::cmp::Reverse(Scored(neighbor_score, neighbor)));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut out: Vec<(f64, usize)> =
            results.into_iter().map(|std::cmp::Reverse(Scored(score, pos))| (score, pos)).collect();
        out.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    fn search(&self, query: &[f32], top_k: usize, ef: usize) -> Result<Vec<(f64, String)>> {
        let Some(entry) = self.entry else {
            return Ok(Vec::new());
        };
        if query.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let entry_level = self.nodes[entry].neighbors.len() - 1;
        let mut current = entry;
        for layer in (1..=entry_level).rev() {
            current = self.greedy(query, current, layer);
        }
        let ef = ef.clamp(top_k.clamp(1, MAX_TOP_K), 256);
        let hits = self
            .beam(query, vec![current], ef, 0)
            .into_iter()
            .map(|(score, pos)| (score, self.nodes[pos].id.clone()))
            .take(top_k.clamp(1, MAX_TOP_K))
            .collect();
        Ok(hits)
    }

    fn remove(&mut self, id: &str) -> bool {
        if let Some(pos) = self.id_to_pos.remove(id) {
            if let Some(node) = self.nodes.get_mut(pos) {
                node.tombstoned = true;
            }
            true
        } else {
            false
        }
    }
}

fn select_neighbors(candidates: &[(f64, usize)], limit: usize, nodes: &[HnswNode]) -> Vec<usize> {
    let mut selected: Vec<usize> = Vec::new();
    for (_, pos) in candidates.iter().take(limit * 2) {
        if selected.len() >= limit {
            break;
        }
        if nodes.get(*pos).is_some_and(|n| !n.tombstoned) && !selected.contains(pos) {
            selected.push(*pos);
        }
    }
    selected
}

fn prune_neighbors(pos: usize, layer: usize, nodes: &[HnswNode]) -> Vec<usize> {
    let mut scored: Vec<(f64, usize)> = nodes[pos]
        .neighbors
        .get(layer)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|p| nodes.get(*p).is_some_and(|n| !n.tombstoned))
        .map(|p| (dot(&nodes[pos].vector, &nodes[p].vector), p))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(HNSW_M).map(|(_, p)| p).collect()
}

impl VectorSpace {
    fn upsert(&mut self, id: String, vector: Vec<f32>) -> Result<()> {
        if vector.is_empty() || vector.len() > MAX_DIM {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if self.vectors.is_empty() {
            self.dim = vector.len();
        }
        if vector.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if !self.vectors.contains_key(&id) && self.vectors.len() >= MAX_VECTORS_PER_SPACE {
            return Err(RymeError::Overload(String::from("vector space")));
        }
        let normalized = normalize(&vector)?;
        self.ann.upsert(self.dim, id.clone(), normalized.clone())?;
        self.vectors.insert(id, normalized);
        Ok(())
    }

    fn remove(&mut self, id: &str) -> bool {
        self.ann.remove(id);
        self.vectors.remove(id).is_some()
    }

    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<VectorHit>> {
        if query.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let norm = (query.iter().map(|v| (*v as f64) * (*v as f64)).sum::<f64>()).sqrt();
        if norm == 0.0 {
            return Err(RymeError::InvalidArgument(String::from("zero vector")));
        }
        let normalized: Vec<f64> = query.iter().map(|v| (*v as f64) / norm).collect();
        let mut hits: Vec<VectorHit> = self
            .vectors
            .iter()
            .map(|(id, stored)| {
                let score =
                    stored.iter().zip(normalized.iter()).map(|(a, b)| (*a as f64) * b).sum();
                VectorHit { id: id.clone(), score }
            })
            .collect();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(top_k.clamp(1, MAX_TOP_K));
        Ok(hits)
    }

    fn ann_search(&self, query: &[f32], top_k: usize, ef: usize) -> Result<Vec<VectorHit>> {
        let normalized = normalize(query)?;
        if normalized.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        Ok(self
            .ann
            .search(&normalized, top_k, ef)?
            .into_iter()
            .map(|(score, id)| VectorHit { id, score })
            .collect())
    }
}

#[derive(Debug, Default)]
struct TextSpace {
    docs: HashMap<String, HashMap<String, u32>>,
    postings: HashMap<String, HashMap<String, u32>>,
}

impl TextSpace {
    fn index_doc(&mut self, id: String, text: &str) -> usize {
        self.remove(&id);
        if self.docs.len() >= MAX_DOCS_PER_SPACE {
            return 0;
        }
        let mut terms: HashMap<String, u32> = HashMap::new();
        for term in tokenize(text) {
            *terms.entry(term).or_insert(0) += 1;
        }
        let count = terms.len();
        for (term, freq) in &terms {
            self.postings.entry(term.clone()).or_default().insert(id.clone(), *freq);
        }
        self.docs.insert(id, terms);
        count
    }

    fn remove(&mut self, id: &str) -> bool {
        let Some(terms) = self.docs.remove(id) else {
            return false;
        };
        for term in terms.keys() {
            if let Some(posting) = self.postings.get_mut(term) {
                posting.remove(id);
                if posting.is_empty() {
                    self.postings.remove(term);
                }
            }
        }
        true
    }

    fn search(&self, query: &str, top_k: usize) -> Vec<TextHit> {
        let terms = tokenize(query);
        if terms.is_empty() {
            return Vec::new();
        }
        let doc_count = self.docs.len().max(1) as f64;
        let mut scores: HashMap<&str, f64> = HashMap::new();
        for term in &terms {
            let Some(posting) = self.postings.get(term) else {
                continue;
            };
            let idf = (1.0 + doc_count / (posting.len() as f64 + 0.5)).ln();
            for (doc, freq) in posting {
                let tf = (*freq as f64) / ((*freq as f64) + 1.2);
                *scores.entry(doc.as_str()).or_insert(0.0) += idf * tf;
            }
        }
        let mut hits: Vec<TextHit> =
            scores.into_iter().map(|(id, score)| TextHit { id: id.to_string(), score }).collect();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(top_k.clamp(1, MAX_TOP_K));
        hits
    }
}

pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[derive(Debug, Default)]
pub struct IndexRegistry {
    vectors: HashMap<String, VectorSpace>,
    texts: HashMap<String, TextSpace>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn vector_upsert(&mut self, space: &str, id: String, vector: Vec<f32>) -> Result<()> {
        if space.is_empty() || id.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("vector target")));
        }
        self.vectors.entry(space.to_string()).or_default().upsert(id, vector)
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        self.vectors.get_mut(space).map(|s| s.remove(id)).unwrap_or(false)
    }

    pub fn vector_search(
        &self,
        space: &str,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<VectorHit>> {
        self.vectors
            .get(space)
            .ok_or_else(|| RymeError::NotFound(String::from("vector space")))?
            .search(query, top_k)
    }

    pub fn ann_search(
        &self,
        space: &str,
        query: &[f32],
        top_k: usize,
        ef: usize,
    ) -> Result<Vec<VectorHit>> {
        self.vectors
            .get(space)
            .ok_or_else(|| RymeError::NotFound(String::from("vector space")))?
            .ann_search(query, top_k, ef)
    }

    pub fn text_index(&mut self, space: &str, id: String, text: &str) -> Result<usize> {
        if space.is_empty() || id.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("text target")));
        }
        Ok(self.texts.entry(space.to_string()).or_default().index_doc(id, text))
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        self.texts.get_mut(space).map(|s| s.remove(id)).unwrap_or(false)
    }

    pub fn text_search(&self, space: &str, query: &str, top_k: usize) -> Vec<TextHit> {
        self.texts.get(space).map(|s| s.search(query, top_k)).unwrap_or_default()
    }

    pub fn spaces(&self) -> Vec<String> {
        let mut out: Vec<String> = self.vectors.keys().chain(self.texts.keys()).cloned().collect();
        out.sort();
        out.dedup();
        out
    }
}

pub const MAX_PARTITIONS: usize = 64;

#[derive(Debug)]
pub struct PartitionedIndex {
    partitions: Vec<IndexRegistry>,
    dims: HashMap<String, usize>,
}

impl PartitionedIndex {
    pub fn new(partitions: usize) -> Self {
        let count = partitions.clamp(1, MAX_PARTITIONS);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(IndexRegistry::new());
        }
        Self { partitions: out, dims: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    fn route(&self, space: &str, id: &str) -> usize {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in space.bytes().chain([0u8]).chain(id.bytes()) {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash % self.partitions.len().max(1) as u64) as usize
    }

    pub fn vector_upsert(&mut self, space: &str, id: String, vector: Vec<f32>) -> Result<()> {
        if space.is_empty() || id.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("vector target")));
        }
        if vector.is_empty() || vector.len() > MAX_DIM {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        match self.dims.get(space) {
            Some(dim) if *dim != vector.len() => {
                return Err(RymeError::InvalidArgument(String::from("vector dim")));
            }
            Some(_) => {}
            None => {
                self.dims.insert(space.to_string(), vector.len());
            }
        }
        let partition = self.route(space, &id);
        self.partitions[partition].vector_upsert(space, id, vector)
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        let partition = self.route(space, id);
        self.partitions[partition].vector_remove(space, id)
    }

    pub fn vector_search(
        &self,
        space: &str,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<VectorHit>> {
        let Some(dim) = self.dims.get(space) else {
            return Err(RymeError::NotFound(String::from("vector space")));
        };
        if query.len() != *dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let mut merged: Vec<VectorHit> = Vec::new();
        for partition in &self.partitions {
            match partition.vector_search(space, query, top_k) {
                Ok(hits) => merged.extend(hits),
                Err(RymeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(top_k.clamp(1, MAX_TOP_K));
        Ok(merged)
    }

    pub fn ann_search(
        &self,
        space: &str,
        query: &[f32],
        top_k: usize,
        ef: usize,
    ) -> Result<Vec<VectorHit>> {
        let Some(dim) = self.dims.get(space) else {
            return Err(RymeError::NotFound(String::from("vector space")));
        };
        if query.len() != *dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let mut merged: Vec<VectorHit> = Vec::new();
        for partition in &self.partitions {
            match partition.ann_search(space, query, top_k, ef) {
                Ok(hits) => merged.extend(hits),
                Err(RymeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(top_k.clamp(1, MAX_TOP_K));
        Ok(merged)
    }

    pub fn text_index(&mut self, space: &str, id: String, text: &str) -> Result<usize> {
        if space.is_empty() || id.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("text target")));
        }
        let partition = self.route(space, &id);
        self.partitions[partition].text_index(space, id, text)
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        let partition = self.route(space, id);
        self.partitions[partition].text_remove(space, id)
    }

    pub fn text_search(&self, space: &str, query: &str, top_k: usize) -> Vec<TextHit> {
        let mut merged: Vec<TextHit> = Vec::new();
        for partition in &self.partitions {
            merged.extend(partition.text_search(space, query, top_k));
        }
        merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(top_k.clamp(1, MAX_TOP_K));
        merged
    }

    pub fn spaces(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for partition in &self.partitions {
            out.extend(partition.spaces());
        }
        out.sort();
        out.dedup();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_nearest_first() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        registry.vector_upsert("s", String::from("b"), vec![0.0, 1.0]).unwrap();
        let hits = registry.vector_search("s", &[0.9, 0.1], 2).unwrap();
        assert_eq!(hits[0].id, "a");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn vector_rejects_bad_dim() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        assert!(registry.vector_upsert("s", String::from("b"), vec![1.0]).is_err());
        assert!(registry.vector_upsert("s", String::from("c"), vec![0.0, 0.0]).is_err());
        assert!(registry.vector_search("s", &[1.0], 1).is_err());
        assert!(registry.vector_search("missing", &[1.0, 0.0], 1).is_err());
    }

    #[test]
    fn vector_remove_drops_hits() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        assert!(registry.vector_remove("s", "a"));
        assert!(!registry.vector_remove("s", "a"));
        assert!(registry.vector_search("s", &[1.0, 0.0], 1).unwrap().is_empty());
    }

    #[test]
    fn text_ranks_term_match() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("d1"), "the quick brown fox").unwrap();
        registry.text_index("docs", String::from("d2"), "lorem ipsum dolor").unwrap();
        let hits = registry.text_search("docs", "quick fox", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "d1");
    }

    #[test]
    fn text_remove_and_empty_query() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("d1"), "hello world").unwrap();
        assert!(registry.text_search("docs", "!!!", 10).is_empty());
        assert!(registry.text_remove("docs", "d1"));
        assert!(registry.text_search("docs", "hello", 10).is_empty());
        assert!(registry.text_search("missing", "hello", 10).is_empty());
    }

    #[test]
    fn spaces_lists_namespaces() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("v", String::from("a"), vec![1.0]).unwrap();
        registry.text_index("t", String::from("d"), "hi").unwrap();
        assert_eq!(registry.spaces(), vec![String::from("t"), String::from("v")]);
    }

    #[test]
    fn ann_recall_against_exact() {
        let mut state = 0x12345678u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) as f64) / ((1u64 << 53) as f64) - 0.5
        };
        let mut registry = IndexRegistry::new();
        for index in 0..300 {
            let vector: Vec<f32> = (0..16).map(|_| next() as f32).collect();
            registry.vector_upsert("s", format!("v{index}"), vector).unwrap();
        }
        let query: Vec<f32> = (0..16).map(|_| next() as f32).collect();
        let exact = registry.vector_search("s", &query, 10).unwrap();
        let approx = registry.ann_search("s", &query, 10, 64).unwrap();
        let exact_ids: std::collections::HashSet<&str> =
            exact.iter().map(|h| h.id.as_str()).collect();
        let overlap = approx.iter().filter(|h| exact_ids.contains(h.id.as_str())).count();
        assert!(overlap >= 8, "overlap {overlap}");
        assert!(registry.ann_search("missing", &query, 10, 64).is_err());
    }

    #[test]
    fn partitioned_matches_single() {
        let mut single = IndexRegistry::new();
        let mut sharded = PartitionedIndex::new(4);
        assert_eq!(sharded.len(), 4);
        for index in 0..50 {
            let id = format!("k{index}");
            let vector = vec![index as f32, (50 - index) as f32];
            single.vector_upsert("s", id.clone(), vector.clone()).unwrap();
            sharded.vector_upsert("s", id, vector).unwrap();
            single.text_index("t", format!("d{index}"), "alpha beta").unwrap();
            sharded.text_index("t", format!("d{index}"), "alpha beta").unwrap();
        }
        let query = vec![25.0f32, 25.0];
        let mut expected: Vec<String> =
            single.vector_search("s", &query, 5).unwrap().into_iter().map(|h| h.id).collect();
        expected.sort();
        let mut actual: Vec<String> =
            sharded.vector_search("s", &query, 5).unwrap().into_iter().map(|h| h.id).collect();
        actual.sort();
        assert_eq!(expected, actual);
        assert_eq!(sharded.text_search("t", "alpha", 50).len(), 50);
        assert!(sharded.vector_remove("s", "k1"));
        assert!(!sharded.vector_remove("s", "k1"));
        assert!(sharded.text_remove("t", "d1"));
        assert_eq!(sharded.spaces(), vec![String::from("s"), String::from("t")]);
        assert!(!PartitionedIndex::new(0).is_empty());
        assert!(PartitionedIndex::new(10000).len() <= MAX_PARTITIONS);
    }

    #[test]
    fn partitioned_enforces_dim_globally() {
        let mut sharded = PartitionedIndex::new(4);
        sharded.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        assert!(sharded.vector_upsert("s", String::from("b"), vec![1.0]).is_err());
        assert!(sharded.vector_search("s", &[1.0], 1).is_err());
        assert!(sharded.ann_search("s", &[1.0], 1, 16).is_err());
    }

    #[test]
    fn partitioned_ann_recall() {
        let mut state = 0xabcdefu64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 11) as f64) / ((1u64 << 53) as f64) - 0.5
        };
        let mut sharded = PartitionedIndex::new(4);
        for index in 0..300 {
            let vector: Vec<f32> = (0..16).map(|_| next() as f32).collect();
            sharded.vector_upsert("s", format!("v{index}"), vector).unwrap();
        }
        let query: Vec<f32> = (0..16).map(|_| next() as f32).collect();
        let exact = sharded.vector_search("s", &query, 10).unwrap();
        let approx = sharded.ann_search("s", &query, 10, 64).unwrap();
        let exact_ids: std::collections::HashSet<&str> =
            exact.iter().map(|h| h.id.as_str()).collect();
        let overlap = approx.iter().filter(|h| exact_ids.contains(h.id.as_str())).count();
        assert!(overlap >= 7, "overlap {overlap}");
        assert!(sharded.vector_search("missing", &query, 10).is_err());
    }
}
