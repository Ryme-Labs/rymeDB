use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet};

pub const MAX_DIM: usize = 4096;
pub const MAX_TOP_K: usize = 100;
pub const MAX_VECTORS_PER_SPACE: usize = 100_000;
pub const MAX_DOCS_PER_SPACE: usize = 100_000;
pub const MAX_INDEX_SPACES: usize = 10_000;
pub const MAX_INDEX_SPACE_BYTES: usize = 1024;
pub const MAX_INDEX_ID_BYTES: usize = 1024;
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;
pub const MAX_TEXT_QUERY_BYTES: usize = 64 * 1024;
pub const MAX_TERMS_PER_DOCUMENT: usize = 65_536;
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

#[derive(Debug)]
struct ScoredId<'a> {
    id: Cow<'a, str>,
    score: f64,
}

impl PartialEq for ScoredId<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal && self.id == other.id
    }
}

impl Eq for ScoredId<'_> {}

impl PartialOrd for ScoredId<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredId<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap keeps the worst retained hit at the root: lower scores
        // are worse, and for equal scores lexicographically larger IDs lose.
        other.score.total_cmp(&self.score).then_with(|| self.id.cmp(&other.id))
    }
}

fn top_k_vector_hits<'a, I, S>(hits: I, top_k: usize) -> Vec<VectorHit>
where
    I: IntoIterator<Item = (S, f64)>,
    S: Into<Cow<'a, str>>,
{
    let limit = top_k.clamp(1, MAX_TOP_K);
    let mut best = BinaryHeap::with_capacity(limit);
    for (id, score) in hits {
        let candidate = ScoredId { id: id.into(), score };
        if best.len() < limit {
            best.push(candidate);
            continue;
        }
        if best.peek().is_some_and(|worst| candidate.cmp(worst) == Ordering::Less) {
            best.pop();
            best.push(candidate);
        }
    }
    let mut selected: Vec<VectorHit> = best
        .into_iter()
        .map(|hit| VectorHit { id: hit.id.into_owned(), score: hit.score })
        .collect();
    selected.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    selected
}

#[derive(Debug, Default)]
struct VectorSpace {
    dim: usize,
    ann: HnswIndex,
}

fn normalize(vector: &[f32]) -> Result<Vec<f32>> {
    if vector.is_empty() || vector.len() > MAX_DIM {
        return Err(RymeError::InvalidArgument(String::from("vector dim")));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(RymeError::InvalidArgument(String::from("non-finite vector value")));
    }
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
    tombstones: usize,
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
        if self.id_to_pos.contains_key(&id) {
            self.remove(&id);
        }
        self.insert_new(id, vector);
        self.compact_if_needed();
        Ok(())
    }

    fn insert_new(&mut self, id: String, vector: Vec<f32>) {
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
            return;
        };
        let mut current = entry;
        let entry_level = self.nodes[entry].neighbors.len() - 1;
        for layer in ((level + 1)..=entry_level).rev() {
            current = self.greedy(&self.nodes[pos].vector, current, layer);
        }
        for layer in (0..=level.min(entry_level)).rev() {
            let candidates = self.beam(&self.nodes[pos].vector, &[current], HNSW_EF_BUILD, layer);
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
    }

    fn compact_if_needed(&mut self) {
        if self.tombstones < 64 || self.tombstones < self.id_to_pos.len() {
            return;
        }
        self.rebuild();
    }

    fn rebuild(&mut self) {
        let dim = self.dim;
        let old_nodes = std::mem::take(&mut self.nodes);
        let old_positions = std::mem::take(&mut self.id_to_pos);
        *self = Self { dim, ..Self::default() };
        for (pos, node) in old_nodes.into_iter().enumerate() {
            if !node.tombstoned && old_positions.get(&node.id) == Some(&pos) {
                self.insert_new(node.id, node.vector);
            }
        }
    }

    fn greedy(&self, query: &[f32], start: usize, layer: usize) -> usize {
        let mut current = start;
        let mut best = dot(query, &self.nodes[start].vector);
        loop {
            let mut moved = false;
            let neighbors =
                self.nodes[current].neighbors.get(layer).map(Vec::as_slice).unwrap_or(&[]);
            for &neighbor in neighbors {
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

    fn beam(&self, query: &[f32], starts: &[usize], ef: usize, layer: usize) -> Vec<(f64, usize)> {
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
        for &start in starts {
            let score = dot(query, &self.nodes[start].vector);
            candidates.push(Scored(score, start));
            if !self.nodes[start].tombstoned {
                results.push(std::cmp::Reverse(Scored(score, start)));
            }
        }
        while let Some(Scored(score, pos)) = candidates.pop() {
            let worst = results.peek().map(|s| (s.0).0).unwrap_or(f64::NEG_INFINITY);
            if score < worst && results.len() >= ef {
                break;
            }
            let neighbors = self.nodes[pos].neighbors.get(layer).map(Vec::as_slice).unwrap_or(&[]);
            for &neighbor in neighbors {
                if !visited.insert(neighbor) {
                    continue;
                }
                let neighbor_score = dot(query, &self.nodes[neighbor].vector);
                let worst = results.peek().map(|s| (s.0).0).unwrap_or(f64::NEG_INFINITY);
                if neighbor_score > worst || results.len() < ef {
                    candidates.push(Scored(neighbor_score, neighbor));
                    if !self.nodes[neighbor].tombstoned {
                        results.push(std::cmp::Reverse(Scored(neighbor_score, neighbor)));
                        if results.len() > ef {
                            results.pop();
                        }
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
            .beam(query, &[current], ef, 0)
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
                self.tombstones += 1;
            }
            self.compact_if_needed();
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
        if self.dim != 0 && vector.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if !self.ann.id_to_pos.contains_key(&id)
            && self.ann.id_to_pos.len() >= MAX_VECTORS_PER_SPACE
        {
            return Err(RymeError::Overload(String::from("vector space")));
        }
        let normalized = normalize(&vector)?;
        let dim = if self.dim == 0 { vector.len() } else { self.dim };
        self.ann.upsert(dim, id, normalized)?;
        self.dim = dim;
        Ok(())
    }

    fn remove(&mut self, id: &str) -> bool {
        let removed = self.ann.remove(id);
        if removed && self.ann.id_to_pos.is_empty() {
            self.ann = HnswIndex::default();
        }
        removed
    }

    fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<VectorHit>> {
        if query.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if query.iter().any(|value| !value.is_finite()) {
            return Err(RymeError::InvalidArgument(String::from("non-finite vector value")));
        }
        let norm_squared = query.iter().map(|v| (*v as f64) * (*v as f64)).sum::<f64>();
        if norm_squared == 0.0 {
            return Err(RymeError::InvalidArgument(String::from("zero vector")));
        }
        let inverse_norm = norm_squared.sqrt().recip();
        Ok(top_k_vector_hits(
            self.ann.id_to_pos.iter().map(|(id, pos)| {
                let stored = &self.ann.nodes[*pos].vector;
                let dot = stored
                    .iter()
                    .zip(query.iter())
                    .map(|(a, b)| (*a as f64) * (*b as f64))
                    .sum::<f64>();
                let score = dot * inverse_norm;
                (id.as_str(), score)
            }),
            top_k,
        ))
    }

    fn ann_search(&self, query: &[f32], top_k: usize, ef: usize) -> Result<Vec<VectorHit>> {
        if query.is_empty() || query.len() > MAX_DIM || query.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let effective_ef = ef.clamp(top_k.clamp(1, MAX_TOP_K), 256);
        // A full scan is cheaper than graph traversal while the local vector
        // count is no larger than the approximate search's candidate-work
        // estimate. This also avoids ANN recall loss on small partitions.
        if self.ann.id_to_pos.len() <= effective_ef.saturating_mul(HNSW_M) {
            return self.search(query, top_k);
        }
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
    fn index_doc(&mut self, id: String, text: &str) -> Result<usize> {
        if id.is_empty() || id.len() > MAX_INDEX_ID_BYTES {
            return Err(RymeError::InvalidArgument(String::from("text id")));
        }
        if text.len() > MAX_TEXT_BYTES {
            return Err(RymeError::Overload(String::from("text document")));
        }
        if !self.docs.contains_key(&id) && self.docs.len() >= MAX_DOCS_PER_SPACE {
            return Err(RymeError::Overload(String::from("text space")));
        }
        let mut terms: HashMap<String, u32> = HashMap::new();
        for term in tokenize(text) {
            if terms.len() >= MAX_TERMS_PER_DOCUMENT && !terms.contains_key(&term) {
                return Err(RymeError::Overload(String::from("text terms")));
            }
            *terms.entry(term).or_insert(0) += 1;
        }
        let count = terms.len();
        self.remove(&id);
        for (term, freq) in &terms {
            self.postings.entry(term.clone()).or_default().insert(id.clone(), *freq);
        }
        self.docs.insert(id, terms);
        Ok(count)
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

    fn search(&self, query: &str, top_k: usize) -> Result<Vec<TextHit>> {
        if query.len() > MAX_TEXT_QUERY_BYTES {
            return Err(RymeError::Overload(String::from("text query")));
        }
        let terms = tokenize(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let doc_count = self.docs.len().max(1) as f64;
        let document_frequencies: HashMap<String, usize> = terms
            .iter()
            .map(|term| (term.clone(), self.postings.get(term).map_or(0, HashMap::len)))
            .collect();
        Ok(self.search_terms(&terms, top_k, doc_count, &document_frequencies))
    }

    fn search_terms(
        &self,
        terms: &[String],
        top_k: usize,
        doc_count: f64,
        document_frequencies: &HashMap<String, usize>,
    ) -> Vec<TextHit> {
        let mut scores: HashMap<&str, f64> = HashMap::new();
        for term in terms {
            let Some(posting) = self.postings.get(term) else {
                continue;
            };
            let document_frequency = document_frequencies.get(term).copied().unwrap_or(0);
            let idf = (1.0 + doc_count / (document_frequency as f64 + 0.5)).ln();
            for (doc, freq) in posting {
                let tf = (*freq as f64) / ((*freq as f64) + 1.2);
                *scores.entry(doc.as_str()).or_insert(0.0) += idf * tf;
            }
        }
        let mut hits: Vec<TextHit> =
            scores.into_iter().map(|(id, score)| TextHit { id: id.to_string(), score }).collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        hits.truncate(top_k.clamp(1, MAX_TOP_K));
        hits
    }
}

pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            if ch.is_ascii() {
                current.push(ch.to_ascii_lowercase());
            } else {
                current.extend(ch.to_lowercase());
            }
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
    spaces: HashSet<String>,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn vector_upsert(&mut self, space: &str, id: String, vector: Vec<f32>) -> Result<()> {
        validate_space(space)?;
        validate_id(&id, "vector id")?;
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        let result = match self.vectors.entry(space.to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().upsert(id, vector),
            Entry::Vacant(entry) => {
                let mut vectors = VectorSpace::default();
                vectors.upsert(id, vector)?;
                entry.insert(vectors);
                Ok(())
            }
        };
        if result.is_ok() {
            self.spaces.insert(space.to_string());
        }
        result
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        let removed = self.vectors.get_mut(space).map(|s| s.remove(id)).unwrap_or(false);
        self.remove_empty_space(space);
        removed
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
        validate_space(space)?;
        validate_id(&id, "text id")?;
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        let result = match self.texts.entry(space.to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().index_doc(id, text),
            Entry::Vacant(entry) => {
                let mut texts = TextSpace::default();
                let count = texts.index_doc(id, text)?;
                entry.insert(texts);
                Ok(count)
            }
        };
        if result.is_ok() {
            self.spaces.insert(space.to_string());
        }
        result
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        let removed = self.texts.get_mut(space).map(|s| s.remove(id)).unwrap_or(false);
        self.remove_empty_space(space);
        removed
    }

    pub fn text_search(&self, space: &str, query: &str, top_k: usize) -> Result<Vec<TextHit>> {
        validate_space(space)?;
        if query.len() > MAX_TEXT_QUERY_BYTES {
            return Err(RymeError::Overload(String::from("text query")));
        }
        self.texts.get(space).map(|s| s.search(query, top_k)).unwrap_or_else(|| Ok(Vec::new()))
    }

    pub fn spaces(&self) -> Vec<String> {
        let mut out: Vec<String> = self.spaces.iter().cloned().collect();
        out.sort();
        out.dedup();
        out
    }

    fn remove_empty_space(&mut self, space: &str) {
        let vectors_empty =
            self.vectors.get(space).is_none_or(|vectors| vectors.ann.id_to_pos.is_empty());
        let texts_empty = self.texts.get(space).is_none_or(|texts| texts.docs.is_empty());
        if vectors_empty && texts_empty {
            self.vectors.remove(space);
            self.texts.remove(space);
            self.spaces.remove(space);
        }
    }
}

fn validate_space(space: &str) -> Result<()> {
    if space.is_empty() || space.len() > MAX_INDEX_SPACE_BYTES || space.contains('\0') {
        return Err(RymeError::InvalidArgument(String::from("index space")));
    }
    Ok(())
}

fn validate_id(id: &str, field: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_INDEX_ID_BYTES || id.contains('\0') {
        return Err(RymeError::InvalidArgument(String::from(field)));
    }
    Ok(())
}

pub const MAX_PARTITIONS: usize = 64;

#[derive(Debug)]
pub struct PartitionedIndex {
    partitions: Vec<IndexRegistry>,
    dims: HashMap<String, usize>,
    vector_counts: HashMap<String, usize>,
    text_counts: HashMap<String, usize>,
    spaces: HashSet<String>,
}

impl PartitionedIndex {
    pub fn new(partitions: usize) -> Self {
        let count = partitions.clamp(1, MAX_PARTITIONS);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(IndexRegistry::new());
        }
        Self {
            partitions: out,
            dims: HashMap::new(),
            vector_counts: HashMap::new(),
            text_counts: HashMap::new(),
            spaces: HashSet::new(),
        }
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
        validate_space(space)?;
        validate_id(&id, "vector id")?;
        if vector.is_empty() || vector.len() > MAX_DIM {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        match self.dims.get(space) {
            Some(dim) if *dim != vector.len() => {
                return Err(RymeError::InvalidArgument(String::from("vector dim")));
            }
            Some(_) => {}
            None => {}
        }
        let partition = self.route(space, &id);
        let is_new = !self.partitions[partition]
            .vectors
            .get(space)
            .is_some_and(|vectors| vectors.ann.id_to_pos.contains_key(&id));
        if is_new && self.vector_counts.get(space).copied().unwrap_or(0) >= MAX_VECTORS_PER_SPACE {
            return Err(RymeError::Overload(String::from("vector space")));
        }
        let dim = vector.len();
        self.partitions[partition].vector_upsert(space, id, vector)?;
        self.dims.entry(space.to_string()).or_insert(dim);
        self.spaces.insert(space.to_string());
        if is_new {
            *self.vector_counts.entry(space.to_string()).or_default() += 1;
        }
        Ok(())
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        let partition = self.route(space, id);
        let removed = self.partitions[partition].vector_remove(space, id);
        if removed {
            decrement_count(&mut self.vector_counts, space);
            self.remove_empty_space(space);
        }
        removed
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
        Ok(top_k_vector_hits(merged.into_iter().map(|hit| (hit.id, hit.score)), top_k))
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
        merged.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        merged.truncate(top_k.clamp(1, MAX_TOP_K));
        Ok(merged)
    }

    pub fn text_index(&mut self, space: &str, id: String, text: &str) -> Result<usize> {
        validate_space(space)?;
        validate_id(&id, "text id")?;
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        let partition = self.route(space, &id);
        let is_new = !self.partitions[partition]
            .texts
            .get(space)
            .is_some_and(|docs| docs.docs.contains_key(&id));
        if is_new && self.text_counts.get(space).copied().unwrap_or(0) >= MAX_DOCS_PER_SPACE {
            return Err(RymeError::Overload(String::from("text space")));
        }
        let terms = self.partitions[partition].text_index(space, id, text)?;
        self.spaces.insert(space.to_string());
        if is_new {
            *self.text_counts.entry(space.to_string()).or_default() += 1;
        }
        Ok(terms)
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        let partition = self.route(space, id);
        let removed = self.partitions[partition].text_remove(space, id);
        if removed {
            decrement_count(&mut self.text_counts, space);
            self.remove_empty_space(space);
        }
        removed
    }

    pub fn text_search(&self, space: &str, query: &str, top_k: usize) -> Result<Vec<TextHit>> {
        validate_space(space)?;
        if query.len() > MAX_TEXT_QUERY_BYTES {
            return Err(RymeError::Overload(String::from("text query")));
        }
        let terms = tokenize(query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let doc_count = self
            .partitions
            .iter()
            .filter_map(|partition| partition.texts.get(space))
            .map(|texts| texts.docs.len())
            .sum::<usize>()
            .max(1) as f64;
        let document_frequencies: HashMap<String, usize> = terms
            .iter()
            .map(|term| {
                let frequency = self
                    .partitions
                    .iter()
                    .filter_map(|partition| partition.texts.get(space))
                    .map(|texts| texts.postings.get(term).map_or(0, HashMap::len))
                    .sum();
                (term.clone(), frequency)
            })
            .collect();
        let mut merged: Vec<TextHit> = Vec::new();
        for partition in &self.partitions {
            if let Some(texts) = partition.texts.get(space) {
                merged.extend(texts.search_terms(&terms, top_k, doc_count, &document_frequencies));
            }
        }
        merged.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        merged.truncate(top_k.clamp(1, MAX_TOP_K));
        Ok(merged)
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

    fn remove_empty_space(&mut self, space: &str) {
        let still_present = self.partitions.iter().any(|partition| {
            partition.vectors.contains_key(space) || partition.texts.contains_key(space)
        });
        if !still_present {
            self.spaces.remove(space);
            self.dims.remove(space);
        }
    }
}

fn decrement_count(counts: &mut HashMap<String, usize>, space: &str) {
    if let Some(count) = counts.get_mut(space) {
        *count -= 1;
        if *count == 0 {
            counts.remove(space);
        }
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
    fn exact_vector_scores_are_cosine_similarity() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("x"), vec![1.0, 0.0]).unwrap();
        registry.vector_upsert("s", String::from("y"), vec![0.0, 1.0]).unwrap();

        let hits = registry.vector_search("s", &[3.0, 4.0], 2).unwrap();
        let scores: HashMap<_, _> = hits.into_iter().map(|hit| (hit.id, hit.score)).collect();
        assert!((scores["x"] - 0.6).abs() < 1e-12);
        assert!((scores["y"] - 0.8).abs() < 1e-12);
    }

    #[test]
    fn text_search_breaks_score_ties_by_id_before_truncating() {
        let mut index = PartitionedIndex::new(4);
        let low_id_later_partition = (0..1_000)
            .map(|index| format!("a-{index:04}"))
            .find(|id| index.route("s", id) == 3)
            .unwrap();
        let high_id_earlier_partition = (0..1_000)
            .map(|index| format!("z-{index:04}"))
            .find(|id| index.route("s", id) == 0)
            .unwrap();
        index.text_index("s", high_id_earlier_partition, "equal score").unwrap();
        index.text_index("s", low_id_later_partition.clone(), "equal score").unwrap();

        let hits = index.text_search("s", "equal", 1).unwrap();
        assert_eq!(hits[0].id, low_id_later_partition);
    }

    #[test]
    fn partitioned_text_search_uses_global_document_frequencies() {
        let mut single = IndexRegistry::new();
        let mut partitioned = PartitionedIndex::new(4);
        let mut documents = Vec::new();
        for (prefix, target_partition, text) in
            [("needle", 0, "needle common"), ("background", 3, "common")]
        {
            let ids = (0..1_000)
                .map(|index| format!("{prefix}-{index:04}"))
                .filter(|id| partitioned.route("s", id) == target_partition)
                .take(2)
                .collect::<Vec<_>>();
            assert_eq!(ids.len(), 2);
            documents.extend(ids.into_iter().map(|id| (id, text)));
        }
        for (id, text) in documents {
            single.text_index("s", id.clone(), text).unwrap();
            partitioned.text_index("s", id, text).unwrap();
        }

        let expected = single.text_search("s", "needle common", 10).unwrap();
        let actual = partitioned.text_search("s", "needle common", 10).unwrap();
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_eq!(actual.id, expected.id);
            assert!((actual.score - expected.score).abs() < 1e-12);
        }
    }

    #[test]
    fn exact_vector_top_k_orders_equal_scores_deterministically() {
        let mut registry = IndexRegistry::new();
        for id in ["z", "a", "m"] {
            registry.vector_upsert("s", id.to_string(), vec![1.0, 0.0]).unwrap();
        }
        let hits = registry.vector_search("s", &[1.0, 0.0], 2).unwrap();
        assert_eq!(hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(), ["a", "m"]);

        let mut partitioned = PartitionedIndex::new(4);
        for id in ["z", "a", "m"] {
            partitioned.vector_upsert("s", id.to_string(), vec![1.0, 0.0]).unwrap();
        }
        let hits = partitioned.vector_search("s", &[1.0, 0.0], 2).unwrap();
        assert_eq!(hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(), ["a", "m"]);
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
    fn vector_operations_reject_non_finite_values() {
        let mut registry = IndexRegistry::new();
        for (index, value) in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY].into_iter().enumerate() {
            assert!(registry.vector_upsert("s", format!("bad-{index}"), vec![value, 1.0]).is_err());
        }
        registry.vector_upsert("s", String::from("valid"), vec![1.0, 0.0]).unwrap();
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(registry.vector_search("s", &[value, 0.0], 1).is_err());
            assert!(registry.ann_search("s", &[value, 0.0], 1, 16).is_err());
        }
        assert_eq!(registry.vector_search("s", &[1.0, 0.0], 1).unwrap()[0].id, "valid");
    }

    #[test]
    fn index_boundaries_reject_large_identifiers_and_queries() {
        let mut registry = IndexRegistry::new();
        let large_space = "s".repeat(MAX_INDEX_SPACE_BYTES + 1);
        let large_id = "i".repeat(MAX_INDEX_ID_BYTES + 1);
        assert!(registry.vector_upsert(&large_space, String::from("id"), vec![1.0]).is_err());
        assert!(registry.vector_upsert("s", large_id.clone(), vec![1.0]).is_err());
        assert!(registry.text_index("s", large_id, "ok").is_err());
        assert!(registry
            .text_index("s", String::from("id"), &"x".repeat(MAX_TEXT_BYTES + 1))
            .is_err());
        assert!(registry.text_search("s", &"q".repeat(MAX_TEXT_QUERY_BYTES + 1), 1).is_err());
        assert!(registry.spaces().is_empty());
    }

    #[test]
    fn failed_text_reindex_preserves_existing_document() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("id"), "before").unwrap();
        assert!(registry
            .text_index("docs", String::from("id"), &"x".repeat(MAX_TEXT_BYTES + 1))
            .is_err());
        assert_eq!(registry.text_search("docs", "before", 1).unwrap()[0].id, "id");
    }

    #[test]
    fn ann_rejects_oversized_query_before_search() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("vectors", String::from("id"), vec![1.0, 0.0]).unwrap();
        assert!(registry.ann_search("vectors", &vec![1.0; MAX_DIM + 1], 1, 64).is_err());
    }

    #[test]
    fn rejected_first_vector_does_not_lock_dimension() {
        let mut registry = PartitionedIndex::new(4);
        assert!(registry.vector_upsert("s", String::from("bad"), vec![0.0, 0.0]).is_err());
        assert!(registry.partitions.iter().all(|partition| !partition.vectors.contains_key("s")));
        registry.vector_upsert("s", String::from("good"), vec![1.0, 0.0, 0.0]).unwrap();
        assert_eq!(registry.vector_search("s", &[1.0, 0.0, 0.0], 1).unwrap()[0].id, "good");
    }

    #[test]
    fn partitioned_vector_capacity_is_global_and_reclaimed_on_remove() {
        let mut registry = PartitionedIndex::new(4);
        registry.vector_upsert("s", String::from("existing"), vec![1.0]).unwrap();
        registry.vector_counts.insert(String::from("s"), MAX_VECTORS_PER_SPACE);
        registry.vector_upsert("s", String::from("existing"), vec![0.5]).unwrap();
        assert!(matches!(
            registry.vector_upsert("s", String::from("new"), vec![0.25]),
            Err(RymeError::Overload(_))
        ));
        assert!(registry.vector_remove("s", "existing"));
        registry.vector_upsert("s", String::from("new"), vec![0.25]).unwrap();
    }

    #[test]
    fn vector_remove_drops_hits() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        assert!(registry.vector_remove("s", "a"));
        assert!(!registry.vector_remove("s", "a"));
        assert!(matches!(registry.vector_search("s", &[1.0, 0.0], 1), Err(RymeError::NotFound(_))));
    }

    #[test]
    fn ann_rebuilds_entry_after_removing_every_vector() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("old"), vec![1.0, 0.0]).unwrap();
        assert!(registry.vector_remove("s", "old"));
        registry.vector_upsert("s", String::from("new"), vec![0.0, 1.0]).unwrap();
        let hits = registry.ann_search("s", &[0.0, 1.0], 1, 16).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "new");
    }

    #[test]
    fn ann_vector_updates_relink_nodes_and_bound_tombstones() {
        let mut registry = IndexRegistry::new();
        for index in 0..128 {
            let angle = index as f32 * std::f32::consts::TAU / 128.0;
            registry
                .vector_upsert("s", format!("v{index}"), vec![angle.cos(), angle.sin()])
                .unwrap();
        }

        for revision in 0..256 {
            let angle = (revision as f32 * 0.37) % std::f32::consts::TAU;
            registry
                .vector_upsert("s", String::from("v0"), vec![angle.cos(), angle.sin()])
                .unwrap();
        }

        let query = [0.0, 1.0];
        let exact = registry.vector_search("s", &query, 1).unwrap();
        let approximate = registry.ann_search("s", &query, 1, 256).unwrap();
        assert_eq!(approximate[0].id, exact[0].id);

        let ann = &registry.vectors["s"].ann;
        assert!(ann.nodes.len() <= ann.id_to_pos.len() + 63);
        assert!(ann.nodes.iter().filter(|node| node.tombstoned).count() < 64);

        let mut delete_heavy = IndexRegistry::new();
        for index in 0..200 {
            let angle = index as f32 * std::f32::consts::TAU / 200.0;
            delete_heavy
                .vector_upsert("s", format!("v{index}"), vec![angle.cos(), angle.sin()])
                .unwrap();
        }
        for index in 0..180 {
            assert!(delete_heavy.vector_remove("s", &format!("v{index}")));
        }
        let query = [
            (199.0f32 * std::f32::consts::TAU / 200.0).cos(),
            (199.0f32 * std::f32::consts::TAU / 200.0).sin(),
        ];
        let exact = delete_heavy.vector_search("s", &query, 1).unwrap();
        let approximate = delete_heavy.ann_search("s", &query, 1, 256).unwrap();
        assert_eq!(approximate[0].id, exact[0].id);
        let ann = &delete_heavy.vectors["s"].ann;
        assert!(ann.nodes.len() <= ann.id_to_pos.len() + 63);
    }

    #[test]
    fn text_ranks_term_match() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("d1"), "the quick brown fox").unwrap();
        registry.text_index("docs", String::from("d2"), "lorem ipsum dolor").unwrap();
        let hits = registry.text_search("docs", "quick fox", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "d1");
    }

    #[test]
    fn tokenizer_lowercases_unicode_letters() {
        assert_eq!(tokenize("RÉSUMÉ"), tokenize("résumé"));
    }

    #[test]
    fn text_remove_and_empty_query() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("d1"), "hello world").unwrap();
        assert!(registry.text_search("docs", "!!!", 10).unwrap().is_empty());
        assert!(registry.text_remove("docs", "d1"));
        assert!(registry.text_search("docs", "hello", 10).unwrap().is_empty());
        assert!(registry.text_search("missing", "hello", 10).unwrap().is_empty());
    }

    #[test]
    fn partitioned_text_capacity_is_global_and_reindex_is_atomic() {
        let mut registry = PartitionedIndex::new(4);
        registry.text_index("docs", String::from("existing"), "before").unwrap();
        registry.text_counts.insert(String::from("docs"), MAX_DOCS_PER_SPACE);

        assert!(matches!(
            registry.text_index("docs", String::from("new"), "blocked"),
            Err(RymeError::Overload(_))
        ));
        registry.text_index("docs", String::from("existing"), "after").unwrap();
        assert!(registry.text_search("docs", "before", 10).unwrap().is_empty());
        assert_eq!(registry.text_search("docs", "after", 10).unwrap()[0].id, "existing");
        assert!(registry.text_remove("docs", "existing"));
        registry.text_index("docs", String::from("new"), "admitted").unwrap();
        assert_eq!(registry.text_counts.get("docs"), Some(&MAX_DOCS_PER_SPACE));
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
        for index in 0..2_048 {
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
    fn small_ann_search_matches_exact_results() {
        let mut registry = IndexRegistry::new();
        for index in 0..256 {
            registry
                .vector_upsert("s", format!("v{index}"), vec![index as f32 + 1.0, 3.0, 7.0])
                .unwrap();
        }
        let query = [1.0, 2.0, 3.0];
        let exact = registry.vector_search("s", &query, 10).unwrap();
        let approximate = registry.ann_search("s", &query, 10, 64).unwrap();
        assert_eq!(approximate.len(), exact.len());
        for (actual, expected) in approximate.iter().zip(exact) {
            assert_eq!(actual.id, expected.id);
            assert_eq!(actual.score, expected.score);
        }
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
        assert_eq!(sharded.text_search("t", "alpha", 50).unwrap().len(), 50);
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
        assert_eq!(overlap, 10, "small partitions should use exact search");
        assert!(sharded.vector_search("missing", &query, 10).is_err());
    }
}
