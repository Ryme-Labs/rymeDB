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
/// Conservative accounting budget for retained full-text index structures.
pub const MAX_TEXT_INDEX_BYTES: usize = 128 * 1024 * 1024;
/// Per-tenant share of the process-wide full-text index budget.
pub const MAX_TENANT_TEXT_INDEX_BYTES: usize = MAX_TEXT_INDEX_BYTES / 4;
/// Conservative accounting budget for retained HNSW vector index structures.
pub const MAX_VECTOR_INDEX_BYTES: usize = 256 * 1024 * 1024;
/// Per-tenant share of the process-wide HNSW vector index budget.
pub const MAX_TENANT_VECTOR_INDEX_BYTES: usize = MAX_VECTOR_INDEX_BYTES / 4;
pub const HNSW_M: usize = 16;
pub const HNSW_EF_BUILD: usize = 64;
const HNSW_HIGH_DIM_M: usize = 32;
const HNSW_HIGH_DIM_THRESHOLD: usize = 64;
const HNSW_EXACT_FALLBACK_MAX: usize = HNSW_M * HNSW_EF_BUILD * 2;
const HNSW_NODE_OVERHEAD_BYTES: usize = 8 * 1024;
const MAX_INDEX_TENANT_BYTES: usize = 256;
const DEFAULT_INDEX_TENANT: &str = "default";

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

fn top_k_scored_ids<'a, I, S>(hits: I, top_k: usize) -> Vec<ScoredId<'a>>
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
    let mut selected = best.into_vec();
    selected.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
    selected
}

fn top_k_vector_hits<'a, I, S>(hits: I, top_k: usize) -> Vec<VectorHit>
where
    I: IntoIterator<Item = (S, f64)>,
    S: Into<Cow<'a, str>>,
{
    top_k_scored_ids(hits, top_k)
        .into_iter()
        .map(|hit| VectorHit { id: hit.id.into_owned(), score: hit.score })
        .collect()
}

#[derive(Debug, Default)]
struct VectorSpace {
    dim: usize,
    ann: HnswIndex,
}

fn vector_norm(vector: &[f32]) -> Result<f64> {
    if vector.is_empty() || vector.len() > MAX_DIM {
        return Err(RymeError::InvalidArgument(String::from("vector dim")));
    }
    let mut norm_squared = 0.0;
    for value in vector {
        if !value.is_finite() {
            return Err(RymeError::InvalidArgument(String::from("non-finite vector value")));
        }
        norm_squared += (*value as f64) * (*value as f64);
    }
    let norm = norm_squared.sqrt();
    if norm == 0.0 {
        return Err(RymeError::InvalidArgument(String::from("zero vector")));
    }
    Ok(norm)
}

fn normalize(vector: &[f32]) -> Result<Vec<f32>> {
    let norm = vector_norm(vector)?;
    Ok(vector.iter().map(|value| (*value as f64 / norm) as f32).collect())
}

fn estimate_vector_node_bytes(id_bytes: usize, dimension: usize) -> usize {
    HNSW_NODE_OVERHEAD_BYTES
        .saturating_add(dimension.saturating_mul(std::mem::size_of::<f32>()))
        .saturating_add(id_bytes.saturating_mul(2))
}

fn estimate_vector_id_map_bytes(id_bytes: usize) -> usize {
    256usize.saturating_add(id_bytes)
}

fn projected_vector_index_bytes(
    current: usize,
    id_bytes: usize,
    dimension: usize,
    is_new: bool,
) -> Result<usize> {
    projected_vector_index_bytes_with_limit(
        current,
        id_bytes,
        dimension,
        is_new,
        MAX_VECTOR_INDEX_BYTES,
        "vector index memory",
    )
}

fn projected_vector_index_bytes_with_limit(
    current: usize,
    id_bytes: usize,
    dimension: usize,
    is_new: bool,
    limit: usize,
    error: &'static str,
) -> Result<usize> {
    let growth = estimate_vector_node_bytes(id_bytes, dimension).saturating_add(if is_new {
        estimate_vector_id_map_bytes(id_bytes)
    } else {
        0
    });
    let projected =
        current.checked_add(growth).ok_or_else(|| RymeError::Overload(String::from(error)))?;
    if projected > limit {
        return Err(RymeError::Overload(String::from(error)));
    }
    Ok(projected)
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
    m: usize,
    nodes: Vec<HnswNode>,
    id_to_pos: HashMap<String, usize>,
    estimated_bytes: usize,
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
        ((-uniform.ln() / (self.m as f64).ln()).floor() as usize).min(8)
    }

    fn upsert(&mut self, dim: usize, id: String, vector: Vec<f32>) -> Result<()> {
        if self.nodes.is_empty() {
            self.dim = dim;
            self.m = if dim >= HNSW_HIGH_DIM_THRESHOLD { HNSW_HIGH_DIM_M } else { HNSW_M };
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
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_add(estimate_vector_node_bytes(id.len(), vector.len()))
            .saturating_add(estimate_vector_id_map_bytes(id.len()));
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
            let selected = select_neighbors(&candidates, self.m, &self.nodes);
            self.nodes[pos].neighbors[layer] = selected.clone();
            for neighbor in selected {
                self.nodes[neighbor].neighbors[layer].push(pos);
                if self.nodes[neighbor].neighbors[layer].len() > self.m * 2 {
                    let pruned = prune_neighbors(neighbor, layer, self.m, &self.nodes);
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
        let m = self.m;
        let old_nodes = std::mem::take(&mut self.nodes);
        let old_positions = std::mem::take(&mut self.id_to_pos);
        *self = Self { dim, m, ..Self::default() };
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
        use std::collections::BinaryHeap;
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
        // Positions are internal dense integers, so keyed collision resistance is unnecessary.
        let mut visited = rustc_hash::FxHashSet::with_capacity_and_hasher(
            ef.saturating_mul(self.m).min(self.nodes.len()),
            Default::default(),
        );
        visited.extend(starts.iter().copied());
        let mut candidates: BinaryHeap<Scored> = BinaryHeap::with_capacity(ef);
        let mut results: BinaryHeap<std::cmp::Reverse<Scored>> = BinaryHeap::with_capacity(ef);
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
        let top_k = top_k.clamp(1, MAX_TOP_K);
        let ef = ef.clamp(top_k, 256);
        let hits = self
            .beam(query, &[current], ef, 0)
            .into_iter()
            .take(top_k)
            .map(|(score, pos)| (score, self.nodes[pos].id.clone()))
            .collect();
        Ok(hits)
    }

    fn compact_tombstones(&mut self) {
        if self.tombstones > 0 {
            self.rebuild();
        }
    }

    fn remove(&mut self, id: &str) -> bool {
        if let Some(pos) = self.id_to_pos.remove(id) {
            self.estimated_bytes =
                self.estimated_bytes.saturating_sub(estimate_vector_id_map_bytes(id.len()));
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

fn prune_neighbors(pos: usize, layer: usize, limit: usize, nodes: &[HnswNode]) -> Vec<usize> {
    let neighbors = nodes[pos].neighbors.get(layer).map(Vec::as_slice).unwrap_or(&[]);
    let mut scored: Vec<(f64, usize)> = neighbors
        .iter()
        .copied()
        .filter(|p| nodes.get(*p).is_some_and(|n| !n.tombstoned))
        .map(|p| (dot(&nodes[pos].vector, &nodes[p].vector), p))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(limit).map(|(_, p)| p).collect()
}

impl VectorSpace {
    fn estimated_bytes(&self) -> usize {
        self.ann.estimated_bytes
    }

    fn compact_tombstones(&mut self) {
        self.ann.compact_tombstones();
    }

    #[cfg(test)]
    fn upsert(&mut self, id: String, vector: Vec<f32>) -> Result<()> {
        let normalized = normalize(&vector)?;
        self.upsert_normalized(id, normalized)
    }

    fn upsert_normalized(&mut self, id: String, normalized: Vec<f32>) -> Result<()> {
        if normalized.is_empty() || normalized.len() > MAX_DIM {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if self.dim != 0 && normalized.len() != self.dim {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if !self.ann.id_to_pos.contains_key(&id)
            && self.ann.id_to_pos.len() >= MAX_VECTORS_PER_SPACE
        {
            return Err(RymeError::Overload(String::from("vector space")));
        }
        let dim = if self.dim == 0 { normalized.len() } else { self.dim };
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
        // Keep small partitions exact near the measured crossover. Cap this
        // threshold so larger search widths do not force scans of large spaces.
        let exact_fallback_limit =
            effective_ef.saturating_mul(self.ann.m).saturating_mul(2).min(HNSW_EXACT_FALLBACK_MAX);
        if self.ann.id_to_pos.len() <= exact_fallback_limit {
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
    doc_index_bytes: HashMap<String, usize>,
}

fn parse_text_terms(id: &str, text: &str) -> Result<HashMap<String, u32>> {
    validate_id(id, "text id")?;
    if text.len() > MAX_TEXT_BYTES {
        return Err(RymeError::Overload(String::from("text document")));
    }
    let mut terms: HashMap<String, u32> = HashMap::new();
    let complete = visit_tokens(text, |term| {
        if let Some(frequency) = terms.get_mut(term) {
            *frequency += 1;
            true
        } else if terms.len() >= MAX_TERMS_PER_DOCUMENT {
            false
        } else {
            terms.insert(term.to_owned(), 1);
            true
        }
    });
    if !complete {
        return Err(RymeError::Overload(String::from("text terms")));
    }
    Ok(terms)
}

fn estimate_text_index_bytes(id: &str, terms: &HashMap<String, u32>) -> usize {
    terms.iter().fold(256usize.saturating_add(id.len().saturating_mul(2)), |bytes, (term, _)| {
        bytes
            .saturating_add(256)
            .saturating_add(term.len().saturating_mul(2))
            .saturating_add(id.len())
    })
}

fn updated_text_index_bytes(current: usize, previous: usize, next: usize) -> Result<usize> {
    updated_index_bytes(current, previous, next, MAX_TEXT_INDEX_BYTES, "text index memory")
}

fn updated_index_bytes(
    current: usize,
    previous: usize,
    next: usize,
    limit: usize,
    error: &'static str,
) -> Result<usize> {
    let total = current
        .saturating_sub(previous)
        .checked_add(next)
        .ok_or_else(|| RymeError::Overload(String::from(error)))?;
    if total > limit {
        return Err(RymeError::Overload(String::from(error)));
    }
    Ok(total)
}

fn validate_index_tenant(tenant: &str) -> Result<()> {
    if tenant.is_empty() || tenant.len() > MAX_INDEX_TENANT_BYTES || tenant.contains('\0') {
        return Err(RymeError::InvalidArgument(String::from("index tenant")));
    }
    Ok(())
}

fn update_tenant_index_bytes(
    usage: &mut HashMap<String, usize>,
    tenant: &str,
    previous: usize,
    next: usize,
) {
    let total =
        usage.get(tenant).copied().unwrap_or(0).saturating_sub(previous).saturating_add(next);
    if total == 0 {
        usage.remove(tenant);
    } else {
        usage.insert(tenant.to_string(), total);
    }
}

impl TextSpace {
    fn index_doc(
        &mut self,
        id: String,
        terms: HashMap<String, u32>,
        index_bytes: usize,
    ) -> Result<(usize, usize)> {
        if !self.docs.contains_key(&id) && self.docs.len() >= MAX_DOCS_PER_SPACE {
            return Err(RymeError::Overload(String::from("text space")));
        }
        let count = terms.len();
        let previous_index_bytes = self.doc_index_bytes.get(&id).copied().unwrap_or(0);
        let _ = self.remove(&id);
        for (term, freq) in &terms {
            self.postings.entry(term.clone()).or_default().insert(id.clone(), *freq);
        }
        self.docs.insert(id.clone(), terms);
        self.doc_index_bytes.insert(id, index_bytes);
        Ok((count, previous_index_bytes))
    }

    fn remove(&mut self, id: &str) -> Option<usize> {
        let terms = self.docs.remove(id)?;
        let index_bytes = self.doc_index_bytes.remove(id).unwrap_or(0);
        for term in terms.keys() {
            if let Some(posting) = self.postings.get_mut(term) {
                posting.remove(id);
                if posting.is_empty() {
                    self.postings.remove(term);
                }
            }
        }
        Some(index_bytes)
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
        if let [term] = terms {
            let Some(posting) = self.postings.get(term) else {
                return Vec::new();
            };
            let document_frequency = document_frequencies.get(term).copied().unwrap_or(0);
            let idf = (1.0 + doc_count / (document_frequency as f64 + 0.5)).ln();
            return top_k_scored_ids(
                posting.iter().map(|(doc, freq)| {
                    let freq = *freq as f64;
                    (doc.as_str(), idf * (freq / (freq + 1.2)))
                }),
                top_k,
            )
            .into_iter()
            .map(|hit| TextHit { id: hit.id.into_owned(), score: hit.score })
            .collect();
        }

        let candidate_capacity = terms
            .iter()
            .filter_map(|term| self.postings.get(term))
            .fold(0usize, |count, posting| count.saturating_add(posting.len()))
            .min(self.docs.len());
        let mut scores: HashMap<&str, f64> = HashMap::with_capacity(candidate_capacity);
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
        top_k_scored_ids(scores, top_k)
            .into_iter()
            .map(|hit| TextHit { id: hit.id.into_owned(), score: hit.score })
            .collect()
    }
}

fn visit_tokens(text: &str, mut visit: impl FnMut(&str) -> bool) -> bool {
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            if ch.is_ascii() {
                current.push(ch.to_ascii_lowercase());
            } else {
                current.extend(ch.to_lowercase());
            }
        } else if !current.is_empty() {
            if !visit(&current) {
                return false;
            }
            current.clear();
        }
    }
    current.is_empty() || visit(&current)
}

pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    visit_tokens(text, |term| {
        out.push(term.to_owned());
        true
    });
    out
}

#[derive(Debug, Default)]
pub struct IndexRegistry {
    vectors: HashMap<String, VectorSpace>,
    texts: HashMap<String, TextSpace>,
    spaces: HashSet<String>,
    vector_index_bytes: usize,
    text_index_bytes: usize,
}

impl IndexRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn vector_upsert(&mut self, space: &str, id: String, vector: Vec<f32>) -> Result<()> {
        validate_space(space)?;
        validate_id(&id, "vector id")?;
        if vector.is_empty() || vector.len() > MAX_DIM {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        if self
            .vectors
            .get(space)
            .is_some_and(|vectors| vectors.dim != 0 && vectors.dim != vector.len())
        {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let normalized = normalize(&vector)?;
        self.vector_upsert_normalized(space, id, normalized)
    }

    fn vector_upsert_normalized(
        &mut self,
        space: &str,
        id: String,
        normalized: Vec<f32>,
    ) -> Result<()> {
        validate_space(space)?;
        validate_id(&id, "vector id")?;
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        if self
            .vectors
            .get(space)
            .is_some_and(|vectors| vectors.dim != 0 && vectors.dim != normalized.len())
        {
            return Err(RymeError::InvalidArgument(String::from("vector dim")));
        }
        let is_new =
            !self.vectors.get(space).is_some_and(|vectors| vectors.ann.id_to_pos.contains_key(&id));
        if is_new
            && self
                .vectors
                .get(space)
                .is_some_and(|vectors| vectors.ann.id_to_pos.len() >= MAX_VECTORS_PER_SPACE)
        {
            return Err(RymeError::Overload(String::from("vector space")));
        }
        if projected_vector_index_bytes(self.vector_index_bytes, id.len(), normalized.len(), is_new)
            .is_err()
        {
            self.compact_vector_space(space);
            projected_vector_index_bytes(
                self.vector_index_bytes,
                id.len(),
                normalized.len(),
                is_new,
            )?;
        }
        let previous_space_bytes =
            self.vectors.get(space).map(VectorSpace::estimated_bytes).unwrap_or(0);
        let result = match self.vectors.entry(space.to_string()) {
            Entry::Occupied(mut entry) => entry.get_mut().upsert_normalized(id, normalized),
            Entry::Vacant(entry) => {
                let mut vectors = VectorSpace::default();
                vectors.upsert_normalized(id, normalized)?;
                entry.insert(vectors);
                Ok(())
            }
        };
        if result.is_ok() {
            let next_space_bytes =
                self.vectors.get(space).map(VectorSpace::estimated_bytes).unwrap_or(0);
            self.vector_index_bytes = self
                .vector_index_bytes
                .saturating_sub(previous_space_bytes)
                .saturating_add(next_space_bytes);
            self.spaces.insert(space.to_string());
        }
        result
    }

    fn compact_vector_space(&mut self, space: &str) {
        let Some(vectors) = self.vectors.get_mut(space) else {
            return;
        };
        let previous_bytes = vectors.estimated_bytes();
        vectors.compact_tombstones();
        self.vector_index_bytes = self
            .vector_index_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(vectors.estimated_bytes());
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        let previous_space_bytes =
            self.vectors.get(space).map(VectorSpace::estimated_bytes).unwrap_or(0);
        let removed = self.vectors.get_mut(space).map(|s| s.remove(id)).unwrap_or(false);
        if removed {
            let next_space_bytes =
                self.vectors.get(space).map(VectorSpace::estimated_bytes).unwrap_or(0);
            self.vector_index_bytes = self
                .vector_index_bytes
                .saturating_sub(previous_space_bytes)
                .saturating_add(next_space_bytes);
        }
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

    fn validate_text_index_target(&self, space: &str, id: &str) -> Result<()> {
        validate_space(space)?;
        validate_id(id, "text id")?;
        if !self.spaces.contains(space) && self.spaces.len() >= MAX_INDEX_SPACES {
            return Err(RymeError::Overload(String::from("index spaces")));
        }
        if self.texts.get(space).is_some_and(|texts| {
            !texts.docs.contains_key(id) && texts.docs.len() >= MAX_DOCS_PER_SPACE
        }) {
            return Err(RymeError::Overload(String::from("text space")));
        }
        Ok(())
    }

    pub fn text_index(&mut self, space: &str, id: String, text: &str) -> Result<usize> {
        self.validate_text_index_target(space, &id)?;
        let terms = parse_text_terms(&id, text)?;
        self.text_index_parsed(space, id, terms)
    }

    fn text_index_parsed(
        &mut self,
        space: &str,
        id: String,
        terms: HashMap<String, u32>,
    ) -> Result<usize> {
        self.validate_text_index_target(space, &id)?;
        let index_bytes = estimate_text_index_bytes(&id, &terms);
        let previous_index_bytes = self
            .texts
            .get(space)
            .and_then(|texts| texts.doc_index_bytes.get(&id))
            .copied()
            .unwrap_or(0);
        let next_index_bytes =
            updated_text_index_bytes(self.text_index_bytes, previous_index_bytes, index_bytes)?;
        let result = match self.texts.entry(space.to_string()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().index_doc(id, terms, index_bytes).map(|(count, _)| count)
            }
            Entry::Vacant(entry) => {
                let mut texts = TextSpace::default();
                let (count, _) = texts.index_doc(id, terms, index_bytes)?;
                entry.insert(texts);
                Ok(count)
            }
        };
        if result.is_ok() {
            self.text_index_bytes = next_index_bytes;
            self.spaces.insert(space.to_string());
        }
        result
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        let removed_bytes = self.texts.get_mut(space).and_then(|texts| texts.remove(id));
        let removed = removed_bytes.is_some();
        if let Some(bytes) = removed_bytes {
            self.text_index_bytes = self.text_index_bytes.saturating_sub(bytes);
        }
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

#[derive(Debug, Clone, Serialize)]
pub struct IndexMemoryBudget {
    pub used_bytes: usize,
    pub limit_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TenantIndexMemoryUsage {
    pub tenant: String,
    pub full_text: IndexMemoryBudget,
    pub vector: IndexMemoryBudget,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexMemoryUsage {
    pub full_text: IndexMemoryBudget,
    pub vector: IndexMemoryBudget,
    pub per_tenant: Vec<TenantIndexMemoryUsage>,
}

#[derive(Debug)]
pub struct PartitionedIndex {
    partitions: Vec<IndexRegistry>,
    dims: HashMap<String, usize>,
    vector_counts: HashMap<String, usize>,
    vector_index_bytes: usize,
    tenant_vector_index_bytes: HashMap<String, usize>,
    text_counts: HashMap<String, usize>,
    text_index_bytes: usize,
    tenant_text_index_bytes: HashMap<String, usize>,
    space_tenants: HashMap<String, String>,
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
            vector_index_bytes: 0,
            tenant_vector_index_bytes: HashMap::new(),
            text_counts: HashMap::new(),
            text_index_bytes: 0,
            tenant_text_index_bytes: HashMap::new(),
            space_tenants: HashMap::new(),
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

    fn validate_space_tenant(&self, tenant: &str, space: &str) -> Result<()> {
        if self.space_tenants.get(space).is_some_and(|owner| owner != tenant) {
            return Err(RymeError::Forbidden);
        }
        Ok(())
    }

    pub fn vector_upsert(&mut self, space: &str, id: String, vector: Vec<f32>) -> Result<()> {
        self.vector_upsert_scoped(DEFAULT_INDEX_TENANT, space, id, vector)
    }

    pub fn vector_upsert_scoped(
        &mut self,
        tenant: &str,
        space: &str,
        id: String,
        vector: Vec<f32>,
    ) -> Result<()> {
        validate_index_tenant(tenant)?;
        validate_space(space)?;
        self.validate_space_tenant(tenant, space)?;
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
        let normalized = normalize(&vector)?;
        let tenant_bytes = self.tenant_vector_index_bytes.get(tenant).copied().unwrap_or(0);
        let needs_compaction = projected_vector_index_bytes(
            self.vector_index_bytes,
            id.len(),
            normalized.len(),
            is_new,
        )
        .is_err()
            || projected_vector_index_bytes_with_limit(
                tenant_bytes,
                id.len(),
                normalized.len(),
                is_new,
                MAX_TENANT_VECTOR_INDEX_BYTES,
                "tenant vector index memory",
            )
            .is_err();
        if needs_compaction {
            let previous_space_bytes = self.partitions[partition]
                .vectors
                .get(space)
                .map(VectorSpace::estimated_bytes)
                .unwrap_or(0);
            self.partitions[partition].compact_vector_space(space);
            let next_space_bytes = self.partitions[partition]
                .vectors
                .get(space)
                .map(VectorSpace::estimated_bytes)
                .unwrap_or(0);
            self.vector_index_bytes = self
                .vector_index_bytes
                .saturating_sub(previous_space_bytes)
                .saturating_add(next_space_bytes);
            update_tenant_index_bytes(
                &mut self.tenant_vector_index_bytes,
                tenant,
                previous_space_bytes,
                next_space_bytes,
            );
            projected_vector_index_bytes(
                self.vector_index_bytes,
                id.len(),
                normalized.len(),
                is_new,
            )?;
            projected_vector_index_bytes_with_limit(
                self.tenant_vector_index_bytes.get(tenant).copied().unwrap_or(0),
                id.len(),
                normalized.len(),
                is_new,
                MAX_TENANT_VECTOR_INDEX_BYTES,
                "tenant vector index memory",
            )?;
        }
        let previous_space_bytes = self.partitions[partition]
            .vectors
            .get(space)
            .map(VectorSpace::estimated_bytes)
            .unwrap_or(0);
        let dim = normalized.len();
        self.partitions[partition].vector_upsert_normalized(space, id, normalized)?;
        let next_space_bytes = self.partitions[partition]
            .vectors
            .get(space)
            .map(VectorSpace::estimated_bytes)
            .unwrap_or(0);
        self.vector_index_bytes = self
            .vector_index_bytes
            .saturating_sub(previous_space_bytes)
            .saturating_add(next_space_bytes);
        update_tenant_index_bytes(
            &mut self.tenant_vector_index_bytes,
            tenant,
            previous_space_bytes,
            next_space_bytes,
        );
        self.dims.entry(space.to_string()).or_insert(dim);
        self.spaces.insert(space.to_string());
        self.space_tenants.entry(space.to_string()).or_insert_with(|| tenant.to_string());
        if is_new {
            *self.vector_counts.entry(space.to_string()).or_default() += 1;
        }
        Ok(())
    }

    pub fn vector_remove(&mut self, space: &str, id: &str) -> bool {
        self.vector_remove_scoped(DEFAULT_INDEX_TENANT, space, id)
    }

    pub fn vector_remove_scoped(&mut self, tenant: &str, space: &str, id: &str) -> bool {
        if validate_index_tenant(tenant).is_err()
            || self.validate_space_tenant(tenant, space).is_err()
        {
            return false;
        }
        let partition = self.route(space, id);
        let previous_space_bytes = self.partitions[partition]
            .vectors
            .get(space)
            .map(VectorSpace::estimated_bytes)
            .unwrap_or(0);
        let removed = self.partitions[partition].vector_remove(space, id);
        if removed {
            let next_space_bytes = self.partitions[partition]
                .vectors
                .get(space)
                .map(VectorSpace::estimated_bytes)
                .unwrap_or(0);
            self.vector_index_bytes = self
                .vector_index_bytes
                .saturating_sub(previous_space_bytes)
                .saturating_add(next_space_bytes);
            update_tenant_index_bytes(
                &mut self.tenant_vector_index_bytes,
                tenant,
                previous_space_bytes,
                next_space_bytes,
            );
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
        self.text_index_scoped(DEFAULT_INDEX_TENANT, space, id, text)
    }

    pub fn text_index_scoped(
        &mut self,
        tenant: &str,
        space: &str,
        id: String,
        text: &str,
    ) -> Result<usize> {
        validate_index_tenant(tenant)?;
        validate_space(space)?;
        self.validate_space_tenant(tenant, space)?;
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
        let terms = parse_text_terms(&id, text)?;
        let index_bytes = estimate_text_index_bytes(&id, &terms);
        let previous_index_bytes = self.partitions[partition]
            .texts
            .get(space)
            .and_then(|texts| texts.doc_index_bytes.get(&id))
            .copied()
            .unwrap_or(0);
        let next_index_bytes =
            updated_text_index_bytes(self.text_index_bytes, previous_index_bytes, index_bytes)?;
        let next_tenant_index_bytes = updated_index_bytes(
            self.tenant_text_index_bytes.get(tenant).copied().unwrap_or(0),
            previous_index_bytes,
            index_bytes,
            MAX_TENANT_TEXT_INDEX_BYTES,
            "tenant text index memory",
        )?;
        let terms = self.partitions[partition].text_index_parsed(space, id, terms)?;
        self.text_index_bytes = next_index_bytes;
        self.tenant_text_index_bytes.insert(tenant.to_string(), next_tenant_index_bytes);
        self.spaces.insert(space.to_string());
        self.space_tenants.entry(space.to_string()).or_insert_with(|| tenant.to_string());
        if is_new {
            *self.text_counts.entry(space.to_string()).or_default() += 1;
        }
        Ok(terms)
    }

    pub fn text_remove(&mut self, space: &str, id: &str) -> bool {
        self.text_remove_scoped(DEFAULT_INDEX_TENANT, space, id)
    }

    pub fn text_remove_scoped(&mut self, tenant: &str, space: &str, id: &str) -> bool {
        if validate_index_tenant(tenant).is_err()
            || self.validate_space_tenant(tenant, space).is_err()
        {
            return false;
        }
        let partition = self.route(space, id);
        let index_bytes = self.partitions[partition]
            .texts
            .get(space)
            .and_then(|texts| texts.doc_index_bytes.get(id))
            .copied()
            .unwrap_or(0);
        let removed = self.partitions[partition].text_remove(space, id);
        if removed {
            self.text_index_bytes = self.text_index_bytes.saturating_sub(index_bytes);
            update_tenant_index_bytes(&mut self.tenant_text_index_bytes, tenant, index_bytes, 0);
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
        Ok(top_k_scored_ids(merged.into_iter().map(|hit| (hit.id, hit.score)), top_k)
            .into_iter()
            .map(|hit| TextHit { id: hit.id.into_owned(), score: hit.score })
            .collect())
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

    pub fn memory_usage(&self) -> IndexMemoryUsage {
        let mut tenants: Vec<String> = self
            .tenant_text_index_bytes
            .keys()
            .chain(self.tenant_vector_index_bytes.keys())
            .cloned()
            .collect();
        tenants.sort();
        tenants.dedup();
        let per_tenant = tenants
            .into_iter()
            .map(|tenant| TenantIndexMemoryUsage {
                full_text: IndexMemoryBudget {
                    used_bytes: self.tenant_text_index_bytes.get(&tenant).copied().unwrap_or(0),
                    limit_bytes: MAX_TENANT_TEXT_INDEX_BYTES,
                },
                vector: IndexMemoryBudget {
                    used_bytes: self.tenant_vector_index_bytes.get(&tenant).copied().unwrap_or(0),
                    limit_bytes: MAX_TENANT_VECTOR_INDEX_BYTES,
                },
                tenant,
            })
            .collect();
        IndexMemoryUsage {
            full_text: IndexMemoryBudget {
                used_bytes: self.text_index_bytes,
                limit_bytes: MAX_TEXT_INDEX_BYTES,
            },
            vector: IndexMemoryBudget {
                used_bytes: self.vector_index_bytes,
                limit_bytes: MAX_VECTOR_INDEX_BYTES,
            },
            per_tenant,
        }
    }

    fn remove_empty_space(&mut self, space: &str) {
        let still_present = self.partitions.iter().any(|partition| {
            partition.vectors.contains_key(space) || partition.texts.contains_key(space)
        });
        if !still_present {
            self.spaces.remove(space);
            self.dims.remove(space);
            self.space_tenants.remove(space);
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
    fn vector_normalization_handles_large_finite_components() {
        let mut registry = IndexRegistry::new();
        registry.vector_upsert("s", String::from("large"), vec![f32::MAX, f32::MAX]).unwrap();

        let hits = registry.vector_search("s", &[1.0, 1.0], 1).unwrap();
        assert_eq!(hits[0].id, "large");
        assert!((hits[0].score - 1.0).abs() < 1e-6);
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
    fn vector_index_memory_accounting_tracks_tombstones_and_reclaims_on_compaction() {
        let mut registry = PartitionedIndex::new(2);
        registry.vector_upsert("s", String::from("a"), vec![1.0, 0.0]).unwrap();
        registry.vector_upsert("s", String::from("b"), vec![0.0, 1.0]).unwrap();
        let two_nodes = registry.vector_index_bytes;
        assert!(two_nodes > 0);

        registry.vector_upsert("s", String::from("a"), vec![0.5, 0.5]).unwrap();
        assert!(registry.vector_index_bytes > two_nodes);

        assert!(registry.vector_remove("s", "a"));
        assert!(registry.vector_index_bytes > 0);
        assert!(registry.vector_remove("s", "b"));
        assert_eq!(registry.vector_index_bytes, 0);
    }

    #[test]
    fn vector_index_memory_budget_rejects_growth_without_mutating_indexes() {
        let mut registry = PartitionedIndex::new(2);
        let id = String::from("v");
        let growth =
            estimate_vector_node_bytes(id.len(), 2) + estimate_vector_id_map_bytes(id.len());
        registry.vector_index_bytes = MAX_VECTOR_INDEX_BYTES - growth + 1;

        assert!(matches!(
            registry.vector_upsert("vectors", id.clone(), vec![1.0, 0.0]),
            Err(RymeError::Overload(message)) if message == "vector index memory"
        ));
        assert_eq!(registry.vector_index_bytes, MAX_VECTOR_INDEX_BYTES - growth + 1);
        assert!(!registry.spaces.contains("vectors"));
        assert!(registry
            .partitions
            .iter()
            .all(|partition| !partition.vectors.contains_key("vectors")));

        registry.vector_index_bytes -= 1;
        registry.vector_upsert("vectors", id, vec![1.0, 0.0]).unwrap();
        assert_eq!(registry.vector_index_bytes, MAX_VECTOR_INDEX_BYTES);
    }

    #[test]
    fn vector_index_budget_is_tenant_scoped_and_spaces_keep_their_owner() {
        let mut registry = PartitionedIndex::new(2);
        let id = String::from("v");
        let growth =
            estimate_vector_node_bytes(id.len(), 2) + estimate_vector_id_map_bytes(id.len());
        registry
            .tenant_vector_index_bytes
            .insert(String::from("tenant-a"), MAX_TENANT_VECTOR_INDEX_BYTES - growth + 1);
        assert!(matches!(
            registry.vector_upsert_scoped("tenant-a", "space-a", id.clone(), vec![1.0, 0.0]),
            Err(RymeError::Overload(message)) if message == "tenant vector index memory"
        ));
        assert!(!registry.spaces.contains("space-a"));

        registry.tenant_vector_index_bytes.remove("tenant-a");
        registry.vector_upsert_scoped("tenant-a", "space-a", id.clone(), vec![1.0, 0.0]).unwrap();
        assert_eq!(registry.tenant_vector_index_bytes.get("tenant-a"), Some(&growth));
        assert!(matches!(
            registry.vector_upsert_scoped("tenant-b", "space-a", id.clone(), vec![0.0, 1.0]),
            Err(RymeError::Forbidden)
        ));
        assert!(!registry.vector_remove_scoped("tenant-b", "space-a", &id));
        assert!(registry.vector_remove_scoped("tenant-a", "space-a", &id));
        assert!(!registry.tenant_vector_index_bytes.contains_key("tenant-a"));
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
    fn hnsw_uses_and_preserves_wider_graph_for_embedding_dimensions() {
        let mut low_dim = VectorSpace::default();
        low_dim.upsert(String::from("v"), vec![1.0; HNSW_HIGH_DIM_THRESHOLD - 1]).unwrap();
        assert_eq!(low_dim.ann.m, HNSW_M);

        let mut high_dim = VectorSpace::default();
        for index in 0..65 {
            let mut vector = vec![0.0; HNSW_HIGH_DIM_THRESHOLD];
            vector[index % HNSW_HIGH_DIM_THRESHOLD] = 1.0;
            high_dim.upsert(format!("v{index}"), vector).unwrap();
        }
        assert_eq!(high_dim.ann.m, HNSW_HIGH_DIM_M);

        for revision in 0..65 {
            let mut vector = vec![0.0; HNSW_HIGH_DIM_THRESHOLD];
            vector[revision % HNSW_HIGH_DIM_THRESHOLD] = 1.0;
            high_dim.upsert(String::from("v0"), vector).unwrap();
        }
        assert_eq!(high_dim.ann.m, HNSW_HIGH_DIM_M);
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
    fn text_top_k_ties_are_stable_and_bounded() {
        let mut registry = IndexRegistry::new();
        for id in ["z", "a", "m", "b"] {
            registry.text_index("docs", id.to_string(), "same score").unwrap();
        }
        let hits = registry.text_search("docs", "same", 2).unwrap();
        assert_eq!(hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    }

    #[test]
    fn single_term_fast_path_preserves_frequency_scores() {
        let mut registry = IndexRegistry::new();
        registry.text_index("docs", String::from("repeated"), "needle needle").unwrap();
        registry.text_index("docs", String::from("once"), "needle").unwrap();
        let hits = registry.text_search("docs", "needle", 2).unwrap();
        let idf = (1.0f64 + 2.0 / 2.5).ln();
        assert_eq!(hits[0].id, "repeated");
        assert!((hits[0].score - idf * (2.0 / 3.2)).abs() < 1e-12);
        assert!((hits[1].score - idf * (1.0 / 2.2)).abs() < 1e-12);
        let repeated_query = registry.text_search("docs", "needle needle", 2).unwrap();
        assert!((repeated_query[0].score - 2.0 * hits[0].score).abs() < 1e-12);
    }

    #[test]
    fn tokenizer_lowercases_unicode_letters() {
        assert_eq!(tokenize("RÉSUMÉ"), tokenize("résumé"));
    }

    #[test]
    fn text_index_streams_repeated_terms_and_enforces_distinct_term_limit() {
        let mut registry = IndexRegistry::new();
        let repeated = "needle ".repeat(MAX_TERMS_PER_DOCUMENT + 1);
        assert_eq!(registry.text_index("docs", String::from("existing"), &repeated).unwrap(), 1);

        let mut too_many_distinct = String::new();
        for term in 0..=MAX_TERMS_PER_DOCUMENT {
            if term > 0 {
                too_many_distinct.push(' ');
            }
            too_many_distinct.push('x');
            too_many_distinct.push_str(&term.to_string());
        }
        assert!(matches!(
            registry.text_index("docs", String::from("existing"), &too_many_distinct),
            Err(RymeError::Overload(message)) if message == "text terms"
        ));
        assert_eq!(registry.text_search("docs", "needle", 1).unwrap()[0].id, "existing");
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
    fn text_index_memory_accounting_reclaims_replaced_and_removed_documents() {
        let mut registry = PartitionedIndex::new(4);
        registry.text_index("docs", String::from("d1"), "alpha beta gamma").unwrap();
        let original_bytes = registry.text_index_bytes;
        assert!(original_bytes > 0);

        registry.text_index("docs", String::from("d1"), "alpha").unwrap();
        assert!(registry.text_index_bytes < original_bytes);

        assert!(registry.text_remove("docs", "d1"));
        assert_eq!(registry.text_index_bytes, 0);
    }

    #[test]
    fn text_index_memory_budget_rejects_growth_without_mutating_indexes() {
        let mut registry = PartitionedIndex::new(2);
        let id = String::from("doc");
        let terms = parse_text_terms(&id, "alpha beta").unwrap();
        let cost = estimate_text_index_bytes(&id, &terms);
        registry.text_index_bytes = MAX_TEXT_INDEX_BYTES - cost + 1;

        assert!(matches!(
            registry.text_index("docs", id.clone(), "alpha beta"),
            Err(RymeError::Overload(message)) if message == "text index memory"
        ));
        assert_eq!(registry.text_index_bytes, MAX_TEXT_INDEX_BYTES - cost + 1);
        assert!(!registry.spaces.contains("docs"));
        assert!(registry.partitions.iter().all(|partition| !partition.texts.contains_key("docs")));

        registry.text_index_bytes -= 1;
        registry.text_index("docs", id, "alpha beta").unwrap();
        assert_eq!(registry.text_index_bytes, MAX_TEXT_INDEX_BYTES);
    }

    #[test]
    fn text_index_budget_is_tenant_scoped_and_reclaimed_on_delete() {
        let mut registry = PartitionedIndex::new(2);
        let id = String::from("doc");
        let terms = parse_text_terms(&id, "alpha beta").unwrap();
        let cost = estimate_text_index_bytes(&id, &terms);
        registry
            .tenant_text_index_bytes
            .insert(String::from("tenant-a"), MAX_TENANT_TEXT_INDEX_BYTES - cost + 1);
        assert!(matches!(
            registry.text_index_scoped("tenant-a", "space-a", id.clone(), "alpha beta"),
            Err(RymeError::Overload(message)) if message == "tenant text index memory"
        ));
        assert!(!registry.spaces.contains("space-a"));

        registry.tenant_text_index_bytes.remove("tenant-a");
        registry.text_index_scoped("tenant-a", "space-a", id.clone(), "alpha beta").unwrap();
        assert_eq!(registry.tenant_text_index_bytes.get("tenant-a"), Some(&cost));
        assert!(registry.text_remove_scoped("tenant-a", "space-a", &id));
        assert!(!registry.tenant_text_index_bytes.contains_key("tenant-a"));
    }

    #[test]
    fn index_memory_usage_reports_global_and_sorted_tenant_budgets() {
        let mut registry = PartitionedIndex::new(2);
        registry
            .vector_upsert_scoped("tenant-z", "space-z", String::from("v"), vec![1.0, 0.0])
            .unwrap();
        registry.text_index_scoped("tenant-a", "space-a", String::from("d"), "alpha beta").unwrap();

        let usage = registry.memory_usage();
        assert_eq!(usage.full_text.used_bytes, registry.text_index_bytes);
        assert_eq!(usage.full_text.limit_bytes, MAX_TEXT_INDEX_BYTES);
        assert_eq!(usage.vector.used_bytes, registry.vector_index_bytes);
        assert_eq!(usage.vector.limit_bytes, MAX_VECTOR_INDEX_BYTES);
        assert_eq!(
            usage.per_tenant.iter().map(|entry| entry.tenant.as_str()).collect::<Vec<_>>(),
            ["tenant-a", "tenant-z"]
        );
        assert!(registry.text_remove_scoped("tenant-a", "space-a", "d"));
        assert!(registry.vector_remove_scoped("tenant-z", "space-z", "v"));
        assert!(registry.memory_usage().per_tenant.is_empty());
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
