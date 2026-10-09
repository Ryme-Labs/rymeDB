use ryme_error::{Result, RymeError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportReport {
    pub table: String,
    pub rows: usize,
    pub bytes: u64,
    pub skipped: usize,
    pub oversized: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverPlan {
    pub ready: bool,
    pub reason: String,
    pub snapshot_rows: u64,
    pub cdc_lag_ms: u64,
}

pub fn parse_copy_text(table: &str, text: &str) -> Result<(Vec<ImportRow>, ImportReport)> {
    if table.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("table")));
    }
    let mut rows = Vec::new();
    let mut skipped = 0usize;
    let mut oversized = 0usize;
    let mut bytes = 0u64;
    for line in text.lines() {
        if line.is_empty() || line.starts_with("\\.") {
            continue;
        }
        let Some((key, value)) = split_copy_line(line) else {
            skipped += 1;
            continue;
        };
        if key.is_empty() || key.len() > 1024 || value.len() > 4 * 1024 * 1024 {
            oversized += 1;
            continue;
        }
        bytes = bytes.saturating_add((key.len() + value.len()) as u64);
        rows.push(ImportRow { key: key.into_bytes(), value: value.into_bytes() });
        if rows.len() > 100000 {
            break;
        }
    }
    let report =
        ImportReport { table: table.to_string(), rows: rows.len(), bytes, skipped, oversized };
    Ok((rows, report))
}

fn split_copy_line(line: &str) -> Option<(String, String)> {
    let mut parts = line.splitn(2, '\t');
    let key = parts.next()?.trim().to_string();
    let value = parts.next().unwrap_or("").to_string();
    let key = unescape_copy(&key);
    let value = unescape_copy(&value);
    Some((key, value))
}

fn unescape_copy(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

pub fn parse_insert_line(line: &str) -> Option<(String, ImportRow)> {
    let upper = line.to_ascii_uppercase();
    let pos = upper.find("INSERT INTO")?;
    let rest = line[pos + "INSERT INTO".len()..].trim();
    let mut table = String::new();
    for ch in rest.chars() {
        if ch.is_alphanumeric() || ch == '_' || ch == '"' {
            if ch != '"' {
                table.push(ch);
            }
        } else {
            break;
        }
    }
    if table.is_empty() {
        return None;
    }
    let values_pos = upper.find("VALUES")?;
    let values = line[values_pos + "VALUES".len()..].trim();
    let mut quoted: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = values.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if in_quote && chars.peek() == Some(&'\'') {
                current.push('\'');
                chars.next();
            } else {
                in_quote = !in_quote;
                if !in_quote {
                    quoted.push(std::mem::take(&mut current));
                }
            }
        } else if in_quote {
            current.push(ch);
        }
    }
    if quoted.len() < 2 {
        return None;
    }
    Some((
        table,
        ImportRow { key: quoted[0].clone().into_bytes(), value: quoted[1].clone().into_bytes() },
    ))
}

pub fn validate_rows(rows: &[ImportRow]) -> ImportReport {
    let mut bytes = 0u64;
    let mut oversized = 0usize;
    for row in rows {
        if row.key.is_empty() || row.key.len() > 1024 || row.value.len() > 4 * 1024 * 1024 {
            oversized += 1;
        } else {
            bytes = bytes.saturating_add((row.key.len() + row.value.len()) as u64);
        }
    }
    ImportReport { table: String::from("batch"), rows: rows.len(), bytes, skipped: 0, oversized }
}

pub fn plan_cutover(snapshot_rows: u64, cdc_lag_ms: u64) -> CutoverPlan {
    if cdc_lag_ms <= 1000 {
        CutoverPlan {
            ready: true,
            reason: String::from("lag_within_window"),
            snapshot_rows,
            cdc_lag_ms,
        }
    } else {
        CutoverPlan {
            ready: false,
            reason: String::from("cdc_lag_too_high"),
            snapshot_rows,
            cdc_lag_ms,
        }
    }
}

pub fn chunk_rows(rows: Vec<ImportRow>, size: usize) -> Vec<Vec<ImportRow>> {
    let size = size.clamp(1, 500);
    let mut out = Vec::new();
    let mut current = Vec::new();
    for row in rows {
        current.push(row);
        if current.len() >= size {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupabasePolicy {
    pub name: String,
    pub table: String,
    pub command: String,
    pub expression: String,
    pub tenant_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupabaseSchema {
    pub tables: Vec<String>,
    pub policies: Vec<SupabasePolicy>,
}

pub fn parse_supabase_dump(text: &str) -> SupabaseSchema {
    let mut tables: Vec<String> = Vec::new();
    let mut policies: Vec<SupabasePolicy> = Vec::new();
    for chunk in split_sql_statements(text) {
        let line = chunk.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let upper = line.to_ascii_uppercase();
        if upper.starts_with("CREATE TABLE") {
            if let Some(name) = identifier_after(&line, "CREATE TABLE") {
                if !tables.contains(&name) {
                    tables.push(name);
                }
            }
            continue;
        }
        if upper.starts_with("CREATE POLICY") {
            if let Some(policy) = parse_policy_line(&line) {
                policies.push(policy);
            }
        }
    }
    SupabaseSchema { tables, policies }
}

fn split_sql_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote: Option<u8> = None;
    let mut dollar_quote: Option<Vec<u8>> = None;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if line_comment {
            if byte == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if byte == b'*' && bytes.get(index + 1) == Some(&b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(tag) = dollar_quote.as_ref() {
            if bytes[index..].starts_with(tag) {
                index += tag.len();
                dollar_quote = None;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(open) = quote {
            if byte == open {
                if bytes.get(index + 1) == Some(&open) {
                    index += 1;
                } else {
                    quote = None;
                }
            }
            index += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
        } else if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            line_comment = true;
            index += 2;
            continue;
        } else if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            block_comment = true;
            index += 2;
            continue;
        } else if byte == b'$' {
            if let Some(end) = dollar_quote_end(bytes, index) {
                dollar_quote = Some(bytes[index..end].to_vec());
                index = end;
                continue;
            }
        } else if byte == b';' {
            parts.push(&sql[start..index]);
            start = index + 1;
        }
        index += 1;
    }
    parts.push(&sql[start..]);
    parts
}

fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some(index + 1);
    }
    if !bytes.get(index).is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_') {
        return None;
    }
    index += 1;
    while bytes.get(index).is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_') {
        index += 1;
    }
    (bytes.get(index) == Some(&b'$')).then_some(index + 1)
}

fn identifier_after(line: &str, keyword: &str) -> Option<String> {
    let pos = line.to_ascii_uppercase().find(&keyword.to_ascii_uppercase())?;
    let rest = line[pos + keyword.len()..].trim_start();
    let rest = rest
        .strip_prefix("IF NOT EXISTS")
        .or_else(|| rest.strip_prefix("if not exists"))
        .unwrap_or(rest)
        .trim_start();
    let mut name = String::new();
    for ch in rest.chars() {
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            name.push(ch);
        } else {
            break;
        }
    }
    if name.is_empty() {
        return None;
    }
    Some(name.rsplit('.').next().unwrap_or(&name).trim_matches('"').to_string())
}

fn parse_policy_line(line: &str) -> Option<SupabasePolicy> {
    let upper = line.to_ascii_uppercase();
    let on_pos = upper.find(" ON ")?;
    let name_part = line["CREATE POLICY".len()..on_pos].trim();
    let name = name_part.trim_matches('"').to_string();
    let after_on = line[on_pos + 4..].trim();
    let mut table_iter = after_on.split_whitespace();
    let raw_table = table_iter.next()?.trim_matches('"').trim_matches(';').to_string();
    let table = raw_table.rsplit('.').next().unwrap_or(&raw_table).trim_matches('"').to_string();
    let rest_upper = after_on.to_ascii_uppercase();
    let command = ["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"]
        .iter()
        .find(|verb| rest_upper.contains(&format!("FOR {verb}")))
        .unwrap_or(&"ALL")
        .to_string();
    let expression = extract_expression(after_on);
    let tenant_column =
        expression.as_ref().and_then(|expr| detect_tenant_column(expr)).map(|s| s.to_string());
    if name.is_empty() || table.is_empty() {
        return None;
    }
    Some(SupabasePolicy {
        name,
        table,
        command,
        expression: expression.unwrap_or_default(),
        tenant_column,
    })
}

fn extract_expression(after_on: &str) -> Option<String> {
    let upper = after_on.to_ascii_uppercase();
    let using = upper.find("USING")?;
    let mut depth = 0usize;
    let bytes: Vec<char> = after_on.chars().collect();
    let mut index = using + "USING".len();
    while index < bytes.len() && bytes[index].is_whitespace() {
        index += 1;
    }
    if bytes.get(index) != Some(&'(') {
        return Some(after_on[index..].trim().trim_end_matches(';').trim().to_string());
    }
    let start = index;
    while index < bytes.len() {
        match bytes[index] {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let expr: String = bytes[start..=index].iter().collect();
                    return Some(expr);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn detect_tenant_column(expression: &str) -> Option<String> {
    let lower = expression.to_ascii_lowercase();
    if !lower.contains("auth.uid()") {
        return None;
    }
    for side in expression.split('=').map(|s| s.trim()) {
        let candidate = side
            .trim_matches(|c| c == '(' || c == ')' || c == ' ' || c == '"' || c == '\'')
            .trim()
            .to_string();
        if candidate.is_empty()
            || candidate.to_ascii_lowercase().contains("auth.uid")
            || candidate.contains(' ')
        {
            continue;
        }
        if candidate.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            return Some(candidate.rsplit('.').next().unwrap_or(&candidate).to_string());
        }
    }
    None
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeonBranch {
    pub name: String,
    pub parent: Option<String>,
    pub lsn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeonBranchPlan {
    pub id: String,
    pub parent: String,
    pub base_commit_ts: u64,
}

pub fn parse_neon_branches(text: &str) -> Result<Vec<NeonBranchPlan>> {
    let branches: Vec<NeonBranch> =
        serde_json::from_str(text).map_err(|e| RymeError::InvalidArgument(e.to_string()))?;
    let mut out = Vec::new();
    for branch in branches {
        if branch.name.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("neon branch")));
        }
        out.push(NeonBranchPlan {
            id: branch.name.clone(),
            parent: branch.parent.unwrap_or_else(|| String::from("main")),
            base_commit_ts: parse_lsn(&branch.lsn)?,
        });
    }
    Ok(out)
}

fn parse_lsn(lsn: &str) -> Result<u64> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return Err(RymeError::InvalidArgument(String::from("neon lsn")));
    }
    let high = u64::from_str_radix(parts[0], 16)
        .map_err(|_| RymeError::InvalidArgument(String::from("neon lsn")))?;
    let low = u64::from_str_radix(parts[1], 16)
        .map_err(|_| RymeError::InvalidArgument(String::from("neon lsn")))?;
    Ok((high << 32) | low)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LedgerEntry {
    pub migration_id: String,
    pub checksum: String,
    pub parent_checksum: String,
    pub applied_unix: u64,
    pub result_schema_version: u64,
    pub author: String,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Ledger {
    #[serde(default)]
    entries: Vec<LedgerEntry>,
}

impl Ledger {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn head_checksum(&self) -> String {
        self.entries.last().map(|e| e.checksum.clone()).unwrap_or_default()
    }

    pub fn schema_version(&self) -> u64 {
        self.entries.last().map(|e| e.result_schema_version).unwrap_or(0)
    }

    pub fn apply(
        &mut self,
        migration_id: String,
        sql: &str,
        author: String,
        applied_unix: u64,
    ) -> Result<LedgerEntry> {
        if migration_id.is_empty() || sql.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("migration")));
        }
        if self.entries.iter().any(|e| e.migration_id == migration_id) {
            return Err(RymeError::Conflict(String::from("migration")));
        }
        let parent = self.head_checksum();
        let checksum = checksum_of(&parent, &migration_id, sql);
        let entry = LedgerEntry {
            migration_id,
            checksum,
            parent_checksum: parent,
            applied_unix,
            result_schema_version: self.schema_version() + 1,
            author,
        };
        self.entries.push(entry.clone());
        Ok(entry)
    }

    pub fn verify(&self) -> Result<()> {
        let mut parent = String::new();
        let mut version = 0u64;
        for entry in &self.entries {
            if entry.parent_checksum != parent {
                return Err(RymeError::Corrupt(String::from("ledger chain")));
            }
            version += 1;
            if entry.result_schema_version != version {
                return Err(RymeError::Corrupt(String::from("ledger version")));
            }
            parent = entry.checksum.clone();
        }
        Ok(())
    }

    pub fn entries(&self) -> &[LedgerEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn from_entries(entries: Vec<LedgerEntry>) -> Result<Self> {
        let ledger = Self { entries };
        ledger.verify()?;
        Ok(ledger)
    }
}

fn checksum_of(parent: &str, migration_id: &str, sql: &str) -> String {
    let mut state = 0xcbf29ce484222325u64;
    for byte in parent.bytes().chain(migration_id.bytes()).chain(sql.bytes()) {
        state ^= byte as u64;
        state = state.wrapping_mul(0x100000001b3);
    }
    format!("{state:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_parses_tabs() {
        let (rows, report) = parse_copy_text("docs", "k1\tv1\nk2\tv2\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(report.rows, 2);
        assert_eq!(report.skipped, 0);
    }

    #[test]
    fn insert_parses_values() {
        let parsed = parse_insert_line("INSERT INTO docs VALUES ('k1', 'v1');").unwrap();
        assert_eq!(parsed.0, "docs");
        assert_eq!(parsed.1.key, b"k1".to_vec());
    }

    #[test]
    fn cutover_gates_on_lag() {
        assert!(plan_cutover(100, 200).ready);
        assert!(!plan_cutover(100, 5000).ready);
    }

    #[test]
    fn chunks_bound_size() {
        let rows: Vec<ImportRow> =
            (0..5).map(|i| ImportRow { key: vec![i], value: vec![i] }).collect();
        let chunks = chunk_rows(rows, 2);
        assert_eq!(chunks.len(), 3);
    }

    #[test]
    fn supabase_dump_extracts_tables_and_policies() {
        let dump = "CREATE TABLE public.messages (id uuid, user_id uuid);\nCREATE POLICY \"own rows\" ON public.messages FOR SELECT USING (auth.uid() = user_id);\nCREATE POLICY \"insert own\" ON messages FOR INSERT WITH CHECK (auth.uid() = user_id);";
        let schema = parse_supabase_dump(dump);
        assert_eq!(schema.tables, vec![String::from("messages")]);
        assert_eq!(schema.policies.len(), 2);
        let policy = &schema.policies[0];
        assert_eq!(policy.table, "messages");
        assert_eq!(policy.command, "SELECT");
        assert_eq!(policy.tenant_column, Some(String::from("user_id")));
        assert_eq!(schema.policies[1].command, "INSERT");
        assert_eq!(schema.policies[1].tenant_column, None);
    }

    #[test]
    fn supabase_dump_keeps_dollar_quoted_function_bodies_together() {
        let dump = "CREATE FUNCTION touch_row() RETURNS trigger AS $$ BEGIN PERFORM 1; RETURN NEW; END; $$ LANGUAGE plpgsql; CREATE TABLE public.messages (id uuid); CREATE POLICY own ON public.messages FOR SELECT USING (auth.uid() = id);";
        let schema = parse_supabase_dump(dump);
        assert_eq!(schema.tables, vec![String::from("messages")]);
        assert_eq!(schema.policies.len(), 1);
        assert_eq!(schema.policies[0].name, "own");
    }

    #[test]
    fn neon_branches_map_lsn() {
        let plans = parse_neon_branches(
            r#"[{"name":"preview-1","parent":"main","lsn":"0/16B4C50"},{"name":"main","parent":null,"lsn":"0/1000000"}]"#,
        )
        .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].id, "preview-1");
        assert_eq!(plans[0].parent, "main");
        assert_eq!(plans[0].base_commit_ts, 0x16B4C50);
        assert_eq!(plans[1].parent, "main");
        assert!(parse_neon_branches(r#"[{"name":"","parent":null,"lsn":"0/1"}]"#).is_err());
        assert!(parse_neon_branches(r#"[{"name":"x","parent":null,"lsn":"bogus"}]"#).is_err());
    }

    #[test]
    fn ledger_chains_and_verifies() {
        let mut ledger = Ledger::new();
        let first = ledger
            .apply(String::from("0001-init"), "CREATE TABLE docs", String::from("ada"), 1000)
            .unwrap();
        assert_eq!(first.result_schema_version, 1);
        let second = ledger
            .apply(
                String::from("0002-add"),
                "UPSERT INTO docs KEY 'a' VALUE 'b'",
                String::from("ada"),
                1001,
            )
            .unwrap();
        assert_eq!(second.parent_checksum, first.checksum);
        assert!(ledger.verify().is_ok());
        assert!(ledger
            .apply(String::from("0001-init"), "CREATE TABLE docs", String::from("ada"), 1002)
            .is_err());
    }

    #[test]
    fn ledger_rejects_empty() {
        let mut ledger = Ledger::new();
        assert!(ledger.apply(String::from(""), "SELECT 1", String::from("ada"), 1).is_err());
    }
}
