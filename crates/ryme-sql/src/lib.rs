use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{Isolation, Transaction, TxnBackend, TxnManager};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    CreateTable {
        table: String,
        columns: Vec<ColumnDefinition>,
    },
    CreateIndex {
        name: String,
        table: String,
        field: Field,
        #[serde(default)]
        column: Option<String>,
        unique: bool,
    },
    Insert {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
    },
    InsertRow {
        table: String,
        columns: Vec<String>,
        values: Vec<InsertValue>,
        upsert: bool,
    },
    InsertRows {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
    },
    Upsert {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
    },
    SelectByKey {
        table: String,
        pk: Vec<u8>,
    },
    SelectScan {
        table: String,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    },
    SelectColumns {
        table: String,
        columns: Vec<String>,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    },
    Aggregate {
        table: String,
        func: AggFunc,
        field: Field,
        filter: Vec<Predicate>,
    },
    Join {
        left: String,
        right: String,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    },
    GroupBy {
        table: String,
        select: Vec<SelectItem>,
        group: Field,
        filter: Vec<Predicate>,
        limit: usize,
        offset: usize,
        order: Order,
    },
    Update {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
    },
    UpdateRow {
        table: String,
        pk: Vec<u8>,
        assignments: Vec<(String, InsertValue)>,
    },
    Delete {
        table: String,
        pk: Vec<u8>,
    },
    CopyFrom {
        table: String,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Returning {
        statement: Box<Statement>,
        fields: Vec<ReturningField>,
    },
    Explain {
        plan: String,
        inner: Box<Statement>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertValue {
    Value(Vec<u8>),
    Default,
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub column_default: Option<String>,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    pub field: Field,
    #[serde(default)]
    pub column: Option<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    #[serde(default)]
    pub tables: BTreeMap<String, Vec<ColumnDefinition>>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
}

pub fn persist_schema_snapshot(path: &Path, snapshot: &SchemaSnapshot) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec(snapshot)
        .map_err(|error| RymeError::Internal(format!("schema snapshot: {error}")))?;
    let temporary = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(temporary, path)?;
    Ok(())
}

impl Statement {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Statement::Insert { .. }
                | Statement::InsertRow { .. }
                | Statement::InsertRows { .. }
                | Statement::Upsert { .. }
                | Statement::Update { .. }
                | Statement::UpdateRow { .. }
                | Statement::Delete { .. }
                | Statement::CopyFrom { .. }
                | Statement::Returning { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryResult {
    Ok,
    Row { pk: Vec<u8>, value: Vec<u8> },
    Rows { rows: Vec<(Vec<u8>, Vec<u8>)> },
    Scalar { label: String, value: Vec<u8> },
    Returning { columns: Vec<String>, rows: Vec<Vec<Vec<u8>>> },
    Table { columns: Vec<String>, rows: Vec<Vec<Vec<u8>>> },
}

pub type Row = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionChange {
    pub table: String,
    pub pk: Vec<u8>,
    pub op: Operation,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Field {
    Key,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReturningField {
    Key,
    Value,
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmp {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    IsNull,
    IsNotNull,
    Contains,
    Like,
    ILike,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Predicate {
    pub field: Field,
    #[serde(default)]
    pub column: Option<String>,
    pub op: Cmp,
    pub operand: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        if token.eq_ignore_ascii_case("COUNT") {
            Some(Self::Count)
        } else if token.eq_ignore_ascii_case("SUM") {
            Some(Self::Sum)
        } else if token.eq_ignore_ascii_case("AVG") {
            Some(Self::Avg)
        } else if token.eq_ignore_ascii_case("MIN") {
            Some(Self::Min)
        } else if token.eq_ignore_ascii_case("MAX") {
            Some(Self::Max)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectItem {
    Field(Field),
    Agg(AggFunc, Field),
}

impl SelectItem {
    pub fn label(&self) -> String {
        match self {
            Self::Field(Field::Key) => String::from("key"),
            Self::Field(Field::Value) => String::from("value"),
            Self::Agg(func, _) => func.label().to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub field: Field,
    pub direction: Direction,
}

impl Default for Order {
    fn default() -> Self {
        Self { field: Field::Key, direction: Direction::Asc }
    }
}

impl Predicate {
    pub fn matches(&self, pk: &[u8], value: &[u8]) -> bool {
        if matches!(self.op, Cmp::IsNull | Cmp::IsNotNull) {
            let is_null = if let Some(column) = self.column.as_deref() {
                let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
                    return false;
                };
                json_column_value(column, &object).is_some_and(serde_json::Value::is_null)
            } else {
                match self.field {
                    Field::Key => pk.is_empty() || pk.eq_ignore_ascii_case(b"null"),
                    Field::Value => value.is_empty() || value.eq_ignore_ascii_case(b"null"),
                }
            };
            return if self.op == Cmp::IsNull { is_null } else { !is_null };
        }
        let column_value;
        let target = if let Some(column) = self.column.as_deref() {
            let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
                return false;
            };
            let selected = json_column_value(column, &object).map(json_result_bytes);
            let Some(selected) = selected else { return false };
            column_value = selected;
            column_value.as_slice()
        } else {
            match self.field {
                Field::Key => pk,
                Field::Value => value,
            }
        };
        match self.op {
            Cmp::Eq => target == self.operand.as_slice(),
            Cmp::NotEq => target != self.operand.as_slice(),
            Cmp::Gt => compare_operands(target, &self.operand).is_some_and(|order| order.is_gt()),
            Cmp::Gte => compare_operands(target, &self.operand).is_some_and(|order| !order.is_lt()),
            Cmp::Lt => compare_operands(target, &self.operand).is_some_and(|order| order.is_lt()),
            Cmp::Lte => compare_operands(target, &self.operand).is_some_and(|order| !order.is_gt()),
            Cmp::IsNull | Cmp::IsNotNull => false,
            Cmp::Contains => {
                let text = String::from_utf8_lossy(target);
                let want = String::from_utf8_lossy(&self.operand);
                text.contains(want.as_ref())
            }
            Cmp::Like | Cmp::ILike => {
                let text = String::from_utf8_lossy(target);
                let pattern = String::from_utf8_lossy(&self.operand);
                if self.op == Cmp::ILike {
                    sql_like(&text.to_lowercase(), &pattern.to_lowercase())
                } else {
                    sql_like(&text, &pattern)
                }
            }
        }
    }
}

fn sql_like(value: &str, pattern: &str) -> bool {
    let value: Vec<char> = value.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut matches = vec![false; value.len() + 1];
    matches[0] = true;
    for token in pattern {
        if token == '%' {
            for index in 1..=value.len() {
                matches[index] = matches[index] || matches[index - 1];
            }
        } else {
            let mut next = vec![false; matches.len()];
            for index in 1..next.len() {
                if matches[index - 1]
                    && (token == '_' || value.get(index - 1).is_some_and(|value| *value == token))
                {
                    next[index] = true;
                }
            }
            matches = next;
        }
    }
    matches[value.len()]
}

fn compare_operands(left: &[u8], right: &[u8]) -> Option<std::cmp::Ordering> {
    let left_text = std::str::from_utf8(left).ok()?.trim();
    let right_text = std::str::from_utf8(right).ok()?.trim();
    match (left_text.parse::<f64>(), right_text.parse::<f64>()) {
        (Ok(left), Ok(right)) => left.partial_cmp(&right),
        _ => Some(left_text.as_bytes().cmp(right_text.as_bytes())),
    }
}

impl Statement {
    pub fn table(&self) -> &str {
        match self {
            Self::CreateTable { table, .. }
            | Self::CreateIndex { table, .. }
            | Self::Insert { table, .. }
            | Self::InsertRow { table, .. }
            | Self::InsertRows { table, .. }
            | Self::Upsert { table, .. }
            | Self::SelectByKey { table, .. }
            | Self::SelectScan { table, .. }
            | Self::SelectColumns { table, .. }
            | Self::Aggregate { table, .. }
            | Self::Join { left: table, .. }
            | Self::GroupBy { table, .. }
            | Self::Update { table, .. }
            | Self::UpdateRow { table, .. }
            | Self::Delete { table, .. }
            | Self::CopyFrom { table, .. } => table,
            Self::Returning { statement, .. } => statement.table(),
            Self::Explain { inner, .. } => inner.table(),
        }
    }
}

pub fn parse(input: &str) -> Result<Statement> {
    let tokens = tokenize(input);
    if tokens.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("empty statement")));
    }
    let head = tokens[0].to_ascii_uppercase();
    let statement = match head.as_str() {
        "CREATE" => parse_create(&tokens, input),
        "UPSERT" => parse_upsert(&tokens),
        "INSERT" => parse_insert(&tokens, input),
        "SELECT" => parse_select(&tokens),
        "UPDATE" => parse_update(&tokens, input),
        "DELETE" => parse_delete(&tokens),
        "COPY" => parse_copy(&tokens),
        "EXPLAIN" => parse_explain(input),
        _ => Err(RymeError::InvalidArgument(String::from("unknown statement"))),
    }?;
    if tokens.iter().any(|token| token.eq_ignore_ascii_case("RETURNING")) {
        let fields = parse_returning_fields(&tokens)?;
        if statement.is_write() && !matches!(statement, Statement::CopyFrom { .. }) {
            return Ok(Statement::Returning { statement: Box::new(statement), fields });
        }
        return Err(RymeError::InvalidArgument(String::from("returning statement")));
    }
    Ok(statement)
}

fn parse_returning_fields(tokens: &[String]) -> Result<Vec<ReturningField>> {
    let start = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("RETURNING"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("returning fields")))?;
    let mut fields = Vec::new();
    for token in &tokens[start + 1..] {
        if token == "*" {
            fields.extend([ReturningField::Key, ReturningField::Value]);
        } else if let Some(field) = parse_field(token) {
            fields.push(match field {
                Field::Key => ReturningField::Key,
                Field::Value => ReturningField::Value,
            });
        } else {
            fields.push(ReturningField::Column(unquote(token)));
        }
    }
    if fields.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("returning fields")));
    }
    Ok(fields)
}

pub fn bind(sql: &str, params: &[String]) -> String {
    let mut out = sql.to_string();
    for (index, value) in params.iter().enumerate() {
        let placeholder = format!("${}", index + 1);
        let literal = format!("'{}'", value.replace('\'', "''"));
        out = out.replace(&placeholder, &literal);
    }
    out
}

fn tokenize(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted: Option<char> = None;
    for ch in input.chars() {
        if let Some(q) = quoted {
            current.push(ch);
            if ch == q {
                quoted = None;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quoted = Some(ch);
            current.push(ch);
            continue;
        }
        if ch.is_whitespace() || ch == ',' || ch == ';' || ch == '(' || ch == ')' {
            if !current.is_empty() {
                out.push(current.clone());
                current.clear();
            }
            continue;
        }
        if matches!(ch, '=' | '<' | '>') {
            if ch == '>' && (current.ends_with('-') || current.ends_with("->")) {
                current.push(ch);
                continue;
            }
            if !current.is_empty() {
                out.push(current.clone());
                current.clear();
            }
            if let Some(previous) = out.last_mut() {
                if (ch == '=' && previous == "!")
                    || ((ch == '=' || ch == '>') && previous == "<")
                    || (ch == '=' && previous == ">")
                {
                    previous.push(ch);
                    continue;
                }
            }
            if ch != '=' {
                out.push(ch.to_string());
            }
            continue;
        }
        current.push(ch);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn unquote(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn parse_create(tokens: &[String], raw: &str) -> Result<Statement> {
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("INDEX"))
        || (tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("UNIQUE"))
            && tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("INDEX")))
    {
        return parse_create_index(tokens);
    }
    let table_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("TABLE"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create table")))?;
    let mut name_index = table_index + 1;
    if tokens.get(name_index).is_some_and(|token| token.eq_ignore_ascii_case("IF")) {
        if !tokens.get(name_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            || !tokens.get(name_index + 2).is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"))
        {
            return Err(RymeError::InvalidArgument(String::from("create table")));
        }
        name_index += 3;
    }
    let table = tokens
        .get(name_index)
        .map(|value| unquote(value))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create table")))?;
    Ok(Statement::CreateTable { table, columns: parse_column_definitions(raw)? })
}

fn parse_create_index(tokens: &[String]) -> Result<Statement> {
    let unique = tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("UNIQUE"));
    let index_pos = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("INDEX"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create index")))?;
    let mut name_pos = index_pos + 1;
    if tokens.get(name_pos).is_some_and(|token| token.eq_ignore_ascii_case("IF")) {
        if !tokens.get(name_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            || !tokens.get(name_pos + 2).is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"))
        {
            return Err(RymeError::InvalidArgument(String::from("index name")));
        }
        name_pos += 3;
    }
    let name = tokens
        .get(name_pos)
        .map(|token| unquote(token))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index name")))?;
    let table = table_after(tokens, "ON")?;
    let on_pos = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index table")))?;
    let field_token = tokens
        .get(on_pos + 2)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index field")))?;
    let (field, column) = parse_field(field_token)
        .map(|field| (field, None))
        .unwrap_or_else(|| (Field::Value, Some(unquote(field_token))));
    Ok(Statement::CreateIndex { name, table, field, column, unique })
}

fn parse_column_definitions(raw: &str) -> Result<Vec<ColumnDefinition>> {
    let Some(open) = raw.find('(') else { return Ok(Vec::new()) };
    let Some(close) = raw.rfind(')') else { return Ok(Vec::new()) };
    if close <= open {
        return Ok(Vec::new());
    }
    let items = split_sql_items(&raw[open + 1..close]);
    let mut table_primary = Vec::new();
    let mut table_unique = Vec::new();
    for item in &items {
        let words: Vec<&str> = item.split_whitespace().collect();
        let table_constraint = words.first().is_some_and(|word| {
            word.eq_ignore_ascii_case("PRIMARY")
                || word.eq_ignore_ascii_case("UNIQUE")
                || word.eq_ignore_ascii_case("CONSTRAINT")
        });
        let upper = item.to_ascii_uppercase();
        if !table_constraint || (!upper.contains("PRIMARY KEY") && !upper.contains("UNIQUE")) {
            continue;
        }
        let constraint_open = item
            .find('(')
            .ok_or_else(|| RymeError::InvalidArgument(String::from("constraint columns")))?;
        let constraint_close = matching_paren(item, constraint_open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("constraint columns")))?;
        let columns = split_sql_items(&item[constraint_open + 1..constraint_close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .collect::<Vec<_>>();
        if upper.contains("PRIMARY KEY") {
            table_primary.extend(columns);
        } else {
            if columns.len() > 1 {
                return Err(RymeError::InvalidArgument(String::from(
                    "composite unique constraints are not supported",
                )));
            }
            table_unique.extend(columns);
        }
    }
    if table_primary.len() > 1 {
        return Err(RymeError::InvalidArgument(String::from(
            "composite primary keys are not supported",
        )));
    }
    let mut columns: Vec<ColumnDefinition> = items
        .into_iter()
        .filter_map(|definition| {
            let words: Vec<&str> = definition.split_whitespace().collect();
            if words.len() < 2
                || words[0].eq_ignore_ascii_case("CONSTRAINT")
                || words[0].eq_ignore_ascii_case("PRIMARY")
                || words[0].eq_ignore_ascii_case("UNIQUE")
                || words[0].eq_ignore_ascii_case("CHECK")
            {
                return None;
            }
            let upper = definition.to_ascii_uppercase();
            let column_default = words
                .iter()
                .position(|word| word.eq_ignore_ascii_case("DEFAULT"))
                .and_then(|default_pos| {
                    let mut expression = Vec::new();
                    for word in words.iter().skip(default_pos + 1) {
                        if [
                            "NOT",
                            "PRIMARY",
                            "UNIQUE",
                            "CHECK",
                            "REFERENCES",
                            "COLLATE",
                            "GENERATED",
                        ]
                        .iter()
                        .any(|keyword| word.eq_ignore_ascii_case(keyword))
                        {
                            break;
                        }
                        expression.push(*word);
                    }
                    (!expression.is_empty()).then(|| expression.join(" "))
                });
            let auto_increment = matches!(
                words[1].to_ascii_lowercase().as_str(),
                "serial" | "bigserial" | "smallserial"
            ) || upper.contains("IDENTITY")
                || column_default
                    .as_deref()
                    .is_some_and(|default| default.to_ascii_uppercase().contains("NEXTVAL("));
            Some(ColumnDefinition {
                name: unquote(words[0]),
                data_type: words[1].to_ascii_lowercase(),
                nullable: !upper.contains("NOT NULL") && !upper.contains("PRIMARY KEY"),
                primary_key: upper.contains("PRIMARY KEY"),
                column_default,
                auto_increment,
                unique: upper.contains("UNIQUE"),
            })
        })
        .collect();
    if let Some(primary) = table_primary.first() {
        let Some(column) =
            columns.iter_mut().find(|column| column.name.eq_ignore_ascii_case(primary))
        else {
            return Err(RymeError::InvalidArgument(format!(
                "unknown primary key column {primary}"
            )));
        };
        column.primary_key = true;
        column.nullable = false;
    }
    for unique in table_unique {
        let Some(column) =
            columns.iter_mut().find(|column| column.name.eq_ignore_ascii_case(&unique))
        else {
            return Err(RymeError::InvalidArgument(format!("unknown unique column {unique}")));
        };
        column.unique = true;
    }
    Ok(columns)
}

fn split_sql_items(input: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut quote = None;
    for ch in input.chars() {
        if let Some(open) = quote {
            current.push(ch);
            if ch == open {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                current.push(ch);
            }
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 && bracket_depth == 0 => {
                if !current.trim().is_empty() {
                    items.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn split_assignment(input: &str) -> Option<(&str, &str)> {
    let mut quote = None;
    let mut depth = 0usize;
    for (index, ch) in input.char_indices() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '=' if depth == 0 => return Some((&input[..index], &input[index + 1..])),
            _ => {}
        }
    }
    None
}

fn parse_insert(tokens: &[String], raw: &str) -> Result<Statement> {
    let table = table_after(tokens, "INTO")?;
    if let Some((columns, rows)) = parse_standard_insert_rows(raw)? {
        let upsert = tokens.iter().any(|token| token.eq_ignore_ascii_case("CONFLICT"));
        if rows.len() == 1 {
            let values = rows.into_iter().next().unwrap_or_default();
            return Ok(Statement::InsertRow { table, columns, values, upsert });
        }
        return Ok(Statement::InsertRows { table, columns, rows, upsert });
    }
    let (pk, value) = if let Some(values) =
        tokens.iter().position(|token| token.eq_ignore_ascii_case("VALUES"))
    {
        let pk = tokens
            .get(values + 1)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("missing values")))?;
        let value = tokens
            .get(values + 2)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("missing values")))?;
        (eval_operand(pk)?.into_bytes(), eval_operand(value)?.into_bytes())
    } else {
        key_value_from(tokens)?
    };
    let _ = raw;
    if tokens.iter().any(|t| t.eq_ignore_ascii_case("CONFLICT")) {
        return Ok(Statement::Upsert { table, pk, value });
    }
    Ok(Statement::Insert { table, pk, value })
}

fn parse_standard_insert_rows(raw: &str) -> Result<Option<(Vec<String>, Vec<Vec<InsertValue>>)>> {
    let upper = raw.to_ascii_uppercase();
    let Some(values_keyword) = upper.find("VALUES") else {
        return Ok(None);
    };
    let before_values = &raw[..values_keyword];
    let Some(columns_open) = before_values.find('(') else {
        return Ok(None);
    };
    let Some(columns_close) = matching_paren(before_values, columns_open) else {
        return Err(RymeError::InvalidArgument(String::from("insert columns")));
    };
    if columns_close <= columns_open {
        return Err(RymeError::InvalidArgument(String::from("insert columns")));
    }
    let columns = split_sql_items(&before_values[columns_open + 1..columns_close])
        .into_iter()
        .map(|column| unquote(column.trim()))
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("insert columns")));
    }

    let values_clause = &raw[values_keyword + "VALUES".len()..];
    let mut rows = Vec::new();
    let mut cursor = values_clause
        .char_indices()
        .find(|(_, ch)| !ch.is_whitespace())
        .map(|(index, _)| index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("insert values")))?;
    loop {
        if values_clause.as_bytes().get(cursor) != Some(&b'(') {
            break;
        }
        let values_close = matching_paren(values_clause, cursor)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("insert values")))?;
        if values_close <= cursor {
            return Err(RymeError::InvalidArgument(String::from("insert values")));
        }
        let values = split_sql_items(&values_clause[cursor + 1..values_close])
            .into_iter()
            .map(|value| parse_insert_value(value.trim()))
            .collect::<Result<Vec<_>>>()?;
        if columns.len() != values.len() {
            return Err(RymeError::InvalidArgument(String::from("insert column/value count")));
        }
        rows.push(values);
        cursor = values_close + 1;
        while values_clause.as_bytes().get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if values_clause.as_bytes().get(cursor) == Some(&b',') {
            cursor += 1;
            while values_clause.as_bytes().get(cursor).is_some_and(u8::is_ascii_whitespace) {
                cursor += 1;
            }
            continue;
        }
        break;
    }
    if rows.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("insert values")));
    }
    Ok(Some((columns, rows)))
}

fn matching_paren(input: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote = None;
    for (index, ch) in input.char_indices().skip_while(|(index, _)| *index < open) {
        if let Some(delimiter) = quote {
            if ch == delimiter {
                quote = None;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}

fn parse_insert_value(raw: &str) -> Result<InsertValue> {
    if raw.eq_ignore_ascii_case("DEFAULT") {
        return Ok(InsertValue::Default);
    }
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok(InsertValue::Null);
    }
    Ok(InsertValue::Value(eval_operand(raw)?.into_bytes()))
}

fn parse_upsert(tokens: &[String]) -> Result<Statement> {
    let table = if let Ok(table) = table_after(tokens, "INTO") {
        table
    } else {
        tokens
            .get(1)
            .cloned()
            .ok_or_else(|| RymeError::InvalidArgument(String::from("upsert table")))?
    };
    let (pk, value) = key_value_from(tokens)?;
    Ok(Statement::Upsert { table, pk, value })
}

fn parse_update(tokens: &[String], raw: &str) -> Result<Statement> {
    let table = tokens
        .get(1)
        .cloned()
        .ok_or_else(|| RymeError::InvalidArgument(String::from("update table")))?;
    if let Some(set) = tokens.iter().position(|token| token.eq_ignore_ascii_case("SET")) {
        let where_pos = tokens
            .iter()
            .position(|token| token.eq_ignore_ascii_case("WHERE"))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("update predicate")))?;
        if where_pos <= set + 1 {
            return Err(RymeError::InvalidArgument(String::from("update assignments")));
        }
        let pk = value_after(&tokens[where_pos..], &["KEY", "PK", "ID"])?.into_bytes();
        let upper = raw.to_ascii_uppercase();
        let set_offset = upper
            .find(" SET ")
            .map(|offset| offset + 5)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("update assignments")))?;
        let where_offset = upper[set_offset..]
            .find(" WHERE ")
            .map(|offset| set_offset + offset)
            .unwrap_or(raw.len());
        let assignment_text = raw[set_offset..where_offset].trim();
        let mut assignments = Vec::new();
        for assignment in split_sql_items(assignment_text) {
            let (column, value) = split_assignment(&assignment)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("update assignment")))?;
            assignments.push((unquote(column.trim()), parse_insert_value(value.trim())?));
        }
        if assignments.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("update assignments")));
        }
        return Ok(Statement::UpdateRow { table, pk, assignments });
    }
    let (pk, value) = key_value_from(tokens).or_else(|_| {
        let pk = value_after(tokens, &["KEY", "PK", "ID"])?;
        let set = tokens
            .iter()
            .position(|token| token.eq_ignore_ascii_case("SET"))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("update values")))?;
        let value = tokens
            .get(set + 2)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("update value")))?;
        Ok::<(Vec<u8>, Vec<u8>), RymeError>((pk.into_bytes(), eval_operand(value)?.into_bytes()))
    })?;
    Ok(Statement::Update { table, pk, value })
}

fn parse_delete(tokens: &[String]) -> Result<Statement> {
    let table = table_after(tokens, "FROM")?;
    let pk = value_after(tokens, &["KEY", "PK", "ID"])?;
    Ok(Statement::Delete { table, pk: pk.into_bytes() })
}

fn parse_select(tokens: &[String]) -> Result<Statement> {
    let table = table_after(tokens, "FROM")?;
    if tokens.iter().any(|t| t.eq_ignore_ascii_case("GROUP")) {
        return parse_group(tokens, &table);
    }
    if tokens.len() > 2 {
        let func = if tokens[1].eq_ignore_ascii_case("COUNT") {
            Some(AggFunc::Count)
        } else if tokens[1].eq_ignore_ascii_case("SUM") {
            Some(AggFunc::Sum)
        } else if tokens[1].eq_ignore_ascii_case("AVG") {
            Some(AggFunc::Avg)
        } else if tokens[1].eq_ignore_ascii_case("MIN") {
            Some(AggFunc::Min)
        } else if tokens[1].eq_ignore_ascii_case("MAX") {
            Some(AggFunc::Max)
        } else {
            None
        };
        if let Some(func) = func {
            let field = match tokens.get(2).map(|s| s.as_str()) {
                Some("*") if func == AggFunc::Count => Field::Value,
                Some("*") => {
                    return Err(RymeError::InvalidArgument(String::from("aggregate field")));
                }
                Some(name) => parse_field(name)
                    .ok_or_else(|| RymeError::InvalidArgument(String::from("aggregate field")))?,
                None => {
                    return Err(RymeError::InvalidArgument(String::from("aggregate field")));
                }
            };
            let filter = parse_where_filter(tokens)?;
            return Ok(Statement::Aggregate { table, func, field, filter });
        }
    }
    let has_where = tokens.iter().any(|t| t.eq_ignore_ascii_case("WHERE"));
    let has_join = tokens.iter().any(|t| t.eq_ignore_ascii_case("JOIN"));
    let projection = parse_projection(tokens)?;
    let filter = parse_where_filter(tokens)?;
    if projection.is_none() && !has_join {
        if let Some(pk) = point_lookup_key(tokens) {
            let exact_where = filter.len() == 1
                && filter[0].field == Field::Key
                && filter[0].op == Cmp::Eq
                && filter[0].operand == pk.as_bytes();
            if !has_where || exact_where {
                return Ok(Statement::SelectByKey { table, pk: pk.into_bytes() });
            }
        }
    }
    if has_join {
        return parse_join(tokens, &table);
    }
    let (limit, offset, order) = parse_scan_tail(tokens)?;
    if let Some(columns) = projection {
        return Ok(Statement::SelectColumns { table, columns, limit, offset, order, filter });
    }
    Ok(Statement::SelectScan { table, limit, offset, order, filter })
}

fn parse_projection(tokens: &[String]) -> Result<Option<Vec<String>>> {
    let from = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("missing table")))?;
    let selected = &tokens[1..from];
    if selected.is_empty() || (selected.len() == 1 && selected[0] == "*") {
        return Ok(None);
    }
    let columns = selected
        .iter()
        .map(|token| {
            if token == "*" {
                Err(RymeError::InvalidArgument(String::from("select projection")))
            } else {
                Ok(unquote(token))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(columns))
}

fn parse_group(tokens: &[String], table: &str) -> Result<Statement> {
    let from_pos = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("missing table")))?;
    let mut select = Vec::new();
    let mut index = 1usize;
    while index < from_pos {
        let token = &tokens[index];
        if let Some(func) = AggFunc::parse(token) {
            let field = match tokens.get(index + 1).map(|s| s.as_str()) {
                Some("*") if func == AggFunc::Count => Field::Value,
                Some("*") => {
                    return Err(RymeError::InvalidArgument(String::from("aggregate field")));
                }
                Some(name) => parse_field(name)
                    .ok_or_else(|| RymeError::InvalidArgument(String::from("aggregate field")))?,
                None => {
                    return Err(RymeError::InvalidArgument(String::from("aggregate field")));
                }
            };
            select.push(SelectItem::Agg(func, field));
            index += 2;
            continue;
        }
        if let Some(field) = parse_field(token) {
            select.push(SelectItem::Field(field));
            index += 1;
            continue;
        }
        return Err(RymeError::InvalidArgument(String::from("select item")));
    }
    if select.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("select item")));
    }
    let group_pos = tokens
        .iter()
        .enumerate()
        .skip(from_pos)
        .find(|(_, t)| t.eq_ignore_ascii_case("GROUP"))
        .map(|(index, _)| index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("group field")))?;
    if !tokens.get(group_pos + 1).is_some_and(|t| t.eq_ignore_ascii_case("BY")) {
        return Err(RymeError::InvalidArgument(String::from("group field")));
    }
    let group = tokens
        .get(group_pos + 2)
        .and_then(|t| parse_field(t))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("group field")))?;
    for item in &select {
        if let SelectItem::Field(field) = item {
            if *field != group {
                return Err(RymeError::InvalidArgument(String::from("group field")));
            }
        }
    }
    let (limit, offset, order) = parse_scan_tail(tokens)?;
    let filter = parse_where_filter(tokens)?;
    Ok(Statement::GroupBy { table: table.to_string(), select, group, filter, limit, offset, order })
}

fn parse_join(tokens: &[String], left: &str) -> Result<Statement> {
    let from_pos = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("missing table")))?;
    let table_pos = from_pos + 1;
    let join_pos = tokens
        .iter()
        .enumerate()
        .skip(table_pos + 1)
        .find(|(_, t)| t.eq_ignore_ascii_case("JOIN"))
        .map(|(index, _)| index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("join table")))?;
    let right = tokens
        .get(join_pos + 1)
        .cloned()
        .ok_or_else(|| RymeError::InvalidArgument(String::from("join table")))?;
    if right.eq_ignore_ascii_case("ON") || right.eq_ignore_ascii_case("WHERE") {
        return Err(RymeError::InvalidArgument(String::from("join table")));
    }
    let on_pos = tokens
        .iter()
        .enumerate()
        .skip(join_pos + 2)
        .find(|(_, t)| t.eq_ignore_ascii_case("ON"))
        .map(|(index, _)| index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("join condition")))?;
    let left_field = tokens
        .get(on_pos + 1)
        .and_then(|t| parse_field(t))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("join condition")))?;
    let right_field = tokens
        .get(on_pos + 2)
        .and_then(|t| parse_field(t))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("join condition")))?;
    if left_field != Field::Key || right_field != Field::Key {
        return Err(RymeError::InvalidArgument(String::from("key join only")));
    }
    let _ = table_pos;
    let (limit, offset, order) = parse_scan_tail(tokens)?;
    let filter = parse_where_filter(tokens)?;
    Ok(Statement::Join {
        left: left.to_string(),
        right: unquote(&right),
        limit,
        offset,
        order,
        filter,
    })
}

fn parse_scan_tail(tokens: &[String]) -> Result<(usize, usize, Order)> {
    let mut limit = 100usize;
    let mut offset = 0usize;
    let mut order = Order::default();
    let mut index = 0usize;
    while index < tokens.len() {
        if tokens[index].eq_ignore_ascii_case("LIMIT") {
            if let Some(next) = tokens.get(index + 1) {
                if let Ok(parsed) = next.parse::<usize>() {
                    limit = parsed.min(10000);
                }
            }
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("OFFSET") {
            if let Some(next) = tokens.get(index + 1) {
                if let Ok(parsed) = next.parse::<usize>() {
                    offset = parsed.min(10000);
                }
            }
            index += 2;
        } else if tokens[index].eq_ignore_ascii_case("ORDER")
            && tokens.get(index + 1).is_some_and(|t| t.eq_ignore_ascii_case("BY"))
        {
            if let Some(field) = tokens.get(index + 2) {
                if let Some(parsed) = parse_field(field) {
                    order.field = parsed;
                }
            }
            if let Some(direction) = tokens.get(index + 3) {
                if direction.eq_ignore_ascii_case("DESC") {
                    order.direction = Direction::Desc;
                }
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    Ok((limit, offset, order))
}

fn point_lookup_key(tokens: &[String]) -> Option<String> {
    let start = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("FROM"))
        .map(|index| index + 1)
        .unwrap_or(0);
    for (relative, window) in tokens[start..].windows(2).enumerate() {
        let index = start + relative;
        let is_key = window[0].eq_ignore_ascii_case("KEY")
            || window[0].eq_ignore_ascii_case("PK")
            || window[0].eq_ignore_ascii_case("ID");
        if !is_key {
            continue;
        }
        if index > 0 && tokens[index - 1].eq_ignore_ascii_case("BY") {
            continue;
        }
        return Some(unquote(&window[1]));
    }
    None
}

fn parse_where_filter(tokens: &[String]) -> Result<Vec<Predicate>> {
    let Some(start) = tokens.iter().position(|t| t.eq_ignore_ascii_case("WHERE")) else {
        return Ok(Vec::new());
    };
    let mut clause: Vec<String> = Vec::new();
    let mut index = start + 1;
    while index < tokens.len()
        && !tokens[index].eq_ignore_ascii_case("LIMIT")
        && !tokens[index].eq_ignore_ascii_case("OFFSET")
        && !tokens[index].eq_ignore_ascii_case("ORDER")
        && !tokens[index].eq_ignore_ascii_case("GROUP")
        && !tokens[index].eq_ignore_ascii_case("JOIN")
        && !tokens[index].eq_ignore_ascii_case("ON")
    {
        clause.push(tokens[index].clone());
        index += 1;
    }
    parse_filter(&clause)
}

fn parse_field(token: &str) -> Option<Field> {
    if token.eq_ignore_ascii_case("KEY")
        || token.eq_ignore_ascii_case("PK")
        || token.eq_ignore_ascii_case("ID")
    {
        Some(Field::Key)
    } else if token.eq_ignore_ascii_case("VALUE")
        || token.eq_ignore_ascii_case("VAL")
        || token.eq_ignore_ascii_case("DATA")
    {
        Some(Field::Value)
    } else {
        None
    }
}

fn parse_filter(clause: &[String]) -> Result<Vec<Predicate>> {
    let mut out = Vec::new();
    let mut current: Vec<String> = Vec::new();
    for token in clause.iter().chain(std::iter::once(&String::from("AND"))) {
        if token.eq_ignore_ascii_case("AND") {
            if !current.is_empty() {
                out.push(parse_predicate(&current)?);
                current.clear();
            }
            continue;
        }
        current.push(token.clone());
    }
    Ok(out)
}

fn parse_predicate_field(raw: &str) -> (Field, Option<String>) {
    parse_field(raw)
        .map(|field| (field, None))
        .unwrap_or_else(|| (Field::Value, Some(unquote(raw))))
}

fn parse_predicate(parts: &[String]) -> Result<Predicate> {
    if parts.len() == 2 {
        let (field, column) = parse_predicate_field(&parts[0]);
        return Ok(Predicate {
            field,
            column,
            op: Cmp::Eq,
            operand: unquote(&parts[1]).into_bytes(),
        });
    }
    if parts.len() == 3 {
        let (field, column) = parse_predicate_field(&parts[0]);
        if parts[1].eq_ignore_ascii_case("IS") {
            let op = if parts[2].eq_ignore_ascii_case("NULL") {
                Cmp::IsNull
            } else {
                return Err(RymeError::InvalidArgument(String::from("where predicate")));
            };
            return Ok(Predicate { field, column, op, operand: Vec::new() });
        }
        if parts[1] == "=" {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::Eq,
                operand: unquote(&parts[2]).into_bytes(),
            });
        }
        if parts[1] == "!" || parts[1] == "!=" || parts[1] == "<>" {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::NotEq,
                operand: unquote(&parts[2]).into_bytes(),
            });
        }
        let op = match parts[1].as_str() {
            ">" => Some(Cmp::Gt),
            ">=" => Some(Cmp::Gte),
            "<" => Some(Cmp::Lt),
            "<=" => Some(Cmp::Lte),
            _ => None,
        };
        if let Some(op) = op {
            return Ok(Predicate { field, column, op, operand: unquote(&parts[2]).into_bytes() });
        }
        if parts[1].eq_ignore_ascii_case("CONTAINS") {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::Contains,
                operand: unquote(&parts[2]).into_bytes(),
            });
        }
        let op = if parts[1].eq_ignore_ascii_case("LIKE") {
            Some(Cmp::Like)
        } else if parts[1].eq_ignore_ascii_case("ILIKE") {
            Some(Cmp::ILike)
        } else {
            None
        };
        if let Some(op) = op {
            return Ok(Predicate { field, column, op, operand: unquote(&parts[2]).into_bytes() });
        }
    }
    if parts.len() == 4 {
        let (field, column) = parse_predicate_field(&parts[0]);
        if parts[1].eq_ignore_ascii_case("IS")
            && parts[2].eq_ignore_ascii_case("NOT")
            && parts[3].eq_ignore_ascii_case("NULL")
        {
            return Ok(Predicate { field, column, op: Cmp::IsNotNull, operand: Vec::new() });
        }
    }
    Err(RymeError::InvalidArgument(String::from("where predicate")))
}

fn table_after(tokens: &[String], keyword: &str) -> Result<String> {
    for (index, token) in tokens.iter().enumerate() {
        if token.eq_ignore_ascii_case(keyword) {
            if let Some(next) = tokens.get(index + 1) {
                return Ok(unquote(next));
            }
        }
    }
    Err(RymeError::InvalidArgument(String::from("missing table")))
}

fn value_after(tokens: &[String], names: &[&str]) -> Result<String> {
    for window in tokens.windows(2) {
        if names.iter().any(|name| window[0].eq_ignore_ascii_case(name)) {
            return Ok(unquote(&window[1]));
        }
    }
    Err(RymeError::InvalidArgument(String::from("missing key")))
}

fn key_value_from(tokens: &[String]) -> Result<(Vec<u8>, Vec<u8>)> {
    let pk = eval_operand(&value_after_raw(tokens, &["KEY", "PK", "ID"])?)?;
    let value = eval_operand(&value_after_raw(tokens, &["VALUE", "VAL", "DATA"])?)?;
    Ok((pk.into_bytes(), value.into_bytes()))
}

fn value_after_raw(tokens: &[String], names: &[&str]) -> Result<String> {
    for window in tokens.windows(2) {
        if names.iter().any(|name| window[0].eq_ignore_ascii_case(name)) {
            return Ok(window[1].clone());
        }
    }
    Err(RymeError::InvalidArgument(String::from("missing key")))
}

fn eval_operand(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let quoted = trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')));
    if quoted {
        return Ok(unquote(trimmed));
    }
    if trimmed.eq_ignore_ascii_case("gen_random_uuid")
        || trimmed.eq_ignore_ascii_case("gen_random_uuid()")
    {
        return new_uuid_v4();
    }
    if trimmed.eq_ignore_ascii_case("now") || trimmed.eq_ignore_ascii_case("now()") {
        return Ok(ryme_txn::now_unix().to_string());
    }
    Ok(unquote(trimmed))
}

fn eval_default(raw: &str) -> Result<Option<Vec<u8>>> {
    if raw.trim().eq_ignore_ascii_case("NULL") {
        return Ok(None);
    }
    Ok(Some(eval_operand(raw)?.into_bytes()))
}

fn json_insert_value(value: Option<Vec<u8>>, data_type: &str) -> serde_json::Value {
    let Some(value) = value else { return serde_json::Value::Null };
    let text = String::from_utf8_lossy(&value);
    let trimmed = text.trim();
    if data_type.ends_with("[]") || data_type.eq_ignore_ascii_case("array") {
        if let Some(parsed) = parse_array_literal(trimmed) {
            return parsed;
        }
    }
    if matches!(data_type, "json" | "jsonb") {
        if let Ok(parsed) = serde_json::from_slice(&value) {
            return parsed;
        }
    }
    if matches!(data_type, "bool" | "boolean") {
        if let Ok(parsed) = trimmed.parse::<bool>() {
            return serde_json::Value::Bool(parsed);
        }
    }
    if data_type.contains("int")
        || data_type.contains("serial")
        || data_type.contains("numeric")
        || data_type.contains("decimal")
    {
        if let Ok(parsed) = trimmed.parse::<i64>() {
            return serde_json::Value::Number(parsed.into());
        }
        if let Ok(parsed) = trimmed.parse::<f64>() {
            if let Some(number) = serde_json::Number::from_f64(parsed) {
                return serde_json::Value::Number(number);
            }
        }
    }
    serde_json::Value::String(text.into_owned())
}

fn json_update_value(
    value: Option<Vec<u8>>,
    previous: Option<&serde_json::Value>,
) -> serde_json::Value {
    let data_type = match previous {
        Some(serde_json::Value::Bool(_)) => "boolean",
        Some(serde_json::Value::Number(_)) => "numeric",
        Some(serde_json::Value::Array(_)) => "array",
        Some(serde_json::Value::Object(_)) => "jsonb",
        _ => "text",
    };
    json_insert_value(value, data_type)
}

fn parse_array_literal(raw: &str) -> Option<serde_json::Value> {
    let trimmed = raw.trim();
    let inner = if trimmed.get(..6).is_some_and(|prefix| prefix.eq_ignore_ascii_case("ARRAY[")) {
        trimmed.strip_suffix(']')?.get(6..)?
    } else if trimmed.starts_with('{') && trimmed.ends_with('}') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        return None;
    };
    if inner.trim().is_empty() {
        return Some(serde_json::Value::Array(Vec::new()));
    }
    let values = split_sql_items(inner)
        .into_iter()
        .map(|item| {
            let item = item.trim();
            if item.eq_ignore_ascii_case("NULL") {
                return serde_json::Value::Null;
            }
            if let Ok(value) = item.parse::<bool>() {
                return serde_json::Value::Bool(value);
            }
            if let Ok(value) = item.parse::<i64>() {
                return serde_json::Value::Number(value.into());
            }
            if let Ok(value) = item.parse::<f64>() {
                if let Some(number) = serde_json::Number::from_f64(value) {
                    return serde_json::Value::Number(number);
                }
            }
            serde_json::Value::String(unquote(item))
        })
        .collect();
    Some(serde_json::Value::Array(values))
}

fn json_result_bytes(value: &serde_json::Value) -> Vec<u8> {
    match value {
        serde_json::Value::String(text) => text.as_bytes().to_vec(),
        serde_json::Value::Null => Vec::new(),
        serde_json::Value::Bool(value) => value.to_string().into_bytes(),
        serde_json::Value::Number(value) => value.to_string().into_bytes(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_vec(value).unwrap_or_default()
        }
    }
}

fn json_projection_bytes(
    expression: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<u8>> {
    let (current, text_result) = json_projection_value(expression, object)?;
    Some(if text_result { json_result_bytes(current) } else { serde_json::to_vec(current).ok()? })
}

fn json_column_value<'a>(
    expression: &str,
    object: &'a serde_json::Map<String, serde_json::Value>,
) -> Option<&'a serde_json::Value> {
    if expression.contains("->") {
        json_projection_value(expression, object).map(|(value, _)| value)
    } else {
        object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(expression))
            .map(|(_, value)| value)
    }
}

fn index_value(definition: &IndexDefinition, pk: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    if let Some(column) = definition.column.as_deref() {
        let serde_json::Value::Object(object) = serde_json::from_slice(value).ok()? else {
            return None;
        };
        let selected = json_column_value(column, &object)?;
        if selected.is_null() {
            return None;
        }
        return Some(json_result_bytes(selected));
    }
    Some(match definition.field {
        Field::Key => pk.to_vec(),
        Field::Value => value.to_vec(),
    })
}

fn json_projection_value<'a>(
    expression: &str,
    object: &'a serde_json::Map<String, serde_json::Value>,
) -> Option<(&'a serde_json::Value, bool)> {
    let operator = expression.find("->")?;
    let base = expression[..operator].trim();
    let mut current = object.get(base)?;
    let mut tail = &expression[operator + 2..];
    loop {
        let text_result = tail.starts_with('>');
        if text_result {
            tail = &tail[1..];
        }
        let (selector, consumed) = if tail.starts_with('\'') || tail.starts_with('"') {
            let quote = tail.as_bytes()[0] as char;
            let end = tail
                .char_indices()
                .skip(1)
                .find_map(|(index, ch)| (ch == quote).then_some(index))?;
            (&tail[1..end], end + 1)
        } else {
            let end = tail.find("->").unwrap_or(tail.len());
            (&tail[..end], end)
        };
        let selector = unquote(selector.trim());
        current = match current {
            serde_json::Value::Object(values) => values.get(&selector)?,
            serde_json::Value::Array(values) => values.get(selector.parse::<usize>().ok()?)?,
            _ => return None,
        };
        tail = &tail[consumed..];
        if tail.is_empty() {
            return Some((current, text_result));
        }
        if !tail.starts_with("->") {
            return None;
        }
        tail = &tail[2..];
    }
}

fn new_uuid_v4() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| RymeError::Internal(e.to_string()))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]))
}

fn parse_copy(tokens: &[String]) -> Result<Statement> {
    let table = tokens
        .get(1)
        .filter(|token| !token.eq_ignore_ascii_case("FROM") && !token.eq_ignore_ascii_case("TO"))
        .map(|value| unquote(value))
        .or_else(|| table_after(tokens, "FROM").ok())
        .or_else(|| table_after(tokens, "TO").ok())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("copy table")))?;
    Ok(Statement::CopyFrom { table, rows: Vec::new() })
}

fn parse_explain(input: &str) -> Result<Statement> {
    let trimmed = input.trim();
    let inner_sql = trimmed
        .strip_prefix("EXPLAIN")
        .or_else(|| trimmed.strip_prefix("explain"))
        .or_else(|| trimmed.strip_prefix("Explain"))
        .unwrap_or("")
        .trim()
        .trim_start_matches([':', ';', ' '])
        .to_string();
    if inner_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("explain input")));
    }
    let inner = parse(&inner_sql)?;
    let plan = describe_plan(&inner);
    Ok(Statement::Explain { plan, inner: Box::new(inner) })
}

pub fn describe_plan(statement: &Statement) -> String {
    match statement {
        Statement::CreateTable { table, columns } => {
            format!("ddl create_table({table}) columns {}", columns.len())
        }
        Statement::CreateIndex { name, table, field, column, unique } => {
            let field = column.as_deref().unwrap_or(match field {
                Field::Key => "key",
                Field::Value => "value",
            });
            format!("ddl {}index({name}) on {table}({field})", if *unique { "unique " } else { "" })
        }
        Statement::Insert { table, .. } => format!("write insert({table}) point"),
        Statement::InsertRow { table, upsert, .. } => {
            format!("write {}({table}) row", if *upsert { "upsert" } else { "insert" })
        }
        Statement::InsertRows { table, rows, upsert, .. } => {
            format!(
                "write {}({table}) rows {}",
                if *upsert { "upsert" } else { "insert" },
                rows.len()
            )
        }
        Statement::Upsert { table, .. } => format!("write upsert({table}) point"),
        Statement::SelectByKey { table, .. } => {
            format!("point_lookup({table}) using primary index")
        }
        Statement::SelectScan { table, limit, offset, order, filter } => {
            let direction = match order.direction {
                Direction::Asc => "asc",
                Direction::Desc => "desc",
            };
            let field = match order.field {
                Field::Key => "key",
                Field::Value => "value",
            };
            format!(
                "scan({table}) limit {limit} offset {offset} order {field} {direction} filters {} using ordered range",
                filter.len()
            )
        }
        Statement::SelectColumns { table, columns, limit, offset, .. } => {
            format!("project({table}) columns {} limit {limit} offset {offset}", columns.len())
        }
        Statement::Update { table, .. } => format!("write update({table}) point"),
        Statement::UpdateRow { table, assignments, .. } => {
            format!("write update({table}) columns {}", assignments.len())
        }
        Statement::Delete { table, .. } => format!("write delete({table}) point"),
        Statement::CopyFrom { table, .. } => format!("bulk ingest({table}) batched put"),
        Statement::Returning { statement, fields } => {
            format!("{} returning {} fields", describe_plan(statement), fields.len())
        }
        Statement::Aggregate { table, func, filter, .. } => {
            format!("aggregate({table}) {} filters {}", func.label(), filter.len())
        }
        Statement::GroupBy { table, select, filter, .. } => {
            format!("group_by({table}) items {} filters {}", select.len(), filter.len())
        }
        Statement::Join { left, right, limit, offset, order, filter } => {
            let direction = match order.direction {
                Direction::Asc => "asc",
                Direction::Desc => "desc",
            };
            format!(
                "hash_join({left},{right}) limit {limit} offset {offset} order {direction} filters {} using key index",
                filter.len()
            )
        }
        Statement::Explain { plan, .. } => format!("explain({plan})"),
    }
}

pub fn aggregate_rows(rows: &[(Vec<u8>, Vec<u8>)], func: AggFunc, field: Field) -> Vec<u8> {
    match func {
        AggFunc::Count => {
            if field == Field::Value {
                rows.len().to_string().into_bytes()
            } else {
                rows.iter().filter(|(pk, _)| !pk.is_empty()).count().to_string().into_bytes()
            }
        }
        AggFunc::Sum => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for (pk, value) in rows {
                let target = match field {
                    Field::Key => pk,
                    Field::Value => value,
                };
                if let Some(number) = parse_number(target) {
                    total += number;
                    count += 1;
                }
            }
            if count == 0 {
                b"0".to_vec()
            } else {
                format_number(total).into_bytes()
            }
        }
        AggFunc::Avg => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for (pk, value) in rows {
                let target = match field {
                    Field::Key => pk,
                    Field::Value => value,
                };
                if let Some(number) = parse_number(target) {
                    total += number;
                    count += 1;
                }
            }
            if count == 0 {
                b"null".to_vec()
            } else {
                format_number(total / count as f64).into_bytes()
            }
        }
        AggFunc::Min => rows
            .iter()
            .map(|(pk, value)| match field {
                Field::Key => pk.clone(),
                Field::Value => value.clone(),
            })
            .min()
            .unwrap_or_else(|| b"null".to_vec()),
        AggFunc::Max => rows
            .iter()
            .map(|(pk, value)| match field {
                Field::Key => pk.clone(),
                Field::Value => value.clone(),
            })
            .max()
            .unwrap_or_else(|| b"null".to_vec()),
    }
}

fn parse_number(raw: &[u8]) -> Option<f64> {
    let text = String::from_utf8_lossy(raw);
    let trimmed = text.trim().trim_matches('"');
    trimmed.parse::<f64>().ok().filter(|v| v.is_finite())
}

fn format_number(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        let mut text = format!("{value:.17}");
        while text.contains('.') && (text.ends_with('0') || text.ends_with('.')) {
            if text.ends_with('.') {
                text.pop();
                break;
            }
            text.pop();
        }
        text
    }
}

fn returning_result(fields: &[ReturningField], pk: Vec<u8>, value: Vec<u8>) -> QueryResult {
    let columns = returning_columns(fields);
    let row = fields.iter().map(|field| returning_field_value(field, &pk, &value)).collect();
    QueryResult::Returning { columns, rows: vec![row] }
}

fn returning_columns(fields: &[ReturningField]) -> Vec<String> {
    fields
        .iter()
        .map(|field| match field {
            ReturningField::Key => String::from("id"),
            ReturningField::Value => String::from("value"),
            ReturningField::Column(column) => column.clone(),
        })
        .collect()
}

fn returning_field_value(field: &ReturningField, pk: &[u8], value: &[u8]) -> Vec<u8> {
    let Some(serde_json::Value::Object(object)) = serde_json::from_slice(value).ok() else {
        return match field {
            ReturningField::Key => pk.to_vec(),
            ReturningField::Value | ReturningField::Column(_) => value.to_vec(),
        };
    };
    match field {
        ReturningField::Key => pk.to_vec(),
        ReturningField::Value => object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("value"))
            .map(|(_, value)| json_result_bytes(value))
            .unwrap_or_else(|| value.to_vec()),
        ReturningField::Column(column) => object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, value)| json_result_bytes(value))
            .unwrap_or_default(),
    }
}

#[derive(Debug, Clone)]
pub struct Executor<B = TxnManager> {
    tenant: String,
    database: String,
    branch: String,
    manager: B,
    read_ts: Option<u64>,
    realtime: Option<Realtime>,
    read_only: bool,
    isolation: Isolation,
    catalog: Arc<Mutex<HashMap<String, Vec<ColumnDefinition>>>>,
    indexes: Arc<Mutex<HashMap<String, Vec<IndexState>>>>,
    rls_tables: Arc<HashMap<String, String>>,
    schema_path: Arc<Mutex<Option<PathBuf>>>,
    schema_persist_lock: Arc<Mutex<()>>,
    schema_dirty: Arc<AtomicBool>,
    sequence_next: Arc<Mutex<BTreeMap<String, u64>>>,
}

#[derive(Debug, Clone)]
struct IndexState {
    definition: IndexDefinition,
    entries: BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>>,
}

impl Executor<TxnManager> {
    pub fn new(tenant: String, database: String) -> Self {
        Self {
            tenant,
            database,
            branch: String::from("main"),
            manager: TxnManager::new(),
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(Mutex::new(HashMap::new())),
            rls_tables: Arc::new(HashMap::new()),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_manager(tenant: String, database: String, manager: TxnManager) -> Self {
        Self {
            tenant,
            database,
            branch: String::from("main"),
            manager,
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(Mutex::new(HashMap::new())),
            rls_tables: Arc::new(HashMap::new()),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl<B> Executor<B>
where
    B: TxnBackend,
{
    pub fn with_backend(tenant: String, database: String, manager: B) -> Self {
        Self {
            tenant,
            database,
            branch: String::from("main"),
            manager,
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(Mutex::new(HashMap::new())),
            rls_tables: Arc::new(HashMap::new()),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_realtime(mut self, realtime: Realtime) -> Self {
        self.realtime = Some(realtime);
        self
    }

    pub fn with_rls_tables(mut self, rls_tables: HashMap<String, String>) -> Self {
        self.rls_tables = Arc::new(rls_tables);
        self
    }

    pub fn set_rls_tables(&mut self, rls_tables: HashMap<String, String>) {
        self.rls_tables = Arc::new(rls_tables);
    }

    pub fn with_branch(mut self, branch: String) -> Self {
        self.branch = branch;
        self
    }

    pub fn with_read_ts(mut self, read_ts: u64) -> Self {
        self.read_ts = Some(read_ts);
        self
    }

    pub fn with_backend_manager<C>(self, manager: C) -> Executor<C> {
        Executor {
            tenant: self.tenant,
            database: self.database,
            branch: self.branch,
            manager,
            read_ts: None,
            realtime: self.realtime,
            read_only: self.read_only,
            isolation: self.isolation,
            catalog: self.catalog,
            indexes: self.indexes,
            rls_tables: self.rls_tables,
            schema_path: self.schema_path,
            schema_persist_lock: self.schema_persist_lock,
            schema_dirty: self.schema_dirty,
            sequence_next: self.sequence_next,
        }
    }

    fn begin_with(&self, isolation: Isolation) -> Transaction {
        let mut txn = self.manager.begin_with(isolation);
        if let Some(read_ts) = self.read_ts {
            txn.restamp(read_ts);
        }
        txn
    }

    pub fn manager(&self) -> &B {
        &self.manager
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    /// Returns an executor using a different tenant namespace while keeping
    /// the shared backend, catalog, indexes, and policy settings.
    pub fn with_tenant(mut self, tenant: String) -> Self {
        self.tenant = tenant;
        self
    }

    pub fn set_tenant(&mut self, tenant: String) {
        self.tenant = tenant;
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_schema_path(&mut self, path: PathBuf) {
        if let Ok(mut stored) = self.schema_path.lock() {
            *stored = Some(path);
        }
    }

    pub fn schema_snapshot(&self) -> SchemaSnapshot {
        let tables = self
            .catalog
            .lock()
            .map(|catalog| {
                catalog.iter().map(|(table, columns)| (table.clone(), columns.clone())).collect()
            })
            .unwrap_or_default();
        let mut indexes = self
            .indexes
            .lock()
            .map(|catalog| {
                catalog
                    .values()
                    .flat_map(|states| states.iter().map(|state| state.definition.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        indexes.sort_by(|left, right| {
            left.table.cmp(&right.table).then_with(|| left.name.cmp(&right.name))
        });
        SchemaSnapshot { tables, indexes }
    }

    pub fn restore_schema_snapshot(&self, snapshot: SchemaSnapshot) -> Result<()> {
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            *catalog = snapshot.tables.into_iter().collect();
        }
        if let Ok(mut indexes) = self.indexes.lock() {
            indexes.clear();
        } else {
            return Err(RymeError::Internal(String::from("index lock")));
        }
        for definition in snapshot.indexes {
            self.create_index(definition)?;
        }
        self.schema_dirty.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn with_isolated_schema<C>(self, manager: C) -> Executor<C>
    where
        C: TxnBackend,
    {
        let catalog = self.catalog.lock().map(|catalog| catalog.clone()).unwrap_or_default();
        let indexes = self.indexes.lock().map(|indexes| indexes.clone()).unwrap_or_default();
        Executor {
            tenant: self.tenant,
            database: self.database,
            branch: self.branch,
            manager,
            read_ts: None,
            realtime: self.realtime,
            read_only: self.read_only,
            isolation: self.isolation,
            catalog: Arc::new(Mutex::new(catalog)),
            indexes: Arc::new(Mutex::new(indexes)),
            rls_tables: self.rls_tables,
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn set_isolation(&mut self, isolation: Isolation) {
        self.isolation = isolation;
    }

    pub fn catalog_tables(&self) -> Vec<String> {
        let Ok(catalog) = self.catalog.lock() else { return Vec::new() };
        let mut tables: Vec<String> = catalog.keys().cloned().collect();
        tables.sort();
        tables
    }

    pub fn catalog_columns(&self, table: &str) -> Vec<ColumnDefinition> {
        self.catalog
            .lock()
            .ok()
            .and_then(|catalog| {
                catalog.get(table).cloned().or_else(|| {
                    catalog.iter().find_map(|(name, columns)| {
                        (name.rsplit('.').next() == Some(table)).then(|| columns.clone())
                    })
                })
            })
            .unwrap_or_default()
    }

    fn next_sequence_value(&self, table: &str, definition: &ColumnDefinition) -> Result<Vec<u8>> {
        let key = format!("{}\0{}\0{}\0{}", self.tenant, self.database, table, definition.name);
        let mut sequences = self
            .sequence_next
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?;
        let next = if let Some(next) = sequences.get(&key).copied() {
            next
        } else {
            let mut highest = 0u64;
            for (pk, value) in self.scan_all_rows(table)? {
                let candidate = if definition.primary_key
                    || definition.name.eq_ignore_ascii_case("id")
                    || definition.name.eq_ignore_ascii_case("pk")
                    || definition.name.eq_ignore_ascii_case("key")
                {
                    std::str::from_utf8(&pk).ok().and_then(|value| value.parse::<u64>().ok())
                } else {
                    serde_json::from_slice::<serde_json::Value>(&value)
                        .ok()
                        .and_then(|row| row.get(&definition.name).cloned())
                        .and_then(|value| match value {
                            serde_json::Value::Number(value) => value.as_u64(),
                            serde_json::Value::String(value) => value.parse::<u64>().ok(),
                            _ => None,
                        })
                };
                if let Some(candidate) = candidate {
                    highest = highest.max(candidate);
                }
            }
            highest.saturating_add(1)
        };
        sequences.insert(key, next.saturating_add(1));
        Ok(next.to_string().into_bytes())
    }

    pub fn catalog_indexes(&self, table: &str) -> Vec<IndexDefinition> {
        self.indexes
            .lock()
            .ok()
            .and_then(|indexes| {
                indexes.get(table).cloned().or_else(|| {
                    indexes.iter().find_map(|(name, definitions)| {
                        (name.rsplit('.').next() == Some(table)).then(|| definitions.clone())
                    })
                })
            })
            .unwrap_or_default()
            .into_iter()
            .map(|state| state.definition)
            .collect()
    }

    fn rls_allows(&self, table: &str, value: &[u8]) -> bool {
        let Some(column) = self.rls_tables.get(table) else { return true };
        let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
            return false;
        };
        object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .and_then(|(_, value)| value.as_str())
            .is_some_and(|tenant| tenant == self.tenant)
    }

    fn enforce_rls(&self, table: &str, value: &[u8]) -> Result<()> {
        if self.rls_allows(table, value) {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
    }

    fn filter_rls_rows(&self, table: &str, rows: impl IntoIterator<Item = Row>) -> Vec<Row> {
        rows.into_iter().filter(|(_, value)| self.rls_allows(table, value)).collect()
    }

    fn materialize_insert_row(
        &self,
        table: &str,
        columns: Vec<String>,
        values: Vec<InsertValue>,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        if columns.is_empty() || columns.len() != values.len() {
            return Err(RymeError::InvalidArgument(String::from("insert column/value count")));
        }
        let definitions = self.catalog_columns(table);
        if definitions.is_empty()
            && columns.len() == 2
            && (columns[0].eq_ignore_ascii_case("id")
                || columns[0].eq_ignore_ascii_case("pk")
                || columns[0].eq_ignore_ascii_case("key"))
        {
            if let [InsertValue::Value(pk), InsertValue::Value(value)] = values.as_slice() {
                return Ok((pk.clone(), value.clone()));
            }
            return Err(RymeError::InvalidArgument(String::from("default key/value insert")));
        }
        let mut supplied = HashMap::new();
        for (column, value) in columns.iter().zip(values) {
            let name = column.to_ascii_lowercase();
            if supplied.contains_key(&name) {
                return Err(RymeError::InvalidArgument(format!("duplicate column {column}")));
            }
            if !definitions.is_empty()
                && !definitions
                    .iter()
                    .any(|definition| definition.name.eq_ignore_ascii_case(column))
            {
                return Err(RymeError::InvalidArgument(format!("unknown column {column}")));
            }
            let definition =
                definitions.iter().find(|definition| definition.name.eq_ignore_ascii_case(column));
            let resolved = match value {
                InsertValue::Value(value) => Some(value),
                InsertValue::Null => None,
                InsertValue::Default => {
                    if let Some(definition) = definition {
                        if definition.auto_increment {
                            Some(self.next_sequence_value(table, definition)?)
                        } else {
                            definition
                                .column_default
                                .as_deref()
                                .map(eval_default)
                                .transpose()?
                                .flatten()
                        }
                    } else {
                        None
                    }
                }
            };
            supplied.insert(name, resolved);
        }

        let mut row = Vec::new();
        if definitions.is_empty() {
            for (column, value) in &supplied {
                row.push((column.clone(), value.clone(), String::new()));
            }
        } else {
            for definition in &definitions {
                let value =
                    if let Some(value) = supplied.remove(&definition.name.to_ascii_lowercase()) {
                        value
                    } else if definition.auto_increment {
                        Some(self.next_sequence_value(table, definition)?)
                    } else if let Some(default) = definition.column_default.as_deref() {
                        eval_default(default)?
                    } else if definition.nullable {
                        None
                    } else {
                        return Err(RymeError::InvalidArgument(format!(
                            "null value in column {} violates not-null constraint",
                            definition.name
                        )));
                    };
                row.push((definition.name.clone(), value, definition.data_type.clone()));
            }
            for (column, value) in supplied {
                row.push((column, value, String::new()));
            }
        }

        let primary_key = definitions
            .iter()
            .find(|definition| definition.primary_key)
            .map(|definition| definition.name.to_ascii_lowercase())
            .or_else(|| {
                row.iter()
                    .find(|(name, _, _)| {
                        name.eq_ignore_ascii_case("id") || name.eq_ignore_ascii_case("pk")
                    })
                    .map(|(name, _, _)| name.to_ascii_lowercase())
            })
            .or_else(|| row.first().map(|(name, _, _)| name.to_ascii_lowercase()))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("insert primary key")))?;
        let pk = row
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(&primary_key))
            .and_then(|(_, value, _)| value.clone())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("null value in primary key")))?;

        let mut object = serde_json::Map::new();
        for (name, value, data_type) in row {
            object.insert(name, json_insert_value(value, &data_type));
        }
        let encoded = serde_json::to_vec(&serde_json::Value::Object(object))
            .map_err(|error| RymeError::Internal(error.to_string()))?;
        Ok((pk, encoded))
    }

    fn materialize_update_row(
        &self,
        table: &str,
        pk: &[u8],
        assignments: Vec<(String, InsertValue)>,
        current: &[u8],
    ) -> Result<Vec<u8>> {
        let definitions = self.catalog_columns(table);
        if definitions.is_empty() {
            if let Ok(serde_json::Value::Object(mut object)) =
                serde_json::from_slice::<serde_json::Value>(current)
            {
                let primary_key = object
                    .keys()
                    .find(|name| {
                        name.eq_ignore_ascii_case("id")
                            || name.eq_ignore_ascii_case("pk")
                            || name.eq_ignore_ascii_case("key")
                    })
                    .cloned();
                for (column, value) in assignments {
                    if primary_key.as_deref().is_some_and(|name| name.eq_ignore_ascii_case(&column))
                    {
                        return Err(RymeError::InvalidArgument(String::from(
                            "updating the primary key is not supported",
                        )));
                    }
                    let previous = object.get(&column);
                    let resolved = match value {
                        InsertValue::Value(value) => json_update_value(Some(value), previous),
                        InsertValue::Null => serde_json::Value::Null,
                        InsertValue::Default => {
                            return Err(RymeError::InvalidArgument(String::from(
                                "default update value requires table schema",
                            )));
                        }
                    };
                    object.insert(column, resolved);
                }
                return serde_json::to_vec(&serde_json::Value::Object(object))
                    .map_err(|error| RymeError::Internal(error.to_string()));
            }
            if assignments.len() == 1 {
                let (_, value) = assignments.into_iter().next().ok_or_else(|| {
                    RymeError::InvalidArgument(String::from("update assignments"))
                })?;
                return match value {
                    InsertValue::Value(value) => Ok(value),
                    InsertValue::Null => Ok(Vec::new()),
                    InsertValue::Default => {
                        Err(RymeError::InvalidArgument(String::from("default update value")))
                    }
                };
            }
            return Err(RymeError::InvalidArgument(String::from(
                "schema required for column update",
            )));
        }
        let mut object = serde_json::from_slice::<serde_json::Value>(current)
            .map_err(|_| RymeError::InvalidArgument(String::from("row is not a schema record")))?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                RymeError::InvalidArgument(String::from("row is not a schema record"))
            })?;
        let primary_key = definitions
            .iter()
            .find(|definition| definition.primary_key)
            .map(|definition| definition.name.as_str())
            .or_else(|| {
                definitions
                    .iter()
                    .find(|definition| definition.name.eq_ignore_ascii_case("id"))
                    .map(|definition| definition.name.as_str())
            });
        for (column, value) in assignments {
            let definition = definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(&column))
                .ok_or_else(|| RymeError::InvalidArgument(format!("unknown column {column}")))?;
            if primary_key.is_some_and(|name| name.eq_ignore_ascii_case(&column)) {
                return Err(RymeError::InvalidArgument(String::from(
                    "updating the primary key is not supported",
                )));
            }
            let resolved = match value {
                InsertValue::Value(value) => Some(value),
                InsertValue::Null => None,
                InsertValue::Default => {
                    definition.column_default.as_deref().map(eval_default).transpose()?.flatten()
                }
            };
            if resolved.is_none() && !definition.nullable {
                return Err(RymeError::InvalidArgument(format!(
                    "null value in column {} violates not-null constraint",
                    definition.name
                )));
            }
            object.insert(
                definition.name.clone(),
                json_insert_value(resolved, &definition.data_type),
            );
        }
        object.insert(
            primary_key.unwrap_or("id").to_string(),
            serde_json::Value::String(String::from_utf8_lossy(pk).to_string()),
        );
        serde_json::to_vec(&serde_json::Value::Object(object))
            .map_err(|error| RymeError::Internal(error.to_string()))
    }

    fn project_row(
        &self,
        table: &str,
        columns: &[String],
        pk: &[u8],
        value: &[u8],
    ) -> Vec<Vec<u8>> {
        let definitions = self.catalog_columns(table);
        let primary_key = definitions
            .iter()
            .find(|definition| definition.primary_key)
            .map(|definition| definition.name.as_str());
        let object = serde_json::from_slice::<serde_json::Value>(value).ok();
        columns
            .iter()
            .map(|column| {
                let schema_column = definitions
                    .iter()
                    .any(|definition| definition.name.eq_ignore_ascii_case(column));
                if column.eq_ignore_ascii_case("key")
                    || column.eq_ignore_ascii_case("pk")
                    || column.eq_ignore_ascii_case("id")
                    || primary_key.is_some_and(|name| name.eq_ignore_ascii_case(column))
                {
                    return pk.to_vec();
                }
                if !schema_column
                    && (column.eq_ignore_ascii_case("value")
                        || column.eq_ignore_ascii_case("val")
                        || column.eq_ignore_ascii_case("data"))
                {
                    return value.to_vec();
                }
                let Some(serde_json::Value::Object(object)) = object.as_ref() else {
                    return value.to_vec();
                };
                if let Some(projected) = json_projection_bytes(column, object) {
                    return projected;
                }
                object
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(column))
                    .map(|(_, value)| json_result_bytes(value))
                    .unwrap_or_default()
            })
            .collect()
    }

    fn select_columns_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    ) -> Result<QueryResult> {
        let exact_key = filter.len() == 1
            && filter[0].column.is_none()
            && filter[0].field == Field::Key
            && filter[0].op == Cmp::Eq;
        let rows = if exact_key {
            let pk = filter[0].operand.clone();
            self.manager
                .get(txn, &RecordKey::new(&self.tenant, &self.database, &table, &pk))?
                .filter(|value| self.rls_allows(&table, value))
                .map(|value| vec![(pk, value)])
                .unwrap_or_default()
        } else {
            let cap = if filter.is_empty() && offset == 0 && order == Order::default() {
                limit.clamp(1, 10000)
            } else {
                10000
            };
            self.scan_rows(txn, &table, &filter, cap)?
        };
        let mut rows: Vec<Row> = rows
            .into_iter()
            .filter(|(pk, value)| filter.iter().all(|predicate| predicate.matches(pk, value)))
            .collect();
        rows.sort_by(|left, right| {
            let (first, second) = match order.field {
                Field::Key => (&left.0, &right.0),
                Field::Value => (&left.1, &right.1),
            };
            match order.direction {
                Direction::Asc => first.cmp(second),
                Direction::Desc => second.cmp(first),
            }
        });
        let rows = rows
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(pk, value)| self.project_row(&table, &columns, &pk, &value))
            .collect();
        Ok(QueryResult::Table { columns, rows })
    }

    fn scan_all_rows(&self, table: &str) -> Result<Vec<Row>> {
        const PAGE: usize = 10_000;
        let mut txn = self.begin_with(self.isolation);
        let mut rows = self.manager.scan(&mut txn, &self.tenant, &self.database, table, PAGE)?;
        loop {
            if rows.len() < PAGE {
                break;
            }
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next = self.manager.scan_after(
                &mut txn,
                &self.tenant,
                &self.database,
                table,
                &last,
                PAGE,
            )?;
            if next.is_empty() {
                break;
            }
            rows.extend(next);
        }
        Ok(rows)
    }

    fn create_index(&self, definition: IndexDefinition) -> Result<()> {
        self.reject_if_read_only()?;
        let rows = self.scan_all_rows(&definition.table)?;
        let mut entries: BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>> = BTreeMap::new();
        for (pk, value) in rows {
            let Some(indexed) = index_value(&definition, &pk, &value) else { continue };
            let pks = entries.entry(indexed).or_default();
            if definition.unique && !pks.is_empty() && !pks.contains(&pk) {
                return Err(RymeError::Conflict(format!("unique index {}", definition.name)));
            }
            pks.insert(pk);
        }
        let mut indexes =
            self.indexes.lock().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let table_indexes = indexes.entry(definition.table.clone()).or_default();
        if table_indexes.iter().any(|state| state.definition.name == definition.name) {
            return Ok(());
        }
        table_indexes.push(IndexState { definition, entries });
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn indexed_candidates(&self, table: &str, filter: &[Predicate]) -> Option<Vec<Vec<u8>>> {
        let predicate = filter.iter().find(|predicate| predicate.op == Cmp::Eq)?;
        let indexes = self.indexes.lock().ok()?;
        let state = indexes.get(table)?.iter().find(|state| {
            state.definition.field == predicate.field && state.definition.column == predicate.column
        })?;
        Some(state.entries.get(&predicate.operand)?.iter().cloned().collect())
    }

    fn check_unique(&self, table: &str, pk: &[u8], value: &[u8]) -> Result<()> {
        let indexes =
            self.indexes.lock().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let Some(table_indexes) = indexes.get(table) else { return Ok(()) };
        for state in table_indexes.iter().filter(|state| state.definition.unique) {
            let Some(indexed) = index_value(&state.definition, pk, value) else { continue };
            if state
                .entries
                .get(&indexed)
                .is_some_and(|pks| pks.iter().any(|existing| existing.as_slice() != pk))
            {
                return Err(RymeError::Conflict(format!("unique index {}", state.definition.name)));
            }
        }
        Ok(())
    }

    fn apply_index_change(&self, change: &TransactionChange) {
        let Ok(mut indexes) = self.indexes.lock() else { return };
        let Some(table_indexes) = indexes.get_mut(&change.table) else { return };
        for state in table_indexes {
            if let Some(before) = change.before.as_ref() {
                let Some(indexed) = index_value(&state.definition, &change.pk, before) else {
                    continue;
                };
                if let Some(pks) = state.entries.get_mut(&indexed) {
                    pks.remove(&change.pk);
                    if pks.is_empty() {
                        state.entries.remove(&indexed);
                    }
                }
            }
            if let Some(after) = change.after.as_ref() {
                let Some(indexed) = index_value(&state.definition, &change.pk, after) else {
                    continue;
                };
                state.entries.entry(indexed).or_default().insert(change.pk.clone());
            }
        }
    }

    fn scan_rows(
        &self,
        txn: &mut Transaction,
        table: &str,
        filter: &[Predicate],
        limit: usize,
    ) -> Result<Vec<Row>> {
        if self.rls_tables.contains_key(table) {
            if limit == 0 {
                return Ok(Vec::new());
            }
            const PAGE: usize = 1024;
            if txn.writes().is_empty() {
                let mut visible = Vec::new();
                let mut after = None;
                loop {
                    let page = match after.as_deref() {
                        Some(after) => self.manager.scan_after(
                            txn,
                            &self.tenant,
                            &self.database,
                            table,
                            after,
                            PAGE,
                        )?,
                        None => {
                            self.manager.scan(txn, &self.tenant, &self.database, table, PAGE)?
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    let page_len = page.len();
                    let last = page.last().map(|(pk, _)| pk.clone());
                    visible.extend(page.into_iter().filter(|(pk, value)| {
                        self.rls_allows(table, value)
                            && filter.iter().all(|predicate| predicate.matches(pk, value))
                    }));
                    if visible.len() >= limit || page_len < PAGE {
                        break;
                    }
                    after = last;
                }
                visible.truncate(limit);
                return Ok(visible);
            }

            let mut merged = BTreeMap::new();
            let mut after = None;
            loop {
                let page = match after.as_deref() {
                    Some(after) => self.manager.scan_after(
                        txn,
                        &self.tenant,
                        &self.database,
                        table,
                        after,
                        PAGE,
                    )?,
                    None => self.manager.scan(txn, &self.tenant, &self.database, table, PAGE)?,
                };
                if page.is_empty() {
                    break;
                }
                let page_len = page.len();
                let last = page.last().map(|(pk, _)| pk.clone());
                merged.extend(page);
                if page_len < PAGE {
                    break;
                }
                after = last;
            }
            for (key, write) in txn.writes() {
                if key.tenant != self.tenant || key.database != self.database || key.table != table
                {
                    continue;
                }
                match write.value.as_ref() {
                    Some(value) => {
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            return Ok(merged
                .into_iter()
                .filter(|(pk, value)| {
                    self.rls_allows(table, value)
                        && filter.iter().all(|predicate| predicate.matches(pk, value))
                })
                .take(limit)
                .collect());
        }
        if txn.writes().is_empty() {
            if let Some(candidates) = self.indexed_candidates(table, filter) {
                let mut rows = Vec::with_capacity(candidates.len());
                for pk in candidates {
                    let key = RecordKey::new(&self.tenant, &self.database, table, &pk);
                    if let Some(value) = self.manager.get(txn, &key)? {
                        rows.push((pk, value));
                    }
                }
                return Ok(self.filter_rls_rows(table, rows));
            }

            if !filter.is_empty() {
                const PAGE: usize = 1024;
                let mut matched = Vec::new();
                let mut after = None;
                loop {
                    let page = match after.as_deref() {
                        Some(after) => self.manager.scan_after(
                            txn,
                            &self.tenant,
                            &self.database,
                            table,
                            after,
                            PAGE,
                        )?,
                        None => {
                            self.manager.scan(txn, &self.tenant, &self.database, table, PAGE)?
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    let page_len = page.len();
                    let last = page.last().map(|(pk, _)| pk.clone());
                    matched.extend(page.into_iter().filter(|(pk, value)| {
                        filter.iter().all(|predicate| predicate.matches(pk, value))
                    }));
                    if matched.len() >= limit || page_len < PAGE {
                        break;
                    }
                    after = last;
                }
                matched.truncate(limit);
                return Ok(matched);
            }
        }
        if !txn.writes().is_empty() && !filter.is_empty() {
            const PAGE: usize = 1024;
            let mut merged = BTreeMap::new();
            let mut after = None;
            loop {
                let page = match after.as_deref() {
                    Some(after) => self.manager.scan_after(
                        txn,
                        &self.tenant,
                        &self.database,
                        table,
                        after,
                        PAGE,
                    )?,
                    None => self.manager.scan(txn, &self.tenant, &self.database, table, PAGE)?,
                };
                if page.is_empty() {
                    break;
                }
                let page_len = page.len();
                let last = page.last().map(|(pk, _)| pk.clone());
                merged.extend(page);
                if page_len < PAGE {
                    break;
                }
                after = last;
            }
            for (key, write) in txn.writes() {
                if key.tenant != self.tenant || key.database != self.database || key.table != table
                {
                    continue;
                }
                match write.value.as_ref() {
                    Some(value) => {
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            return Ok(merged
                .into_iter()
                .filter(|(pk, value)| filter.iter().all(|predicate| predicate.matches(pk, value)))
                .take(limit)
                .collect());
        }
        let mut rows = self.manager.scan(txn, &self.tenant, &self.database, table, limit)?;
        if !txn.writes().is_empty() {
            let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = rows.drain(..).collect();
            for (key, write) in txn.writes() {
                if key.tenant != self.tenant || key.database != self.database || key.table != table
                {
                    continue;
                }
                match write.value.as_ref() {
                    Some(value) => {
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            rows = merged.into_iter().take(limit).collect();
        }
        Ok(self.filter_rls_rows(table, rows))
    }

    fn register_table(&self, table: String, columns: Vec<ColumnDefinition>) {
        if let Ok(mut catalog) = self.catalog.lock() {
            if let std::collections::hash_map::Entry::Vacant(entry) = catalog.entry(table) {
                let unique_columns: Vec<String> = columns
                    .iter()
                    .filter(|column| column.unique)
                    .map(|column| column.name.clone())
                    .collect();
                let table_name = entry.key().clone();
                entry.insert(columns);
                if !unique_columns.is_empty() {
                    if let Ok(mut indexes) = self.indexes.lock() {
                        let table_indexes = indexes.entry(table_name.clone()).or_default();
                        for column in unique_columns {
                            let name = format!("{table_name}_{column}_unique");
                            if table_indexes.iter().any(|state| state.definition.name == name) {
                                continue;
                            }
                            table_indexes.push(IndexState {
                                definition: IndexDefinition {
                                    name,
                                    table: table_name.clone(),
                                    field: Field::Value,
                                    column: Some(column),
                                    unique: true,
                                },
                                entries: BTreeMap::new(),
                            });
                        }
                    }
                }
                self.schema_dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    fn ensure_table(&self, table: &str) {
        self.register_table(table.to_string(), Vec::new());
    }

    fn persist_schema_if_configured(&self) -> Result<()> {
        if !self.schema_dirty.load(Ordering::SeqCst) {
            return Ok(());
        }
        let path = self
            .schema_path
            .lock()
            .map_err(|_| RymeError::Internal(String::from("schema path lock")))?
            .clone();
        let Some(path) = path else { return Ok(()) };
        let _guard = self
            .schema_persist_lock
            .lock()
            .map_err(|_| RymeError::Internal(String::from("schema persist lock")))?;
        if !self.schema_dirty.load(Ordering::SeqCst) {
            return Ok(());
        }
        persist_schema_snapshot(&path, &self.schema_snapshot())?;
        self.schema_dirty.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn reject_if_read_only(&self) -> Result<()> {
        if self.read_only {
            return Err(RymeError::ReadOnly(String::from("read-only follower")));
        }
        Ok(())
    }

    fn emit(
        &self,
        table: &str,
        pk: Vec<u8>,
        op: Operation,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        commit_ts: u64,
    ) -> Result<()> {
        let Some(realtime) = &self.realtime else { return Ok(()) };
        realtime.publish(NewChange {
            tenant: self.tenant.clone(),
            database: self.database.clone(),
            branch: self.branch.clone(),
            table: table.to_string(),
            op,
            pk,
            before,
            after,
            commit_ts,
            tx_id: commit_ts,
        })?;
        self.refresh_table(table, commit_ts);
        Ok(())
    }

    fn refresh_table(&self, table: &str, commit_ts: u64) {
        let Some(realtime) = &self.realtime else { return };
        let Some(limit) =
            realtime.query_limit_branch(&self.tenant, &self.database, &self.branch, table)
        else {
            return;
        };
        let mut txn = self.begin_with(self.isolation);
        let rows = self.filter_rls_rows(
            table,
            self.manager
                .scan(&mut txn, &self.tenant, &self.database, table, limit)
                .unwrap_or_default(),
        );
        let _ = realtime.publish_query_branch(
            &self.tenant,
            &self.database,
            &self.branch,
            table,
            commit_ts,
            rows,
            limit,
        );
    }

    pub async fn execute(&self, statement: Statement) -> Result<QueryResult> {
        self.execute_with(statement, self.isolation).await
    }

    pub fn begin_transaction(&self, isolation: Isolation) -> Transaction {
        self.begin_with(isolation)
    }

    pub async fn execute_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        match &statement {
            Statement::CreateTable { table, columns } => {
                self.register_table(table.clone(), columns.clone());
            }
            Statement::CreateIndex { .. } => {}
            statement if statement.is_write() => self.ensure_table(statement.table()),
            _ => {}
        }
        match statement {
            Statement::Returning { statement, fields } => {
                self.execute_returning_in_transaction(txn, *statement, fields).await
            }
            statement => self.execute_in_transaction_base(txn, statement).await,
        }
    }

    async fn execute_in_transaction_base(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        match statement {
            Statement::CreateTable { .. } => Ok((QueryResult::Ok, Vec::new())),
            Statement::CreateIndex { name, table, field, column, unique } => {
                self.create_index(IndexDefinition { name, table, field, column, unique })?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::Explain { plan, .. } => Ok((
                QueryResult::Row { pk: b"plan".to_vec(), value: plan.into_bytes() },
                Vec::new(),
            )),
            Statement::CopyFrom { table, rows } => {
                self.reject_if_read_only()?;
                if rows.len() > 10000 {
                    return Err(RymeError::InvalidArgument(String::from("batch too large")));
                }
                let mut changes = Vec::with_capacity(rows.len());
                for (pk, value) in rows {
                    if pk.is_empty() || pk.len() > 1024 {
                        return Err(RymeError::InvalidArgument(String::from("key")));
                    }
                    if value.len() > 4 * 1024 * 1024 {
                        return Err(RymeError::Overload(String::from("value")));
                    }
                    self.enforce_rls(&table, &value)?;
                    let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                    let before = self.manager.get(txn, &key)?;
                    self.check_unique(&table, &pk, &value)?;
                    self.manager.put(txn, key, value.clone());
                    changes.push(TransactionChange {
                        table: table.clone(),
                        pk,
                        op: if before.is_some() { Operation::Update } else { Operation::Insert },
                        before,
                        after: Some(value),
                    });
                }
                Ok((QueryResult::Ok, changes))
            }
            Statement::InsertRow { table, columns, values, upsert } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_in_transaction_base(txn, statement)).await
            }
            Statement::InsertRows { table, columns, rows, upsert } => {
                let changes = self
                    .execute_insert_rows_in_transaction(txn, table, columns, rows, upsert)
                    .await?;
                Ok((QueryResult::Ok, changes))
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(txn, &key)?;
                if before.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                self.check_unique(&table, &pk, &value)?;
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: Operation::Insert,
                        before,
                        after: Some(value),
                    }],
                ))
            }
            Statement::Upsert { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(txn, &key)?;
                if let Some(before) = before.as_deref() {
                    self.enforce_rls(&table, before)?;
                }
                self.check_unique(&table, &pk, &value)?;
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: if before.is_some() { Operation::Update } else { Operation::Insert },
                        before,
                        after: Some(value),
                    }],
                ))
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get(txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                Box::pin(
                    self.execute_in_transaction_base(txn, Statement::Update { table, pk, value }),
                )
                .await
            }
            Statement::Update { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(txn, &key)?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(&table, before.as_deref().unwrap_or_default())?;
                self.check_unique(&table, &pk, &value)?;
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: Operation::Update,
                        before,
                        after: Some(value),
                    }],
                ))
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(txn, &key)?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(&table, before.as_deref().unwrap_or_default())?;
                self.manager.delete(txn, key);
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: Operation::Delete,
                        before,
                        after: None,
                    }],
                ))
            }
            statement => Ok((self.execute_read_in_transaction(txn, statement)?, Vec::new())),
        }
    }

    async fn execute_insert_rows_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
    ) -> Result<Vec<TransactionChange>> {
        let mut changes = Vec::new();
        for values in rows {
            let (pk, value) = self.materialize_insert_row(&table, columns.clone(), values)?;
            let statement = if upsert {
                Statement::Upsert { table: table.clone(), pk, value }
            } else {
                Statement::Insert { table: table.clone(), pk, value }
            };
            let (_, mut row_changes) =
                Box::pin(self.execute_in_transaction_base(txn, statement)).await?;
            changes.append(&mut row_changes);
        }
        Ok(changes)
    }

    async fn execute_returning_rows_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
        fields: &[ReturningField],
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        let result_columns = returning_columns(fields);
        let mut result_rows = Vec::new();
        let mut changes = Vec::new();
        for values in rows {
            let (pk, value) = self.materialize_insert_row(&table, columns.clone(), values)?;
            let statement = if upsert {
                Statement::Upsert { table: table.clone(), pk: pk.clone(), value: value.clone() }
            } else {
                Statement::Insert { table: table.clone(), pk: pk.clone(), value: value.clone() }
            };
            let (_, mut row_changes) =
                Box::pin(self.execute_in_transaction_base(txn, statement)).await?;
            changes.append(&mut row_changes);
            let QueryResult::Returning { rows, .. } = returning_result(fields, pk, value) else {
                unreachable!("returning result always contains rows");
            };
            result_rows.extend(rows);
        }
        Ok((QueryResult::Returning { columns: result_columns, rows: result_rows }, changes))
    }

    async fn execute_returning_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
        fields: Vec<ReturningField>,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        match statement {
            Statement::InsertRow { table, columns, values, upsert } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_returning_in_transaction(txn, statement, fields)).await
            }
            Statement::InsertRows { table, columns, rows, upsert } => {
                self.execute_returning_rows_in_transaction(
                    txn, table, columns, rows, upsert, &fields,
                )
                .await
            }
            Statement::Insert { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Insert { table, pk, value })
                    .await?;
                Ok((returning_result(&fields, pk_for_result, value_for_result), changes))
            }
            Statement::Upsert { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Upsert { table, pk, value })
                    .await?;
                Ok((returning_result(&fields, pk_for_result, value_for_result), changes))
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get(txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Update { table, pk, value })
                    .await?;
                Ok((returning_result(&fields, pk_for_result, value_for_result), changes))
            }
            Statement::Update { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Update { table, pk, value })
                    .await?;
                Ok((returning_result(&fields, pk_for_result, value_for_result), changes))
            }
            Statement::Delete { table, pk } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let value = self
                    .manager
                    .get(txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Delete { table, pk: pk.clone() })
                    .await?;
                Ok((returning_result(&fields, pk, value), changes))
            }
            _ => Err(RymeError::InvalidArgument(String::from("RETURNING requires a row mutation"))),
        }
    }

    pub async fn commit_transaction(
        &self,
        txn: Transaction,
        changes: Vec<TransactionChange>,
    ) -> Result<u64> {
        self.check_transaction_uniqueness(&changes)?;
        let commit_ts = self.manager.commit(txn).await?;
        for change in changes {
            self.apply_index_change(&change);
            self.emit(&change.table, change.pk, change.op, change.before, change.after, commit_ts)?;
        }
        self.persist_schema_if_configured()?;
        Ok(commit_ts)
    }

    fn check_transaction_uniqueness(&self, changes: &[TransactionChange]) -> Result<()> {
        let indexes =
            self.indexes.lock().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        for (table, table_indexes) in indexes.iter() {
            let table_changes: Vec<&TransactionChange> =
                changes.iter().filter(|change| &change.table == table).collect();
            if table_changes.is_empty() {
                continue;
            }
            for state in table_indexes.iter().filter(|state| state.definition.unique) {
                let touched: std::collections::BTreeSet<Vec<u8>> =
                    table_changes.iter().map(|change| change.pk.clone()).collect();
                let mut occupied: HashMap<Vec<u8>, Vec<u8>> = state
                    .entries
                    .iter()
                    .filter_map(|(indexed, pks)| {
                        pks.iter()
                            .find(|pk| !touched.contains(*pk))
                            .map(|pk| (indexed.clone(), pk.clone()))
                    })
                    .collect();
                let mut assigned: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                for change in table_changes.iter() {
                    if let Some(previous) = assigned.remove(&change.pk) {
                        occupied.remove(&previous);
                    }
                    let Some(after) = change.after.as_ref() else { continue };
                    let Some(indexed) = index_value(&state.definition, &change.pk, after) else {
                        continue;
                    };
                    if occupied.get(&indexed).is_some_and(|existing| existing != &change.pk) {
                        return Err(RymeError::Conflict(format!(
                            "unique index {}",
                            state.definition.name
                        )));
                    }
                    occupied.insert(indexed.clone(), change.pk.clone());
                    assigned.insert(change.pk.clone(), indexed);
                }
            }
        }
        Ok(())
    }

    fn execute_read_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<QueryResult> {
        match statement {
            Statement::SelectByKey { table, pk } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                match self.manager.get(txn, &key)? {
                    Some(value) if self.rls_allows(&table, &value) => {
                        Ok(QueryResult::Row { pk, value })
                    }
                    None => Ok(QueryResult::Rows { rows: Vec::new() }),
                    Some(_) => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectColumns { table, columns, limit, offset, order, filter } => self
                .select_columns_in_transaction(txn, table, columns, limit, offset, order, filter),
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let plain = filter.is_empty() && offset == 0 && order == Order::default();
                let cap = if plain { limit.clamp(1, 10000) } else { 10000 };
                let rows = self.scan_rows(txn, &table, &filter, cap)?;
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                rows.sort_by(|a, b| {
                    let (left, right) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => left.cmp(right),
                        Direction::Desc => right.cmp(left),
                    }
                });
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Aggregate { table, func, field, filter } => {
                let rows = self.filter_rls_rows(
                    &table,
                    self.manager.scan(txn, &self.tenant, &self.database, &table, 10000)?,
                );
                let rows: Vec<(Vec<u8>, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                Ok(QueryResult::Scalar {
                    label: func.label().to_string(),
                    value: aggregate_rows(&rows, func, field),
                })
            }
            Statement::GroupBy { table, select, group, filter, limit, offset, order } => {
                let rows = self.filter_rls_rows(
                    &table,
                    self.manager.scan(txn, &self.tenant, &self.database, &table, 10000)?,
                );
                let mut groups: BTreeMap<Vec<u8>, Vec<Row>> = BTreeMap::new();
                for (pk, value) in rows {
                    if !filter.iter().all(|p| p.matches(&pk, &value)) {
                        continue;
                    }
                    let key = match group {
                        Field::Key => pk.clone(),
                        Field::Value => value.clone(),
                    };
                    groups.entry(key).or_default().push((pk, value));
                }
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for (group_key, members) in &groups {
                    let mut record = serde_json::Map::new();
                    for item in &select {
                        match item {
                            SelectItem::Field(Field::Key) => {
                                record.insert(
                                    String::from("key"),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(group_key).to_string(),
                                    ),
                                );
                            }
                            SelectItem::Field(Field::Value) => {
                                record.insert(
                                    String::from("value"),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(
                                            &members
                                                .first()
                                                .map(|(_, value)| value.clone())
                                                .unwrap_or_default(),
                                        )
                                        .to_string(),
                                    ),
                                );
                            }
                            SelectItem::Agg(func, field) => {
                                record.insert(
                                    func.label().to_string(),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(&aggregate_rows(
                                            members, *func, *field,
                                        ))
                                        .to_string(),
                                    ),
                                );
                            }
                        }
                    }
                    out.push((
                        group_key.clone(),
                        serde_json::Value::Object(record).to_string().into_bytes(),
                    ));
                }
                out.sort_by(|a, b| {
                    let (first, second) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => first.cmp(second),
                        Direction::Desc => second.cmp(first),
                    }
                });
                Ok(QueryResult::Rows { rows: out.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Join { left, right, limit, offset, order, filter } => {
                let left_rows = self.filter_rls_rows(
                    &left,
                    self.manager.scan(txn, &self.tenant, &self.database, &left, 10000)?,
                );
                let right_rows = self.filter_rls_rows(
                    &right,
                    self.manager.scan(txn, &self.tenant, &self.database, &right, 10000)?,
                );
                let mut index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                for (pk, value) in right_rows {
                    index.entry(pk).or_insert(value);
                }
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = left_rows
                    .into_iter()
                    .filter_map(|(pk, left_value)| {
                        index.get(&pk).map(|right_value| {
                            let merged = serde_json::json!({
                                "left": String::from_utf8_lossy(&left_value),
                                "right": String::from_utf8_lossy(right_value),
                            })
                            .to_string()
                            .into_bytes();
                            (pk, merged)
                        })
                    })
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                rows.sort_by(|a, b| {
                    let (first, second) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => first.cmp(second),
                        Direction::Desc => second.cmp(first),
                    }
                });
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            _ => Err(RymeError::InvalidArgument(String::from(
                "statement is not readable in a transaction",
            ))),
        }
    }

    pub async fn execute_with(
        &self,
        statement: Statement,
        isolation: Isolation,
    ) -> Result<QueryResult> {
        match &statement {
            Statement::CreateTable { table, columns } => {
                self.register_table(table.clone(), columns.clone());
            }
            Statement::CreateIndex { .. } => {}
            statement if statement.is_write() => self.ensure_table(statement.table()),
            _ => {}
        }
        let schema_statement =
            matches!(&statement, Statement::CreateTable { .. } | Statement::CreateIndex { .. });
        let result = match statement {
            Statement::Returning { statement, fields } => {
                self.execute_returning(*statement, fields, isolation).await
            }
            statement => self.execute_with_base(statement, isolation).await,
        };
        if result.is_ok() && schema_statement {
            self.persist_schema_if_configured()?;
        }
        result
    }

    async fn execute_returning(
        &self,
        statement: Statement,
        fields: Vec<ReturningField>,
        isolation: Isolation,
    ) -> Result<QueryResult> {
        match statement {
            Statement::InsertRow { table, columns, values, upsert } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_returning(statement, fields, isolation)).await
            }
            Statement::InsertRows { table, columns, rows, upsert } => {
                let mut txn = self.begin_with(isolation);
                let (result, changes) = self
                    .execute_returning_rows_in_transaction(
                        &mut txn, table, columns, rows, upsert, &fields,
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Insert { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                self.execute_with_base(Statement::Insert { table, pk, value }, isolation).await?;
                Ok(returning_result(&fields, pk_for_result, value_for_result))
            }
            Statement::Upsert { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                self.execute_with_base(Statement::Upsert { table, pk, value }, isolation).await?;
                Ok(returning_result(&fields, pk_for_result, value_for_result))
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get(&mut txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_result, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Update { table, pk, value })
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(returning_result(&fields, pk_for_result, value_for_result))
            }
            Statement::Update { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                self.execute_with_base(Statement::Update { table, pk, value }, isolation).await?;
                Ok(returning_result(&fields, pk_for_result, value_for_result))
            }
            Statement::Delete { table, pk } => {
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let value = self
                    .manager
                    .get(&mut txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                self.execute_with_base(Statement::Delete { table, pk: pk.clone() }, isolation)
                    .await?;
                Ok(returning_result(&fields, pk, value))
            }
            _ => Err(RymeError::InvalidArgument(String::from("RETURNING requires a row mutation"))),
        }
    }

    async fn execute_with_base(
        &self,
        statement: Statement,
        isolation: Isolation,
    ) -> Result<QueryResult> {
        match statement {
            Statement::CreateTable { .. } => Ok(QueryResult::Ok),
            Statement::CreateIndex { name, table, field, column, unique } => {
                self.create_index(IndexDefinition { name, table, field, column, unique })?;
                Ok(QueryResult::Ok)
            }
            Statement::CopyFrom { table, rows } => {
                self.reject_if_read_only()?;
                self.bulk_upsert(table, rows).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Explain { plan, .. } => {
                Ok(QueryResult::Row { pk: b"plan".to_vec(), value: plan.into_bytes() })
            }
            Statement::InsertRow { table, columns, values, upsert } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_with_base(statement, isolation)).await
            }
            Statement::InsertRows { table, columns, rows, upsert } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let changes = self
                    .execute_insert_rows_in_transaction(&mut txn, table, columns, rows, upsert)
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(&mut txn, &key)?;
                if before.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                self.check_unique(&table, &pk, &value)?;
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value.clone());
                let commit_ts = self.manager.commit(txn).await?;
                self.apply_index_change(&TransactionChange {
                    table: table.clone(),
                    pk: pk.clone(),
                    op: Operation::Insert,
                    before: before.clone(),
                    after: Some(value.clone()),
                });
                if let Some(after) = after {
                    self.emit(&table, pk, Operation::Insert, None, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Upsert { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(&mut txn, &key)?;
                let existed = before.is_some();
                if let Some(before) = before.as_deref() {
                    self.enforce_rls(&table, before)?;
                }
                self.check_unique(&table, &pk, &value)?;
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value.clone());
                let commit_ts = self.manager.commit(txn).await?;
                self.apply_index_change(&TransactionChange {
                    table: table.clone(),
                    pk: pk.clone(),
                    op: if existed { Operation::Update } else { Operation::Insert },
                    before: before.clone(),
                    after: Some(value.clone()),
                });
                if let Some(after) = after {
                    let op = if existed { Operation::Update } else { Operation::Insert };
                    self.emit(&table, pk, op, before, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::SelectByKey { table, pk } => {
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                match self.manager.get(&mut txn, &key)? {
                    Some(value) if self.rls_allows(&table, &value) => {
                        Ok(QueryResult::Row { pk, value })
                    }
                    None => Ok(QueryResult::Rows { rows: Vec::new() }),
                    Some(_) => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectColumns { table, columns, limit, offset, order, filter } => {
                let mut txn = self.begin_with(isolation);
                self.select_columns_in_transaction(
                    &mut txn, table, columns, limit, offset, order, filter,
                )
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let mut txn = self.begin_with(isolation);
                let plain = filter.is_empty() && offset == 0 && order == Order::default();
                let cap = if plain { limit.clamp(1, 10000) } else { 10000 };
                let rows = self.scan_rows(&mut txn, &table, &filter, cap)?;
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                rows.sort_by(|a, b| {
                    let (left, right) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => left.cmp(right),
                        Direction::Desc => right.cmp(left),
                    }
                });
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Aggregate { table, func, field, filter } => {
                let mut txn = self.begin_with(isolation);
                let rows = self.filter_rls_rows(
                    &table,
                    self.manager.scan(&mut txn, &self.tenant, &self.database, &table, 10000)?,
                );
                let rows: Vec<(Vec<u8>, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                Ok(QueryResult::Scalar {
                    label: func.label().to_string(),
                    value: aggregate_rows(&rows, func, field),
                })
            }
            Statement::GroupBy { table, select, group, filter, limit, offset, order } => {
                let mut txn = self.begin_with(isolation);
                let rows = self.filter_rls_rows(
                    table.as_str(),
                    self.manager.scan(
                        &mut txn,
                        &self.tenant,
                        &self.database,
                        table.as_str(),
                        10000,
                    )?,
                );
                let mut groups: BTreeMap<Vec<u8>, Vec<Row>> = BTreeMap::new();
                for (pk, value) in rows {
                    if !filter.iter().all(|p| p.matches(&pk, &value)) {
                        continue;
                    }
                    let key = match group {
                        Field::Key => pk.clone(),
                        Field::Value => value.clone(),
                    };
                    groups.entry(key).or_default().push((pk, value));
                }
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for (group_key, members) in &groups {
                    let mut record = serde_json::Map::new();
                    for item in &select {
                        match item {
                            SelectItem::Field(Field::Key) => {
                                record.insert(
                                    String::from("key"),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(group_key).to_string(),
                                    ),
                                );
                            }
                            SelectItem::Field(Field::Value) => {
                                record.insert(
                                    String::from("value"),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(
                                            &members
                                                .first()
                                                .map(|(_, v)| v.clone())
                                                .unwrap_or_default(),
                                        )
                                        .to_string(),
                                    ),
                                );
                            }
                            SelectItem::Agg(func, field) => {
                                record.insert(
                                    func.label().to_string(),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(&aggregate_rows(
                                            members, *func, *field,
                                        ))
                                        .to_string(),
                                    ),
                                );
                            }
                        }
                    }
                    out.push((
                        group_key.clone(),
                        serde_json::Value::Object(record).to_string().into_bytes(),
                    ));
                }
                out.sort_by(|a, b| {
                    let (first, second) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => first.cmp(second),
                        Direction::Desc => second.cmp(first),
                    }
                });
                Ok(QueryResult::Rows { rows: out.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Join { left, right, limit, offset, order, filter } => {
                let mut txn = self.begin_with(isolation);
                let left_rows = self.filter_rls_rows(
                    left.as_str(),
                    self.manager.scan(
                        &mut txn,
                        &self.tenant,
                        &self.database,
                        left.as_str(),
                        10000,
                    )?,
                );
                let right_rows = self.filter_rls_rows(
                    right.as_str(),
                    self.manager.scan(
                        &mut txn,
                        &self.tenant,
                        &self.database,
                        right.as_str(),
                        10000,
                    )?,
                );
                let mut index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
                for (pk, value) in right_rows {
                    index.entry(pk).or_insert(value);
                }
                let mut rows: Vec<(Vec<u8>, Vec<u8>)> = left_rows
                    .into_iter()
                    .filter_map(|(pk, left_value)| {
                        index.get(&pk).map(|right_value| {
                            let merged = serde_json::json!({
                                "left": String::from_utf8_lossy(&left_value),
                                "right": String::from_utf8_lossy(right_value),
                            })
                            .to_string()
                            .into_bytes();
                            (pk, merged)
                        })
                    })
                    .filter(|(pk, value)| filter.iter().all(|p| p.matches(pk, value)))
                    .collect();
                rows.sort_by(|a, b| {
                    let (first, second) = match order.field {
                        Field::Key => (&a.0, &b.0),
                        Field::Value => (&a.1, &b.1),
                    };
                    match order.direction {
                        Direction::Asc => first.cmp(second),
                        Direction::Desc => second.cmp(first),
                    }
                });
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::UpdateRow { table, pk, assignments } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get(&mut txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                let (result, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Update { table, pk, value })
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Update { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value)?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(&mut txn, &key)?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(&table, before.as_deref().unwrap_or_default())?;
                self.check_unique(&table, &pk, &value)?;
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value.clone());
                let commit_ts = self.manager.commit(txn).await?;
                self.apply_index_change(&TransactionChange {
                    table: table.clone(),
                    pk: pk.clone(),
                    op: Operation::Update,
                    before: before.clone(),
                    after: Some(value.clone()),
                });
                if let Some(after) = after {
                    self.emit(&table, pk, Operation::Update, before, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get(&mut txn, &key)?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(&table, before.as_deref().unwrap_or_default())?;
                self.manager.delete(&mut txn, key);
                let commit_ts = self.manager.commit(txn).await?;
                self.apply_index_change(&TransactionChange {
                    table: table.clone(),
                    pk: pk.clone(),
                    op: Operation::Delete,
                    before: before.clone(),
                    after: None,
                });
                if self.realtime.is_some() {
                    self.emit(&table, pk, Operation::Delete, before, None, commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Returning { .. } => {
                Err(RymeError::InvalidArgument(String::from("nested RETURNING statement")))
            }
        }
    }

    pub async fn bulk_upsert(&self, table: String, rows: Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize> {
        self.reject_if_read_only()?;
        if rows.len() > 10000 {
            return Err(RymeError::InvalidArgument(String::from("batch too large")));
        }
        for chunk in rows.chunks(500) {
            let mut txn = self.begin_with(self.isolation);
            let mut staged: Vec<TransactionChange> = Vec::new();
            for (pk, value) in chunk {
                if pk.is_empty() || pk.len() > 1024 {
                    return Err(RymeError::InvalidArgument(String::from("key")));
                }
                if value.len() > 4 * 1024 * 1024 {
                    return Err(RymeError::Overload(String::from("value")));
                }
                let key = RecordKey::new(&self.tenant, &self.database, &table, pk);
                let before = self.manager.get(&mut txn, &key)?;
                self.check_unique(&table, pk, value)?;
                self.manager.put(&mut txn, key, value.clone());
                staged.push(TransactionChange {
                    table: table.clone(),
                    pk: pk.clone(),
                    op: if before.is_some() { Operation::Update } else { Operation::Insert },
                    before,
                    after: Some(value.clone()),
                });
            }
            self.check_transaction_uniqueness(&staged)?;
            let commit_ts = self.manager.commit(txn).await?;
            for change in staged {
                self.apply_index_change(&change);
                if self.realtime.is_some() {
                    self.emit(
                        &change.table,
                        change.pk,
                        change.op,
                        change.before,
                        change.after,
                        commit_ts,
                    )?;
                }
            }
        }
        Ok(rows.len())
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        let statement = parse(sql)?;
        match statement {
            Statement::Explain { plan, .. } => Ok(plan),
            other => Ok(describe_plan(&other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_classification() {
        assert!(parse("INSERT INTO docs KEY 'k' VALUE 'v'").unwrap().is_write());
        assert!(parse("UPSERT INTO docs KEY 'k' VALUE 'v'").unwrap().is_write());
        assert!(parse("UPDATE docs KEY 'k' VALUE 'v'").unwrap().is_write());
        assert!(parse("DELETE FROM docs KEY 'k'").unwrap().is_write());
        assert!(parse("COPY docs FROM stdin").unwrap().is_write());
        assert!(!parse("SELECT * FROM docs KEY 'k'").unwrap().is_write());
        assert!(!parse("SELECT * FROM docs LIMIT 10").unwrap().is_write());
        assert!(!parse("EXPLAIN SELECT * FROM docs LIMIT 10").unwrap().is_write());
    }

    #[tokio::test]
    async fn crud_roundtrip() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users").unwrap()).await.unwrap();
        executor.execute(parse("INSERT INTO users KEY '1' VALUE 'ada'").unwrap()).await.unwrap();
        let row = executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        assert!(matches!(row, QueryResult::Row { .. }));
        executor.execute(parse("UPDATE users KEY '1' VALUE 'grace'").unwrap()).await.unwrap();
        executor.execute(parse("DELETE FROM users KEY '1'").unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn postgres_values_insert_and_conflict_upsert() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let insert = parse("INSERT INTO users (id, value) VALUES ('1', 'ada')").unwrap();
        assert!(
            matches!(insert, Statement::InsertRow { ref table, ref columns, ref values, upsert: false }
            if table == "users"
                && columns == &[String::from("id"), String::from("value")]
                && values == &[InsertValue::Value(b"1".to_vec()), InsertValue::Value(b"ada".to_vec())])
        );
        executor.execute(insert).await.unwrap();

        let upsert = parse(
            "INSERT INTO users (id, value) VALUES ('1', 'grace') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .unwrap();
        assert!(matches!(upsert, Statement::InsertRow { upsert: true, .. }));
        executor.execute(upsert).await.unwrap();
        let row = executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        match row {
            QueryResult::Row { value, .. } => assert_eq!(value, b"grace"),
            _ => panic!("expected row"),
        }
    }

    #[tokio::test]
    async fn standard_row_insert_applies_defaults_and_enforces_not_null() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE events (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let returned = executor
            .execute(parse("INSERT INTO events (payload) VALUES ('hello') RETURNING *").unwrap())
            .await
            .unwrap();
        let (pk, value) = match returned {
            QueryResult::Returning { ref rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].len(), 36);
                (rows[0][0].clone(), rows[0][1].clone())
            }
            _ => panic!("expected returning row"),
        };
        let object: serde_json::Value = serde_json::from_slice(&value).unwrap();
        assert_eq!(object["id"], serde_json::Value::String(String::from_utf8(pk).unwrap()));
        assert_eq!(object["payload"], "hello");
        assert!(object["created_at"].as_str().is_some_and(|value| !value.is_empty()));

        let explicit_default = executor
            .execute(
                parse("INSERT INTO events (id, payload, created_at) VALUES (DEFAULT, 'world', DEFAULT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(explicit_default, QueryResult::Ok));

        let missing =
            executor.execute(parse("INSERT INTO events (id) VALUES (DEFAULT)").unwrap()).await;
        assert!(
            matches!(missing, Err(RymeError::InvalidArgument(message)) if message.contains("not-null"))
        );
    }

    #[tokio::test]
    async fn named_projection_reads_schema_row_fields() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT, count INTEGER)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO events (id, payload, count) VALUES ('e1', 'hello', 3)").unwrap(),
            )
            .await
            .unwrap();

        let statement = parse("SELECT payload, count FROM events WHERE id = 'e1'").unwrap();
        assert!(matches!(statement, Statement::SelectColumns { ref columns, .. }
            if columns == &[String::from("payload"), String::from("count")]));
        let result = executor.execute(statement).await.unwrap();
        assert!(matches!(result, QueryResult::Table { ref columns, ref rows }
            if columns == &[String::from("payload"), String::from("count")]
                && rows == &vec![vec![b"hello".to_vec(), b"3".to_vec()]]));

        let result = executor
            .execute(parse("SELECT id, payload FROM events WHERE id = 'e1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { ref rows, .. }
            if rows == &vec![vec![b"e1".to_vec(), b"hello".to_vec()]]));

        let returned = executor
            .execute(
                parse(
                    "UPDATE events SET payload = 'changed', count = 4 WHERE id = 'e1' RETURNING id, payload, count",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(returned, QueryResult::Returning { ref columns, ref rows }
            if columns == &[String::from("id"), String::from("payload"), String::from("count")]
                && rows == &vec![vec![b"e1".to_vec(), b"changed".to_vec(), b"4".to_vec()]]));
    }

    #[tokio::test]
    async fn sql_rls_filters_reads_and_rejects_hidden_mutations() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d")).with_rls_tables(
            HashMap::from([(String::from("messages"), String::from("tenant_id"))]),
        );
        executor
            .execute(
                parse("CREATE TABLE messages (id TEXT PRIMARY KEY, tenant_id TEXT, body TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO messages (id, tenant_id, body) VALUES ('visible', 'tenant-a', 'hello')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let manager = executor.manager().clone();
        let mut txn = manager.begin();
        manager.put(
            &mut txn,
            RecordKey::new("tenant-a", "d", "messages", b"hidden"),
            br#"{"id":"hidden","tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
        );
        for index in 0..1023 {
            manager.put(
                &mut txn,
                RecordKey::new(
                    "tenant-a",
                    "d",
                    "messages",
                    format!("hidden-{index:04}").as_bytes(),
                ),
                br#"{"id":"hidden","tenant_id":"tenant-b","body":"secret"}"#.to_vec(),
            );
        }
        manager.commit(txn).unwrap();

        let result = executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1));
        let hidden = executor
            .execute(parse("SELECT * FROM messages WHERE id = 'hidden'").unwrap())
            .await
            .unwrap();
        assert!(matches!(hidden, QueryResult::Rows { ref rows } if rows.is_empty()));

        let bad_insert = executor
            .execute(
                parse(
                    "INSERT INTO messages (id, tenant_id, body) VALUES ('bad', 'tenant-b', 'nope')",
                )
                .unwrap(),
            )
            .await;
        assert!(matches!(bad_insert, Err(RymeError::Forbidden)));
        let hidden_update = executor
            .execute(parse("UPDATE messages SET body = 'changed' WHERE id = 'hidden'").unwrap())
            .await;
        assert!(matches!(hidden_update, Err(RymeError::Forbidden)));
        let hidden_delete =
            executor.execute(parse("DELETE FROM messages WHERE id = 'hidden'").unwrap()).await;
        assert!(matches!(hidden_delete, Err(RymeError::Forbidden)));
    }

    #[tokio::test]
    async fn standard_update_preserves_unmodified_schema_columns() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE profiles (id TEXT PRIMARY KEY, payload TEXT, count INTEGER DEFAULT 0)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO profiles (id, payload, count) VALUES ('p1', 'before', 3)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let statement = parse(
            "UPDATE profiles SET payload = 'after', count = DEFAULT WHERE id = 'p1' RETURNING *",
        )
        .unwrap();
        assert!(matches!(statement, Statement::Returning { ref statement, .. }
            if matches!(statement.as_ref(), Statement::UpdateRow { assignments, .. } if assignments.len() == 2)));
        let result = executor.execute(statement).await.unwrap();
        let returned = match result {
            QueryResult::Returning { rows, .. } => rows,
            other => panic!("expected returning rows: {other:?}"),
        };
        assert_eq!(returned[0][0], b"p1".to_vec());
        let returned_object: serde_json::Value = serde_json::from_slice(&returned[0][1]).unwrap();
        assert_eq!(returned_object["payload"], "after");
        assert_eq!(returned_object["count"], 0);

        let result = executor
            .execute(parse("SELECT payload, count FROM profiles WHERE id = 'p1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { ref rows, .. }
            if rows == &vec![vec![b"after".to_vec(), b"0".to_vec()]]));

        executor
            .execute(parse("CREATE TABLE simple (id TEXT PRIMARY KEY, value TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO simple (id, value) VALUES ('s1', 'before')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("UPDATE simple SET value = 'after' WHERE id = 's1'").unwrap())
            .await
            .unwrap();
        let result = executor
            .execute(parse("SELECT value FROM simple WHERE id = 's1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { ref rows, .. }
            if rows == &vec![vec![b"after".to_vec()]]));
    }

    #[tokio::test]
    async fn multi_row_insert_is_atomic() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE batch (id TEXT PRIMARY KEY, payload TEXT NOT NULL)").unwrap(),
            )
            .await
            .unwrap();
        let statement =
            parse("INSERT INTO batch (id, payload) VALUES ('a', 'one'), ('b', 'two')").unwrap();
        assert!(matches!(statement, Statement::InsertRows { ref rows, .. } if rows.len() == 2));
        executor.execute(statement).await.unwrap();

        let failed = executor
            .execute(
                parse("INSERT INTO batch (id, payload) VALUES ('c', 'three'), ('a', 'duplicate')")
                    .unwrap(),
            )
            .await;
        assert!(matches!(failed, Err(RymeError::Conflict(_))));
        let result = executor
            .execute(parse("SELECT id, payload FROM batch ORDER BY id ASC").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { ref rows, .. }
        if rows == &vec![
            vec![b"a".to_vec(), b"one".to_vec()],
            vec![b"b".to_vec(), b"two".to_vec()]
        ]));
    }

    #[tokio::test]
    async fn postgres_primary_key_where_uses_point_path_and_standard_dml() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let select = parse("SELECT * FROM users WHERE id = '1'").unwrap();
        assert!(matches!(select, Statement::SelectByKey { ref table, ref pk }
            if table == "users" && pk == b"1"));

        executor
            .execute(parse("INSERT INTO users (id, payload) VALUES ('1', 'ada')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("UPDATE users SET payload = 'grace' WHERE id = '1'").unwrap())
            .await
            .unwrap();
        let row = executor.execute(select).await.unwrap();
        assert!(matches!(row, QueryResult::Row { value, .. } if value == b"grace"));
        executor.execute(parse("DELETE FROM users WHERE id = '1'").unwrap()).await.unwrap();
        let gone = executor.execute(parse("SELECT * FROM users WHERE id = '1'").unwrap()).await;
        assert!(matches!(gone, Ok(QueryResult::Rows { rows }) if rows.is_empty()));
    }

    #[tokio::test]
    async fn postgres_returning_reports_mutated_rows_and_stages_in_transactions() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let inserted = executor
            .execute(
                parse("INSERT INTO users (id, value) VALUES ('1', 'ada') RETURNING id, value")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(inserted, QueryResult::Returning { ref columns, ref rows }
            if columns == &["id", "value"] && rows == &vec![vec![b"1".to_vec(), b"ada".to_vec()]]));

        let updated = executor
            .execute(parse("UPDATE users SET value = 'grace' WHERE id = '1' RETURNING *").unwrap())
            .await
            .unwrap();
        assert!(matches!(updated, QueryResult::Returning { ref rows, .. }
            if rows == &vec![vec![b"1".to_vec(), b"grace".to_vec()]]));

        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let (returned, changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO users (id, value) VALUES ('2', 'inside') RETURNING value")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(returned, QueryResult::Returning { ref columns, ref rows }
            if columns == &["value"] && rows == &vec![vec![b"inside".to_vec()]]));
        assert_eq!(changes.len(), 1);
        executor.commit_transaction(txn, changes).await.unwrap();

        let deleted = executor
            .execute(parse("DELETE FROM users WHERE id = '1' RETURNING id, value").unwrap())
            .await
            .unwrap();
        assert!(matches!(deleted, QueryResult::Returning { ref rows, .. }
            if rows == &vec![vec![b"1".to_vec(), b"grace".to_vec()]]));
    }

    #[tokio::test]
    async fn create_table_keeps_column_metadata_for_introspection() {
        let statement = parse(
            "CREATE TABLE IF NOT EXISTS public.messages (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), payload JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT now())",
        )
        .unwrap();
        let (table, columns) = match &statement {
            Statement::CreateTable { table, columns } => (table.clone(), columns.clone()),
            _ => panic!("expected create table"),
        };
        assert_eq!(table, "public.messages");
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, "uuid");
        assert!(columns[0].primary_key);
        assert!(!columns[0].nullable);
        assert_eq!(columns[0].column_default.as_deref(), Some("gen_random_uuid()"));
        assert_eq!(columns[1].data_type, "jsonb");
        assert!(!columns[1].nullable);
        assert_eq!(columns[2].column_default.as_deref(), Some("now()"));

        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(statement).await.unwrap();
        assert_eq!(executor.catalog_tables(), vec![String::from("public.messages")]);
        assert_eq!(executor.catalog_columns("public.messages"), columns);
    }

    #[tokio::test]
    async fn schema_snapshot_restores_catalog_and_indexes() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-schema-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("schema.json");
        let manager = TxnManager::new();
        let mut executor =
            Executor::with_manager(String::from("t"), String::from("d"), manager.clone());
        executor.set_schema_path(path.clone());
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, body) VALUES ('m1', 'hello')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX messages_body_idx ON messages (body)").unwrap())
            .await
            .unwrap();
        let snapshot: SchemaSnapshot =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(snapshot.tables.len(), 1);
        assert_eq!(snapshot.indexes.len(), 1);

        let restored = Executor::with_manager(String::from("t"), String::from("d"), manager);
        restored.restore_schema_snapshot(snapshot).unwrap();
        assert_eq!(restored.catalog_tables(), vec![String::from("messages")]);
        assert_eq!(restored.catalog_indexes("messages").len(), 1);
        let result =
            restored.execute(parse("SELECT * FROM messages KEY 'm1'").unwrap()).await.unwrap();
        assert!(matches!(
            result,
            QueryResult::Row { pk, value }
                if pk == b"m1".to_vec()
                    && serde_json::from_slice::<serde_json::Value>(&value)
                        .ok()
                        .and_then(|row| row.get("body").and_then(serde_json::Value::as_str).map(String::from))
                        == Some(String::from("hello"))
        ));

        let isolated = restored.clone().with_isolated_schema(TxnManager::new());
        isolated.execute(parse("CREATE TABLE branch_only").unwrap()).await.unwrap();
        assert!(!restored.catalog_tables().contains(&String::from("branch_only")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn postgres_array_columns_materialize_array_literals() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE rooms (id TEXT PRIMARY KEY, tags TEXT[], seats INTEGER[])")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO rooms (id, tags, seats) VALUES ('r1', ARRAY['chat','game'], '{2,4,8}')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = executor
            .execute(parse("SELECT tags, seats FROM rooms WHERE key = 'r1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![vec![
                    b"[\"chat\",\"game\"]".to_vec(),
                    b"[2,4,8]".to_vec()
                ]]
        ));
    }

    #[tokio::test]
    async fn postgres_json_projection_supports_chained_paths() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE docs (id TEXT PRIMARY KEY, payload JSONB)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO docs (id, payload) VALUES ('d1', '{\"name\":\"Ada\",\"meta\":{\"role\":\"admin\"}}')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = executor
            .execute(
                parse(
                    "SELECT payload->>'name', payload->'meta'->>'role' FROM docs WHERE key = 'd1'",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![vec![b"Ada".to_vec(), b"admin".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn standard_filters_match_schema_columns_and_json_paths() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE events (id TEXT PRIMARY KEY, count INTEGER, payload JSONB, note TEXT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO events (id, count, payload, note) VALUES ('e1', 2, '{\"name\":\"Ada\"}', NULL)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO events (id, count, payload, note) VALUES ('e2', 4, '{\"name\":\"Grace\"}', 'ok')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT id FROM events WHERE count >= 3").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"e2".to_vec()]])
        );

        let result = executor
            .execute(parse("SELECT id FROM events WHERE payload->>'name' = 'Ada'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"e1".to_vec()]])
        );

        let result = executor
            .execute(parse("SELECT id FROM events WHERE note IS NULL").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"e1".to_vec()]])
        );

        let result = executor
            .execute(parse("SELECT id FROM events WHERE note IS NOT NULL").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"e2".to_vec()]])
        );
    }

    #[tokio::test]
    async fn filtered_scans_page_past_non_matching_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY, note TEXT)").unwrap())
            .await
            .unwrap();
        let rows = (0..10_000)
            .map(|index| {
                let id = format!("row{index:05}");
                let value = format!("{{\"id\":\"{id}\",\"note\":\"other\"}}");
                (id.into_bytes(), value.into_bytes())
            })
            .collect();
        executor.bulk_upsert(String::from("events"), rows).await.unwrap();
        executor
            .execute(parse("INSERT INTO events (id, note) VALUES ('zz-match', 'late')").unwrap())
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT id FROM events WHERE note = 'late' LIMIT 1").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"zz-match".to_vec()]])
        );
    }

    #[tokio::test]
    async fn serial_columns_generate_and_recover_next_ids() {
        let manager = TxnManager::new();
        let executor =
            Executor::with_manager(String::from("t"), String::from("d"), manager.clone());
        executor
            .execute(parse("CREATE TABLE events (id SERIAL PRIMARY KEY, name TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (name) VALUES ('first'), ('second')").unwrap())
            .await
            .unwrap();
        let snapshot = executor.schema_snapshot();
        assert!(snapshot.tables["events"][0].auto_increment);

        let restored = Executor::with_manager(String::from("t"), String::from("d"), manager);
        restored.restore_schema_snapshot(snapshot).unwrap();
        restored
            .execute(parse("INSERT INTO events (name) VALUES ('third')").unwrap())
            .await
            .unwrap();
        let result = restored
            .execute(parse("SELECT id, name FROM events ORDER BY id ASC").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"1".to_vec(), b"first".to_vec()],
                    vec![b"2".to_vec(), b"second".to_vec()],
                    vec![b"3".to_vec(), b"third".to_vec()]
                ]
        ));
    }

    #[test]
    fn parses_column_defaults_without_consuming_constraints() {
        let statement = parse(
            "CREATE TABLE events (name TEXT DEFAULT 'new event' NOT NULL, count INTEGER DEFAULT 0, active BOOLEAN DEFAULT true)",
        )
        .unwrap();
        let Statement::CreateTable { columns, .. } = statement else {
            panic!("expected create table")
        };
        assert_eq!(columns[0].column_default.as_deref(), Some("'new event'"));
        assert!(!columns[0].nullable);
        assert_eq!(columns[1].column_default.as_deref(), Some("0"));
        assert_eq!(columns[2].column_default.as_deref(), Some("true"));
    }

    #[test]
    fn parses_table_level_primary_keys() {
        let statement = parse(
            "CREATE TABLE users (id BIGINT, email TEXT, CONSTRAINT users_pkey PRIMARY KEY (id))",
        )
        .unwrap();
        let Statement::CreateTable { columns, .. } = statement else {
            panic!("expected create table")
        };
        assert!(columns[0].primary_key);
        assert!(!columns[0].nullable);
        assert!(!columns[1].primary_key);
        assert!(parse(
            "CREATE TABLE users (left_id BIGINT, right_id BIGINT, PRIMARY KEY (left_id, right_id))"
        )
        .is_err());
    }

    #[test]
    fn parses_unique_column_metadata() {
        let statement =
            parse("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE)").unwrap();
        let Statement::CreateTable { columns, .. } = statement else {
            panic!("expected create table")
        };
        assert!(columns[1].unique);

        let statement = parse("CREATE TABLE users (id TEXT, email TEXT, UNIQUE (email))").unwrap();
        let Statement::CreateTable { columns, .. } = statement else {
            panic!("expected create table")
        };
        assert!(columns[1].unique);
    }

    #[test]
    fn parses_standard_create_index() {
        assert_eq!(
            parse("CREATE INDEX messages_value_idx ON messages (value)").unwrap(),
            Statement::CreateIndex {
                name: String::from("messages_value_idx"),
                table: String::from("messages"),
                field: Field::Value,
                column: None,
                unique: false,
            }
        );
        assert!(matches!(
            parse("CREATE UNIQUE INDEX messages_value_unique ON messages (value)").unwrap(),
            Statement::CreateIndex { unique: true, .. }
        ));
        assert!(matches!(
            parse("CREATE INDEX IF NOT EXISTS messages_value_idx ON messages (value)").unwrap(),
            Statement::CreateIndex { name, .. } if name == "messages_value_idx"
        ));
    }

    #[tokio::test]
    async fn secondary_index_tracks_mutations_and_transaction_writes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE messages").unwrap()).await.unwrap();
        executor.execute(parse("INSERT INTO messages KEY '1' VALUE 'one'").unwrap()).await.unwrap();
        executor.execute(parse("INSERT INTO messages KEY '2' VALUE 'two'").unwrap()).await.unwrap();
        executor
            .execute(parse("CREATE INDEX messages_value_idx ON messages (value)").unwrap())
            .await
            .unwrap();
        assert_eq!(executor.catalog_indexes("messages").len(), 1);

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE value = 'two'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows == vec![(b"2".to_vec(), b"two".to_vec())])
        );

        executor
            .execute(parse("UPSERT INTO messages KEY '2' VALUE 'changed'").unwrap())
            .await
            .unwrap();
        let old = executor
            .execute(parse("SELECT * FROM messages WHERE value = 'two'").unwrap())
            .await
            .unwrap();
        assert!(matches!(old, QueryResult::Rows { rows } if rows.is_empty()));
        let new = executor
            .execute(parse("SELECT * FROM messages WHERE value = 'changed'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(new, QueryResult::Rows { rows } if rows == vec![(b"2".to_vec(), b"changed".to_vec())])
        );

        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let (_, changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO messages KEY '3' VALUE 'three'").unwrap(),
            )
            .await
            .unwrap();
        let in_transaction = executor
            .execute_in_transaction(
                &mut txn,
                parse("SELECT * FROM messages WHERE value = 'three'").unwrap(),
            )
            .await
            .unwrap()
            .0;
        assert!(
            matches!(in_transaction, QueryResult::Rows { rows } if rows == vec![(b"3".to_vec(), b"three".to_vec())])
        );
        executor.commit_transaction(txn, changes).await.unwrap();
        let after_commit = executor
            .execute(parse("SELECT * FROM messages WHERE value = 'three'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(after_commit, QueryResult::Rows { rows } if rows == vec![(b"3".to_vec(), b"three".to_vec())])
        );

        executor
            .execute(
                parse("CREATE UNIQUE INDEX messages_value_unique ON messages (value)").unwrap(),
            )
            .await
            .unwrap();
        assert!(executor
            .execute(parse("INSERT INTO messages KEY '4' VALUE 'three'").unwrap())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn named_unique_indexes_enforce_schema_columns() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE profiles (id TEXT PRIMARY KEY, email TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO profiles (id, email) VALUES ('p1', 'ada@example.com'), ('p2', 'grace@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE UNIQUE INDEX profiles_email_unique ON profiles (email)").unwrap(),
            )
            .await
            .unwrap();
        let indexes = executor.catalog_indexes("profiles");
        assert_eq!(indexes[0].column.as_deref(), Some("email"));

        let result = executor
            .execute(parse("SELECT id FROM profiles WHERE email = 'grace@example.com'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"p2".to_vec()]])
        );
        assert!(executor
            .execute(
                parse("INSERT INTO profiles (id, email) VALUES ('p3', 'ada@example.com')").unwrap(),
            )
            .await
            .is_err());
        assert!(executor
            .execute(
                parse("UPDATE profiles SET email = 'ada@example.com' WHERE id = 'p2'").unwrap(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn table_unique_constraints_enforce_inline_and_table_forms() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE inline_users (id TEXT PRIMARY KEY, email TEXT UNIQUE)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO inline_users (id, email) VALUES ('1', 'ada@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(executor
            .execute(
                parse("INSERT INTO inline_users (id, email) VALUES ('2', 'ada@example.com')")
                    .unwrap(),
            )
            .await
            .is_err());

        executor
            .execute(
                parse("CREATE TABLE table_users (id TEXT PRIMARY KEY, email TEXT, UNIQUE (email))")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO table_users (id, email) VALUES ('1', 'grace@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(executor
            .execute(
                parse("INSERT INTO table_users (id, email) VALUES ('2', 'grace@example.com')")
                    .unwrap(),
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn transaction_stages_reads_until_commit() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let (_, changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO users KEY '1' VALUE 'ada'").unwrap(),
            )
            .await
            .unwrap();
        let (row, more_changes) = executor
            .execute_in_transaction(&mut txn, parse("SELECT * FROM users KEY '1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(row, QueryResult::Row { value, .. } if value == b"ada"));
        assert!(more_changes.is_empty());
        executor.commit_transaction(txn, changes).await.unwrap();

        let committed =
            executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        assert!(matches!(committed, QueryResult::Row { .. }));
    }

    #[tokio::test]
    async fn upsert_overwrites() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("INSERT INTO users KEY '1' VALUE 'a'").unwrap()).await.unwrap();
        executor.execute(parse("UPSERT INTO users KEY '1' VALUE 'b'").unwrap()).await.unwrap();
        let row = executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        match row {
            QueryResult::Row { value, .. } => assert_eq!(value, b"b".to_vec()),
            _ => panic!("expected row"),
        }
    }

    #[test]
    fn bind_params() {
        let sql = bind("SELECT * FROM users KEY $1", &[String::from("7")]);
        let statement = parse(&sql).unwrap();
        assert_eq!(
            statement,
            Statement::SelectByKey { table: String::from("users"), pk: b"7".to_vec() }
        );
    }

    #[tokio::test]
    async fn copy_bulk_ingest() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let count = executor
            .bulk_upsert(
                String::from("docs"),
                vec![(b"k1".to_vec(), b"v1".to_vec()), (b"k2".to_vec(), b"v2".to_vec())],
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        let parsed = parse("COPY docs FROM stdin").unwrap();
        assert!(matches!(parsed, Statement::CopyFrom { table, .. } if table == "docs"));
    }

    #[tokio::test]
    async fn cdc_emits_write_ops() {
        let realtime = Realtime::new(64);
        let mut rx = realtime.subscribe("t", "d", "users");
        let executor = Executor::new(String::from("t"), String::from("d")).with_realtime(realtime);
        executor.execute(parse("INSERT INTO users KEY '1' VALUE 'a'").unwrap()).await.unwrap();
        executor.execute(parse("UPSERT INTO users KEY '1' VALUE 'b'").unwrap()).await.unwrap();
        executor.execute(parse("UPSERT INTO users KEY '2' VALUE 'c'").unwrap()).await.unwrap();
        executor.execute(parse("UPDATE users KEY '1' VALUE 'd'").unwrap()).await.unwrap();
        executor.execute(parse("DELETE FROM users KEY '2'").unwrap()).await.unwrap();
        executor
            .bulk_upsert(String::from("users"), vec![(b"3".to_vec(), b"e".to_vec())])
            .await
            .unwrap();
        let mut ops = Vec::new();
        for _ in 0..6 {
            let record = rx.try_recv().unwrap();
            ops.push((record.op, record.pk.clone(), record.after.clone()));
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(
            ops,
            vec![
                (Operation::Insert, b"1".to_vec(), Some(b"a".to_vec())),
                (Operation::Update, b"1".to_vec(), Some(b"b".to_vec())),
                (Operation::Insert, b"2".to_vec(), Some(b"c".to_vec())),
                (Operation::Update, b"1".to_vec(), Some(b"d".to_vec())),
                (Operation::Delete, b"2".to_vec(), None),
                (Operation::Insert, b"3".to_vec(), Some(b"e".to_vec())),
            ]
        );
    }

    #[test]
    fn explain_plan() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let plan = executor.explain("SELECT * FROM docs KEY 'k'").unwrap();
        assert!(plan.contains("point_lookup"));
        let nested = parse("EXPLAIN SELECT * FROM docs LIMIT 10").unwrap();
        assert!(matches!(nested, Statement::Explain { .. }));
    }

    #[tokio::test]
    async fn select_where_order_offset() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (pk, value) in [("a", "apple"), ("b", "banana"), ("c", "apricot"), ("d", "cherry")] {
            executor
                .execute(parse(&format!("INSERT INTO docs KEY '{pk}' VALUE '{value}'")).unwrap())
                .await
                .unwrap();
        }
        let rows = executor
            .execute(parse("SELECT * FROM docs WHERE value CONTAINS 'ap'").unwrap())
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                let keys: Vec<String> =
                    rows.iter().map(|(pk, _)| String::from_utf8_lossy(pk).to_string()).collect();
                assert_eq!(keys, vec![String::from("a"), String::from("c")]);
            }
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(parse("SELECT * FROM docs WHERE value LIKE 'ap%'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(rows, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"a".as_slice(), b"c".as_slice()])
        );
        let rows = executor
            .execute(parse("SELECT * FROM docs WHERE value ILIKE 'AP%'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(rows, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"a".as_slice(), b"c".as_slice()])
        );
        let rows = executor
            .execute(
                parse("SELECT * FROM docs WHERE key != 'b' ORDER BY key DESC LIMIT 2").unwrap(),
            )
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                let keys: Vec<String> =
                    rows.iter().map(|(pk, _)| String::from_utf8_lossy(pk).to_string()).collect();
                assert_eq!(keys, vec![String::from("d"), String::from("c")]);
            }
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(parse("SELECT * FROM docs ORDER BY key ASC LIMIT 2 OFFSET 1").unwrap())
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                let keys: Vec<String> =
                    rows.iter().map(|(pk, _)| String::from_utf8_lossy(pk).to_string()).collect();
                assert_eq!(keys, vec![String::from("b"), String::from("c")]);
            }
            _ => panic!("expected rows"),
        }
        let plan =
            executor.explain("SELECT * FROM docs WHERE value = 'x' ORDER BY value DESC").unwrap();
        assert!(plan.contains("filters 1"));
        assert!(plan.contains("desc"));
        assert!(parse("SELECT * FROM docs WHERE key BETWEEN 'a' AND 'b'").is_err());
    }

    #[test]
    fn parses_standard_comparison_predicates() {
        for (sql, expected) in [
            ("SELECT * FROM docs WHERE key <> '2'", Cmp::NotEq),
            ("SELECT * FROM docs WHERE key > '2'", Cmp::Gt),
            ("SELECT * FROM docs WHERE key >= '2'", Cmp::Gte),
            ("SELECT * FROM docs WHERE key < '2'", Cmp::Lt),
            ("SELECT * FROM docs WHERE key <= '2'", Cmp::Lte),
        ] {
            let Statement::SelectScan { filter, .. } = parse(sql).unwrap() else {
                panic!("expected scan for {sql}")
            };
            assert_eq!(filter[0].op, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn comparison_predicates_use_numeric_order_when_possible() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for key in ["1", "2", "3"] {
            executor
                .execute(parse(&format!("INSERT INTO docs KEY '{key}' VALUE 'value'")).unwrap())
                .await
                .unwrap();
        }
        let result =
            executor.execute(parse("SELECT * FROM docs WHERE key > '2'").unwrap()).await.unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows == vec![(b"3".to_vec(), b"value".to_vec())])
        );

        let result =
            executor.execute(parse("SELECT * FROM docs WHERE key <= '2'").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows == vec![
            (b"1".to_vec(), b"value".to_vec()),
            (b"2".to_vec(), b"value".to_vec()),
        ]));
    }

    #[tokio::test]
    async fn select_aggregates() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (pk, value) in [("a", "10"), ("b", "20"), ("c", "oops"), ("d", "30")] {
            executor
                .execute(parse(&format!("INSERT INTO nums KEY '{pk}' VALUE '{value}'")).unwrap())
                .await
                .unwrap();
        }
        for (sql, label, value) in [
            ("SELECT COUNT(*) FROM nums", "count", "4"),
            ("SELECT COUNT(*) FROM nums WHERE value != 'oops'", "count", "3"),
            ("SELECT SUM(value) FROM nums", "sum", "60"),
            ("SELECT AVG(value) FROM nums", "avg", "20"),
            ("SELECT MIN(value) FROM nums", "min", "10"),
            ("SELECT MAX(value) FROM nums", "max", "oops"),
            ("SELECT MIN(key) FROM nums", "min", "a"),
            ("SELECT AVG(value) FROM nums WHERE key = 'ghost'", "avg", "null"),
            ("SELECT SUM(value) FROM nums WHERE key = 'ghost'", "sum", "0"),
        ] {
            match executor.execute(parse(sql).unwrap()).await.unwrap() {
                QueryResult::Scalar { label: got_label, value: got_value } => {
                    assert_eq!(got_label, label, "{sql}");
                    assert_eq!(String::from_utf8(got_value).unwrap(), value, "{sql}");
                }
                _ => panic!("expected scalar for {sql}"),
            }
        }
        let plan = executor.explain("SELECT COUNT(*) FROM nums WHERE key != 'a'").unwrap();
        assert!(plan.contains("aggregate"));
        assert!(plan.contains("filters 1"));
        assert!(parse("SELECT SUM(*) FROM nums").is_err());
    }

    #[tokio::test]
    async fn select_group_by() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (pk, value) in [("a", "red"), ("b", "blue"), ("c", "red"), ("d", "blue"), ("e", "red")]
        {
            executor
                .execute(parse(&format!("INSERT INTO tags KEY '{pk}' VALUE '{value}'")).unwrap())
                .await
                .unwrap();
        }
        let rows = executor
            .execute(parse("SELECT value, COUNT(*) FROM tags GROUP BY value").unwrap())
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].0, b"blue".to_vec());
                let first: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert_eq!(first["value"], "blue");
                assert_eq!(first["count"], "2");
                let second: serde_json::Value = serde_json::from_slice(&rows[1].1).unwrap();
                assert_eq!(second["count"], "3");
            }
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(parse("SELECT value, COUNT(*) FROM tags WHERE key != 'a' GROUP BY value ORDER BY key DESC LIMIT 1").unwrap())
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].0, b"red".to_vec());
                let only: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert_eq!(only["count"], "2");
            }
            _ => panic!("expected rows"),
        }
        let plan = executor.explain("SELECT key, COUNT(*) FROM tags GROUP BY key").unwrap();
        assert!(plan.contains("group_by(tags)"));
        assert!(parse("SELECT * FROM tags GROUP BY key").is_err());
        assert!(parse("SELECT color, COUNT(*) FROM tags GROUP BY value").is_err());
        assert!(parse("SELECT value, COUNT(*) FROM tags GROUP BY nonsense").is_err());
        assert!(parse("SELECT value, COUNT(*) FROM tags GROUP value").is_err());
    }

    #[tokio::test]
    async fn select_key_join() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (table, pk, value) in [
            ("users", "a", "ada"),
            ("users", "b", "grace"),
            ("users", "z", "orphan"),
            ("orders", "a", "o1"),
            ("orders", "b", "o2"),
            ("orders", "q", "stray"),
        ] {
            executor
                .execute(parse(&format!("INSERT INTO {table} KEY '{pk}' VALUE '{value}'")).unwrap())
                .await
                .unwrap();
        }
        let rows = executor
            .execute(parse("SELECT * FROM users JOIN orders ON KEY = KEY").unwrap())
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].0, b"a".to_vec());
                let merged: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert_eq!(merged["left"], "ada");
                assert_eq!(merged["right"], "o1");
                assert_eq!(rows[1].0, b"b".to_vec());
            }
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(
                parse("SELECT * FROM users JOIN orders ON KEY = KEY WHERE value CONTAINS 'o1'")
                    .unwrap(),
            )
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].0, b"a".to_vec());
            }
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(
                parse("SELECT * FROM users JOIN orders ON KEY = KEY WHERE value CONTAINS 'zzz'")
                    .unwrap(),
            )
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => assert!(rows.is_empty()),
            _ => panic!("expected rows"),
        }
        let rows = executor
            .execute(
                parse("SELECT * FROM users JOIN orders ON KEY = KEY ORDER BY key DESC LIMIT 1")
                    .unwrap(),
            )
            .await
            .unwrap();
        match rows {
            QueryResult::Rows { rows } => assert_eq!(rows[0].0, b"b".to_vec()),
            _ => panic!("expected rows"),
        }
        let plan = executor.explain("SELECT * FROM users JOIN orders ON KEY = KEY").unwrap();
        assert!(plan.contains("hash_join(users,orders)"));
        assert!(parse("SELECT * FROM users JOIN orders ON VALUE = VALUE").is_err());
        assert!(parse("SELECT * FROM users JOIN orders").is_err());
        assert!(parse("SELECT * FROM users JOIN ON KEY = KEY").is_err());
    }

    #[test]
    fn scalar_gen_random_uuid() {
        let first = parse("INSERT INTO docs KEY gen_random_uuid() VALUE 'v'").unwrap();
        let second = parse("INSERT INTO docs KEY gen_random_uuid() VALUE 'v'").unwrap();
        match (first, second) {
            (Statement::Insert { pk: first_pk, .. }, Statement::Insert { pk: second_pk, .. }) => {
                assert_ne!(first_pk, second_pk);
                for pk in [first_pk, second_pk] {
                    let text = String::from_utf8(pk).unwrap();
                    assert_eq!(text.len(), 36);
                    assert_eq!(&text[14..15], "4");
                    assert_eq!(text.matches('-').count(), 4);
                }
            }
            _ => panic!("expected insert"),
        }
        let quoted = parse("INSERT INTO docs KEY 'gen_random_uuid' VALUE 'v'").unwrap();
        match quoted {
            Statement::Insert { pk, .. } => assert_eq!(pk, b"gen_random_uuid".to_vec()),
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn scalar_now_is_numeric() {
        let parsed = parse("INSERT INTO docs KEY 'k' VALUE now()").unwrap();
        match parsed {
            Statement::Insert { value, .. } => {
                let text = String::from_utf8(value).unwrap();
                assert!(text.parse::<u64>().unwrap() > 1_700_000_000);
            }
            _ => panic!("expected insert"),
        }
    }
}
