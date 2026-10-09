use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{Isolation, Transaction, TxnBackend, TxnManager};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    CreateTable {
        table: String,
    },
    Insert {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
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
        fields: Vec<Field>,
    },
    Explain {
        plan: String,
        inner: Box<Statement>,
    },
}

impl Statement {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Statement::Insert { .. }
                | Statement::Upsert { .. }
                | Statement::Update { .. }
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
}

pub type Row = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionChange {
    pub table: String,
    pub pk: Vec<u8>,
    pub op: Operation,
    pub after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Field {
    Key,
    Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cmp {
    Eq,
    NotEq,
    Contains,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Predicate {
    pub field: Field,
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
        let target = match self.field {
            Field::Key => pk,
            Field::Value => value,
        };
        match self.op {
            Cmp::Eq => target == self.operand.as_slice(),
            Cmp::NotEq => target != self.operand.as_slice(),
            Cmp::Contains => {
                let text = String::from_utf8_lossy(target);
                let want = String::from_utf8_lossy(&self.operand);
                text.contains(want.as_ref())
            }
        }
    }
}

impl Statement {
    pub fn table(&self) -> &str {
        match self {
            Self::CreateTable { table }
            | Self::Insert { table, .. }
            | Self::Upsert { table, .. }
            | Self::SelectByKey { table, .. }
            | Self::SelectScan { table, .. }
            | Self::Aggregate { table, .. }
            | Self::Join { left: table, .. }
            | Self::GroupBy { table, .. }
            | Self::Update { table, .. }
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
        "CREATE" => parse_create(&tokens),
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

fn parse_returning_fields(tokens: &[String]) -> Result<Vec<Field>> {
    let start = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("RETURNING"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("returning fields")))?;
    let mut fields = Vec::new();
    for token in &tokens[start + 1..] {
        if token == "*" {
            fields.extend([Field::Key, Field::Value]);
        } else if let Some(field) = parse_field(token) {
            fields.push(field);
        } else {
            return Err(RymeError::InvalidArgument(String::from("returning field")));
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
        if ch.is_whitespace() || ch == ',' || ch == ';' || ch == '(' || ch == ')' || ch == '=' {
            if !current.is_empty() {
                out.push(current.clone());
                current.clear();
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

fn parse_create(tokens: &[String]) -> Result<Statement> {
    let mut table = None;
    for (index, token) in tokens.iter().enumerate() {
        if token.eq_ignore_ascii_case("TABLE") {
            if let Some(next) = tokens.get(index + 1) {
                let cleaned = next.trim_matches(|c| c == '"' || c == '\'' || c == ';');
                if !cleaned.eq_ignore_ascii_case("IF") {
                    table = Some(cleaned.to_string());
                    break;
                }
            }
            if let Some(next) = tokens.get(index + 3) {
                table = Some(next.clone());
                break;
            }
        }
    }
    table
        .map(|table| Statement::CreateTable { table })
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create table")))
}

fn parse_insert(tokens: &[String], raw: &str) -> Result<Statement> {
    let table = table_after(tokens, "INTO")?;
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
    let _ = raw;
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
    let filter = parse_where_filter(tokens)?;
    if !has_join {
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
    Ok(Statement::SelectScan { table, limit, offset, order, filter })
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

fn parse_predicate(parts: &[String]) -> Result<Predicate> {
    if parts.len() == 2 {
        let field = parse_field(&parts[0])
            .ok_or_else(|| RymeError::InvalidArgument(String::from("where field")))?;
        return Ok(Predicate { field, op: Cmp::Eq, operand: unquote(&parts[1]).into_bytes() });
    }
    if parts.len() == 3 {
        let field = parse_field(&parts[0])
            .ok_or_else(|| RymeError::InvalidArgument(String::from("where field")))?;
        if parts[1] == "!" {
            return Ok(Predicate {
                field,
                op: Cmp::NotEq,
                operand: unquote(&parts[2]).into_bytes(),
            });
        }
        if parts[1].eq_ignore_ascii_case("CONTAINS") {
            return Ok(Predicate {
                field,
                op: Cmp::Contains,
                operand: unquote(&parts[2]).into_bytes(),
            });
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
    if trimmed.eq_ignore_ascii_case("gen_random_uuid") {
        return new_uuid_v4();
    }
    if trimmed.eq_ignore_ascii_case("now") {
        return Ok(ryme_txn::now_unix().to_string());
    }
    Ok(unquote(trimmed))
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
        Statement::CreateTable { table } => format!("ddl create_table({table})"),
        Statement::Insert { table, .. } => format!("write insert({table}) point"),
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
        Statement::Update { table, .. } => format!("write update({table}) point"),
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

fn returning_result(fields: &[Field], pk: Vec<u8>, value: Vec<u8>) -> QueryResult {
    let columns = fields
        .iter()
        .map(|field| match field {
            Field::Key => String::from("id"),
            Field::Value => String::from("value"),
        })
        .collect();
    let row = fields
        .iter()
        .map(|field| match field {
            Field::Key => pk.clone(),
            Field::Value => value.clone(),
        })
        .collect();
    QueryResult::Returning { columns, rows: vec![row] }
}

#[derive(Debug, Clone)]
pub struct Executor<B = TxnManager> {
    tenant: String,
    database: String,
    branch: String,
    manager: B,
    realtime: Option<Realtime>,
    read_only: bool,
    isolation: Isolation,
}

impl Executor<TxnManager> {
    pub fn new(tenant: String, database: String) -> Self {
        Self {
            tenant,
            database,
            branch: String::from("main"),
            manager: TxnManager::new(),
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
        }
    }

    pub fn with_manager(tenant: String, database: String, manager: TxnManager) -> Self {
        Self {
            tenant,
            database,
            branch: String::from("main"),
            manager,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
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
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
        }
    }

    pub fn with_realtime(mut self, realtime: Realtime) -> Self {
        self.realtime = Some(realtime);
        self
    }

    pub fn with_branch(mut self, branch: String) -> Self {
        self.branch = branch;
        self
    }

    pub fn manager(&self) -> &B {
        &self.manager
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_isolation(&mut self, isolation: Isolation) {
        self.isolation = isolation;
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
            after,
            commit_ts,
        })?;
        self.refresh_table(table, commit_ts);
        Ok(())
    }

    fn refresh_table(&self, table: &str, commit_ts: u64) {
        let Some(realtime) = &self.realtime else { return };
        let Some(limit) = realtime.query_limit(&self.tenant, &self.database, table) else {
            return;
        };
        let mut txn = self.manager.begin();
        let rows = self
            .manager
            .scan(&mut txn, &self.tenant, &self.database, table, limit)
            .unwrap_or_default();
        let _ = realtime.publish_query(&self.tenant, &self.database, table, commit_ts, rows, limit);
    }

    pub async fn execute(&self, statement: Statement) -> Result<QueryResult> {
        self.execute_with(statement, self.isolation).await
    }

    pub fn begin_transaction(&self, isolation: Isolation) -> Transaction {
        self.manager.begin_with(isolation)
    }

    pub async fn execute_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
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
                    let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                    let existed = self.manager.get(txn, &key)?.is_some();
                    self.manager.put(txn, key, value.clone());
                    changes.push(TransactionChange {
                        table: table.clone(),
                        pk,
                        op: if existed { Operation::Update } else { Operation::Insert },
                        after: Some(value),
                    });
                }
                Ok((QueryResult::Ok, changes))
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(txn, &key)?.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: Operation::Insert,
                        after: Some(value),
                    }],
                ))
            }
            Statement::Upsert { table, pk, value } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let existed = self.manager.get(txn, &key)?.is_some();
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: if existed { Operation::Update } else { Operation::Insert },
                        after: Some(value),
                    }],
                ))
            }
            Statement::Update { table, pk, value } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(txn, &key)?.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        op: Operation::Update,
                        after: Some(value),
                    }],
                ))
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(txn, &key)?.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.manager.delete(txn, key);
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange { table, pk, op: Operation::Delete, after: None }],
                ))
            }
            statement => Ok((self.execute_read_in_transaction(txn, statement)?, Vec::new())),
        }
    }

    async fn execute_returning_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
        fields: Vec<Field>,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        match statement {
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
        let commit_ts = self.manager.commit(txn).await?;
        for change in changes {
            self.emit(&change.table, change.pk, change.op, change.after, commit_ts)?;
        }
        Ok(commit_ts)
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
                    Some(value) => Ok(QueryResult::Row { pk, value }),
                    None => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let plain = filter.is_empty() && offset == 0 && order == Order::default();
                let cap = if plain { limit.clamp(1, 10000) } else { 10000 };
                let rows = self.manager.scan(txn, &self.tenant, &self.database, &table, cap)?;
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
                let rows = self.manager.scan(txn, &self.tenant, &self.database, &table, 10000)?;
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
                let rows = self.manager.scan(txn, &self.tenant, &self.database, &table, 10000)?;
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
                let left_rows =
                    self.manager.scan(txn, &self.tenant, &self.database, &left, 10000)?;
                let right_rows =
                    self.manager.scan(txn, &self.tenant, &self.database, &right, 10000)?;
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
        match statement {
            Statement::Returning { statement, fields } => {
                self.execute_returning(*statement, fields, isolation).await
            }
            statement => self.execute_with_base(statement, isolation).await,
        }
    }

    async fn execute_returning(
        &self,
        statement: Statement,
        fields: Vec<Field>,
        isolation: Isolation,
    ) -> Result<QueryResult> {
        match statement {
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
            Statement::Update { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                self.execute_with_base(Statement::Update { table, pk, value }, isolation).await?;
                Ok(returning_result(&fields, pk_for_result, value_for_result))
            }
            Statement::Delete { table, pk } => {
                let mut txn = self.manager.begin_with(isolation);
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
            Statement::CopyFrom { table, rows } => {
                self.reject_if_read_only()?;
                self.bulk_upsert(table, rows).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Explain { plan, .. } => {
                Ok(QueryResult::Row { pk: b"plan".to_vec(), value: plan.into_bytes() })
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                let mut txn = self.manager.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(&mut txn, &key)?.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value);
                let commit_ts = self.manager.commit(txn).await?;
                if let Some(after) = after {
                    self.emit(&table, pk, Operation::Insert, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Upsert { table, pk, value } => {
                self.reject_if_read_only()?;
                let mut txn = self.manager.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let existed = self.manager.get(&mut txn, &key)?.is_some();
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value);
                let commit_ts = self.manager.commit(txn).await?;
                if let Some(after) = after {
                    let op = if existed { Operation::Update } else { Operation::Insert };
                    self.emit(&table, pk, op, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::SelectByKey { table, pk } => {
                let mut txn = self.manager.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                match self.manager.get(&mut txn, &key)? {
                    Some(value) => Ok(QueryResult::Row { pk, value }),
                    None => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let mut txn = self.manager.begin_with(isolation);
                let plain = filter.is_empty() && offset == 0 && order == Order::default();
                let cap = if plain { limit.clamp(1, 10000) } else { 10000 };
                let rows =
                    self.manager.scan(&mut txn, &self.tenant, &self.database, &table, cap)?;
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
                let mut txn = self.manager.begin_with(isolation);
                let rows =
                    self.manager.scan(&mut txn, &self.tenant, &self.database, &table, 10000)?;
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
                let mut txn = self.manager.begin_with(isolation);
                let rows = self.manager.scan(
                    &mut txn,
                    &self.tenant,
                    &self.database,
                    table.as_str(),
                    10000,
                )?;
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
                let mut txn = self.manager.begin_with(isolation);
                let left_rows = self.manager.scan(
                    &mut txn,
                    &self.tenant,
                    &self.database,
                    left.as_str(),
                    10000,
                )?;
                let right_rows = self.manager.scan(
                    &mut txn,
                    &self.tenant,
                    &self.database,
                    right.as_str(),
                    10000,
                )?;
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
            Statement::Update { table, pk, value } => {
                self.reject_if_read_only()?;
                let mut txn = self.manager.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(&mut txn, &key)?.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value);
                let commit_ts = self.manager.commit(txn).await?;
                if let Some(after) = after {
                    self.emit(&table, pk, Operation::Update, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let mut txn = self.manager.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get(&mut txn, &key)?.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.manager.delete(&mut txn, key);
                let commit_ts = self.manager.commit(txn).await?;
                if self.realtime.is_some() {
                    self.emit(&table, pk, Operation::Delete, None, commit_ts)?;
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
            let mut txn = self.manager.begin_with(self.isolation);
            let mut staged: Vec<(Vec<u8>, Operation, Option<Vec<u8>>)> = Vec::new();
            for (pk, value) in chunk {
                if pk.is_empty() || pk.len() > 1024 {
                    return Err(RymeError::InvalidArgument(String::from("key")));
                }
                if value.len() > 4 * 1024 * 1024 {
                    return Err(RymeError::Overload(String::from("value")));
                }
                let key = RecordKey::new(&self.tenant, &self.database, &table, pk);
                let existed = self.manager.get(&mut txn, &key)?.is_some();
                if self.realtime.is_some() {
                    let op = if existed { Operation::Update } else { Operation::Insert };
                    staged.push((pk.clone(), op, Some(value.clone())));
                }
                self.manager.put(&mut txn, key, value.clone());
            }
            let commit_ts = self.manager.commit(txn).await?;
            for (pk, op, after) in staged {
                self.emit(&table, pk, op, after, commit_ts)?;
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
        assert!(matches!(insert, Statement::Insert { ref table, ref pk, ref value }
            if table == "users" && pk == b"1" && value == b"ada"));
        executor.execute(insert).await.unwrap();

        let upsert = parse(
            "INSERT INTO users (id, value) VALUES ('1', 'grace') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .unwrap();
        assert!(matches!(upsert, Statement::Upsert { .. }));
        executor.execute(upsert).await.unwrap();
        let row = executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        match row {
            QueryResult::Row { value, .. } => assert_eq!(value, b"grace"),
            _ => panic!("expected row"),
        }
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
        assert!(parse("SELECT * FROM docs WHERE nonsense 'x'").is_err());
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
