use ryme_error::{Result, RymeError};
use ryme_realtime::{NewChange, Operation, Realtime};
use ryme_storage::RecordKey;
use ryme_txn::{Isolation, Transaction, TxnBackend, TxnManager};
use serde::de::DeserializeSeed;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

type TableDefinitionParts =
    (Vec<ColumnDefinition>, Vec<Vec<String>>, Vec<String>, Vec<ForeignKeyConstraint>);
type ConflictClause = (Vec<String>, Vec<(String, InsertValue)>, bool, Vec<Predicate>);
type StandardInsertRows = Option<(Vec<String>, Vec<Vec<InsertValue>>)>;
type ProjectionColumns = Option<(Vec<String>, Vec<Option<String>>)>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperation {
    #[default]
    Union,
    Intersect,
    Except,
}

fn default_sequence_data_type() -> String {
    String::from("bigint")
}

const fn default_sequence_start() -> i64 {
    1
}

const fn default_sequence_increment() -> i64 {
    1
}

const fn default_sequence_min() -> i64 {
    1
}

const fn default_sequence_max() -> i64 {
    i64::MAX
}

const fn default_sequence_cache() -> i64 {
    1
}

const fn default_sequence_called() -> bool {
    true
}

const MAX_SQL_STATEMENT_BYTES: usize = 1024 * 1024;
const MAX_SQL_TOKENS: usize = 65_536;
const MAX_SQL_PARSE_DEPTH: usize = 64;

thread_local! {
    static SQL_PARSE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct SqlParseDepthGuard;

impl SqlParseDepthGuard {
    fn enter() -> bool {
        SQL_PARSE_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= MAX_SQL_PARSE_DEPTH {
                false
            } else {
                depth.set(current + 1);
                true
            }
        })
    }
}

impl Drop for SqlParseDepthGuard {
    fn drop(&mut self) {
        SQL_PARSE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceOperation {
    Nextval,
    Currval,
    Setval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivilegeGrant {
    pub object_type: String,
    pub object: String,
    pub privilege: String,
    pub grantee: String,
    #[serde(default)]
    pub grant_option: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultPrivilegeGrant {
    pub object_type: String,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    pub privilege: String,
    pub grantee: String,
    #[serde(default)]
    pub grant_option: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDefinition {
    pub name: String,
    #[serde(default = "default_role_login")]
    pub login: bool,
    #[serde(default)]
    pub superuser: bool,
    #[serde(default)]
    pub createdb: bool,
    #[serde(default)]
    pub createrole: bool,
    #[serde(default = "default_role_inherit")]
    pub inherit: bool,
}

/// Roles supplied by the platform rather than persisted as user-created SQL roles.
///
/// These names are part of the authentication and PostgreSQL compatibility
/// contract. Allowing a schema snapshot or CREATE ROLE statement to redefine
/// one would make authorization and catalog introspection disagree.
pub const BUILTIN_SQL_ROLES: &[&str] =
    &["postgres", "anon", "authenticated", "service_role", "realtime_subscriber"];
const SQL_ROLE_MEMBERSHIP_OBJECT: &str = "ROLE_MEMBERSHIP";
const SQL_ROLE_MEMBERSHIP_PRIVILEGE: &str = "MEMBER";
const MAX_ROLE_MEMBERSHIP_CHANGE_EDGES: usize = 10_000;

fn is_builtin_sql_role(name: &str) -> bool {
    BUILTIN_SQL_ROLES.iter().any(|builtin| builtin.eq_ignore_ascii_case(name))
}

fn reserved_role_error(name: &str) -> RymeError {
    RymeError::Conflict(format!("role {name} is reserved"))
}

fn validate_role_catalog(roles: &BTreeMap<String, RoleDefinition>) -> Result<()> {
    let mut names = HashSet::new();
    for role in roles.values() {
        if is_builtin_sql_role(&role.name) {
            return Err(RymeError::Corrupt(format!(
                "schema snapshot contains reserved role {}",
                role.name
            )));
        }
        if !names.insert(role.name.to_ascii_lowercase()) {
            return Err(RymeError::Corrupt(format!(
                "schema snapshot contains duplicate role {}",
                role.name
            )));
        }
    }
    Ok(())
}

fn role_membership_has_cycle(privileges: &[PrivilegeGrant]) -> bool {
    let mut parents = HashMap::<String, Vec<String>>::new();
    for grant in privileges.iter().filter(|grant| {
        grant.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT)
            && grant.privilege.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_PRIVILEGE)
    }) {
        parents
            .entry(grant.grantee.to_ascii_lowercase())
            .or_default()
            .push(grant.object.to_ascii_lowercase());
    }
    let mut state = HashMap::<String, u8>::new();
    for start in parents.keys() {
        if state.get(start) == Some(&2) {
            continue;
        }
        let mut pending = vec![(start.clone(), false)];
        while let Some((role, expanded)) = pending.pop() {
            if expanded {
                state.insert(role, 2);
                continue;
            }
            match state.get(&role).copied().unwrap_or(0) {
                1 => return true,
                2 => continue,
                _ => {}
            }
            state.insert(role.clone(), 1);
            pending.push((role.clone(), true));
            if let Some(role_parents) = parents.get(&role) {
                for parent in role_parents {
                    match state.get(parent).copied().unwrap_or(0) {
                        1 => return true,
                        2 => {}
                        _ => pending.push((parent.clone(), false)),
                    }
                }
            }
        }
    }
    false
}

fn effective_sql_roles(
    current_role: &str,
    roles: &HashMap<String, RoleDefinition>,
    privileges: &[PrivilegeGrant],
) -> Vec<String> {
    let current_role = current_role.to_ascii_lowercase();
    let noinherit = roles
        .values()
        .filter(|role| !role.inherit)
        .map(|role| role.name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut effective = vec![current_role.clone()];
    let mut parents = HashMap::<String, Vec<String>>::new();
    for membership in privileges.iter().filter(|grant| {
        grant.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT)
            && grant.privilege.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_PRIVILEGE)
    }) {
        parents
            .entry(membership.grantee.to_ascii_lowercase())
            .or_default()
            .push(membership.object.to_ascii_lowercase());
    }
    let mut pending = vec![current_role.clone()];
    let mut visited = HashSet::from([current_role]);
    while let Some(member) = pending.pop() {
        // INHERIT controls the privileges this role receives from its own
        // memberships. A NOINHERIT role may still contribute its direct
        // grants, but it blocks traversal to roles above it in the chain.
        if noinherit.contains(&member) {
            continue;
        }
        if let Some(role_parents) = parents.get(&member) {
            for role in role_parents {
                if visited.insert(role.clone()) {
                    effective.push(role.clone());
                    pending.push(role.clone());
                }
            }
        }
    }
    effective
}

fn validate_role_memberships(snapshot: &SchemaSnapshot) -> Result<()> {
    let mut known_roles =
        BUILTIN_SQL_ROLES.iter().map(|role| role.to_ascii_lowercase()).collect::<HashSet<_>>();
    known_roles.extend(snapshot.roles.values().map(|role| role.name.to_ascii_lowercase()));
    let mut unique = HashSet::new();
    for grant in &snapshot.privileges {
        if !grant.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT) {
            continue;
        }
        if !grant.privilege.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_PRIVILEGE)
            || grant.grant_option
            || grant.object.is_empty()
            || grant.grantee.is_empty()
            || grant.grantee.eq_ignore_ascii_case("PUBLIC")
            || grant.object.eq_ignore_ascii_case(&grant.grantee)
            || !known_roles.contains(&grant.object.to_ascii_lowercase())
            || !known_roles.contains(&grant.grantee.to_ascii_lowercase())
        {
            return Err(RymeError::Corrupt(String::from("invalid role membership")));
        }
        let member = grant.grantee.to_ascii_lowercase();
        let role = grant.object.to_ascii_lowercase();
        if !unique.insert((member.clone(), role.clone())) {
            return Err(RymeError::Corrupt(String::from("duplicate role membership")));
        }
    }
    if role_membership_has_cycle(&snapshot.privileges) {
        return Err(RymeError::Corrupt(String::from("role membership cycle")));
    }
    Ok(())
}

fn validate_rls_policy_catalog(snapshot: &SchemaSnapshot) -> Result<()> {
    for (key, policy) in &snapshot.rls_policies {
        if policy.name.is_empty()
            || policy.table.is_empty()
            || key != &format!("{}\0{}", policy.table, policy.name)
        {
            return Err(RymeError::Corrupt(String::from("invalid RLS policy identity")));
        }

        let command = policy.command.to_ascii_uppercase();
        let read = matches!(command.as_str(), "ALL" | "SELECT" | "UPDATE" | "DELETE");
        let write = matches!(command.as_str(), "ALL" | "INSERT" | "UPDATE" | "DELETE");
        if !matches!(command.as_str(), "ALL" | "SELECT" | "INSERT" | "UPDATE" | "DELETE")
            || policy.read != read
            || policy.write != write
        {
            return Err(RymeError::Corrupt(format!("invalid RLS policy command {}", policy.name)));
        }

        let mut targets = HashSet::new();
        for target in &policy.roles {
            let normalized = target.to_ascii_lowercase();
            if target.is_empty()
                || target.eq_ignore_ascii_case("PUBLIC")
                || !targets.insert(normalized)
                || (!is_builtin_sql_role(target)
                    && !snapshot.roles.values().any(|role| role.name.eq_ignore_ascii_case(target)))
            {
                return Err(RymeError::Corrupt(format!(
                    "invalid RLS policy role in {}",
                    policy.name
                )));
            }
        }

        let expressions = policy.using.iter().chain(policy.check.iter());
        let mut column: Option<String> = None;
        let mut allow_all = false;
        let mut has_expression = false;
        for expression in expressions {
            has_expression = true;
            if expression.trim().eq_ignore_ascii_case("true") {
                allow_all = true;
                continue;
            }
            let Some(expression_column) = policy_tenant_column(expression) else {
                return Err(RymeError::Corrupt(format!(
                    "unsupported RLS policy expression in {}",
                    policy.name
                )));
            };
            if column.as_deref().is_some_and(|current| current != expression_column) {
                return Err(RymeError::Corrupt(format!(
                    "inconsistent RLS policy columns in {}",
                    policy.name
                )));
            }
            column = Some(expression_column);
        }

        if !has_expression {
            if policy.name != format!("__config__{}", policy.table) || !policy.roles.is_empty() {
                return Err(RymeError::Corrupt(format!(
                    "missing RLS policy expression in {}",
                    policy.name
                )));
            }
        } else {
            let expected_column = column.or_else(|| allow_all.then(String::new));
            if expected_column.as_deref() != Some(policy.column.as_str()) {
                return Err(RymeError::Corrupt(format!(
                    "RLS policy column mismatch in {}",
                    policy.name
                )));
            }
        }
    }
    Ok(())
}

const fn default_role_login() -> bool {
    false
}

const fn default_role_inherit() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Statement {
    CreateSchema {
        schema: String,
        #[serde(default)]
        if_not_exists: bool,
    },
    CreateExtension {
        name: String,
        #[serde(default)]
        schema: Option<String>,
        #[serde(default)]
        if_not_exists: bool,
    },
    CreateSequence {
        name: String,
        #[serde(default)]
        if_not_exists: bool,
        #[serde(default = "default_sequence_data_type")]
        data_type: String,
        #[serde(default = "default_sequence_start")]
        start: i64,
        #[serde(default = "default_sequence_increment")]
        increment: i64,
        #[serde(default = "default_sequence_min")]
        min_value: i64,
        #[serde(default = "default_sequence_max")]
        max_value: i64,
        #[serde(default = "default_sequence_cache")]
        cache: i64,
        #[serde(default)]
        cycle: bool,
    },
    CreateFunction {
        name: String,
        body: String,
        language: String,
        #[serde(default)]
        replace: bool,
    },
    CreateView {
        name: String,
        query: Box<Statement>,
        #[serde(default)]
        replace: bool,
    },
    CreateMaterializedView {
        name: String,
        query: Box<Statement>,
        #[serde(default)]
        replace: bool,
        #[serde(default = "default_materialized_view_data")]
        with_data: bool,
    },
    CreateTrigger {
        name: String,
        table: String,
        timing: String,
        events: Vec<String>,
        function: String,
    },
    CreatePolicy {
        name: String,
        table: String,
        command: String,
        #[serde(default)]
        roles: Vec<String>,
        #[serde(default)]
        using: Option<String>,
        #[serde(default)]
        check: Option<String>,
    },
    DropPolicy {
        name: String,
        table: String,
        #[serde(default)]
        if_exists: bool,
    },
    AlterTableRls {
        table: String,
        #[serde(default)]
        enabled: Option<bool>,
        #[serde(default)]
        forced: Option<bool>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnDefinition>,
        #[serde(default)]
        unique_constraints: Vec<Vec<String>>,
        #[serde(default)]
        checks: Vec<String>,
        #[serde(default)]
        foreign_keys: Vec<ForeignKeyConstraint>,
        #[serde(default)]
        named_constraints: Vec<TableConstraint>,
        #[serde(default)]
        if_not_exists: bool,
    },
    DropTable {
        table: String,
        #[serde(default)]
        if_exists: bool,
    },
    DropIndex {
        name: String,
        #[serde(default)]
        if_exists: bool,
    },
    DropTrigger {
        name: String,
        table: String,
        #[serde(default)]
        if_exists: bool,
    },
    DropView {
        name: String,
        #[serde(default)]
        if_exists: bool,
    },
    DropMaterializedView {
        name: String,
        #[serde(default)]
        if_exists: bool,
    },
    RefreshMaterializedView {
        name: String,
        #[serde(default)]
        concurrently: bool,
    },
    DropSequence {
        name: String,
        #[serde(default)]
        if_exists: bool,
    },
    AlterSequence {
        name: String,
        #[serde(default)]
        restart: Option<i64>,
        #[serde(default)]
        increment: Option<i64>,
    },
    CreateRole {
        name: String,
        #[serde(default)]
        login: bool,
        #[serde(default)]
        superuser: bool,
        #[serde(default)]
        createdb: bool,
        #[serde(default)]
        createrole: bool,
        #[serde(default = "default_role_inherit")]
        inherit: bool,
    },
    AlterRoleInheritance {
        name: String,
        inherit: bool,
    },
    DropRole {
        name: String,
        #[serde(default)]
        if_exists: bool,
    },
    RoleMembershipChange {
        grant: bool,
        roles: Vec<String>,
        members: Vec<String>,
    },
    PrivilegeChange {
        #[serde(default)]
        grant: bool,
        object_type: String,
        objects: Vec<String>,
        privileges: Vec<String>,
        grantees: Vec<String>,
        #[serde(default)]
        grant_option: bool,
    },
    DefaultPrivilegeChange {
        #[serde(default)]
        grant: bool,
        object_type: String,
        #[serde(default)]
        schema: Option<String>,
        #[serde(default)]
        owner: Option<String>,
        privileges: Vec<String>,
        grantees: Vec<String>,
        #[serde(default)]
        grant_option: bool,
    },
    TruncateTable {
        table: String,
        #[serde(default)]
        restart_identity: bool,
        #[serde(default)]
        cascade: bool,
    },
    AlterTableDropConstraint {
        table: String,
        constraint: String,
        #[serde(default)]
        if_exists: bool,
    },
    AlterTableAddColumn {
        table: String,
        column: ColumnDefinition,
        #[serde(default)]
        if_not_exists: bool,
    },
    AlterTableDropColumn {
        table: String,
        column: String,
        #[serde(default)]
        if_exists: bool,
    },
    AlterTableRenameColumn {
        table: String,
        from: String,
        to: String,
    },
    AlterTableColumn {
        table: String,
        column: String,
        alteration: ColumnAlteration,
    },
    AlterTableAddConstraint {
        table: String,
        constraint: TableConstraint,
    },
    CreateIndex {
        name: String,
        table: String,
        field: Field,
        #[serde(default)]
        column: Option<String>,
        #[serde(default)]
        columns: Vec<String>,
        unique: bool,
        #[serde(default)]
        if_not_exists: bool,
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
        #[serde(default)]
        on_conflict_do_nothing: bool,
        #[serde(default)]
        conflict_target: Vec<String>,
        #[serde(default)]
        conflict_update: Vec<(String, InsertValue)>,
        #[serde(default)]
        conflict_filter: Vec<Predicate>,
    },
    InsertRows {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
        #[serde(default)]
        on_conflict_do_nothing: bool,
        #[serde(default)]
        conflict_target: Vec<String>,
        #[serde(default)]
        conflict_update: Vec<(String, InsertValue)>,
        #[serde(default)]
        conflict_filter: Vec<Predicate>,
    },
    InsertSelect {
        table: String,
        columns: Vec<String>,
        source_table: String,
        source_columns: Vec<String>,
        filter: Vec<Predicate>,
        #[serde(default)]
        upsert: bool,
        #[serde(default)]
        on_conflict_do_nothing: bool,
        #[serde(default)]
        conflict_target: Vec<String>,
        #[serde(default)]
        conflict_update: Vec<(String, InsertValue)>,
        #[serde(default)]
        conflict_filter: Vec<Predicate>,
    },
    Upsert {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
    },
    InsertIgnore {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
    },
    InsertConflict {
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
        #[serde(default)]
        target_columns: Vec<String>,
        #[serde(default)]
        assignments: Vec<(String, InsertValue)>,
        #[serde(default)]
        conflict_filter: Vec<Predicate>,
        #[serde(default)]
        do_nothing: bool,
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
        #[serde(default)]
        aliases: Vec<Option<String>>,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    },
    SelectValues {
        columns: Vec<String>,
        values: Vec<String>,
    },
    SequenceValue {
        operation: SequenceOperation,
        name: String,
        #[serde(default)]
        value: Option<i64>,
        #[serde(default = "default_sequence_called")]
        is_called: bool,
    },
    Aggregate {
        table: String,
        func: AggFunc,
        field: Field,
        #[serde(default)]
        column: Option<String>,
        filter: Vec<Predicate>,
    },
    Join {
        left: String,
        right: String,
        #[serde(default)]
        join_type: JoinType,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    },
    GroupBy {
        table: String,
        select: Vec<SelectItem>,
        group: Field,
        #[serde(default)]
        group_column: Option<String>,
        filter: Vec<Predicate>,
        #[serde(default)]
        having: Vec<Predicate>,
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
    UpdateWhere {
        table: String,
        assignments: Vec<(String, InsertValue)>,
        filter: Vec<Predicate>,
    },
    UpdateFrom {
        table: String,
        assignments: Vec<(String, InsertValue)>,
        source_table: String,
        target_column: String,
        source_column: String,
        filter: Vec<Predicate>,
        #[serde(default)]
        source_filter: Vec<Predicate>,
    },
    Delete {
        table: String,
        pk: Vec<u8>,
    },
    DeleteWhere {
        table: String,
        filter: Vec<Predicate>,
    },
    DeleteUsing {
        table: String,
        source_table: String,
        target_column: String,
        source_column: String,
        filter: Vec<Predicate>,
        #[serde(default)]
        source_filter: Vec<Predicate>,
    },
    CopyFrom {
        table: String,
        #[serde(default)]
        columns: Vec<String>,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    },
    CopyTo {
        table: String,
        #[serde(default)]
        columns: Vec<String>,
    },
    Returning {
        statement: Box<Statement>,
        fields: Vec<ReturningField>,
    },
    Explain {
        plan: String,
        inner: Box<Statement>,
        #[serde(default)]
        analyze: bool,
    },
    Distinct {
        statement: Box<Statement>,
        #[serde(default = "default_distinct_limit")]
        limit: usize,
        #[serde(default)]
        offset: usize,
    },
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        #[serde(default)]
        all: bool,
        #[serde(default)]
        operation: SetOperation,
    },
}

fn default_distinct_limit() -> usize {
    10_000
}

const fn default_materialized_view_data() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InsertValue {
    Value(Vec<u8>),
    Default,
    Null,
    Excluded(String),
    Expression(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnAlteration {
    SetDefault(String),
    DropDefault,
    SetNotNull,
    DropNotNull,
    SetType(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        #[serde(default)]
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        #[serde(default)]
        name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        #[serde(default)]
        name: Option<String>,
        expression: String,
    },
    ForeignKey {
        #[serde(default)]
        name: Option<String>,
        constraint: ForeignKeyConstraint,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintKind {
    PrimaryKey { columns: Vec<String> },
    Unique { index_name: String },
    Check { expression: String },
    ForeignKey { constraint: ForeignKeyConstraint },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintMetadata {
    pub name: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub name: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub language: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDefinition {
    pub name: String,
    pub table: String,
    pub timing: String,
    #[serde(default)]
    pub events: Vec<String>,
    pub function: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub name: String,
    pub query: Statement,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub materialized: bool,
    #[serde(default)]
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
}

fn relation_short_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name).trim_matches('"')
}

fn same_relation(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
        || relation_short_name(left).eq_ignore_ascii_case(relation_short_name(right))
}

fn relation_map_get<'a, T>(map: &'a HashMap<String, T>, table: &str) -> Option<&'a T> {
    map.get(table)
        .or_else(|| map.iter().find(|(name, _)| same_relation(name, table)).map(|(_, value)| value))
}

fn relation_set_contains(tables: &HashSet<String>, table: &str) -> bool {
    tables.iter().any(|name| same_relation(name, table))
}

fn returning_target_table(statement: &Statement) -> Option<&str> {
    match statement {
        Statement::Insert { table, .. }
        | Statement::InsertRow { table, .. }
        | Statement::InsertRows { table, .. }
        | Statement::InsertSelect { table, .. }
        | Statement::Upsert { table, .. }
        | Statement::InsertIgnore { table, .. }
        | Statement::InsertConflict { table, .. }
        | Statement::Update { table, .. }
        | Statement::UpdateRow { table, .. }
        | Statement::UpdateWhere { table, .. }
        | Statement::UpdateFrom { table, .. }
        | Statement::Delete { table, .. }
        | Statement::DeleteWhere { table, .. }
        | Statement::DeleteUsing { table, .. } => Some(table),
        _ => None,
    }
}

fn collect_table_privilege_requirements(
    statement: &Statement,
    required: &mut Vec<(String, &'static str)>,
) {
    let mut require = |table: &str, privilege| required.push((table.to_string(), privilege));
    match statement {
        Statement::Returning { statement, .. } => {
            collect_table_privilege_requirements(statement, required);
            if let Some(table) = returning_target_table(statement) {
                required.push((table.to_string(), "SELECT"));
            }
        }
        Statement::Distinct { statement, .. } | Statement::Explain { inner: statement, .. } => {
            collect_table_privilege_requirements(statement, required);
        }
        Statement::Union { left, right, .. } => {
            collect_table_privilege_requirements(left, required);
            collect_table_privilege_requirements(right, required);
        }
        Statement::SelectByKey { table, .. }
        | Statement::SelectScan { table, .. }
        | Statement::SelectColumns { table, .. }
        | Statement::Aggregate { table, .. }
        | Statement::GroupBy { table, .. }
        | Statement::CopyTo { table, .. } => require(table, "SELECT"),
        Statement::Join { left, right, .. } => {
            require(left, "SELECT");
            require(right, "SELECT");
        }
        Statement::Insert { table, .. }
        | Statement::InsertIgnore { table, .. }
        | Statement::CopyFrom { table, .. } => require(table, "INSERT"),
        Statement::InsertRow { table, upsert, conflict_update, .. }
        | Statement::InsertRows { table, upsert, conflict_update, .. } => {
            require(table, "INSERT");
            if *upsert || !conflict_update.is_empty() {
                require(table, "UPDATE");
            }
        }
        Statement::InsertSelect { table, source_table, upsert, conflict_update, .. } => {
            require(table, "INSERT");
            require(source_table, "SELECT");
            if *upsert || !conflict_update.is_empty() {
                require(table, "UPDATE");
            }
        }
        Statement::Upsert { table, .. } => {
            require(table, "INSERT");
            require(table, "UPDATE");
        }
        Statement::InsertConflict { table, assignments, .. } => {
            require(table, "INSERT");
            if !assignments.is_empty() {
                require(table, "UPDATE");
            }
        }
        Statement::Update { table, .. }
        | Statement::UpdateRow { table, .. }
        | Statement::UpdateWhere { table, .. } => require(table, "UPDATE"),
        Statement::UpdateFrom { table, source_table, .. } => {
            require(table, "UPDATE");
            require(source_table, "SELECT");
        }
        Statement::Delete { table, .. } | Statement::DeleteWhere { table, .. } => {
            require(table, "DELETE");
        }
        Statement::DeleteUsing { table, source_table, .. } => {
            require(table, "DELETE");
            require(source_table, "SELECT");
        }
        Statement::TruncateTable { table, .. } => require(table, "TRUNCATE"),
        _ => {}
    }
}

fn collect_view_dependencies(statement: &Statement, dependencies: &mut BTreeSet<String>) {
    match statement {
        Statement::SelectByKey { table, .. }
        | Statement::SelectScan { table, .. }
        | Statement::SelectColumns { table, .. }
        | Statement::Aggregate { table, .. }
        | Statement::GroupBy { table, .. }
        | Statement::CopyTo { table, .. } => {
            dependencies.insert(table.clone());
        }
        Statement::Join { left, right, .. } => {
            dependencies.insert(left.clone());
            dependencies.insert(right.clone());
        }
        Statement::Returning { statement, .. }
        | Statement::Explain { inner: statement, .. }
        | Statement::Distinct { statement, .. } => {
            collect_view_dependencies(statement, dependencies);
        }
        Statement::Union { left, right, .. } => {
            collect_view_dependencies(left, dependencies);
            collect_view_dependencies(right, dependencies);
        }
        _ => {}
    }
}

fn view_dependencies(query: &Statement) -> Vec<String> {
    let mut dependencies = BTreeSet::new();
    collect_view_dependencies(query, &mut dependencies);
    dependencies.into_iter().collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDefinition {
    pub name: String,
    #[serde(default = "default_sequence_data_type")]
    pub data_type: String,
    #[serde(default = "default_sequence_start")]
    pub start: i64,
    #[serde(default = "default_sequence_increment")]
    pub increment: i64,
    #[serde(default = "default_sequence_min")]
    pub min_value: i64,
    #[serde(default = "default_sequence_max")]
    pub max_value: i64,
    #[serde(default = "default_sequence_cache")]
    pub cache: i64,
    #[serde(default)]
    pub cycle: bool,
    #[serde(default)]
    pub last_value: Option<i64>,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    #[default]
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub columns: Vec<String>,
    pub referenced_table: String,
    #[serde(default)]
    pub referenced_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: ForeignKeyAction,
    #[serde(default)]
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    pub field: Field,
    #[serde(default)]
    pub column: Option<String>,
    #[serde(default)]
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    #[serde(default)]
    pub tables: BTreeMap<String, Vec<ColumnDefinition>>,
    #[serde(default)]
    pub indexes: Vec<IndexDefinition>,
    #[serde(default)]
    pub checks: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub foreign_keys: BTreeMap<String, Vec<ForeignKeyConstraint>>,
    #[serde(default)]
    pub constraints: BTreeMap<String, Vec<ConstraintMetadata>>,
    #[serde(default)]
    pub functions: BTreeMap<String, FunctionDefinition>,
    #[serde(default)]
    pub triggers: BTreeMap<String, TriggerDefinition>,
    #[serde(default)]
    pub views: BTreeMap<String, ViewDefinition>,
    #[serde(default)]
    pub sequences: BTreeMap<String, SequenceDefinition>,
    #[serde(default)]
    pub roles: BTreeMap<String, RoleDefinition>,
    #[serde(default)]
    pub privileges: Vec<PrivilegeGrant>,
    #[serde(default)]
    pub default_privileges: Vec<DefaultPrivilegeGrant>,
    #[serde(default)]
    pub rls_tables: BTreeMap<String, String>,
    #[serde(default)]
    pub rls_write_tables: BTreeMap<String, String>,
    #[serde(default)]
    pub rls_enabled: BTreeSet<String>,
    #[serde(default)]
    pub rls_forced: BTreeSet<String>,
    #[serde(default)]
    pub rls_policies: BTreeMap<String, RlsPolicy>,
}

pub const MAX_SCHEMA_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCHEMA_OBJECTS: usize = 100_000;

pub fn schema_snapshot_serialized_size(snapshot: &SchemaSnapshot) -> Result<usize> {
    struct SizeCounter(usize);

    impl std::io::Write for SizeCounter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "schema snapshot size")
            })?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    schema_snapshot_object_count(snapshot)?;
    validate_role_catalog(&snapshot.roles)?;
    validate_role_memberships(snapshot)?;
    validate_rls_policy_catalog(snapshot)?;
    let mut counter = SizeCounter(0);
    serde_json::to_writer(&mut counter, snapshot)
        .map_err(|error| RymeError::Internal(format!("schema snapshot size: {error}")))?;
    if counter.0 > MAX_SCHEMA_SNAPSHOT_BYTES {
        return Err(RymeError::Overload(String::from("schema snapshot")));
    }
    Ok(counter.0)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RlsPolicy {
    pub name: String,
    pub table: String,
    pub column: String,
    #[serde(default = "default_rls_policy_command")]
    pub command: String,
    #[serde(default)]
    pub using: Option<String>,
    #[serde(default)]
    pub check: Option<String>,
    /// Empty means PUBLIC, preserving the scope of older snapshots.
    #[serde(default)]
    pub roles: Vec<String>,
    pub read: bool,
    pub write: bool,
}

#[derive(Debug, Clone, Default)]
struct RlsPolicyMaps {
    tenant_read: HashMap<String, String>,
    uid_read: HashMap<String, Vec<String>>,
    tenant_write: HashMap<String, String>,
    uid_write: HashMap<String, Vec<String>>,
    tenant_insert: HashMap<String, String>,
    uid_insert: HashMap<String, Vec<String>>,
    tenant_update: HashMap<String, String>,
    uid_update: HashMap<String, Vec<String>>,
    tenant_delete: HashMap<String, String>,
    uid_delete: HashMap<String, Vec<String>>,
}

/// SQL command whose row-level security policy should be evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RlsCommand {
    Select,
    Insert,
    Update,
    Delete,
}

type RlsRoleMapCache = Arc<RwLock<HashMap<String, (u64, Arc<RlsPolicyMaps>)>>>;

fn default_rls_policy_command() -> String {
    String::from("ALL")
}

fn schema_snapshot_object_count(snapshot: &SchemaSnapshot) -> Result<usize> {
    let mut count = 0usize;
    let mut add = |amount: usize| {
        count = count
            .checked_add(amount)
            .ok_or_else(|| RymeError::Overload(String::from("schema snapshot objects")))?;
        if count > MAX_SCHEMA_OBJECTS {
            return Err(RymeError::Overload(String::from("schema snapshot objects")));
        }
        Ok(())
    };

    for columns in snapshot.tables.values() {
        add(1)?;
        add(columns.len())?;
    }
    add(snapshot.indexes.len())?;
    for checks in snapshot.checks.values() {
        add(1 + checks.len())?;
    }
    for constraints in snapshot.foreign_keys.values() {
        add(1 + constraints.len())?;
    }
    for constraints in snapshot.constraints.values() {
        add(1 + constraints.len())?;
    }
    add(snapshot.functions.len())?;
    add(snapshot.triggers.len())?;
    add(snapshot.views.len())?;
    add(snapshot.sequences.len())?;
    add(snapshot.roles.len())?;
    add(snapshot.privileges.len())?;
    add(snapshot.default_privileges.len())?;
    add(snapshot.rls_tables.len())?;
    add(snapshot.rls_write_tables.len())?;
    add(snapshot.rls_enabled.len())?;
    add(snapshot.rls_forced.len())?;
    add(snapshot.rls_policies.len())?;
    for policy in snapshot.rls_policies.values() {
        add(policy.roles.len())?;
    }
    Ok(count)
}

fn schema_snapshot_bytes(snapshot: &SchemaSnapshot) -> Result<Vec<u8>> {
    schema_snapshot_object_count(snapshot)?;
    validate_role_catalog(&snapshot.roles)?;
    validate_role_memberships(snapshot)?;
    validate_rls_policy_catalog(snapshot)?;
    let bytes = serde_json::to_vec(snapshot)
        .map_err(|error| RymeError::Internal(format!("schema snapshot: {error}")))?;
    if bytes.len() > MAX_SCHEMA_SNAPSHOT_BYTES {
        return Err(RymeError::Overload(String::from("schema snapshot")));
    }
    Ok(bytes)
}

pub fn validate_schema_snapshot(snapshot: &SchemaSnapshot) -> Result<()> {
    schema_snapshot_bytes(snapshot).map(|_| ())
}

#[cfg(unix)]
fn sync_schema_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_schema_directory(_path: &Path) -> Result<()> {
    Ok(())
}

pub fn persist_schema_snapshot(path: &Path, snapshot: &SchemaSnapshot) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = schema_snapshot_bytes(snapshot)?;
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
    if let Some(parent) = path.parent() {
        sync_schema_directory(parent)?;
    }
    Ok(())
}

pub type SchemaHook = Arc<
    dyn Fn(SchemaSnapshot, SchemaSnapshot) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

impl Statement {
    pub fn requires_admin(&self) -> bool {
        match self {
            Self::CreateRole { .. }
            | Self::AlterRoleInheritance { .. }
            | Self::DropRole { .. }
            | Self::RoleMembershipChange { .. }
            | Self::PrivilegeChange { .. }
            | Self::DefaultPrivilegeChange { .. } => true,
            Self::Returning { statement, .. }
            | Self::Distinct { statement, .. }
            | Self::Explain { inner: statement, .. } => statement.requires_admin(),
            Self::Union { left, right, .. } => left.requires_admin() || right.requires_admin(),
            _ => self.is_schema_change(),
        }
    }

    pub fn is_write(&self) -> bool {
        if let Statement::Explain { inner, analyze, .. } = self {
            return *analyze && inner.is_write();
        }
        matches!(
            self,
            Statement::CreateSchema { .. }
                | Statement::CreateExtension { .. }
                | Statement::CreateSequence { .. }
                | Statement::CreateFunction { .. }
                | Statement::CreateView { .. }
                | Statement::CreateMaterializedView { .. }
                | Statement::CreateTrigger { .. }
                | Statement::CreatePolicy { .. }
                | Statement::DropPolicy { .. }
                | Statement::DropTrigger { .. }
                | Statement::DropView { .. }
                | Statement::DropMaterializedView { .. }
                | Statement::RefreshMaterializedView { .. }
                | Statement::DropSequence { .. }
                | Statement::AlterSequence { .. }
                | Statement::CreateRole { .. }
                | Statement::AlterRoleInheritance { .. }
                | Statement::DropRole { .. }
                | Statement::RoleMembershipChange { .. }
                | Statement::PrivilegeChange { .. }
                | Statement::DefaultPrivilegeChange { .. }
                | Statement::AlterTableRls { .. }
                | Statement::Insert { .. }
                | Statement::InsertRow { .. }
                | Statement::InsertRows { .. }
                | Statement::InsertSelect { .. }
                | Statement::Upsert { .. }
                | Statement::InsertIgnore { .. }
                | Statement::InsertConflict { .. }
                | Statement::Update { .. }
                | Statement::UpdateRow { .. }
                | Statement::UpdateWhere { .. }
                | Statement::UpdateFrom { .. }
                | Statement::Delete { .. }
                | Statement::DeleteWhere { .. }
                | Statement::DeleteUsing { .. }
                | Statement::CopyFrom { .. }
                | Statement::Returning { .. }
        ) || matches!(
            self,
            Statement::SequenceValue {
                operation: SequenceOperation::Nextval | SequenceOperation::Setval,
                ..
            }
        ) || self.is_schema_change()
    }

    pub fn is_schema_change(&self) -> bool {
        if let Statement::Returning { statement, .. } = self {
            return statement.is_schema_change();
        }
        if let Statement::Explain { inner, analyze, .. } = self {
            return *analyze && inner.is_schema_change();
        }
        matches!(
            self,
            Statement::CreateSchema { .. }
                | Statement::CreateExtension { .. }
                | Statement::CreateSequence { .. }
                | Statement::CreateFunction { .. }
                | Statement::CreateView { .. }
                | Statement::CreateMaterializedView { .. }
                | Statement::CreateTrigger { .. }
                | Statement::CreatePolicy { .. }
                | Statement::DropPolicy { .. }
                | Statement::AlterTableRls { .. }
                | Statement::CreateTable { .. }
                | Statement::DropTable { .. }
                | Statement::DropIndex { .. }
                | Statement::DropTrigger { .. }
                | Statement::DropView { .. }
                | Statement::DropMaterializedView { .. }
                | Statement::RefreshMaterializedView { .. }
                | Statement::DropSequence { .. }
                | Statement::AlterSequence { .. }
                | Statement::CreateRole { .. }
                | Statement::AlterRoleInheritance { .. }
                | Statement::DropRole { .. }
                | Statement::RoleMembershipChange { .. }
                | Statement::PrivilegeChange { .. }
                | Statement::DefaultPrivilegeChange { .. }
                | Statement::TruncateTable { .. }
                | Statement::AlterTableDropConstraint { .. }
                | Statement::AlterTableAddColumn { .. }
                | Statement::AlterTableAddConstraint { .. }
                | Statement::AlterTableDropColumn { .. }
                | Statement::AlterTableRenameColumn { .. }
                | Statement::AlterTableColumn { .. }
                | Statement::CreateIndex { .. }
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

fn explain_analyze_result(
    plan: String,
    result: &QueryResult,
    changes: usize,
    elapsed: std::time::Duration,
) -> QueryResult {
    let result_rows = match result {
        QueryResult::Ok => 0,
        QueryResult::Row { .. } | QueryResult::Scalar { .. } => 1,
        QueryResult::Rows { rows } => rows.len(),
        QueryResult::Returning { rows, .. } | QueryResult::Table { rows, .. } => rows.len(),
    };
    let rows = changes.max(result_rows);
    let value =
        format!("{plan}\nactual rows={rows} duration_us={}", elapsed.as_micros()).into_bytes();
    QueryResult::Row { pk: b"plan".to_vec(), value }
}

/// Internal marker used for SQL NULL cells in byte-oriented query results.
/// Wire encoders translate it to their protocol-level null representation.
pub const SQL_NULL_SENTINEL: &[u8] = b"\0";
const RLS_CONFLICT_COLUMN: &str = "\0";
const MAX_RLS_ROLE_MAP_CACHE_ENTRIES: usize = 128;

fn merge_rls_policy_column(map: &mut HashMap<String, String>, table: &str, column: &str) {
    match map.get_mut(table) {
        None => {
            map.insert(table.to_string(), column.to_string());
        }
        Some(existing) if existing.is_empty() || existing == column => {}
        Some(existing) if column.is_empty() => existing.clear(),
        Some(existing) => *existing = String::from(RLS_CONFLICT_COLUMN),
    }
}

fn merge_uid_rls_policy_column(map: &mut HashMap<String, Vec<String>>, table: &str, column: &str) {
    let columns = map.entry(table.to_string()).or_default();
    if columns.iter().any(String::is_empty) {
        return;
    }
    if column.is_empty() {
        columns.clear();
        columns.push(String::new());
    } else if !columns.iter().any(|existing| existing == column) {
        columns.push(column.to_string());
    }
}

fn rls_policy_maps_for_role<'a>(
    policies: impl Iterator<Item = &'a RlsPolicy>,
    roles: Option<&[String]>,
) -> RlsPolicyMaps {
    let mut maps = RlsPolicyMaps::default();
    for policy in policies {
        if roles.is_some_and(|roles| {
            !policy.roles.is_empty()
                && !policy
                    .roles
                    .iter()
                    .any(|target| roles.iter().any(|role| target.eq_ignore_ascii_case(role)))
        }) {
            continue;
        }
        let is_uid_policy = policy_uses_auth_uid(policy) || policy.column.is_empty();
        if policy.write {
            if is_uid_policy {
                merge_uid_rls_policy_column(&mut maps.uid_write, &policy.table, &policy.column);
            } else {
                merge_rls_policy_column(&mut maps.tenant_write, &policy.table, &policy.column);
            }
        }
        let mut commands = Vec::with_capacity(4);
        match policy.command.to_ascii_uppercase().as_str() {
            "SELECT" if policy.read => commands.push(RlsCommand::Select),
            "INSERT" if policy.write => commands.push(RlsCommand::Insert),
            "UPDATE" if policy.write => commands.push(RlsCommand::Update),
            "DELETE" if policy.write => commands.push(RlsCommand::Delete),
            "ALL" => {
                if policy.read {
                    commands.push(RlsCommand::Select);
                }
                if policy.write {
                    commands.extend([RlsCommand::Insert, RlsCommand::Update, RlsCommand::Delete]);
                }
            }
            "SELECT" | "INSERT" | "UPDATE" | "DELETE" => {}
            _ => continue,
        }
        for command in commands {
            let (tenant_map, uid_map) = match command {
                RlsCommand::Select => (&mut maps.tenant_read, &mut maps.uid_read),
                RlsCommand::Insert => (&mut maps.tenant_insert, &mut maps.uid_insert),
                RlsCommand::Update => (&mut maps.tenant_update, &mut maps.uid_update),
                RlsCommand::Delete => (&mut maps.tenant_delete, &mut maps.uid_delete),
            };
            if is_uid_policy {
                merge_uid_rls_policy_column(uid_map, &policy.table, &policy.column);
            } else {
                merge_rls_policy_column(tenant_map, &policy.table, &policy.column);
            }
        }
    }
    maps
}

fn policy_uses_auth_uid(policy: &RlsPolicy) -> bool {
    policy
        .using
        .iter()
        .chain(policy.check.iter())
        .any(|expression| expression.to_ascii_lowercase().contains("auth.uid()"))
}

fn tenant_field_matches(
    object: &serde_json::Map<String, serde_json::Value>,
    column: &str,
    tenant: &str,
) -> bool {
    let mut values =
        object.iter().filter(|(name, _)| name.eq_ignore_ascii_case(column)).map(|(_, value)| value);
    let Some(value) = values.next() else { return false };
    values.next().is_none() && value.as_str().is_some_and(|value| value == tenant)
}

fn rls_tenant_column_allows(
    object: &serde_json::Map<String, serde_json::Value>,
    column: Option<&String>,
    tenant: &str,
) -> bool {
    match column {
        None => true,
        Some(column) if column.is_empty() => true,
        Some(column) => tenant_field_matches(object, column, tenant),
    }
}

fn rls_uid_policies_allow(
    object: &serde_json::Map<String, serde_json::Value>,
    columns: Option<&[String]>,
    auth_uid: Option<&str>,
) -> bool {
    columns.is_none_or(|columns| {
        columns.iter().any(|column| {
            column.is_empty()
                || auth_uid.is_some_and(|auth_uid| tenant_field_matches(object, column, auth_uid))
        })
    })
}

pub type Row = (Vec<u8>, Vec<u8>);

#[derive(Debug, PartialEq, Eq)]
struct IndexedCandidates {
    pks: Vec<Vec<u8>>,
    covered_predicates: Vec<usize>,
}

enum ProjectionSource {
    PrimaryKey(usize),
    Key,
    RawValue,
    Json(String),
}

struct ProjectedJsonObjectSeed<'a> {
    projection: &'a [ProjectionSource],
}

impl<'de> serde::de::DeserializeSeed<'de> for ProjectedJsonObjectSeed<'_> {
    type Value = serde_json::Map<String, serde_json::Value>;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ProjectedJsonObjectVisitor { projection: self.projection })
    }
}

struct ProjectedJsonObjectVisitor<'a> {
    projection: &'a [ProjectionSource],
}

impl<'de> serde::de::Visitor<'de> for ProjectedJsonObjectVisitor<'_> {
    type Value = serde_json::Map<String, serde_json::Value>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut selected = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let needed = self.projection.iter().any(|source| {
                let ProjectionSource::Json(column) = source else { return false };
                if let Some((root, _)) = column.split_once("->") {
                    key == root.trim()
                } else {
                    key.eq_ignore_ascii_case(column)
                }
            });
            if needed {
                selected.insert(key, map.next_value()?);
            } else {
                map.next_value::<serde::de::IgnoredAny>()?;
            }
        }
        Ok(selected)
    }
}

fn projected_json_object(
    value: &[u8],
    projection: &[ProjectionSource],
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut deserializer = serde_json::Deserializer::from_slice(value);
    let selected = ProjectedJsonObjectSeed { projection }.deserialize(&mut deserializer).ok()?;
    deserializer.end().ok()?;
    Some(selected)
}

// Queries that need a complete row set (aggregates, joins, grouped queries and
// write-from scans) must fail closed before an unbounded tenant-sized
// allocation becomes possible.  The byte limit accounts for row payloads and
// a conservative amount of Vec/tuple bookkeeping; the row limit also bounds
// overhead for many tiny rows.
const MAX_QUERY_MATERIALIZED_ROWS: usize = 100_000;
const MAX_QUERY_MATERIALIZED_BYTES: usize = 64 * 1024 * 1024;
const MATERIALIZED_ROW_OVERHEAD: usize = 64;
const INDEX_READ_BATCH_SIZE: usize = 256;

fn materialization_overload() -> RymeError {
    RymeError::Overload(String::from("query materialization"))
}

#[derive(Default)]
struct MaterializationBudget {
    rows: usize,
    bytes: usize,
}

impl MaterializationBudget {
    fn add_row(&mut self, row: &Row) -> Result<()> {
        self.add_parts(&row.0, &row.1)
    }

    fn add_parts(&mut self, pk: &[u8], value: &[u8]) -> Result<()> {
        let rows = self.rows.checked_add(1).ok_or_else(materialization_overload)?;
        if rows > MAX_QUERY_MATERIALIZED_ROWS {
            return Err(materialization_overload());
        }
        let row_bytes = pk
            .len()
            .checked_add(value.len())
            .and_then(|bytes| bytes.checked_add(MATERIALIZED_ROW_OVERHEAD))
            .ok_or_else(materialization_overload)?;
        let bytes = self.bytes.checked_add(row_bytes).ok_or_else(materialization_overload)?;
        if bytes > MAX_QUERY_MATERIALIZED_BYTES {
            return Err(materialization_overload());
        }
        self.rows = rows;
        self.bytes = bytes;
        Ok(())
    }

    fn add_rows(&mut self, rows: &[Row]) -> Result<()> {
        for row in rows {
            self.add_row(row)?;
        }
        Ok(())
    }
}

fn ensure_materialized_rows(rows: &[Row]) -> Result<()> {
    let mut budget = MaterializationBudget::default();
    budget.add_rows(rows)
}

fn ensure_materialized_part(pk: &[u8], value: &[u8]) -> Result<()> {
    let mut budget = MaterializationBudget::default();
    budget.add_parts(pk, value)
}

fn ensure_materialized_cells(rows: &[Vec<Vec<u8>>]) -> Result<()> {
    if rows.len() > MAX_QUERY_MATERIALIZED_ROWS {
        return Err(materialization_overload());
    }
    let mut bytes = 0usize;
    for row in rows {
        bytes =
            bytes.checked_add(MATERIALIZED_ROW_OVERHEAD).ok_or_else(materialization_overload)?;
        for cell in row {
            bytes = bytes
                .checked_add(cell.len())
                .and_then(|bytes| bytes.checked_add(MATERIALIZED_ROW_OVERHEAD))
                .ok_or_else(materialization_overload)?;
        }
    }
    if bytes > MAX_QUERY_MATERIALIZED_BYTES {
        return Err(materialization_overload());
    }
    Ok(())
}

fn ensure_materialized_result(result: &QueryResult) -> Result<()> {
    match result {
        QueryResult::Ok => Ok(()),
        QueryResult::Row { pk, value } => ensure_materialized_part(pk, value),
        QueryResult::Rows { rows } => ensure_materialized_rows(rows),
        QueryResult::Scalar { value, .. } => ensure_materialized_part(&[], value),
        QueryResult::Returning { rows, .. } | QueryResult::Table { rows, .. } => {
            ensure_materialized_cells(rows)
        }
    }
}

fn bounded_scan_limit(limit: usize) -> usize {
    limit.min(MAX_QUERY_MATERIALIZED_ROWS.saturating_add(1))
}

fn query_cell_value(cell: &[u8]) -> serde_json::Value {
    if cell == SQL_NULL_SENTINEL {
        return serde_json::Value::Null;
    }
    serde_json::from_slice(cell)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(cell).to_string()))
}

fn query_result_rows(result: QueryResult) -> Vec<Row> {
    match result {
        QueryResult::Row { pk, value } => vec![(pk, value)],
        QueryResult::Rows { rows } => rows,
        QueryResult::Table { columns, rows } => rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| {
                let mut object = serde_json::Map::new();
                for (column, cell) in columns.iter().zip(row) {
                    let value = query_cell_value(&cell);
                    object.insert(column.clone(), value);
                }
                (
                    index.to_string().into_bytes(),
                    serde_json::Value::Object(object).to_string().into_bytes(),
                )
            })
            .collect(),
        QueryResult::Scalar { label, value } => {
            let mut object = serde_json::Map::new();
            let parsed = query_cell_value(&value);
            object.insert(label, parsed);
            vec![(b"0".to_vec(), serde_json::Value::Object(object).to_string().into_bytes())]
        }
        QueryResult::Returning { columns, rows } => rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| {
                let mut object = serde_json::Map::new();
                for (column, cell) in columns.iter().zip(row) {
                    let value = query_cell_value(&cell);
                    object.insert(column.clone(), value);
                }
                (
                    index.to_string().into_bytes(),
                    serde_json::Value::Object(object).to_string().into_bytes(),
                )
            })
            .collect(),
        QueryResult::Ok => Vec::new(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionChange {
    pub table: String,
    pub pk: Vec<u8>,
    pub previous_pk: Option<Vec<u8>>,
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
    In,
    NotIn,
    Between,
    NotBetween,
    AnyOf,
    IsDistinct,
    IsNotDistinct,
    NotLike,
    NotILike,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Predicate {
    pub field: Field,
    #[serde(default)]
    pub column: Option<String>,
    pub op: Cmp,
    pub operand: Vec<u8>,
    #[serde(default)]
    pub operands: Vec<Vec<u8>>,
    #[serde(default)]
    pub alternatives: Vec<Vec<Predicate>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    #[default]
    Inner,
    Left,
    Right,
    Full,
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
    Column(String),
    Agg(AggFunc, Field, Option<String>),
}

impl SelectItem {
    pub fn label(&self) -> String {
        match self {
            Self::Field(Field::Key) => String::from("key"),
            Self::Field(Field::Value) => String::from("value"),
            Self::Column(column) => column.clone(),
            Self::Agg(func, _, _) => func.label().to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub field: Field,
    #[serde(default)]
    pub column: Option<String>,
    pub direction: Direction,
}

impl Default for Order {
    fn default() -> Self {
        Self { field: Field::Key, column: None, direction: Direction::Asc }
    }
}

impl Predicate {
    pub fn matches(&self, pk: &[u8], value: &[u8]) -> bool {
        let parsed = self
            .requires_row_object()
            .then(|| serde_json::from_slice::<serde_json::Value>(value).ok())
            .flatten();
        self.matches_with_object(pk, value, parsed.as_ref().and_then(serde_json::Value::as_object))
    }

    fn requires_row_object(&self) -> bool {
        self.column.is_some() || self.alternatives.iter().flatten().any(Self::requires_row_object)
    }

    fn matches_with_object(
        &self,
        pk: &[u8],
        value: &[u8],
        object: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> bool {
        if !self.alternatives.is_empty() {
            return self.alternatives.iter().any(|branch| {
                branch.iter().all(|predicate| predicate.matches_with_object(pk, value, object))
            });
        }
        if matches!(self.op, Cmp::IsDistinct | Cmp::IsNotDistinct) {
            let Some(target_is_null) = self.target_is_null(pk, value, object) else {
                return false;
            };
            let operand_is_null = is_null_bytes(&self.operand);
            let equal = if target_is_null || operand_is_null {
                target_is_null && operand_is_null
            } else {
                let target = if let Some(column) = self.column.as_deref() {
                    let Some(object) = object else { return false };
                    let Some(selected) = json_column_value(column, object) else { return false };
                    json_result_bytes(selected)
                } else {
                    match self.field {
                        Field::Key => pk.to_vec(),
                        Field::Value => value.to_vec(),
                    }
                };
                target == self.operand
            };
            return if self.op == Cmp::IsDistinct { !equal } else { equal };
        }
        if matches!(self.op, Cmp::IsNull | Cmp::IsNotNull) {
            let is_null = if let Some(column) = self.column.as_deref() {
                let Some(object) = object else { return false };
                json_column_value(column, object).is_some_and(serde_json::Value::is_null)
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
            let Some(object) = object else { return false };
            let selected = json_column_value(column, object).map(json_result_bytes);
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
            Cmp::NotLike | Cmp::NotILike => {
                let text = String::from_utf8_lossy(target);
                let pattern = String::from_utf8_lossy(&self.operand);
                let matched = if self.op == Cmp::NotILike {
                    sql_like(&text.to_lowercase(), &pattern.to_lowercase())
                } else {
                    sql_like(&text, &pattern)
                };
                !matched
            }
            Cmp::In => self.operands.iter().any(|operand| target == operand),
            Cmp::NotIn => self.operands.iter().all(|operand| target != operand),
            Cmp::Between | Cmp::NotBetween => {
                let is_between = self.operands.len() == 2
                    && compare_operands(target, &self.operands[0])
                        .is_some_and(|order| !order.is_lt())
                    && compare_operands(target, &self.operands[1])
                        .is_some_and(|order| !order.is_gt());
                if self.op == Cmp::Between {
                    is_between
                } else {
                    !is_between
                }
            }
            Cmp::AnyOf => false,
            Cmp::IsDistinct | Cmp::IsNotDistinct => false,
        }
    }

    fn target_is_null(
        &self,
        pk: &[u8],
        value: &[u8],
        object: Option<&serde_json::Map<String, serde_json::Value>>,
    ) -> Option<bool> {
        if let Some(column) = self.column.as_deref() {
            return Some(json_column_value(column, object?).is_none_or(serde_json::Value::is_null));
        }
        Some(match self.field {
            Field::Key => is_null_bytes(pk),
            Field::Value => is_null_bytes(value),
        })
    }
}

fn predicates_match(predicates: &[Predicate], pk: &[u8], value: &[u8]) -> bool {
    let parsed = predicates
        .iter()
        .any(Predicate::requires_row_object)
        .then(|| serde_json::from_slice::<serde_json::Value>(value).ok())
        .flatten();
    let object = parsed.as_ref().and_then(serde_json::Value::as_object);
    predicates.iter().all(|predicate| predicate.matches_with_object(pk, value, object))
}

fn predicates_match_except(
    predicates: &[Predicate],
    skipped: &[usize],
    pk: &[u8],
    value: &[u8],
) -> bool {
    let needs_object = predicates
        .iter()
        .enumerate()
        .any(|(index, predicate)| !skipped.contains(&index) && predicate.requires_row_object());
    let parsed =
        needs_object.then(|| serde_json::from_slice::<serde_json::Value>(value).ok()).flatten();
    let object = parsed.as_ref().and_then(serde_json::Value::as_object);
    predicates.iter().enumerate().all(|(index, predicate)| {
        skipped.contains(&index) || predicate.matches_with_object(pk, value, object)
    })
}

fn is_null_bytes(value: &[u8]) -> bool {
    value.is_empty() || value.eq_ignore_ascii_case(b"null")
}

fn check_predicate_result(predicate: &Predicate, pk: &[u8], value: &[u8]) -> Option<bool> {
    let parsed = predicate
        .requires_row_object()
        .then(|| serde_json::from_slice::<serde_json::Value>(value).ok())
        .flatten();
    let object = parsed.as_ref().and_then(serde_json::Value::as_object);
    check_predicate_result_with_object(predicate, pk, value, object)
}

fn check_predicate_result_with_object(
    predicate: &Predicate,
    pk: &[u8],
    value: &[u8],
    object: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<bool> {
    if predicate.op == Cmp::AnyOf {
        let mut unknown = false;
        for branch in &predicate.alternatives {
            let mut branch_unknown = false;
            let mut branch_valid = true;
            for predicate in branch {
                match check_predicate_result_with_object(predicate, pk, value, object) {
                    Some(true) => {}
                    Some(false) => {
                        branch_valid = false;
                        break;
                    }
                    None => branch_unknown = true,
                }
            }
            if branch_valid && !branch_unknown {
                return Some(true);
            }
            if branch_valid && branch_unknown {
                unknown = true;
            }
        }
        return if unknown { None } else { Some(false) };
    }
    if predicate.matches_with_object(pk, value, object) {
        return Some(true);
    }
    if !matches!(predicate.op, Cmp::IsNull | Cmp::IsNotNull | Cmp::IsDistinct | Cmp::IsNotDistinct)
        && predicate.target_is_null(pk, value, object) == Some(true)
    {
        return None;
    }
    Some(false)
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
            | Self::DropTable { table, .. }
            | Self::TruncateTable { table, .. }
            | Self::AlterTableDropConstraint { table, .. }
            | Self::AlterTableAddColumn { table, .. }
            | Self::AlterTableAddConstraint { table, .. }
            | Self::AlterTableDropColumn { table, .. }
            | Self::AlterTableRenameColumn { table, .. }
            | Self::AlterTableColumn { table, .. }
            | Self::AlterTableRls { table, .. }
            | Self::CreateIndex { table, .. }
            | Self::Insert { table, .. }
            | Self::InsertRow { table, .. }
            | Self::InsertRows { table, .. }
            | Self::InsertSelect { table, .. }
            | Self::Upsert { table, .. }
            | Self::InsertIgnore { table, .. }
            | Self::InsertConflict { table, .. }
            | Self::SelectByKey { table, .. }
            | Self::SelectScan { table, .. }
            | Self::SelectColumns { table, .. }
            | Self::Aggregate { table, .. }
            | Self::Join { left: table, .. }
            | Self::GroupBy { table, .. }
            | Self::Update { table, .. }
            | Self::UpdateRow { table, .. }
            | Self::UpdateWhere { table, .. }
            | Self::UpdateFrom { table, .. }
            | Self::Delete { table, .. }
            | Self::DeleteWhere { table, .. }
            | Self::DeleteUsing { table, .. }
            | Self::CopyFrom { table, .. }
            | Self::CopyTo { table, .. } => table,
            Self::CreateSchema { .. }
            | Self::CreateExtension { .. }
            | Self::CreateSequence { .. }
            | Self::CreateFunction { .. }
            | Self::CreateView { .. }
            | Self::CreateMaterializedView { .. } => "",
            Self::CreateTrigger { table, .. } => table,
            Self::CreatePolicy { table, .. } => table,
            Self::DropPolicy { table, .. } => table,
            Self::DropIndex { name, .. } => name,
            Self::DropTrigger { table, .. } => table,
            Self::DropView { name, .. } => name,
            Self::DropMaterializedView { name, .. }
            | Self::RefreshMaterializedView { name, .. } => name,
            Self::DropSequence { name, .. } | Self::AlterSequence { name, .. } => name,
            Self::CreateRole { .. }
            | Self::AlterRoleInheritance { .. }
            | Self::RoleMembershipChange { .. }
            | Self::PrivilegeChange { .. }
            | Self::DefaultPrivilegeChange { .. } => "",
            Self::DropRole { name, .. } => name,
            Self::Returning { statement, .. } => statement.table(),
            Self::Explain { inner, .. } => inner.table(),
            Self::Distinct { statement, .. } => statement.table(),
            Self::Union { left, .. } => left.table(),
            Self::SelectValues { .. } => "",
            Self::SequenceValue { .. } => "",
        }
    }
}

pub fn parse(input: &str) -> Result<Statement> {
    if input.len() > MAX_SQL_STATEMENT_BYTES {
        return Err(RymeError::Overload(String::from("SQL statement bytes")));
    }
    if !SqlParseDepthGuard::enter() {
        return Err(RymeError::Overload(String::from("SQL parse nesting")));
    }
    let _depth = SqlParseDepthGuard;
    let tokens = tokenize(input);
    if tokens.len() > MAX_SQL_TOKENS {
        return Err(RymeError::Overload(String::from("SQL statement tokens")));
    }
    if tokens.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("empty statement")));
    }
    let head = tokens[0].to_ascii_uppercase();
    let is_distinct = head == "SELECT"
        && tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("DISTINCT"));
    let mut statement =
        if matches!(head.as_str(), "SELECT" | "WITH") && find_set_operator(input).is_some() {
            parse_set_operation(input)
        } else {
            match head.as_str() {
                "CREATE" => parse_create(&tokens, input),
                "DROP" => parse_drop(&tokens),
                "REFRESH" => parse_refresh(&tokens),
                "GRANT" => parse_privilege_change(&tokens, input, true),
                "REVOKE" => parse_privilege_change(&tokens, input, false),
                "TRUNCATE" => parse_truncate(&tokens),
                "ALTER" => {
                    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("DEFAULT")) {
                        parse_default_privilege_change(&tokens, input)
                    } else {
                        parse_alter(&tokens, input)
                    }
                }
                "UPSERT" => parse_upsert(&tokens),
                "INSERT" => parse_insert(&tokens, input),
                "SELECT" => {
                    if find_sql_keyword(input, "FROM", "SELECT".len()).is_none() {
                        parse_select_values(input)
                    } else {
                        parse_select(&tokens, input)
                    }
                }
                "WITH" => parse_with(input),
                "UPDATE" => parse_update(&tokens, input),
                "DELETE" => parse_delete(&tokens, input),
                "COPY" => parse_copy(&tokens),
                "EXPLAIN" => parse_explain(input),
                _ => Err(RymeError::InvalidArgument(String::from("unknown statement"))),
            }
        }?;
    if is_distinct {
        statement = prepare_distinct(statement);
    }
    if tokens.iter().any(|token| token.eq_ignore_ascii_case("RETURNING"))
        && !matches!(statement, Statement::Returning { .. })
    {
        let fields = parse_returning_fields(&tokens)?;
        if statement.is_write() && !matches!(statement, Statement::CopyFrom { .. }) {
            return Ok(Statement::Returning { statement: Box::new(statement), fields });
        }
        return Err(RymeError::InvalidArgument(String::from("returning statement")));
    }
    Ok(statement)
}

fn prepare_distinct(statement: Statement) -> Statement {
    match statement {
        Statement::SelectScan { table, limit, offset, order, filter } => Statement::Distinct {
            statement: Box::new(Statement::SelectScan {
                table,
                limit: default_distinct_limit(),
                offset: 0,
                order,
                filter,
            }),
            limit,
            offset,
        },
        Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter } => {
            Statement::Distinct {
                statement: Box::new(Statement::SelectColumns {
                    table,
                    columns,
                    aliases,
                    limit: default_distinct_limit(),
                    offset: 0,
                    order,
                    filter,
                }),
                limit,
                offset,
            }
        }
        Statement::GroupBy {
            table,
            select,
            group,
            group_column,
            filter,
            having,
            limit,
            offset,
            order,
        } => Statement::Distinct {
            statement: Box::new(Statement::GroupBy {
                table,
                select,
                group,
                group_column,
                filter,
                having,
                limit: default_distinct_limit(),
                offset: 0,
                order,
            }),
            limit,
            offset,
        },
        Statement::Join { left, right, join_type, limit, offset, order, filter } => {
            Statement::Distinct {
                statement: Box::new(Statement::Join {
                    left,
                    right,
                    join_type,
                    limit: default_distinct_limit(),
                    offset: 0,
                    order,
                    filter,
                }),
                limit,
                offset,
            }
        }
        statement => Statement::Distinct {
            statement: Box::new(statement),
            limit: default_distinct_limit(),
            offset: 0,
        },
    }
}

fn parse_drop(tokens: &[String]) -> Result<Statement> {
    let kind =
        tokens.get(1).ok_or_else(|| RymeError::InvalidArgument(String::from("drop object")))?;
    let is_materialized = kind.eq_ignore_ascii_case("MATERIALIZED")
        && tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("VIEW"));
    let initial_object_pos = if is_materialized
        || (kind.eq_ignore_ascii_case("INDEX")
            && tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("CONCURRENTLY")))
    {
        3
    } else {
        2
    };
    let if_exists =
        tokens.get(initial_object_pos).is_some_and(|token| token.eq_ignore_ascii_case("IF"))
            && tokens
                .get(initial_object_pos + 1)
                .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"));
    let object_pos = if if_exists { initial_object_pos + 2 } else { initial_object_pos };
    let object = tokens
        .get(object_pos)
        .map(|value| unquote(value))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("drop object")))?;
    if kind.eq_ignore_ascii_case("POLICY") {
        let on_pos = tokens
            .iter()
            .enumerate()
            .skip(object_pos + 1)
            .find(|(_, token)| token.eq_ignore_ascii_case("ON"))
            .map(|(position, _)| position)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("drop policy table")))?;
        let table = tokens
            .get(on_pos + 1)
            .map(|value| {
                let qualified = unquote(value);
                qualified.rsplit('.').next().unwrap_or(&qualified).to_string()
            })
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("drop policy table")))?;
        return Ok(Statement::DropPolicy { name: object, table, if_exists });
    }
    if kind.eq_ignore_ascii_case("TRIGGER") {
        let on_pos = tokens
            .iter()
            .enumerate()
            .skip(object_pos + 1)
            .find(|(_, token)| token.eq_ignore_ascii_case("ON"))
            .map(|(position, _)| position)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("drop trigger table")))?;
        let table = tokens
            .get(on_pos + 1)
            .map(|value| unqualified_name(value))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("drop trigger table")))?;
        return Ok(Statement::DropTrigger { name: object, table, if_exists });
    }
    if kind.eq_ignore_ascii_case("VIEW") {
        return Ok(Statement::DropView { name: object, if_exists });
    }
    if is_materialized {
        return Ok(Statement::DropMaterializedView { name: object, if_exists });
    }
    if kind.eq_ignore_ascii_case("SEQUENCE") {
        return Ok(Statement::DropSequence { name: object, if_exists });
    }
    if kind.eq_ignore_ascii_case("ROLE") || kind.eq_ignore_ascii_case("USER") {
        return Ok(Statement::DropRole { name: object, if_exists });
    }
    if kind.eq_ignore_ascii_case("TABLE") {
        return Ok(Statement::DropTable { table: object, if_exists });
    }
    if kind.eq_ignore_ascii_case("INDEX") {
        return Ok(Statement::DropIndex { name: object, if_exists });
    }
    Err(RymeError::InvalidArgument(String::from("drop object")))
}

fn parse_refresh(tokens: &[String]) -> Result<Statement> {
    if !tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("MATERIALIZED"))
        || !tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("VIEW"))
    {
        return Err(RymeError::InvalidArgument(String::from("refresh materialized view")));
    }
    let concurrently =
        tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("CONCURRENTLY"));
    let name_index = if concurrently { 4 } else { 3 };
    let name = tokens
        .get(name_index)
        .map(|value| unquote(value.trim_end_matches(';')))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("refresh materialized view")))?;
    if tokens.len() != name_index + 1 {
        return Err(RymeError::InvalidArgument(String::from("refresh materialized view")));
    }
    Ok(Statement::RefreshMaterializedView { name, concurrently })
}

fn parse_truncate(tokens: &[String]) -> Result<Statement> {
    let mut table_pos =
        if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("TABLE")) { 2 } else { 1 };
    if tokens.get(table_pos).is_some_and(|token| token.eq_ignore_ascii_case("ONLY")) {
        table_pos += 1;
    }
    let table = tokens
        .get(table_pos)
        .map(|value| unquote(value))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("truncate table")))?;
    let mut restart_identity = false;
    let mut cascade = false;
    let mut position = table_pos + 1;
    while position < tokens.len() {
        match tokens[position].to_ascii_uppercase().as_str() {
            "RESTART"
                if tokens
                    .get(position + 1)
                    .is_some_and(|token| token.eq_ignore_ascii_case("IDENTITY")) =>
            {
                restart_identity = true;
                position += 2;
            }
            "CONTINUE"
                if tokens
                    .get(position + 1)
                    .is_some_and(|token| token.eq_ignore_ascii_case("IDENTITY")) =>
            {
                restart_identity = false;
                position += 2;
            }
            "CASCADE" => {
                cascade = true;
                position += 1;
            }
            "RESTRICT" => {
                cascade = false;
                position += 1;
            }
            _ => {
                return Err(RymeError::InvalidArgument(String::from("truncate option")));
            }
        }
    }
    Ok(Statement::TruncateTable { table, restart_identity, cascade })
}

fn parse_alter(tokens: &[String], raw: &str) -> Result<Statement> {
    if tokens.get(1).is_some_and(|token| {
        token.eq_ignore_ascii_case("ROLE") || token.eq_ignore_ascii_case("USER")
    }) {
        return parse_alter_role(tokens);
    }
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("SEQUENCE")) {
        return parse_alter_sequence(tokens);
    }
    if !tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("TABLE")) {
        return Err(RymeError::InvalidArgument(String::from("alter table")));
    }
    let table = tokens
        .get(2)
        .map(|value| unquote(value))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter table")))?;
    if tokens.get(3).is_some_and(|token| {
        token.eq_ignore_ascii_case("ENABLE") || token.eq_ignore_ascii_case("DISABLE")
    }) {
        let enabled = tokens.get(3).map(|token| token.eq_ignore_ascii_case("ENABLE"));
        if !tokens.get(4).is_some_and(|token| token.eq_ignore_ascii_case("ROW"))
            || !tokens.get(5).is_some_and(|token| token.eq_ignore_ascii_case("LEVEL"))
            || !tokens.get(6).is_some_and(|token| token.eq_ignore_ascii_case("SECURITY"))
        {
            return Err(RymeError::InvalidArgument(String::from("alter table row security")));
        }
        return Ok(Statement::AlterTableRls { table, enabled, forced: None });
    }
    if tokens.get(3).is_some_and(|token| {
        token.eq_ignore_ascii_case("FORCE") || token.eq_ignore_ascii_case("NO")
    }) {
        let forced = tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("FORCE"));
        let row_pos = if forced { 4 } else { 5 };
        if (!forced && !tokens.get(4).is_some_and(|token| token.eq_ignore_ascii_case("FORCE")))
            || !tokens.get(row_pos).is_some_and(|token| token.eq_ignore_ascii_case("ROW"))
            || !tokens.get(row_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("LEVEL"))
            || !tokens.get(row_pos + 2).is_some_and(|token| token.eq_ignore_ascii_case("SECURITY"))
        {
            return Err(RymeError::InvalidArgument(String::from("alter table row security")));
        }
        return Ok(Statement::AlterTableRls { table, enabled: None, forced: Some(forced) });
    }
    if let Some(drop_pos) = tokens.iter().enumerate().skip(3).find_map(|(position, token)| {
        if !token.eq_ignore_ascii_case("DROP") {
            return None;
        }
        let is_column_alter =
            tokens.iter().take(position).skip(3).any(|token| token.eq_ignore_ascii_case("ALTER"));
        (!is_column_alter).then_some(position)
    }) {
        if tokens.get(drop_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("CONSTRAINT")) {
            let initial_constraint_pos = drop_pos + 2;
            let if_exists = tokens
                .get(initial_constraint_pos)
                .is_some_and(|token| token.eq_ignore_ascii_case("IF"))
                && tokens
                    .get(initial_constraint_pos + 1)
                    .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"));
            let constraint_pos =
                if if_exists { initial_constraint_pos + 2 } else { initial_constraint_pos };
            let constraint = tokens
                .get(constraint_pos)
                .map(|value| unquote(value))
                .ok_or_else(|| RymeError::InvalidArgument(String::from("alter constraint")))?;
            return Ok(Statement::AlterTableDropConstraint { table, constraint, if_exists });
        }
        let initial_column_pos =
            if tokens.get(drop_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("COLUMN")) {
                drop_pos + 2
            } else {
                drop_pos + 1
            };
        let if_exists =
            tokens.get(initial_column_pos).is_some_and(|token| token.eq_ignore_ascii_case("IF"))
                && tokens
                    .get(initial_column_pos + 1)
                    .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"));
        let column_pos = if if_exists { initial_column_pos + 2 } else { initial_column_pos };
        let column = tokens
            .get(column_pos)
            .map(|value| unquote(value))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
        return Ok(Statement::AlterTableDropColumn { table, column, if_exists });
    }
    if let Some(rename_pos) = tokens
        .iter()
        .enumerate()
        .skip(3)
        .find_map(|(position, token)| token.eq_ignore_ascii_case("RENAME").then_some(position))
    {
        let old_pos =
            if tokens.get(rename_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("COLUMN"))
            {
                rename_pos + 2
            } else {
                rename_pos + 1
            };
        let from = tokens
            .get(old_pos)
            .map(|value| unquote(value))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
        if !tokens.get(old_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("TO")) {
            return Err(RymeError::InvalidArgument(String::from("alter column rename")));
        }
        let to = tokens
            .get(old_pos + 2)
            .map(|value| unquote(value))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
        return Ok(Statement::AlterTableRenameColumn { table, from, to });
    }
    if let Some(alter_pos) = tokens
        .iter()
        .enumerate()
        .skip(3)
        .find_map(|(position, token)| token.eq_ignore_ascii_case("ALTER").then_some(position))
    {
        let column_pos = if tokens
            .get(alter_pos + 1)
            .is_some_and(|token| token.eq_ignore_ascii_case("COLUMN"))
        {
            alter_pos + 2
        } else {
            alter_pos + 1
        };
        let column = tokens
            .get(column_pos)
            .map(|value| unquote(value))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
        let action_pos = column_pos + 1;
        let alteration = if tokens
            .get(action_pos)
            .is_some_and(|token| token.eq_ignore_ascii_case("SET"))
            && tokens.get(action_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("DEFAULT"))
        {
            let default = tokens
                .get(action_pos + 2..)
                .unwrap_or_default()
                .join(" ")
                .trim_end_matches(';')
                .trim()
                .to_string();
            if default.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("alter column default")));
            }
            ColumnAlteration::SetDefault(default)
        } else if tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("DROP"))
            && tokens.get(action_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("DEFAULT"))
        {
            ColumnAlteration::DropDefault
        } else if tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("SET"))
            && tokens.get(action_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            && tokens.get(action_pos + 2).is_some_and(|token| token.eq_ignore_ascii_case("NULL"))
        {
            ColumnAlteration::SetNotNull
        } else if tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("DROP"))
            && tokens.get(action_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            && tokens.get(action_pos + 2).is_some_and(|token| token.eq_ignore_ascii_case("NULL"))
        {
            ColumnAlteration::DropNotNull
        } else if tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("TYPE"))
            || (tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("SET"))
                && tokens
                    .get(action_pos + 1)
                    .is_some_and(|token| token.eq_ignore_ascii_case("DATA"))
                && tokens
                    .get(action_pos + 2)
                    .is_some_and(|token| token.eq_ignore_ascii_case("TYPE")))
        {
            let type_pos =
                if tokens.get(action_pos).is_some_and(|token| token.eq_ignore_ascii_case("TYPE")) {
                    action_pos + 1
                } else {
                    action_pos + 3
                };
            let using_pos =
                tokens.iter().enumerate().skip(type_pos).find_map(|(position, token)| {
                    token.eq_ignore_ascii_case("USING").then_some(position)
                });
            if let Some(using_pos) = using_pos {
                let expression = tokens.get(using_pos + 1..).unwrap_or_default().join(" ");
                if !is_simple_type_using_expression(&expression, &column) {
                    return Err(RymeError::InvalidArgument(String::from(
                        "alter column type using expressions are not supported",
                    )));
                }
            }
            let data_type = tokens
                .get(type_pos..using_pos.unwrap_or(tokens.len()))
                .unwrap_or_default()
                .join(" ")
                .trim_end_matches(';')
                .trim()
                .to_ascii_lowercase();
            if data_type.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("alter column type")));
            }
            ColumnAlteration::SetType(data_type)
        } else {
            return Err(RymeError::InvalidArgument(String::from("alter column")));
        };
        return Ok(Statement::AlterTableColumn { table, column, alteration });
    }
    let add_pos = tokens
        .iter()
        .enumerate()
        .skip(3)
        .find_map(|(position, token)| token.eq_ignore_ascii_case("ADD").then_some(position))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter table add")))?;
    let add_offset = raw
        .to_ascii_uppercase()
        .find("ADD")
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter table add")))?;
    let mut definition = raw[add_offset + 3..].trim().trim_end_matches(';').trim().to_string();
    if let Some(constraint) = parse_alter_table_constraint(&definition)? {
        return Ok(Statement::AlterTableAddConstraint { table, constraint });
    }
    let initial_column_pos =
        if tokens.get(add_pos + 1).is_some_and(|token| token.eq_ignore_ascii_case("COLUMN")) {
            add_pos + 2
        } else {
            add_pos + 1
        };
    let if_not_exists =
        tokens.get(initial_column_pos).is_some_and(|token| token.eq_ignore_ascii_case("IF"))
            && tokens
                .get(initial_column_pos + 1)
                .is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            && tokens
                .get(initial_column_pos + 2)
                .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"));
    let column_pos = if if_not_exists { initial_column_pos + 3 } else { initial_column_pos };
    let column_name = tokens
        .get(column_pos)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
    if definition.get(..6).is_some_and(|prefix| prefix.eq_ignore_ascii_case("COLUMN")) {
        definition = definition[6..].trim().to_string();
    }
    if if_not_exists
        && definition.len() >= 13
        && definition[..13].eq_ignore_ascii_case("IF NOT EXISTS")
    {
        definition = definition[13..].trim().to_string();
    }
    let mut columns = parse_column_definitions(&format!("CREATE TABLE _ ({definition})"))?;
    let column = columns
        .drain(..)
        .next()
        .filter(|column| column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter column")))?;
    Ok(Statement::AlterTableAddColumn { table, column, if_not_exists })
}

fn parse_alter_table_constraint(definition: &str) -> Result<Option<TableConstraint>> {
    let tokens = tokenize(definition);
    let (name, kind_token) =
        if tokens.first().is_some_and(|token| token.eq_ignore_ascii_case("CONSTRAINT")) {
            let name = tokens
                .get(1)
                .map(|token| unquote(token))
                .ok_or_else(|| RymeError::InvalidArgument(String::from("constraint name")))?;
            (Some(name), tokens.get(2).cloned().unwrap_or_default())
        } else {
            (None, tokens.first().cloned().unwrap_or_default())
        };
    if kind_token.eq_ignore_ascii_case("CHECK") {
        let upper = definition.to_ascii_uppercase();
        let check = upper
            .find("CHECK")
            .ok_or_else(|| RymeError::InvalidArgument(String::from("check constraint")))?;
        let open = definition[check + 5..]
            .find('(')
            .map(|offset| check + 5 + offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("check constraint")))?;
        let close = matching_paren(definition, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("check constraint")))?;
        let expression = definition[open + 1..close].trim().to_string();
        if expression.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("check constraint")));
        }
        return Ok(Some(TableConstraint::Check { name, expression }));
    }
    if kind_token.eq_ignore_ascii_case("UNIQUE") {
        let upper = definition.to_ascii_uppercase();
        let unique = upper
            .find("UNIQUE")
            .ok_or_else(|| RymeError::InvalidArgument(String::from("unique constraint")))?;
        let open = definition[unique + 6..]
            .find('(')
            .map(|offset| unique + 6 + offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("unique constraint")))?;
        let close = matching_paren(definition, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("unique constraint")))?;
        let columns = split_sql_items(&definition[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("unique constraint")));
        }
        return Ok(Some(TableConstraint::Unique { name, columns }));
    }
    if kind_token.eq_ignore_ascii_case("PRIMARY") {
        let upper = definition.to_ascii_uppercase();
        let primary = upper
            .find("PRIMARY KEY")
            .ok_or_else(|| RymeError::InvalidArgument(String::from("primary key constraint")))?;
        let open = definition[primary + 11..]
            .find('(')
            .map(|offset| primary + 11 + offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("primary key constraint")))?;
        let close = matching_paren(definition, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("primary key constraint")))?;
        let columns = split_sql_items(&definition[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("primary key constraint")));
        }
        return Ok(Some(TableConstraint::PrimaryKey { name, columns }));
    }
    if kind_token.eq_ignore_ascii_case("FOREIGN") {
        let foreign = definition
            .to_ascii_uppercase()
            .find("FOREIGN")
            .ok_or_else(|| RymeError::InvalidArgument(String::from("foreign key constraint")))?;
        let constraint = parse_foreign_key(&definition[foreign..])?
            .ok_or_else(|| RymeError::InvalidArgument(String::from("foreign key constraint")))?;
        return Ok(Some(TableConstraint::ForeignKey { name, constraint }));
    }
    Ok(None)
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
            fields.push(ReturningField::Column(normalize_column_reference(token)));
        }
    }
    if fields.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("returning fields")));
    }
    Ok(fields)
}

pub fn bind(sql: &str, params: &[String]) -> String {
    let chars = sql.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    let mut quote = None;
    let mut line_comment = false;
    let mut block_comment_depth = 0usize;
    let mut dollar_quote: Option<Vec<char>> = None;
    while index < chars.len() {
        let current = chars[index];
        if line_comment {
            out.push(current);
            if current == '\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment_depth > 0 {
            out.push(current);
            if current == '/' && chars.get(index + 1) == Some(&'*') {
                out.push('*');
                block_comment_depth += 1;
                index += 2;
            } else if current == '*' && chars.get(index + 1) == Some(&'/') {
                out.push('/');
                block_comment_depth -= 1;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = dollar_quote.as_ref() {
            if chars[index..].starts_with(delimiter.as_slice()) {
                out.extend(delimiter.iter());
                index += delimiter.len();
                dollar_quote = None;
            } else {
                out.push(current);
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            out.push(current);
            if current == '\\' && delimiter == '\'' {
                if let Some(escaped) = chars.get(index + 1) {
                    out.push(*escaped);
                    index += 2;
                    continue;
                }
            }
            if current == delimiter {
                if chars.get(index + 1) == Some(&delimiter) {
                    out.push(delimiter);
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if current == '-' && chars.get(index + 1) == Some(&'-') {
            out.push('-');
            out.push('-');
            line_comment = true;
            index += 2;
            continue;
        }
        if current == '/' && chars.get(index + 1) == Some(&'*') {
            out.push('/');
            out.push('*');
            block_comment_depth = 1;
            index += 2;
            continue;
        }
        if current == '$' && dollar_token_boundary(&chars, index) {
            if let Some((delimiter, end)) = dollar_quote_delimiter(&chars, index) {
                out.extend(delimiter.iter());
                dollar_quote = Some(delimiter);
                index = end;
                continue;
            }
        }
        if current == '\'' || current == '"' {
            quote = Some(current);
            out.push(current);
            index += 1;
            continue;
        }
        if current == '$' && dollar_token_boundary(&chars, index) {
            let mut end = index + 1;
            while chars.get(end).is_some_and(char::is_ascii_digit) {
                end += 1;
            }
            if end > index + 1 {
                let placeholder = chars[index + 1..end].iter().collect::<String>();
                let placeholder = placeholder.parse::<usize>().ok();
                if let Some(placeholder) = placeholder
                    .filter(|placeholder| *placeholder > 0)
                    .and_then(|placeholder| params.get(placeholder - 1))
                {
                    out.push_str(&bind_literal(placeholder));
                } else {
                    out.extend(chars[index..end].iter());
                }
                index = end;
                continue;
            }
        }
        out.push(current);
        index += 1;
    }
    out
}

fn dollar_token_boundary(chars: &[char], index: usize) -> bool {
    index == 0
        || !chars[index - 1].is_alphanumeric() && chars[index - 1] != '_' && chars[index - 1] != '$'
}

fn dollar_quote_delimiter(chars: &[char], start: usize) -> Option<(Vec<char>, usize)> {
    if chars.get(start) != Some(&'$') {
        return None;
    }
    let mut end = start + 1;
    if chars.get(end) == Some(&'$') {
        end += 1;
        return Some((chars[start..end].to_vec(), end));
    }
    if !chars.get(end).is_some_and(|character| character.is_ascii_alphabetic() || *character == '_')
    {
        return None;
    }
    end += 1;
    while chars
        .get(end)
        .is_some_and(|character| character.is_ascii_alphanumeric() || *character == '_')
    {
        end += 1;
    }
    if chars.get(end) != Some(&'$') {
        return None;
    }
    end += 1;
    Some((chars[start..end].to_vec(), end))
}

fn bind_literal(value: &str) -> String {
    let trimmed = value.trim();
    if value == "\0" {
        return String::from("NULL");
    }
    if trimmed.eq_ignore_ascii_case("TRUE")
        || trimmed.eq_ignore_ascii_case("FALSE")
        || trimmed.parse::<i64>().is_ok()
        || trimmed.parse::<f64>().is_ok()
    {
        return trimmed.to_string();
    }
    format!("'{}'", value.replace('\'', "''"))
}

fn tokenize(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted: Option<char> = None;
    for ch in input.chars() {
        if out.len() > MAX_SQL_TOKENS {
            return out;
        }
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
    let kind_index = if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("OR"))
        && tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("REPLACE"))
    {
        3
    } else {
        1
    };
    if tokens.get(kind_index).is_some_and(|token| {
        token.eq_ignore_ascii_case("ROLE") || token.eq_ignore_ascii_case("USER")
    }) {
        return parse_create_role(tokens, kind_index);
    }
    if tokens.get(kind_index).is_some_and(|token| {
        token.eq_ignore_ascii_case("FUNCTION") || token.eq_ignore_ascii_case("PROCEDURE")
    }) {
        return parse_create_function(tokens, raw, kind_index);
    }
    if tokens.get(kind_index).is_some_and(|token| token.eq_ignore_ascii_case("TRIGGER")) {
        return parse_create_trigger(tokens);
    }
    if tokens.get(kind_index).is_some_and(|token| token.eq_ignore_ascii_case("VIEW")) {
        return parse_create_view(tokens, raw, kind_index);
    }
    if tokens.get(kind_index).is_some_and(|token| token.eq_ignore_ascii_case("MATERIALIZED"))
        && tokens.get(kind_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("VIEW"))
    {
        return parse_create_materialized_view(tokens, raw, kind_index);
    }
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("POLICY")) {
        return parse_create_policy(tokens, raw);
    }
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("SCHEMA")) {
        let mut name_index = 2;
        let if_not_exists =
            tokens.get(name_index).is_some_and(|token| token.eq_ignore_ascii_case("IF"));
        if if_not_exists {
            if !tokens.get(name_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
                || !tokens
                    .get(name_index + 2)
                    .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"))
            {
                return Err(RymeError::InvalidArgument(String::from("create schema")));
            }
            name_index += 3;
        }
        let schema = tokens
            .get(name_index)
            .map(|value| unquote(value))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("create schema")))?;
        return Ok(Statement::CreateSchema { schema, if_not_exists });
    }
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("EXTENSION")) {
        let mut name_index = 2;
        let if_not_exists =
            tokens.get(name_index).is_some_and(|token| token.eq_ignore_ascii_case("IF"));
        if if_not_exists {
            if !tokens.get(name_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
                || !tokens
                    .get(name_index + 2)
                    .is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"))
            {
                return Err(RymeError::InvalidArgument(String::from("create extension")));
            }
            name_index += 3;
        }
        let name = tokens
            .get(name_index)
            .map(|value| unquote(value))
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("create extension")))?;
        let schema = tokens
            .iter()
            .position(|token| token.eq_ignore_ascii_case("SCHEMA"))
            .and_then(|index| tokens.get(index + 1))
            .map(|value| unquote(value));
        return Ok(Statement::CreateExtension { name, schema, if_not_exists });
    }
    if tokens.get(kind_index).is_some_and(|token| token.eq_ignore_ascii_case("SEQUENCE")) {
        return parse_create_sequence(tokens, kind_index);
    }
    if tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("INDEX"))
        || (tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("UNIQUE"))
            && tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("INDEX")))
    {
        return parse_create_index(tokens, raw);
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
    let if_not_exists =
        tokens.get(table_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("IF"));
    let (columns, unique_constraints, checks, foreign_keys) = parse_table_definition(raw)?;
    let named_constraints = parse_named_table_constraints(raw)?;
    Ok(Statement::CreateTable {
        table,
        columns,
        unique_constraints,
        checks,
        foreign_keys,
        named_constraints,
        if_not_exists,
    })
}

fn parse_create_role(tokens: &[String], kind_index: usize) -> Result<Statement> {
    let name = tokens
        .get(kind_index + 1)
        .map(|value| unquote(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create role")))?;
    let mut login = tokens[kind_index].eq_ignore_ascii_case("USER");
    let mut superuser = false;
    let mut createdb = false;
    let mut createrole = false;
    let mut inherit = true;
    let options_start =
        if tokens.get(kind_index + 2).is_some_and(|token| token.eq_ignore_ascii_case("WITH")) {
            kind_index + 3
        } else {
            kind_index + 2
        };
    for token in tokens.iter().skip(options_start) {
        match token.to_ascii_uppercase().as_str() {
            "LOGIN" => login = true,
            "NOLOGIN" => login = false,
            "SUPERUSER" => superuser = true,
            "NOSUPERUSER" => superuser = false,
            "CREATEDB" => createdb = true,
            "NOCREATEDB" => createdb = false,
            "CREATEROLE" => createrole = true,
            "NOCREATEROLE" => createrole = false,
            "INHERIT" => inherit = true,
            "NOINHERIT" => inherit = false,
            _ => {
                return Err(RymeError::InvalidArgument(String::from(
                    "unsupported create role option",
                )));
            }
        }
    }
    Ok(Statement::CreateRole { name, login, superuser, createdb, createrole, inherit })
}

fn parse_privilege_change(tokens: &[String], raw: &str, grant: bool) -> Result<Statement> {
    let command = if grant { "GRANT" } else { "REVOKE" };
    let Some(on_pos) = find_sql_keyword(raw, "ON", command.len()) else {
        return parse_role_membership_change(raw, grant);
    };
    let recipient_keyword = if grant { "TO" } else { "FROM" };
    let recipient_pos = find_sql_keyword(raw, recipient_keyword, on_pos + 2)
        .ok_or_else(|| RymeError::InvalidArgument(format!("{command} grantee")))?;
    let mut privilege_text = raw[command.len()..on_pos].trim();
    if !grant {
        privilege_text =
            privilege_text.strip_prefix("GRANT OPTION FOR").unwrap_or(privilege_text).trim();
    }
    let privileges = split_sql_items(privilege_text)
        .into_iter()
        .map(|privilege| unquote(privilege.trim()).to_ascii_uppercase())
        .filter(|privilege| !privilege.is_empty())
        .map(
            |privilege| if privilege == "ALL PRIVILEGES" { String::from("ALL") } else { privilege },
        )
        .collect::<Vec<_>>();
    let object_text = raw[on_pos + 2..recipient_pos].trim();
    let object_tokens = tokenize(object_text);
    let (object_type, objects_text) =
        if object_tokens.first().is_some_and(|token| token.eq_ignore_ascii_case("ALL")) {
            let kind = object_tokens
                .get(1)
                .map(|token| token.trim_end_matches('S').to_ascii_uppercase())
                .ok_or_else(|| RymeError::InvalidArgument(format!("{command} object")))?;
            let schema_pos =
                object_tokens.iter().position(|token| token.eq_ignore_ascii_case("SCHEMA"));
            let schema = schema_pos
                .and_then(|position| object_tokens.get(position + 1))
                .map(|value| unquote(value))
                .ok_or_else(|| RymeError::InvalidArgument(format!("{command} schema")))?;
            (kind, format!("{schema}.*"))
        } else {
            let object_type = object_tokens
                .first()
                .map(|value| unquote(value).to_ascii_uppercase())
                .ok_or_else(|| RymeError::InvalidArgument(format!("{command} object")))?;
            let type_len = object_tokens.first().map_or(0, String::len);
            (object_type, object_text[type_len..].trim().to_string())
        };
    let objects = split_sql_items(&objects_text)
        .into_iter()
        .map(|object| unquote(object.trim()))
        .filter(|object| !object.is_empty())
        .collect::<Vec<_>>();
    let mut grantee_text =
        raw[recipient_pos + recipient_keyword.len()..].trim().trim_end_matches(';').trim();
    let grant_option = grant && grantee_text.to_ascii_uppercase().ends_with("WITH GRANT OPTION");
    if grant_option {
        grantee_text = grantee_text[..grantee_text.len() - "WITH GRANT OPTION".len()].trim();
    }
    let grantees = split_sql_items(grantee_text)
        .into_iter()
        .map(|grantee| unquote(grantee.trim()))
        .filter(|grantee| !grantee.is_empty())
        .collect::<Vec<_>>();
    if privileges.is_empty() || objects.is_empty() || grantees.is_empty() {
        return Err(RymeError::InvalidArgument(format!("{command} privilege")));
    }
    let _ = tokens;
    Ok(Statement::PrivilegeChange {
        grant,
        object_type,
        objects,
        privileges,
        grantees,
        grant_option,
    })
}

fn parse_role_membership_change(raw: &str, grant: bool) -> Result<Statement> {
    let command = if grant { "GRANT" } else { "REVOKE" };
    let recipient_keyword = if grant { "TO" } else { "FROM" };
    let recipient_pos = find_sql_keyword(raw, recipient_keyword, command.len())
        .ok_or_else(|| RymeError::InvalidArgument(format!("{command} role member")))?;
    let roles = split_sql_items(raw[command.len()..recipient_pos].trim())
        .into_iter()
        .map(|role| unquote(role.trim()))
        .filter(|role| !role.is_empty())
        .collect::<Vec<_>>();
    let member_text =
        raw[recipient_pos + recipient_keyword.len()..].trim().trim_end_matches(';').trim();
    if member_text.to_ascii_uppercase().ends_with("WITH ADMIN OPTION") {
        return Err(RymeError::InvalidArgument(String::from(
            "role membership admin option is unsupported",
        )));
    }
    if member_text.is_empty() {
        return Err(RymeError::InvalidArgument(format!("{command} role member")));
    }
    let members = split_sql_items(member_text)
        .into_iter()
        .map(|member| unquote(member.trim()))
        .filter(|member| !member.is_empty())
        .collect::<Vec<_>>();
    if roles.is_empty()
        || members.is_empty()
        || members.iter().any(|m| m.eq_ignore_ascii_case("PUBLIC"))
    {
        return Err(RymeError::InvalidArgument(format!("{command} role member")));
    }
    Ok(Statement::RoleMembershipChange { grant, roles, members })
}

fn parse_default_privilege_change(tokens: &[String], raw: &str) -> Result<Statement> {
    let grant_pos = find_sql_keyword(raw, "GRANT", "ALTER DEFAULT PRIVILEGES".len());
    let revoke_pos = find_sql_keyword(raw, "REVOKE", "ALTER DEFAULT PRIVILEGES".len());
    let (command_pos, grant) = match (grant_pos, revoke_pos) {
        (Some(position), None) => (position, true),
        (None, Some(position)) => (position, false),
        (Some(grant), Some(revoke)) if grant < revoke => (grant, true),
        (Some(_), Some(revoke)) => (revoke, false),
        _ => return Err(RymeError::InvalidArgument(String::from("default privileges command"))),
    };
    let command = if grant { "GRANT" } else { "REVOKE" };
    let on_pos = find_sql_keyword(raw, "ON", command_pos + command.len())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("default privilege object")))?;
    let recipient_keyword = if grant { "TO" } else { "FROM" };
    let recipient_pos = find_sql_keyword(raw, recipient_keyword, on_pos + 2)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("default privilege grantee")))?;
    let mut privilege_text = raw[command_pos + command.len()..on_pos].trim();
    if !grant {
        privilege_text =
            privilege_text.strip_prefix("GRANT OPTION FOR").unwrap_or(privilege_text).trim();
    }
    let privileges = split_sql_items(privilege_text)
        .into_iter()
        .map(|privilege| unquote(privilege.trim()).to_ascii_uppercase())
        .filter(|privilege| !privilege.is_empty())
        .map(
            |privilege| if privilege == "ALL PRIVILEGES" { String::from("ALL") } else { privilege },
        )
        .collect::<Vec<_>>();
    let object_type = tokenize(raw[on_pos + 2..recipient_pos].trim())
        .first()
        .map(|value| value.trim_end_matches('S').to_ascii_uppercase())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("default privilege object")))?;
    let mut grantee_text =
        raw[recipient_pos + recipient_keyword.len()..].trim().trim_end_matches(';').trim();
    let grant_option = grant && grantee_text.to_ascii_uppercase().ends_with("WITH GRANT OPTION");
    if grant_option {
        grantee_text = grantee_text[..grantee_text.len() - "WITH GRANT OPTION".len()].trim();
    }
    let grantees = split_sql_items(grantee_text)
        .into_iter()
        .map(|grantee| unquote(grantee.trim()))
        .filter(|grantee| !grantee.is_empty())
        .collect::<Vec<_>>();
    let schema = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("SCHEMA"))
        .and_then(|position| tokens.get(position + 1))
        .map(|value| unquote(value));
    let owner = tokens
        .windows(2)
        .position(|window| window[0].eq_ignore_ascii_case("ROLE"))
        .and_then(|position| tokens.get(position + 1))
        .map(|value| unquote(value));
    if privileges.is_empty() || grantees.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("default privilege")));
    }
    Ok(Statement::DefaultPrivilegeChange {
        grant,
        object_type,
        schema,
        owner,
        privileges,
        grantees,
        grant_option,
    })
}

fn parse_create_sequence(tokens: &[String], kind_index: usize) -> Result<Statement> {
    let mut name_index = kind_index + 1;
    let if_not_exists =
        tokens.get(name_index).is_some_and(|token| token.eq_ignore_ascii_case("IF"));
    if if_not_exists {
        if !tokens.get(name_index + 1).is_some_and(|token| token.eq_ignore_ascii_case("NOT"))
            || !tokens.get(name_index + 2).is_some_and(|token| token.eq_ignore_ascii_case("EXISTS"))
        {
            return Err(RymeError::InvalidArgument(String::from("create sequence")));
        }
        name_index += 3;
    }
    let name = tokens
        .get(name_index)
        .map(|value| unquote(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create sequence")))?;
    let mut data_type = default_sequence_data_type();
    let mut increment = default_sequence_increment();
    let mut min_value = default_sequence_min();
    let mut max_value = default_sequence_max();
    let mut start = default_sequence_start();
    let mut cache = default_sequence_cache();
    let mut cycle = false;
    let mut position = name_index + 1;
    while position < tokens.len() {
        let token = tokens[position].to_ascii_uppercase();
        match token.as_str() {
            "AS" => {
                data_type =
                    tokens.get(position + 1).map(|value| value.to_ascii_lowercase()).ok_or_else(
                        || RymeError::InvalidArgument(String::from("sequence data type")),
                    )?;
                position += 2;
            }
            "INCREMENT" => {
                let value_pos = if tokens
                    .get(position + 1)
                    .is_some_and(|value| value.eq_ignore_ascii_case("BY"))
                {
                    position + 2
                } else {
                    position + 1
                };
                increment = parse_sequence_number(tokens.get(value_pos), "sequence increment")?;
                position = value_pos + 1;
            }
            "START" => {
                let value_pos = if tokens
                    .get(position + 1)
                    .is_some_and(|value| value.eq_ignore_ascii_case("WITH"))
                {
                    position + 2
                } else {
                    position + 1
                };
                start = parse_sequence_number(tokens.get(value_pos), "sequence start")?;
                position = value_pos + 1;
            }
            "MINVALUE" => {
                min_value = parse_sequence_number(tokens.get(position + 1), "sequence minimum")?;
                position += 2;
            }
            "MAXVALUE" => {
                max_value = parse_sequence_number(tokens.get(position + 1), "sequence maximum")?;
                position += 2;
            }
            "NO" if tokens
                .get(position + 1)
                .is_some_and(|value| value.eq_ignore_ascii_case("MINVALUE")) =>
            {
                min_value = if increment < 0 { i64::MIN } else { 1 };
                position += 2;
            }
            "NO" if tokens
                .get(position + 1)
                .is_some_and(|value| value.eq_ignore_ascii_case("MAXVALUE")) =>
            {
                max_value = if increment < 0 { -1 } else { i64::MAX };
                position += 2;
            }
            "CACHE" => {
                cache = parse_sequence_number(tokens.get(position + 1), "sequence cache")?;
                position += 2;
            }
            "CYCLE" => {
                cycle = true;
                position += 1;
            }
            "NO" if tokens
                .get(position + 1)
                .is_some_and(|value| value.eq_ignore_ascii_case("CYCLE")) =>
            {
                cycle = false;
                position += 2;
            }
            _ => position += 1,
        }
    }
    if increment == 0
        || cache <= 0
        || min_value > max_value
        || start < min_value
        || start > max_value
    {
        return Err(RymeError::InvalidArgument(String::from("invalid sequence definition")));
    }
    Ok(Statement::CreateSequence {
        name,
        if_not_exists,
        data_type,
        start,
        increment,
        min_value,
        max_value,
        cache,
        cycle,
    })
}

fn parse_sequence_number(value: Option<&String>, message: &str) -> Result<i64> {
    value
        .and_then(|value| value.trim_end_matches(';').parse::<i64>().ok())
        .ok_or_else(|| RymeError::InvalidArgument(message.to_string()))
}

fn parse_alter_role(tokens: &[String]) -> Result<Statement> {
    let name = tokens
        .get(2)
        .map(|value| unquote(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter role name")))?;
    let option_index =
        if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("WITH")) { 4 } else { 3 };
    if option_index + 1 != tokens.len() {
        return Err(RymeError::InvalidArgument(String::from("alter role option")));
    }
    let inherit = match tokens.get(option_index).map(|token| token.to_ascii_uppercase()).as_deref()
    {
        Some("INHERIT") => true,
        Some("NOINHERIT") => false,
        _ => return Err(RymeError::InvalidArgument(String::from("alter role option"))),
    };
    Ok(Statement::AlterRoleInheritance { name, inherit })
}

fn parse_alter_sequence(tokens: &[String]) -> Result<Statement> {
    let name = tokens
        .get(2)
        .map(|value| unquote(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("alter sequence")))?;
    let mut restart = None;
    let mut increment = None;
    let mut position = 3;
    while position < tokens.len() {
        match tokens[position].to_ascii_uppercase().as_str() {
            "RESTART" => {
                let value_pos = if tokens
                    .get(position + 1)
                    .is_some_and(|value| value.eq_ignore_ascii_case("WITH"))
                {
                    position + 2
                } else {
                    position + 1
                };
                restart = Some(parse_sequence_number(tokens.get(value_pos), "sequence restart")?);
                position = value_pos + 1;
            }
            "INCREMENT" => {
                let value_pos = if tokens
                    .get(position + 1)
                    .is_some_and(|value| value.eq_ignore_ascii_case("BY"))
                {
                    position + 2
                } else {
                    position + 1
                };
                increment =
                    Some(parse_sequence_number(tokens.get(value_pos), "sequence increment")?);
                position = value_pos + 1;
            }
            _ => position += 1,
        }
    }
    if increment == Some(0) {
        return Err(RymeError::InvalidArgument(String::from("sequence increment")));
    }
    Ok(Statement::AlterSequence { name, restart, increment })
}

fn unqualified_name(value: &str) -> String {
    let unquoted = unquote(value.trim());
    let value = unquoted.trim_end_matches("() ");
    value.rsplit('.').next().unwrap_or(value).to_string()
}

fn parse_create_function(tokens: &[String], raw: &str, kind_index: usize) -> Result<Statement> {
    let keyword = tokens
        .get(kind_index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create function")))?;
    let keyword_pos = find_sql_keyword(raw, keyword, 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create function")))?;
    let rest = raw[keyword_pos + keyword.len()..].trim_start();
    let name_end = rest
        .find(['(', ' ', '\t', '\n', '\r'])
        .ok_or_else(|| RymeError::InvalidArgument(String::from("function name")))?;
    let name = unqualified_name(&rest[..name_end]);
    if name.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("function name")));
    }
    let body = extract_function_body(raw).unwrap_or_default();
    let language = tokens
        .iter()
        .rposition(|token| token.eq_ignore_ascii_case("LANGUAGE"))
        .and_then(|index| tokens.get(index + 1))
        .map(|value| unquote(value))
        .unwrap_or_default();
    Ok(Statement::CreateFunction { name, body, language, replace: kind_index == 3 })
}

fn parse_create_view(tokens: &[String], raw: &str, kind_index: usize) -> Result<Statement> {
    let keyword = tokens
        .get(kind_index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create view")))?;
    let keyword_pos = find_sql_keyword(raw, keyword, 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create view")))?;
    let rest = raw[keyword_pos + keyword.len()..].trim_start();
    let as_pos = find_sql_keyword(rest, "AS", 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("view query")))?;
    let name = unqualified_name(rest[..as_pos].trim());
    let query_sql = rest[as_pos + 2..].trim().trim_end_matches(';').trim();
    if name.is_empty() || query_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("create view")));
    }
    let query = parse(query_sql)?;
    if query.is_write() {
        return Err(RymeError::InvalidArgument(String::from("view query must be readable")));
    }
    Ok(Statement::CreateView {
        name,
        query: Box::new(query),
        replace: tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("OR")),
    })
}

fn parse_create_materialized_view(
    tokens: &[String],
    raw: &str,
    kind_index: usize,
) -> Result<Statement> {
    let keyword = tokens
        .get(kind_index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("materialized view")))?;
    let keyword_pos = find_sql_keyword(raw, keyword, 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("materialized view")))?;
    let rest = raw[keyword_pos + keyword.len()..].trim_start();
    if !rest.split_whitespace().next().is_some_and(|token| token.eq_ignore_ascii_case("VIEW")) {
        return Err(RymeError::InvalidArgument(String::from("materialized view")));
    }
    let view_offset = find_sql_keyword(rest, "VIEW", 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("materialized view")))?
        + "VIEW".len();
    let rest = rest[view_offset..].trim_start();
    let as_pos = find_sql_keyword(rest, "AS", 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("materialized view query")))?;
    let name = unqualified_name(rest[..as_pos].trim());
    let mut query_sql = rest[as_pos + "AS".len()..].trim().trim_end_matches(';').trim();
    let upper = query_sql.to_ascii_uppercase();
    let (with_data, query_end) = if upper.ends_with("WITH NO DATA") {
        (false, query_sql.len() - "WITH NO DATA".len())
    } else if upper.ends_with("WITH DATA") {
        (true, query_sql.len() - "WITH DATA".len())
    } else {
        (true, query_sql.len())
    };
    query_sql = query_sql[..query_end].trim();
    if name.is_empty() || query_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("materialized view")));
    }
    let query = parse(query_sql)?;
    if query.is_write() {
        return Err(RymeError::InvalidArgument(String::from("view query must be readable")));
    }
    Ok(Statement::CreateMaterializedView {
        name,
        query: Box::new(query),
        replace: tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("OR")),
        with_data,
    })
}

fn extract_function_body(raw: &str) -> Option<String> {
    let as_pos = find_sql_keyword(raw, "AS", 0)?;
    let body = raw[as_pos + 2..].trim_start();
    if body.starts_with('$') {
        let bytes = body.as_bytes();
        let end = dollar_quote_end(bytes, 0)?;
        let tag = &body[..end];
        let close = body[end..].find(tag)? + end;
        return Some(body[end..close].to_string());
    }
    let quoted_end = body.rfind('\'')?;
    (quoted_end > 0).then(|| unquote(&body[..=quoted_end]))
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

fn trigger_now_columns(body: &str) -> Vec<String> {
    let lower = body.to_ascii_lowercase();
    let mut columns: Vec<String> = Vec::new();
    let mut search = 0;
    while let Some(found) = lower[search..].find("new.") {
        let start = search + found + 4;
        let end = start
            + lower[start..]
                .chars()
                .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
                .map(char::len_utf8)
                .sum::<usize>();
        let mut rest = end;
        while lower.as_bytes().get(rest).is_some_and(u8::is_ascii_whitespace) {
            rest += 1;
        }
        let assignment = if lower[rest..].starts_with(":=") {
            rest + 2
        } else if lower[rest..].starts_with('=') {
            rest + 1
        } else {
            search = start;
            continue;
        };
        let expression_end =
            lower[assignment..].find(';').map_or(lower.len(), |offset| assignment + offset);
        let expression = lower[assignment..expression_end].trim();
        if expression.contains("now()")
            || expression.contains("current_timestamp")
            || expression.contains("clock_timestamp()")
        {
            let column = body[start..end].trim().to_string();
            if !column.is_empty()
                && !columns.iter().any(|current| current.eq_ignore_ascii_case(&column))
            {
                columns.push(column);
            }
        }
        search = end;
    }
    columns
}

fn parse_create_trigger(tokens: &[String]) -> Result<Statement> {
    let name = tokens
        .get(2)
        .map(|value| unqualified_name(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger name")))?;
    let timing_index = tokens
        .iter()
        .position(|token| {
            token.eq_ignore_ascii_case("BEFORE")
                || token.eq_ignore_ascii_case("AFTER")
                || token.eq_ignore_ascii_case("INSTEAD")
        })
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger timing")))?;
    let timing = if tokens[timing_index].eq_ignore_ascii_case("INSTEAD") {
        String::from("INSTEAD OF")
    } else {
        tokens[timing_index].to_ascii_uppercase()
    };
    let on_index = tokens
        .iter()
        .enumerate()
        .skip(timing_index + 1)
        .find(|(_, token)| token.eq_ignore_ascii_case("ON"))
        .map(|(index, _)| index)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger table")))?;
    let table = tokens
        .get(on_index + 1)
        .map(|value| unqualified_name(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger table")))?;
    let events = tokens[timing_index + 1..on_index]
        .iter()
        .filter(|token| {
            matches!(
                token.to_ascii_uppercase().as_str(),
                "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE"
            )
        })
        .map(|token| token.to_ascii_uppercase())
        .collect::<Vec<_>>();
    if events.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("trigger event")));
    }
    let function_index = tokens
        .iter()
        .enumerate()
        .skip(on_index + 2)
        .find(|(_, token)| {
            token.eq_ignore_ascii_case("FUNCTION") || token.eq_ignore_ascii_case("PROCEDURE")
        })
        .map(|(index, _)| index + 1)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger function")))?;
    let function = tokens
        .get(function_index)
        .map(|value| unqualified_name(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("trigger function")))?;
    Ok(Statement::CreateTrigger { name, table, timing, events, function })
}

fn parse_create_policy(tokens: &[String], raw: &str) -> Result<Statement> {
    let name = tokens
        .get(2)
        .map(|value| unquote(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create policy name")))?;
    let on_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("ON"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create policy table")))?;
    if on_index < 3 {
        return Err(RymeError::InvalidArgument(String::from("create policy name")));
    }
    let table = tokens
        .get(on_index + 1)
        .map(|value| {
            let qualified = unquote(value);
            qualified.rsplit('.').next().unwrap_or(&qualified).to_string()
        })
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create policy table")))?;
    let options_end = tokens
        .iter()
        .enumerate()
        .skip(on_index + 2)
        .find(|(_, token)| {
            token.eq_ignore_ascii_case("USING") || token.eq_ignore_ascii_case("WITH")
        })
        .map(|(index, _)| index)
        .unwrap_or(tokens.len());
    if let Some(as_index) = (3..options_end).find(|index| tokens[*index].eq_ignore_ascii_case("AS"))
    {
        if !tokens.get(as_index + 1).is_some_and(|value| value.eq_ignore_ascii_case("PERMISSIVE")) {
            return Err(RymeError::InvalidArgument(String::from("unsupported RLS policy mode")));
        }
    }
    let roles = if let Some(to_index) =
        (3..options_end).find(|index| tokens[*index].eq_ignore_ascii_case("TO"))
    {
        let role_tokens = &tokens[to_index + 1..options_end];
        let roles: Vec<String> = role_tokens
            .iter()
            .filter(|token| token.as_str() != ",")
            .map(|token| unquote(token))
            .collect();
        if roles.is_empty()
            || roles.iter().any(|role| role.is_empty())
            || (roles.iter().any(|role| role.eq_ignore_ascii_case("PUBLIC")) && roles.len() != 1)
        {
            return Err(RymeError::InvalidArgument(String::from("RLS policy roles")));
        }
        if roles[0].eq_ignore_ascii_case("PUBLIC") {
            Vec::new()
        } else {
            roles
        }
    } else {
        Vec::new()
    };
    let command = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("FOR"))
        .and_then(|index| tokens.get(index + 1))
        .map(|value| value.to_ascii_uppercase())
        .unwrap_or_else(|| String::from("ALL"));
    let using = parse_policy_expression(raw, "USING");
    let check = parse_policy_expression(raw, "WITH CHECK");
    if using.is_none() && check.is_none() {
        return Err(RymeError::InvalidArgument(String::from("create policy expression")));
    }
    Ok(Statement::CreatePolicy { name, table, command, roles, using, check })
}

fn parse_policy_expression(raw: &str, keyword: &str) -> Option<String> {
    let start = find_sql_keyword(raw, keyword, 0)? + keyword.len();
    let rest = raw[start..].trim_start();
    if rest.starts_with('(') {
        let close = matching_paren(raw, start + raw[start..].find('(')?)?;
        return Some(raw[start + raw[start..].find('(')? + 1..close].trim().to_string());
    }
    let end = [" USING", " WITH CHECK"]
        .iter()
        .filter_map(|suffix| rest.to_ascii_uppercase().find(suffix))
        .min()
        .unwrap_or(rest.len());
    Some(rest[..end].trim().trim_end_matches(';').to_string())
}

fn policy_tenant_column(expression: &str) -> Option<String> {
    let expression = strip_policy_outer_parens(expression.trim());
    let (left, right) = split_policy_equality(expression)?;
    if policy_auth_uid_side(left) {
        policy_column_name(right)
    } else if policy_auth_uid_side(right) {
        policy_column_name(left)
    } else {
        None
    }
}

fn strip_policy_outer_parens(mut expression: &str) -> &str {
    loop {
        expression = expression.trim();
        if !expression.starts_with('(')
            || matching_paren(expression, 0) != Some(expression.len().saturating_sub(1))
        {
            return expression;
        }
        expression = &expression[1..expression.len() - 1];
    }
}

fn split_policy_equality(expression: &str) -> Option<(&str, &str)> {
    let bytes = expression.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut equality = None;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(delimiter) = quote {
            if byte == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if byte == b'\'' || byte == b'"' {
            quote = Some(byte);
        } else if byte == b'(' {
            depth += 1;
        } else if byte == b')' {
            depth = depth.checked_sub(1)?;
        } else if byte == b'=' && depth == 0 {
            if matches!(bytes.get(index.wrapping_sub(1)), Some(b'!' | b'<' | b'>' | b'='))
                || matches!(bytes.get(index + 1), Some(b'='))
                || equality.is_some()
            {
                return None;
            }
            equality = Some(index);
        }
        index += 1;
    }
    if quote.is_some() || depth != 0 {
        return None;
    }
    let equality = equality?;
    Some((expression[..equality].trim(), expression[equality + 1..].trim()))
}

fn policy_column_name(side: &str) -> Option<String> {
    let candidate = strip_policy_outer_parens(side);
    let candidate = if candidate.starts_with('"') && candidate.ends_with('"') {
        unquote(candidate)
    } else {
        candidate.to_string()
    };
    let mut parts = candidate.split('.').peekable();
    while let Some(part) = parts.next() {
        let mut characters = part.chars();
        let first = characters.next()?;
        if !(first.is_ascii_alphabetic() || first == '_')
            || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return None;
        }
        if parts.peek().is_none()
            && matches!(part.to_ascii_uppercase().as_str(), "TRUE" | "FALSE" | "NULL")
        {
            return None;
        }
    }
    candidate.rsplit('.').next().map(str::to_string)
}

fn policy_auth_uid_side(side: &str) -> bool {
    let mut candidate = side.trim();
    while let Some(inner) = candidate.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        candidate = inner.trim();
    }
    if candidate.get(..7).is_some_and(|prefix| prefix.eq_ignore_ascii_case("select ")) {
        candidate = candidate[7..].trim();
    }
    candidate.eq_ignore_ascii_case("auth.uid()")
}

fn parse_named_table_constraints(raw: &str) -> Result<Vec<TableConstraint>> {
    let Some(open) = raw.find('(') else { return Ok(Vec::new()) };
    let Some(close) = matching_paren(raw, open) else { return Ok(Vec::new()) };
    let mut constraints = Vec::new();
    for item in split_sql_items(&raw[open + 1..close]) {
        if !item
            .split_whitespace()
            .next()
            .is_some_and(|token| token.eq_ignore_ascii_case("CONSTRAINT"))
        {
            continue;
        }
        if let Some(constraint) = parse_alter_table_constraint(&item)? {
            constraints.push(constraint);
        }
    }
    Ok(constraints)
}

fn is_simple_type_using_expression(expression: &str, column: &str) -> bool {
    let expression = expression.trim();
    if expression.eq_ignore_ascii_case(column) {
        return true;
    }
    let Some((source, cast_type)) = expression.split_once("::") else { return false };
    source.trim().eq_ignore_ascii_case(column) && !cast_type.trim().is_empty()
}

fn parse_with(raw: &str) -> Result<Statement> {
    let with_offset = find_sql_keyword(raw, "WITH", 0)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("with clause")))?;
    let cte_start = with_offset + "WITH".len();
    if raw[cte_start..]
        .trim_start()
        .get(..9)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("RECURSIVE"))
    {
        return Err(RymeError::InvalidArgument(String::from("recursive CTEs are not supported")));
    }
    let mut cursor = cte_start;
    let mut ctes = Vec::new();
    loop {
        while raw.as_bytes().get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        let as_offset = find_sql_keyword(raw, "AS", cursor)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("with query")))?;
        let cte_name = raw[cursor..as_offset].trim();
        if cte_name.is_empty() || tokenize(cte_name).len() != 1 {
            return Err(RymeError::InvalidArgument(String::from("with name")));
        }
        let cte_name = unquote(cte_name);
        let open = raw[as_offset + "AS".len()..]
            .find('(')
            .map(|offset| as_offset + "AS".len() + offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("with query")))?;
        let close = matching_paren(raw, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("with query")))?;
        let query = parse(raw[open + 1..close].trim())?;
        ctes.push((cte_name, query));
        cursor = close + 1;
        while raw.as_bytes().get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if raw.as_bytes().get(cursor) == Some(&b',') {
            cursor += 1;
            continue;
        }
        break;
    }
    let body_sql = raw[cursor..].trim();
    if body_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("with body")));
    }
    let mut body = parse(body_sql)?;
    for (cte_name, query) in ctes.into_iter().rev() {
        body = rewrite_simple_cte(query, body, &cte_name)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("unsupported CTE shape")))?;
    }
    Ok(body)
}

fn find_set_operator(input: &str) -> Option<(usize, SetOperation)> {
    let keywords = [
        ("UNION", SetOperation::Union, 1usize),
        ("EXCEPT", SetOperation::Except, 1usize),
        ("INTERSECT", SetOperation::Intersect, 2usize),
    ];
    let mut quote = None;
    let mut depth = 0usize;
    let mut selected: Option<(usize, SetOperation, usize)> = None;
    for (index, ch) in input.char_indices() {
        if let Some(delimiter) = quote {
            if ch == delimiter {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                continue;
            }
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                continue;
            }
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        for (keyword, operation, precedence) in keywords {
            if !input[index..]
                .get(..keyword.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
            {
                continue;
            }
            let previous_is_word = input[..index]
                .chars()
                .next_back()
                .is_some_and(|previous| previous.is_ascii_alphanumeric() || previous == '_');
            let next_is_word = input[index + keyword.len()..]
                .chars()
                .next()
                .is_some_and(|next| next.is_ascii_alphanumeric() || next == '_');
            if previous_is_word || next_is_word {
                continue;
            }
            let replace = selected.is_none_or(|(selected_index, _, selected_precedence)| {
                precedence < selected_precedence
                    || (precedence == selected_precedence && index > selected_index)
            });
            if replace {
                selected = Some((index, operation, precedence));
            }
        }
    }
    selected.map(|(index, operation, _)| (index, operation))
}

fn parse_set_operation(raw: &str) -> Result<Statement> {
    let (operation_offset, operation) = find_set_operator(raw)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("set operation")))?;
    let keyword_len = match operation {
        SetOperation::Union => "UNION".len(),
        SetOperation::Intersect => "INTERSECT".len(),
        SetOperation::Except => "EXCEPT".len(),
    };
    let left_sql = raw[..operation_offset].trim();
    let mut right_sql = raw[operation_offset + keyword_len..].trim();
    if left_sql.is_empty() || right_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("set operation")));
    }
    let mut all = false;
    for modifier in ["ALL", "DISTINCT"] {
        if right_sql
            .get(..modifier.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(modifier))
            && right_sql
                .get(modifier.len()..)
                .and_then(|suffix| suffix.chars().next())
                .is_none_or(|next| next.is_ascii_whitespace())
        {
            all = modifier.eq_ignore_ascii_case("ALL");
            right_sql = right_sql[modifier.len()..].trim_start();
            break;
        }
    }
    Ok(Statement::Union {
        left: Box::new(parse(left_sql)?),
        right: Box::new(parse(right_sql)?),
        all,
        operation,
    })
}

fn cte_source(statement: Statement) -> Option<(String, Vec<Predicate>)> {
    match statement {
        Statement::SelectScan { table, filter, .. } => Some((table, filter)),
        Statement::SelectByKey { table, pk } => Some((
            table,
            vec![Predicate {
                field: Field::Key,
                column: None,
                op: Cmp::Eq,
                operand: pk,
                operands: Vec::new(),
                alternatives: Vec::new(),
            }],
        )),
        _ => None,
    }
}

fn rewrite_simple_cte(query: Statement, body: Statement, cte_name: &str) -> Option<Statement> {
    let (source_table, source_filter) = cte_source(query)?;
    match body {
        Statement::Returning { statement, fields } => rewrite_simple_cte(
            Statement::SelectScan {
                table: source_table,
                limit: 100,
                offset: 0,
                order: Order::default(),
                filter: source_filter,
            },
            *statement,
            cte_name,
        )
        .map(|statement| Statement::Returning { statement: Box::new(statement), fields }),
        Statement::SelectByKey { table, pk } if table.eq_ignore_ascii_case(cte_name) => {
            let mut filter = source_filter;
            filter.push(Predicate {
                field: Field::Key,
                column: None,
                op: Cmp::Eq,
                operand: pk,
                operands: Vec::new(),
                alternatives: Vec::new(),
            });
            Some(Statement::SelectScan {
                table: source_table,
                limit: 100,
                offset: 0,
                order: Order::default(),
                filter,
            })
        }
        Statement::SelectScan { table, limit, offset, order, filter: body_filter }
            if table.eq_ignore_ascii_case(cte_name) =>
        {
            let mut filter = source_filter;
            filter.extend(body_filter);
            Some(Statement::SelectScan { table: source_table, limit, offset, order, filter })
        }
        Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter }
            if table.eq_ignore_ascii_case(cte_name) =>
        {
            let mut combined = source_filter;
            combined.extend(filter);
            Some(Statement::SelectColumns {
                table: source_table,
                columns,
                aliases,
                limit,
                offset,
                order,
                filter: combined,
            })
        }
        Statement::Aggregate { table, func, field, column, filter }
            if table.eq_ignore_ascii_case(cte_name) =>
        {
            let mut combined = source_filter;
            combined.extend(filter);
            Some(Statement::Aggregate {
                table: source_table,
                func,
                field,
                column,
                filter: combined,
            })
        }
        Statement::GroupBy {
            table,
            select,
            group,
            group_column,
            filter,
            having,
            limit,
            offset,
            order,
        } if table.eq_ignore_ascii_case(cte_name) => {
            let mut combined = source_filter;
            combined.extend(filter);
            Some(Statement::GroupBy {
                table: source_table,
                select,
                group,
                group_column,
                filter: combined,
                having,
                limit,
                offset,
                order,
            })
        }
        Statement::InsertSelect {
            table,
            columns,
            source_table: body_source,
            source_columns,
            filter,
            upsert,
            on_conflict_do_nothing,
            conflict_target,
            conflict_update,
            conflict_filter,
        } if body_source.eq_ignore_ascii_case(cte_name) => {
            let mut combined = source_filter;
            combined.extend(filter);
            Some(Statement::InsertSelect {
                table,
                columns,
                source_table,
                source_columns,
                filter: combined,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            })
        }
        _ => None,
    }
}

fn parse_create_index(tokens: &[String], raw: &str) -> Result<Statement> {
    let unique = tokens.get(1).is_some_and(|token| token.eq_ignore_ascii_case("UNIQUE"));
    let index_pos = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("INDEX"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("create index")))?;
    let mut name_pos = index_pos + 1;
    if tokens.get(name_pos).is_some_and(|token| token.eq_ignore_ascii_case("CONCURRENTLY")) {
        name_pos += 1;
    }
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
    let if_not_exists = tokens.get(name_pos.saturating_sub(1)).is_some_and(|token| {
        token.eq_ignore_ascii_case("EXISTS")
            && tokens
                .get(name_pos.saturating_sub(2))
                .is_some_and(|previous| previous.eq_ignore_ascii_case("NOT"))
            && tokens
                .get(name_pos.saturating_sub(3))
                .is_some_and(|previous| previous.eq_ignore_ascii_case("IF"))
    });
    let table = table_after(tokens, "ON")?;
    let raw_upper = raw.to_ascii_uppercase();
    let raw_on = raw_upper
        .find(" ON ")
        .map(|position| position + 4)
        .or_else(|| raw_upper.find("ON").map(|position| position + 2))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index table")))?;
    let open = raw[raw_on..]
        .find('(')
        .map(|position| raw_on + position)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index field")))?;
    let close = matching_paren(raw, open)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("index field")))?;
    let fields = split_sql_items(&raw[open + 1..close])
        .into_iter()
        .map(|field| unquote(field.trim()))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("index field")));
    }
    let (field, column, columns) = if fields.len() > 1 {
        (Field::Value, None, fields)
    } else {
        let field_token = fields.first().expect("index fields is not empty");
        parse_field(field_token)
            .map(|field| (field, None, Vec::new()))
            .unwrap_or_else(|| (Field::Value, Some(field_token.clone()), Vec::new()))
    };
    Ok(Statement::CreateIndex { name, table, field, column, columns, unique, if_not_exists })
}

fn parse_column_definitions(raw: &str) -> Result<Vec<ColumnDefinition>> {
    parse_table_definition(raw).map(|(columns, _, _, _)| columns)
}

fn parse_table_definition(raw: &str) -> Result<TableDefinitionParts> {
    let Some(open) = raw.find('(') else {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    };
    let Some(close) = raw.rfind(')') else {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    };
    if close <= open {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    }
    let items = split_sql_items(&raw[open + 1..close]);
    let mut table_primary = Vec::new();
    let mut table_unique = Vec::new();
    let mut checks = Vec::new();
    let mut foreign_keys = Vec::new();
    for item in &items {
        checks.extend(check_expressions(item));
        if let Some(foreign_key) = parse_foreign_key(item)? {
            foreign_keys.push(foreign_key);
        }
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
            table_unique.push(columns);
        }
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
                || words[0].eq_ignore_ascii_case("FOREIGN")
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
    for primary in table_primary {
        let Some(column) =
            columns.iter_mut().find(|column| column.name.eq_ignore_ascii_case(&primary))
        else {
            return Err(RymeError::InvalidArgument(format!(
                "unknown primary key column {primary}"
            )));
        };
        column.primary_key = true;
        column.nullable = false;
    }
    let mut composite_unique = Vec::new();
    for unique in table_unique {
        if unique.len() > 1 {
            composite_unique.push(unique);
            continue;
        }
        let Some(unique) = unique.first() else { continue };
        let Some(column) =
            columns.iter_mut().find(|column| column.name.eq_ignore_ascii_case(unique))
        else {
            return Err(RymeError::InvalidArgument(format!("unknown unique column {unique}")));
        };
        column.unique = true;
    }
    Ok((columns, composite_unique, checks, foreign_keys))
}

fn parse_foreign_key(item: &str) -> Result<Option<ForeignKeyConstraint>> {
    let upper = item.to_ascii_uppercase();
    let Some(references) = upper.find("REFERENCES") else { return Ok(None) };
    let before = &item[..references];
    let before_upper = &upper[..references];
    let (columns, target_start) = if let Some(foreign_key) = before_upper.find("FOREIGN KEY") {
        let open = before[foreign_key + "FOREIGN KEY".len()..]
            .find('(')
            .map(|offset| foreign_key + "FOREIGN KEY".len() + offset)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("foreign key columns")))?;
        let close = matching_paren(item, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("foreign key columns")))?;
        let columns = split_sql_items(&item[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("foreign key columns")));
        }
        (columns, references)
    } else {
        let local = item
            .split_whitespace()
            .next()
            .map(unquote)
            .filter(|column| !column.is_empty())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("foreign key column")))?;
        (vec![local], references)
    };
    let (referenced_table, referenced_columns, on_delete, on_update) =
        parse_references_target(&item[target_start + 10..])?;
    Ok(Some(ForeignKeyConstraint {
        columns,
        referenced_table,
        referenced_columns,
        on_delete,
        on_update,
    }))
}

fn parse_references_target(
    input: &str,
) -> Result<(String, Vec<String>, ForeignKeyAction, ForeignKeyAction)> {
    let trimmed = input.trim();
    let tokens = tokenize(trimmed);
    let referenced_table = tokens
        .first()
        .map(|token| unquote(token))
        .filter(|table| !table.is_empty())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("referenced table")))?;
    let referenced_columns = if let Some(open) = trimmed.find('(') {
        let close = matching_paren(trimmed, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("referenced columns")))?;
        split_sql_items(&trimmed[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect()
    } else {
        Vec::new()
    };
    let close = trimmed
        .find('(')
        .and_then(|open| matching_paren(trimmed, open))
        .unwrap_or(referenced_table.len());
    let suffix = trimmed.get(close + 1..).unwrap_or_default();
    let on_delete = parse_foreign_key_action(suffix, "ON DELETE")?;
    let on_update = parse_foreign_key_action(suffix, "ON UPDATE")?;
    Ok((referenced_table, referenced_columns, on_delete, on_update))
}

fn parse_foreign_key_action(suffix: &str, clause: &str) -> Result<ForeignKeyAction> {
    let upper = suffix.to_ascii_uppercase();
    let Some(offset) = upper.find(clause) else {
        return Ok(ForeignKeyAction::Restrict);
    };
    let action_text = &suffix[offset + clause.len()..];
    let mut actions = action_text.split_whitespace();
    let action = actions.next().unwrap_or_default();
    if action.eq_ignore_ascii_case("CASCADE") {
        return Ok(ForeignKeyAction::Cascade);
    }
    if action.eq_ignore_ascii_case("RESTRICT")
        || action.eq_ignore_ascii_case("NO")
        || action.is_empty()
    {
        return Ok(ForeignKeyAction::Restrict);
    }
    if action.eq_ignore_ascii_case("SET") {
        let set_action = actions.next().unwrap_or_default();
        if set_action.eq_ignore_ascii_case("NULL") {
            return Ok(ForeignKeyAction::SetNull);
        }
        if set_action.eq_ignore_ascii_case("DEFAULT") {
            return Ok(ForeignKeyAction::SetDefault);
        }
        return Err(RymeError::InvalidArgument(format!(
            "unsupported foreign key action {clause} SET {set_action}"
        )));
    }
    Err(RymeError::InvalidArgument(format!("unsupported foreign key action {clause} {action}")))
}

fn check_expressions(item: &str) -> Vec<String> {
    let upper = item.to_ascii_uppercase();
    let mut checks = Vec::new();
    let mut cursor = 0usize;
    while let Some(relative) = upper[cursor..].find("CHECK") {
        let start = cursor + relative;
        let after = start + "CHECK".len();
        let Some(open_relative) = item[after..].find('(') else { break };
        let open = after + open_relative;
        let Some(close) = matching_paren(item, open) else { break };
        let expression = item[open + 1..close].trim();
        if !expression.is_empty() {
            checks.push(expression.to_string());
        }
        cursor = close + 1;
    }
    checks
}

fn check_references_column(expression: &str, column: &str) -> bool {
    tokenize(expression).iter().any(|token| {
        !token.starts_with('\'') && !token.starts_with('"') && token.eq_ignore_ascii_case(column)
    })
}

fn rename_check_column(expression: &str, from: &str, to: &str) -> String {
    tokenize(expression)
        .into_iter()
        .map(|token| {
            if !token.starts_with('\'')
                && !token.starts_with('"')
                && token.eq_ignore_ascii_case(from)
            {
                to.to_string()
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
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
    if tokens.windows(2).any(|window| {
        window[0].eq_ignore_ascii_case("DEFAULT") && window[1].eq_ignore_ascii_case("VALUES")
    }) {
        return Ok(Statement::InsertRow {
            table,
            columns: Vec::new(),
            values: Vec::new(),
            upsert: false,
            on_conflict_do_nothing: false,
            conflict_target: Vec::new(),
            conflict_update: Vec::new(),
            conflict_filter: Vec::new(),
        });
    }
    if let Some(statement) = parse_insert_select(tokens, raw, table.clone())? {
        return Ok(statement);
    }
    if let Some((columns, rows)) = parse_standard_insert_rows(raw)? {
        let conflict = tokens.iter().any(|token| token.eq_ignore_ascii_case("CONFLICT"));
        let (conflict_target, conflict_update, on_conflict_do_nothing, conflict_filter) =
            if conflict {
                parse_conflict_clause(raw)?
            } else {
                (Vec::new(), Vec::new(), false, Vec::new())
            };
        let upsert = conflict && !on_conflict_do_nothing;
        if rows.len() == 1 {
            let values = rows.into_iter().next().unwrap_or_default();
            return Ok(Statement::InsertRow {
                table,
                columns,
                values,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            });
        }
        return Ok(Statement::InsertRows {
            table,
            columns,
            rows,
            upsert,
            on_conflict_do_nothing,
            conflict_target,
            conflict_update,
            conflict_filter,
        });
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
        if tokens.windows(2).any(|window| {
            window[0].eq_ignore_ascii_case("DO") && window[1].eq_ignore_ascii_case("NOTHING")
        }) {
            return Ok(Statement::InsertIgnore { table, pk, value });
        }
        return Ok(Statement::Upsert { table, pk, value });
    }
    Ok(Statement::Insert { table, pk, value })
}

fn parse_insert_select(tokens: &[String], raw: &str, table: String) -> Result<Option<Statement>> {
    let Some(select_start) = find_sql_keyword(raw, "SELECT", 6) else {
        return Ok(None);
    };
    let before_select = &raw[..select_start];
    let columns = if let Some(open) = before_select.find('(') {
        let close = matching_paren(before_select, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("insert columns")))?;
        if !before_select[close + 1..].trim().is_empty() {
            return Err(RymeError::InvalidArgument(String::from("insert columns")));
        }
        split_sql_items(&before_select[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let select_end = [
        find_sql_keyword(raw, "ON CONFLICT", select_start + "SELECT".len()),
        find_sql_keyword(raw, "RETURNING", select_start + "SELECT".len()),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(raw.len());
    let select_sql = raw[select_start..select_end].trim();
    let select_tokens = tokenize(select_sql);
    if select_tokens.iter().any(|token| {
        token.eq_ignore_ascii_case("LIMIT")
            || token.eq_ignore_ascii_case("OFFSET")
            || token.eq_ignore_ascii_case("ORDER")
    }) {
        return Err(RymeError::InvalidArgument(String::from(
            "INSERT SELECT does not support source windows",
        )));
    }
    let select = parse_select(&select_tokens, select_sql)?;
    let (source_table, source_columns, filter) = match select {
        Statement::SelectColumns { table, columns, filter, .. } => (table, columns, filter),
        Statement::SelectScan { table, filter, .. } => (table, Vec::new(), filter),
        _ => {
            return Err(RymeError::InvalidArgument(String::from(
                "INSERT SELECT requires a plain source SELECT",
            )))
        }
    };
    let conflict = tokens.iter().any(|token| token.eq_ignore_ascii_case("CONFLICT"));
    let (conflict_target, conflict_update, on_conflict_do_nothing, conflict_filter) = if conflict {
        parse_conflict_clause(raw)?
    } else {
        (Vec::new(), Vec::new(), false, Vec::new())
    };
    Ok(Some(Statement::InsertSelect {
        table,
        columns,
        source_table,
        source_columns,
        filter,
        upsert: conflict && !on_conflict_do_nothing,
        on_conflict_do_nothing,
        conflict_target,
        conflict_update,
        conflict_filter,
    }))
}

fn parse_conflict_clause(raw: &str) -> Result<ConflictClause> {
    let upper = raw.to_ascii_uppercase();
    let conflict_pos = upper
        .find("ON CONFLICT")
        .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict clause")))?;
    let after_conflict_pos = conflict_pos + "ON CONFLICT".len();
    let do_relative = upper[after_conflict_pos..]
        .find("DO")
        .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict action")))?;
    let do_pos = after_conflict_pos + do_relative;
    let target_text = raw[after_conflict_pos..do_pos].trim();
    let target_columns = if target_text.is_empty() {
        Vec::new()
    } else {
        let open = target_text
            .find('(')
            .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict target")))?;
        let close = matching_paren(target_text, open)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict target")))?;
        if target_text[..open].trim() != "" || !target_text[close + 1..].trim().is_empty() {
            return Err(RymeError::InvalidArgument(String::from("conflict target")));
        }
        split_sql_items(&target_text[open + 1..close])
            .into_iter()
            .map(|column| unquote(column.trim()))
            .filter(|column| !column.is_empty())
            .collect::<Vec<_>>()
    };
    let action = raw[do_pos + 2..].trim().trim_end_matches(';').trim();
    if action.get(..7).is_some_and(|prefix| prefix.eq_ignore_ascii_case("NOTHING")) {
        return Ok((target_columns, Vec::new(), true, Vec::new()));
    }
    let update_pos = action
        .to_ascii_uppercase()
        .find("UPDATE")
        .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict action")))?;
    let set_pos = action[update_pos + "UPDATE".len()..]
        .to_ascii_uppercase()
        .find("SET")
        .map(|offset| update_pos + "UPDATE".len() + offset)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict update")))?;
    let assignments_start = set_pos + "SET".len();
    let returning_pos = find_sql_keyword(action, "RETURNING", assignments_start);
    let where_pos = find_sql_keyword(action, "WHERE", assignments_start);
    let assignments_end =
        [where_pos, returning_pos].into_iter().flatten().min().unwrap_or(action.len());
    let assignments_text = action[assignments_start..assignments_end].trim();
    if assignments_text.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("conflict update")));
    }
    let assignments = split_sql_items(assignments_text)
        .into_iter()
        .map(|assignment| {
            let (column, raw_value) = split_assignment(&assignment)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("conflict assignment")))?;
            let raw_value = raw_value.trim();
            let value = raw_value
                .split_once('.')
                .filter(|(prefix, _)| prefix.trim().eq_ignore_ascii_case("EXCLUDED"))
                .map(|(_, column)| InsertValue::Excluded(unquote(column.trim())))
                .map(Ok)
                .unwrap_or_else(|| parse_assignment_value(raw_value))?;
            Ok((unquote(column.trim()), value))
        })
        .collect::<Result<Vec<_>>>()?;
    let conflict_filter = where_pos
        .map(|start| {
            let end = returning_pos.unwrap_or(action.len());
            parse_where_filter(&tokenize(&action[start..end]))
        })
        .transpose()?
        .unwrap_or_default();
    Ok((target_columns, assignments, false, conflict_filter))
}

fn parse_standard_insert_rows(raw: &str) -> Result<StandardInsertRows> {
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

fn parse_assignment_value(raw: &str) -> Result<InsertValue> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("DEFAULT") {
        return Ok(InsertValue::Default);
    }
    if trimmed.eq_ignore_ascii_case("NULL") {
        return Ok(InsertValue::Null);
    }
    if let Some((prefix, column)) = trimmed.split_once('.') {
        if prefix.trim().eq_ignore_ascii_case("EXCLUDED") {
            return Ok(InsertValue::Excluded(unquote(column.trim())));
        }
    }
    if assignment_contains_expression(trimmed) {
        return Ok(InsertValue::Expression(trimmed.to_string()));
    }
    parse_insert_value(trimmed)
}

fn assignment_contains_expression(raw: &str) -> bool {
    if raw.len() >= 2 && raw.starts_with('-') && raw[1..].trim().parse::<f64>().is_ok() {
        return false;
    }
    if raw.get(..9).is_some_and(|prefix| prefix.eq_ignore_ascii_case("COALESCE("))
        || raw.get(..9).is_some_and(|prefix| prefix.eq_ignore_ascii_case("GREATEST("))
        || raw.get(..6).is_some_and(|prefix| prefix.eq_ignore_ascii_case("LEAST("))
    {
        return true;
    }
    let mut quote = None;
    let mut depth = 0usize;
    for (position, ch) in raw.char_indices() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '+' | '*' | '/' if depth == 0 => return true,
            '-' if depth == 0 && !raw[..position].trim().is_empty() => return true,
            '|' if depth == 0 && raw[..position].ends_with('|') => return true,
            _ => {}
        }
    }
    false
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
        let upper = raw.to_ascii_uppercase();
        let set_offset = upper
            .find(" SET ")
            .map(|offset| offset + 5)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("update assignments")))?;
        if let Some(from_offset) = find_sql_keyword(raw, "FROM", set_offset) {
            return parse_update_from(tokens, raw, table, set_offset, from_offset);
        }
        let where_offset = upper[set_offset..]
            .find(" WHERE ")
            .map(|offset| set_offset + offset)
            .unwrap_or(raw.len());
        let assignment_text = raw[set_offset..where_offset].trim();
        let mut assignments = Vec::new();
        for assignment in split_sql_items(assignment_text) {
            let (column, value) = split_assignment(&assignment)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("update assignment")))?;
            assignments.push((unquote(column.trim()), parse_assignment_value(value.trim())?));
        }
        if assignments.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("update assignments")));
        }
        let where_end = tokens[where_pos..]
            .iter()
            .position(|token| token.eq_ignore_ascii_case("RETURNING"))
            .map(|offset| where_pos + offset)
            .unwrap_or(tokens.len());
        let where_tokens = &tokens[where_pos..where_end];
        if let Ok(pk) = value_after(where_tokens, &["KEY", "PK", "ID"]) {
            return Ok(Statement::UpdateRow { table, pk: pk.into_bytes(), assignments });
        }
        let filter = parse_where_filter(tokens)?;
        if filter.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("update predicate")));
        }
        return Ok(Statement::UpdateWhere { table, assignments, filter });
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

fn parse_update_from(
    tokens: &[String],
    raw: &str,
    table: String,
    set_offset: usize,
    from_offset: usize,
) -> Result<Statement> {
    let where_offset = find_sql_keyword(raw, "WHERE", from_offset + "FROM".len())
        .ok_or_else(|| RymeError::InvalidArgument(String::from("update from predicate")))?;
    let returning_offset = find_sql_keyword(raw, "RETURNING", where_offset + "WHERE".len());
    let assignment_text = raw[set_offset..from_offset].trim();
    let assignments = split_sql_items(assignment_text)
        .into_iter()
        .map(|assignment| {
            let (column, value) = split_assignment(&assignment)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("update assignment")))?;
            let value = value.trim();
            let parsed = parse_assignment_value(value)?;
            let value =
                if value.contains('.') && !value.starts_with('\'') && !value.starts_with('"') {
                    InsertValue::Expression(value.to_string())
                } else {
                    parsed
                };
            Ok((normalize_column_reference(column.trim()), value))
        })
        .collect::<Result<Vec<_>>>()?;
    if assignments.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("update assignments")));
    }

    let source_spec = raw[from_offset + "FROM".len()..where_offset].trim();
    let source_tokens = tokenize(source_spec);
    let source_table = source_tokens
        .first()
        .map(|token| unquote(token))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("update source table")))?;
    let source_alias = relation_alias(&source_tokens, &source_table);
    let target_alias = relation_alias(
        tokens
            .get(1..tokens.iter().position(|token| token.eq_ignore_ascii_case("SET")).unwrap_or(1))
            .unwrap_or_default(),
        &table,
    );

    let where_end = returning_offset.unwrap_or(raw.len());
    let where_tokens = tokenize(&raw[where_offset + "WHERE".len()..where_end]);
    let (target_column, source_column, filter, source_filter) = parse_relation_conditions(
        &where_tokens,
        &target_alias,
        &table,
        &source_alias,
        &source_table,
    )?;
    Ok(Statement::UpdateFrom {
        table,
        assignments,
        source_table,
        target_column,
        source_column,
        filter,
        source_filter,
    })
}

fn relation_alias(tokens: &[String], table: &str) -> String {
    if let Some(position) = tokens.iter().position(|token| token.eq_ignore_ascii_case("AS")) {
        if let Some(alias) = tokens.get(position + 1) {
            return unquote(alias);
        }
    }
    tokens
        .iter()
        .skip(1)
        .find(|token| {
            !token.eq_ignore_ascii_case("SET")
                && !token.eq_ignore_ascii_case("FROM")
                && !token.eq_ignore_ascii_case("WHERE")
        })
        .map(|token| unquote(token))
        .unwrap_or_else(|| table.to_string())
}

fn qualified_reference(raw: &str) -> Option<(String, String)> {
    let value = unquote(raw.trim());
    let (prefix, column) = value.split_once('.')?;
    (!prefix.is_empty() && !column.is_empty()).then_some((prefix.to_string(), column.to_string()))
}

fn split_and_conditions(tokens: &[String]) -> Vec<Vec<String>> {
    let mut conditions = Vec::new();
    let mut current: Vec<String> = Vec::new();
    for token in tokens {
        let between_value_separator = token.eq_ignore_ascii_case("AND")
            && ((current.len() == 3 && current[1].eq_ignore_ascii_case("BETWEEN"))
                || (current.len() == 4
                    && current[1].eq_ignore_ascii_case("NOT")
                    && current[2].eq_ignore_ascii_case("BETWEEN")));
        if token.eq_ignore_ascii_case("AND") && !between_value_separator {
            if !current.is_empty() {
                conditions.push(std::mem::take(&mut current));
            }
        } else {
            current.push(token.clone());
        }
    }
    if !current.is_empty() {
        conditions.push(current);
    }
    conditions
}

fn append_condition_tokens(target: &mut Vec<String>, condition: Vec<String>) {
    if !target.is_empty() {
        target.push(String::from("AND"));
    }
    target.extend(condition);
}

fn parse_relation_conditions(
    where_tokens: &[String],
    target_alias: &str,
    target_table: &str,
    source_alias: &str,
    source_table: &str,
) -> Result<(String, String, Vec<Predicate>, Vec<Predicate>)> {
    let mut target_column = None;
    let mut source_column = None;
    let mut target_filter_tokens = Vec::new();
    let mut source_filter_tokens = Vec::new();
    for condition in split_and_conditions(where_tokens) {
        if condition.len() == 2 {
            if let (Some((left_prefix, left_column)), Some((right_prefix, right_column))) =
                (qualified_reference(&condition[0]), qualified_reference(&condition[1]))
            {
                let left_target = left_prefix.eq_ignore_ascii_case(target_alias)
                    || left_prefix.eq_ignore_ascii_case(target_table);
                let right_target = right_prefix.eq_ignore_ascii_case(target_alias)
                    || right_prefix.eq_ignore_ascii_case(target_table);
                let left_source = left_prefix.eq_ignore_ascii_case(source_alias)
                    || left_prefix.eq_ignore_ascii_case(source_table);
                let right_source = right_prefix.eq_ignore_ascii_case(source_alias)
                    || right_prefix.eq_ignore_ascii_case(source_table);
                if left_target && right_source {
                    target_column = Some(normalize_column_reference(&left_column));
                    source_column = Some(normalize_column_reference(&right_column));
                    continue;
                }
                if right_target && left_source {
                    target_column = Some(normalize_column_reference(&right_column));
                    source_column = Some(normalize_column_reference(&left_column));
                    continue;
                }
            }
        }
        let has_source_prefix = condition.iter().any(|token| {
            qualified_reference(token).is_some_and(|(prefix, _)| {
                prefix.eq_ignore_ascii_case(source_alias)
                    || prefix.eq_ignore_ascii_case(source_table)
            })
        });
        if has_source_prefix {
            append_condition_tokens(&mut source_filter_tokens, condition);
        } else {
            append_condition_tokens(&mut target_filter_tokens, condition);
        }
    }
    let target_column =
        target_column.ok_or_else(|| RymeError::InvalidArgument(String::from("update join")))?;
    let source_column =
        source_column.ok_or_else(|| RymeError::InvalidArgument(String::from("update join")))?;
    let filter = parse_filter(&target_filter_tokens)?;
    let source_filter = parse_filter(&source_filter_tokens)?;
    Ok((target_column, source_column, filter, source_filter))
}

fn parse_delete(tokens: &[String], raw: &str) -> Result<Statement> {
    let table = table_after(tokens, "FROM")?;
    if let Some(from_pos) = tokens.iter().position(|token| token.eq_ignore_ascii_case("FROM")) {
        if let Some(using_pos) =
            tokens.iter().enumerate().skip(from_pos + 1).find_map(|(position, token)| {
                token.eq_ignore_ascii_case("USING").then_some(position)
            })
        {
            let _where_pos = tokens
                .iter()
                .enumerate()
                .skip(using_pos + 1)
                .find_map(|(position, token)| {
                    token.eq_ignore_ascii_case("WHERE").then_some(position)
                })
                .ok_or_else(|| {
                    RymeError::InvalidArgument(String::from("delete using predicate"))
                })?;
            let using_offset = find_sql_keyword(raw, "USING", 0)
                .ok_or_else(|| RymeError::InvalidArgument(String::from("delete using source")))?;
            let where_offset = find_sql_keyword(raw, "WHERE", using_offset + "USING".len())
                .ok_or_else(|| {
                    RymeError::InvalidArgument(String::from("delete using predicate"))
                })?;
            let returning_offset = find_sql_keyword(raw, "RETURNING", where_offset + "WHERE".len());
            let source_spec = raw[using_offset + "USING".len()..where_offset].trim();
            let source_tokens = tokenize(source_spec);
            let source_table = source_tokens
                .first()
                .map(|token| unquote(token))
                .ok_or_else(|| RymeError::InvalidArgument(String::from("delete using source")))?;
            let source_alias = relation_alias(&source_tokens, &source_table);
            let target_alias =
                relation_alias(tokens.get(from_pos + 1..using_pos).unwrap_or_default(), &table);
            let where_end = returning_offset.unwrap_or(raw.len());
            let where_tokens = tokenize(&raw[where_offset + "WHERE".len()..where_end]);
            let (target_column, source_column, filter, source_filter) = parse_relation_conditions(
                &where_tokens,
                &target_alias,
                &table,
                &source_alias,
                &source_table,
            )
            .map_err(|_| RymeError::InvalidArgument(String::from("delete using join")))?;
            return Ok(Statement::DeleteUsing {
                table,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            });
        }
    }
    if tokens.iter().any(|token| token.eq_ignore_ascii_case("WHERE")) {
        let filter = parse_where_filter(tokens)?;
        if filter.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("delete predicate")));
        }
        if filter.len() == 1
            && filter[0].column.is_none()
            && filter[0].field == Field::Key
            && filter[0].op == Cmp::Eq
        {
            return Ok(Statement::Delete { table, pk: filter[0].operand.clone() });
        }
        return Ok(Statement::DeleteWhere { table, filter });
    }
    let pk = value_after(tokens, &["KEY", "PK", "ID"])?;
    Ok(Statement::Delete { table, pk: pk.into_bytes() })
}

fn select_items(raw: &str) -> Option<Vec<String>> {
    let statement = raw.trim().trim_end_matches(';').trim();
    if !statement.get(..6).is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT")) {
        return None;
    }
    let from = find_sql_keyword(statement, "FROM", 6)?;
    let projection = statement[6..from].trim();
    let projection =
        if projection.get(..8).is_some_and(|prefix| prefix.eq_ignore_ascii_case("DISTINCT")) {
            projection[8..].trim_start()
        } else {
            projection
        };
    Some(split_sql_items(projection))
}

fn find_sql_keyword(input: &str, keyword: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    let mut depth = 0usize;
    for (index, ch) in input.char_indices() {
        if index < start {
            continue;
        }
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                continue;
            }
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                continue;
            }
            _ => {}
        }
        if depth != 0
            || !input[index..]
                .get(..keyword.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(keyword))
        {
            continue;
        }
        let previous_is_word = input[..index]
            .chars()
            .next_back()
            .is_some_and(|previous| previous.is_ascii_alphanumeric() || previous == '_');
        let next_is_word = input[index + keyword.len()..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_alphanumeric() || next == '_');
        if !previous_is_word && !next_is_word {
            return Some(index);
        }
    }
    None
}

fn parse_aggregate_item(raw: &str) -> Option<Result<(AggFunc, Field, Option<String>)>> {
    let item = raw.trim();
    let name_end = item.find(|character: char| character.is_whitespace() || character == '(')?;
    let func = AggFunc::parse(&item[..name_end])?;
    if !item[name_end..].trim_start().starts_with('(') {
        return None;
    }
    let tokens = tokenize(item);
    let field = match tokens.get(1).map(String::as_str) {
        Some("*") if func == AggFunc::Count => Ok((Field::Value, None)),
        Some("*") => Err(RymeError::InvalidArgument(String::from("aggregate field"))),
        Some(name) => Ok(parse_predicate_field(name)),
        None => Err(RymeError::InvalidArgument(String::from("aggregate field"))),
    };
    Some(field.map(|(field, column)| (func, field, column)))
}

fn parse_select_values(raw: &str) -> Result<Statement> {
    let trimmed = raw.trim().trim_end_matches(';').trim();
    let values_sql = trimmed
        .get("SELECT".len()..)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("select values")))?
        .trim();
    if values_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("select values")));
    }
    if let Some(item) = split_sql_items(values_sql).first() {
        if split_sql_items(values_sql).len() == 1 {
            if let Some(statement) = parse_sequence_value(item.trim())? {
                return Ok(statement);
            }
        }
    }
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for item in split_sql_items(values_sql) {
        let item = item.trim();
        if item.is_empty() || item == "*" {
            return Err(RymeError::InvalidArgument(String::from("select values")));
        }
        let (expression, column) = if let Some(position) = find_sql_keyword(item, "AS", 0) {
            let expression = item[..position].trim();
            let column = unquote(item[position + "AS".len()..].trim());
            if expression.is_empty() || column.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("select alias")));
            }
            (expression.to_string(), column)
        } else {
            (item.to_string(), item.to_string())
        };
        if !select_value_is_supported(&expression) {
            return Err(RymeError::InvalidArgument(String::from("unsupported select value")));
        }
        columns.push(column);
        values.push(expression);
    }
    if values.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("select values")));
    }
    Ok(Statement::SelectValues { columns, values })
}

fn parse_sequence_value(expression: &str) -> Result<Option<Statement>> {
    let Some(open) = expression.find('(') else { return Ok(None) };
    if !expression.ends_with(')') {
        return Ok(None);
    }
    let function = expression[..open].trim().to_ascii_lowercase();
    let operation = match function.as_str() {
        "nextval" => SequenceOperation::Nextval,
        "currval" => SequenceOperation::Currval,
        "setval" => SequenceOperation::Setval,
        _ => return Ok(None),
    };
    let arguments = split_sql_items(&expression[open + 1..expression.len() - 1]);
    if arguments.is_empty() || (operation != SequenceOperation::Setval && arguments.len() != 1) {
        return Err(RymeError::InvalidArgument(String::from("sequence function arguments")));
    }
    let name = sequence_name_argument(&arguments[0])?;
    let (value, is_called) = if operation == SequenceOperation::Setval {
        let value = arguments
            .get(1)
            .and_then(|value| value.trim().parse::<i64>().ok())
            .ok_or_else(|| RymeError::InvalidArgument(String::from("setval value")))?;
        let is_called =
            arguments.get(2).map(|value| value.trim().eq_ignore_ascii_case("true")).unwrap_or(true);
        (Some(value), is_called)
    } else {
        (None, true)
    };
    Ok(Some(Statement::SequenceValue { operation, name, value, is_called }))
}

fn sequence_name_argument(argument: &str) -> Result<String> {
    let name = argument.split_once("::").map(|(name, _)| name).unwrap_or(argument).trim();
    let name = unquote(name);
    if name.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("sequence name")));
    }
    Ok(name)
}

fn sequence_name_from_default(default: &str) -> Option<String> {
    let open = default.to_ascii_lowercase().find("nextval")?;
    let open = default[open..].find('(')? + open;
    let close = default[open + 1..].rfind(')')? + open + 1;
    let argument = default[open + 1..close].trim();
    sequence_name_argument(argument).ok()
}

fn parse_select(tokens: &[String], raw: &str) -> Result<Statement> {
    let table = table_after(tokens, "FROM")?;
    if tokens.iter().any(|t| t.eq_ignore_ascii_case("GROUP")) {
        return parse_group(tokens, &table, raw);
    }
    if let Some(items) = select_items(raw) {
        if items.len() == 1 {
            if let Some(aggregate) = parse_aggregate_item(&items[0]) {
                let (func, field, column) = aggregate?;
                let filter = parse_where_filter(tokens)?;
                return Ok(Statement::Aggregate { table, func, field, column, filter });
            }
        }
    }
    let has_where = tokens.iter().any(|t| t.eq_ignore_ascii_case("WHERE"));
    let has_join = tokens.iter().any(|t| t.eq_ignore_ascii_case("JOIN"));
    let projection = parse_projection(tokens, raw)?;
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
    if let Some((columns, aliases)) = projection {
        return Ok(Statement::SelectColumns {
            table,
            columns,
            aliases,
            limit,
            offset,
            order,
            filter,
        });
    }
    Ok(Statement::SelectScan { table, limit, offset, order, filter })
}

fn parse_projection(tokens: &[String], raw: &str) -> Result<ProjectionColumns> {
    let _from = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("missing table")))?;
    let selected = select_items(raw)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("select projection")))?;
    if selected.is_empty() || (selected.len() == 1 && selected[0].trim() == "*") {
        return Ok(None);
    }
    let mut columns = Vec::with_capacity(selected.len());
    let mut aliases = Vec::with_capacity(selected.len());
    for item in selected {
        let item = item.trim();
        if item == "*" {
            return Err(RymeError::InvalidArgument(String::from("select projection")));
        }
        let (source, alias) = if let Some(position) = find_sql_keyword(item, "AS", 0) {
            let source = item[..position].trim();
            let alias = unquote(item[position + "AS".len()..].trim());
            if source.is_empty() || alias.is_empty() || tokenize(&alias).len() != 1 {
                return Err(RymeError::InvalidArgument(String::from("select alias")));
            }
            (source, Some(alias))
        } else {
            (item, None)
        };
        let source_tokens = tokenize(source);
        if source_tokens.len() != 1 {
            return Err(RymeError::InvalidArgument(String::from("select projection")));
        }
        columns.push(normalize_column_reference(source));
        aliases.push(alias);
    }
    Ok(Some((columns, aliases)))
}

fn parse_group(tokens: &[String], table: &str, raw: &str) -> Result<Statement> {
    let from_pos = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| RymeError::InvalidArgument(String::from("missing table")))?;
    let mut select = Vec::new();
    let items =
        select_items(raw).ok_or_else(|| RymeError::InvalidArgument(String::from("select item")))?;
    for item in items {
        if let Some(aggregate) = parse_aggregate_item(&item) {
            let (func, field, column) = aggregate?;
            select.push(SelectItem::Agg(func, field, column));
            continue;
        }
        let item_tokens = tokenize(&item);
        if item_tokens.len() == 1 {
            if let Some(field) = parse_field(&item_tokens[0]) {
                select.push(SelectItem::Field(field));
                continue;
            }
            select.push(SelectItem::Column(normalize_column_reference(&item_tokens[0])));
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
    let group_token = tokens
        .get(group_pos + 2)
        .ok_or_else(|| RymeError::InvalidArgument(String::from("group field")))?;
    let (group, group_column) = parse_field(group_token)
        .map(|field| (field, None))
        .unwrap_or((Field::Value, Some(normalize_column_reference(group_token))));
    for item in &select {
        match item {
            SelectItem::Field(field) => {
                if group_column.is_some() || *field != group {
                    return Err(RymeError::InvalidArgument(String::from("group field")));
                }
            }
            SelectItem::Column(column) => {
                if !group_column.as_deref().is_some_and(|group| group.eq_ignore_ascii_case(column))
                {
                    return Err(RymeError::InvalidArgument(String::from("group field")));
                }
            }
            SelectItem::Agg(_, _, _) => {}
        }
    }
    let (limit, offset, order) = parse_scan_tail(tokens)?;
    let filter = parse_where_filter(tokens)?;
    let having = parse_having_filter(tokens)?;
    Ok(Statement::GroupBy {
        table: table.to_string(),
        select,
        group,
        group_column,
        filter,
        having,
        limit,
        offset,
        order,
    })
}

fn parse_having_filter(tokens: &[String]) -> Result<Vec<Predicate>> {
    let Some(start) = tokens.iter().position(|token| token.eq_ignore_ascii_case("HAVING")) else {
        return Ok(Vec::new());
    };
    let mut clause = Vec::new();
    let mut index = start + 1;
    while index < tokens.len()
        && !tokens[index].eq_ignore_ascii_case("ORDER")
        && !tokens[index].eq_ignore_ascii_case("LIMIT")
        && !tokens[index].eq_ignore_ascii_case("OFFSET")
    {
        if AggFunc::parse(&tokens[index]).is_some()
            && tokens.get(index + 1).is_some_and(|token| !is_predicate_operator(token))
        {
            clause.push(tokens[index].clone());
            index += 2;
        } else {
            clause.push(tokens[index].clone());
            index += 1;
        }
    }
    if clause.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("having predicate")));
    }
    parse_filter(&clause)
}

fn is_predicate_operator(token: &str) -> bool {
    matches!(
        token.to_ascii_uppercase().as_str(),
        "=" | "!"
            | "!="
            | "<>"
            | ">"
            | ">="
            | "<"
            | "<="
            | "IN"
            | "NOT"
            | "IS"
            | "LIKE"
            | "ILIKE"
            | "CONTAINS"
            | "BETWEEN"
    )
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
    let join_type = match tokens.get(join_pos.wrapping_sub(1)).map(String::as_str) {
        Some(token) if token.eq_ignore_ascii_case("LEFT") => JoinType::Left,
        Some(token) if token.eq_ignore_ascii_case("RIGHT") => JoinType::Right,
        Some(token) if token.eq_ignore_ascii_case("FULL") => JoinType::Full,
        Some(token) if token.eq_ignore_ascii_case("INNER") => JoinType::Inner,
        Some(token) if token.eq_ignore_ascii_case("OUTER") => {
            match tokens.get(join_pos.wrapping_sub(2)).map(String::as_str) {
                Some(prefix) if prefix.eq_ignore_ascii_case("LEFT") => JoinType::Left,
                Some(prefix) if prefix.eq_ignore_ascii_case("RIGHT") => JoinType::Right,
                Some(prefix) if prefix.eq_ignore_ascii_case("FULL") => JoinType::Full,
                _ => return Err(RymeError::InvalidArgument(String::from("join type"))),
            }
        }
        _ => JoinType::Inner,
    };
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
        join_type,
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
                    order.column = None;
                } else {
                    order.field = Field::Value;
                    order.column = Some(normalize_column_reference(field));
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

fn order_value(order: &Order, pk: &[u8], raw: &[u8]) -> Vec<u8> {
    if let Some(column) = order.column.as_deref() {
        let Some(serde_json::Value::Object(object)) = serde_json::from_slice(raw).ok() else {
            return Vec::new();
        };
        return object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, value)| value)
            .or_else(|| json_column_value(column, &object))
            .filter(|value| !value.is_null())
            .map(json_result_bytes)
            .unwrap_or_default();
    }
    match order.field {
        Field::Key => pk.to_vec(),
        Field::Value => raw.to_vec(),
    }
}

fn compare_order(order: &Order, left: &Row, right: &Row) -> std::cmp::Ordering {
    let comparison =
        order_value(order, &left.0, &left.1).cmp(&order_value(order, &right.0, &right.1));
    match order.direction {
        Direction::Asc => comparison,
        Direction::Desc => comparison.reverse(),
    }
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
        && !tokens[index].eq_ignore_ascii_case("RETURNING")
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
    let mut alternatives = Vec::new();
    let mut conjunction = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut pending_or = false;
    for token in clause.iter().chain(std::iter::once(&String::from("AND"))) {
        let between_value_separator = token.eq_ignore_ascii_case("AND")
            && ((current.len() == 3 && current[1].eq_ignore_ascii_case("BETWEEN"))
                || (current.len() == 4
                    && current[1].eq_ignore_ascii_case("NOT")
                    && current[2].eq_ignore_ascii_case("BETWEEN")));
        if between_value_separator {
            current.push(token.clone());
            continue;
        }
        if token.eq_ignore_ascii_case("AND") || token.eq_ignore_ascii_case("OR") {
            if !current.is_empty() {
                conjunction.push(parse_predicate(&current)?);
                current.clear();
            }
            if token.eq_ignore_ascii_case("OR") {
                if conjunction.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from("where predicate")));
                }
                alternatives.push(std::mem::take(&mut conjunction));
                pending_or = true;
            }
            continue;
        }
        pending_or = false;
        current.push(token.clone());
    }
    if pending_or {
        return Err(RymeError::InvalidArgument(String::from("where predicate")));
    }
    if !conjunction.is_empty() {
        alternatives.push(conjunction);
    }
    if alternatives.len() <= 1 {
        return Ok(alternatives.pop().unwrap_or_default());
    }
    Ok(vec![Predicate {
        field: Field::Value,
        column: None,
        op: Cmp::AnyOf,
        operand: Vec::new(),
        operands: Vec::new(),
        alternatives,
    }])
}

fn parse_predicate_field(raw: &str) -> (Field, Option<String>) {
    let normalized = normalize_column_reference(raw);
    parse_field(&normalized)
        .map(|field| (field, None))
        .unwrap_or_else(|| (Field::Value, Some(normalized)))
}

fn normalize_column_reference(raw: &str) -> String {
    let unquoted = unquote(raw.trim());
    let Some(operator) = unquoted.find("->") else {
        return unquoted.rsplit_once('.').map(|(_, column)| column.to_string()).unwrap_or(unquoted);
    };
    let base = &unquoted[..operator];
    let base = base.rsplit_once('.').map(|(_, column)| column).unwrap_or(base).trim();
    format!("{}{}", base, &unquoted[operator..])
}

fn parse_predicate(parts: &[String]) -> Result<Predicate> {
    if parts.len() == 2 && !parts[1].eq_ignore_ascii_case("IN") {
        let (field, column) = parse_predicate_field(&parts[0]);
        return Ok(Predicate {
            field,
            column,
            op: Cmp::Eq,
            operand: unquote(&parts[1]).into_bytes(),
            operands: Vec::new(),
            alternatives: Vec::new(),
        });
    }
    if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("IN") {
        let (field, column) = parse_predicate_field(&parts[0]);
        let operands = parts[2..].iter().map(|part| unquote(part).into_bytes()).collect();
        return Ok(Predicate {
            field,
            column,
            op: Cmp::In,
            operand: Vec::new(),
            operands,
            alternatives: Vec::new(),
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
            return Ok(Predicate {
                field,
                column,
                op,
                operand: Vec::new(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            });
        }
        if parts[1] == "=" {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::Eq,
                operand: unquote(&parts[2]).into_bytes(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            });
        }
        if parts[1] == "!" || parts[1] == "!=" || parts[1] == "<>" {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::NotEq,
                operand: unquote(&parts[2]).into_bytes(),
                operands: Vec::new(),
                alternatives: Vec::new(),
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
            return Ok(Predicate {
                field,
                column,
                op,
                operand: unquote(&parts[2]).into_bytes(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            });
        }
        if parts[1].eq_ignore_ascii_case("CONTAINS") {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::Contains,
                operand: unquote(&parts[2]).into_bytes(),
                operands: Vec::new(),
                alternatives: Vec::new(),
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
            return Ok(Predicate {
                field,
                column,
                op,
                operand: unquote(&parts[2]).into_bytes(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            });
        }
    }
    if parts.len() == 4
        && parts[1].eq_ignore_ascii_case("NOT")
        && (parts[2].eq_ignore_ascii_case("LIKE") || parts[2].eq_ignore_ascii_case("ILIKE"))
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        let op = if parts[2].eq_ignore_ascii_case("ILIKE") { Cmp::NotILike } else { Cmp::NotLike };
        return Ok(Predicate {
            field,
            column,
            op,
            operand: unquote(&parts[3]).into_bytes(),
            operands: Vec::new(),
            alternatives: Vec::new(),
        });
    }
    if parts.len() >= 4
        && parts[1].eq_ignore_ascii_case("NOT")
        && parts[2].eq_ignore_ascii_case("IN")
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        let operands = parts[3..].iter().map(|part| unquote(part).into_bytes()).collect();
        return Ok(Predicate {
            field,
            column,
            op: Cmp::NotIn,
            operand: Vec::new(),
            operands,
            alternatives: Vec::new(),
        });
    }
    if parts.len() == 5
        && parts[1].eq_ignore_ascii_case("IS")
        && parts[2].eq_ignore_ascii_case("DISTINCT")
        && parts[3].eq_ignore_ascii_case("FROM")
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        return Ok(Predicate {
            field,
            column,
            op: Cmp::IsDistinct,
            operand: unquote(&parts[4]).into_bytes(),
            operands: Vec::new(),
            alternatives: Vec::new(),
        });
    }
    if parts.len() == 6
        && parts[1].eq_ignore_ascii_case("IS")
        && parts[2].eq_ignore_ascii_case("NOT")
        && parts[3].eq_ignore_ascii_case("DISTINCT")
        && parts[4].eq_ignore_ascii_case("FROM")
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        return Ok(Predicate {
            field,
            column,
            op: Cmp::IsNotDistinct,
            operand: unquote(&parts[5]).into_bytes(),
            operands: Vec::new(),
            alternatives: Vec::new(),
        });
    }
    if parts.len() == 5
        && parts[1].eq_ignore_ascii_case("BETWEEN")
        && parts[3].eq_ignore_ascii_case("AND")
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        let operands = vec![unquote(&parts[2]).into_bytes(), unquote(&parts[4]).into_bytes()];
        return Ok(Predicate {
            field,
            column,
            op: Cmp::Between,
            operand: Vec::new(),
            operands,
            alternatives: Vec::new(),
        });
    }
    if parts.len() == 6
        && parts[1].eq_ignore_ascii_case("NOT")
        && parts[2].eq_ignore_ascii_case("BETWEEN")
        && parts[4].eq_ignore_ascii_case("AND")
    {
        let (field, column) = parse_predicate_field(&parts[0]);
        let operands = vec![unquote(&parts[3]).into_bytes(), unquote(&parts[5]).into_bytes()];
        return Ok(Predicate {
            field,
            column,
            op: Cmp::NotBetween,
            operand: Vec::new(),
            operands,
            alternatives: Vec::new(),
        });
    }
    if parts.len() == 4 {
        let (field, column) = parse_predicate_field(&parts[0]);
        if parts[1].eq_ignore_ascii_case("IS")
            && parts[2].eq_ignore_ascii_case("NOT")
            && parts[3].eq_ignore_ascii_case("NULL")
        {
            return Ok(Predicate {
                field,
                column,
                op: Cmp::IsNotNull,
                operand: Vec::new(),
                operands: Vec::new(),
                alternatives: Vec::new(),
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

fn eval_select_value(raw: &str) -> Result<Vec<u8>> {
    let expression = raw.trim();
    let expression =
        expression.split_once("::").map(|(value, _)| value.trim()).unwrap_or(expression);
    let lower = expression.to_ascii_lowercase();
    let quoted = expression.len() >= 2
        && ((expression.starts_with('\'') && expression.ends_with('\''))
            || (expression.starts_with('"') && expression.ends_with('"')));
    let literal = quoted
        || expression.eq_ignore_ascii_case("NULL")
        || expression.eq_ignore_ascii_case("TRUE")
        || expression.eq_ignore_ascii_case("FALSE")
        || expression.parse::<f64>().is_ok();
    let parameter = expression.strip_prefix('$').is_some_and(|digits| {
        !digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit())
    });
    let builtin = matches!(
        lower.as_str(),
        "version()"
            | "current_database()"
            | "current_user"
            | "current_user()"
            | "current_schema()"
            | "current_catalog"
            | "now()"
            | "now"
            | "gen_random_uuid()"
            | "gen_random_uuid"
    );
    if !literal && !builtin && !parameter {
        return Err(RymeError::InvalidArgument(String::from("unsupported select value")));
    }
    if expression.eq_ignore_ascii_case("NULL") {
        return Ok(SQL_NULL_SENTINEL.to_vec());
    }
    let value = match lower.as_str() {
        "version()" => String::from("PostgreSQL 16.0 on rymeDB"),
        "current_database()" | "current_catalog" => String::from("default"),
        "current_user" | "current_user()" => String::from("ryme"),
        "current_schema()" => String::from("public"),
        _ => eval_operand(expression)?,
    };
    Ok(value.into_bytes())
}

fn select_value_is_supported(raw: &str) -> bool {
    let expression =
        raw.trim().split_once("::").map(|(value, _)| value.trim()).unwrap_or(raw.trim());
    let lower = expression.to_ascii_lowercase();
    let quoted = expression.len() >= 2
        && ((expression.starts_with('\'') && expression.ends_with('\''))
            || (expression.starts_with('"') && expression.ends_with('"')));
    let literal = quoted
        || expression.eq_ignore_ascii_case("NULL")
        || expression.eq_ignore_ascii_case("TRUE")
        || expression.eq_ignore_ascii_case("FALSE")
        || expression.parse::<f64>().is_ok();
    let parameter = expression.strip_prefix('$').is_some_and(|digits| {
        !digits.is_empty() && digits.chars().all(|character| character.is_ascii_digit())
    });
    let builtin = matches!(
        lower.as_str(),
        "version()"
            | "current_database()"
            | "current_user"
            | "current_user()"
            | "current_schema()"
            | "current_catalog"
            | "now()"
            | "now"
            | "gen_random_uuid()"
            | "gen_random_uuid"
    );
    literal || parameter || builtin
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
        || data_type.eq_ignore_ascii_case("real")
        || data_type.eq_ignore_ascii_case("double precision")
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

fn expression_value_with_source(
    expression: &str,
    current: &serde_json::Map<String, serde_json::Value>,
    incoming: Option<&[u8]>,
    source: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<serde_json::Value> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("empty update expression")));
    }
    if expression.starts_with('(')
        && matching_paren(expression, 0) == Some(expression.len().saturating_sub(1))
    {
        return expression_value_with_source(
            &expression[1..expression.len() - 1],
            current,
            incoming,
            source,
        );
    }
    for operators in [&["||"][..], &["+", "-"][..], &["*", "/"][..]] {
        if let Some((position, operator)) = find_expression_operator(expression, operators) {
            let left =
                expression_value_with_source(&expression[..position], current, incoming, source)?;
            let right = expression_value_with_source(
                &expression[position + operator.len()..],
                current,
                incoming,
                source,
            )?;
            return apply_expression_operator(&left, &right, operator);
        }
    }
    if let Some(open) = expression.find('(') {
        if expression.ends_with(')') {
            let function = expression[..open].trim();
            let close = matching_paren(expression, open);
            if close == Some(expression.len() - 1) {
                let args = split_sql_items(&expression[open + 1..expression.len() - 1])
                    .into_iter()
                    .map(|argument| {
                        expression_value_with_source(&argument, current, incoming, source)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if function.eq_ignore_ascii_case("COALESCE") {
                    return args.into_iter().find(|value| !value.is_null()).ok_or_else(|| {
                        RymeError::InvalidArgument(String::from("COALESCE needs an argument"))
                    });
                }
                if function.eq_ignore_ascii_case("GREATEST")
                    || function.eq_ignore_ascii_case("LEAST")
                {
                    let mut numbers =
                        args.iter().filter_map(serde_json::Value::as_f64).collect::<Vec<_>>();
                    if numbers.is_empty() {
                        return Ok(serde_json::Value::Null);
                    }
                    if function.eq_ignore_ascii_case("GREATEST") {
                        let value = numbers.drain(..).reduce(f64::max).unwrap_or(f64::NAN);
                        return number_json_value(value);
                    }
                    let value = numbers.drain(..).reduce(f64::min).unwrap_or(f64::NAN);
                    return number_json_value(value);
                }
            }
        }
    }
    let unquoted = unquote(expression);
    if unquoted.eq_ignore_ascii_case("NULL") {
        return Ok(serde_json::Value::Null);
    }
    if unquoted.eq_ignore_ascii_case("TRUE") || unquoted.eq_ignore_ascii_case("FALSE") {
        return Ok(serde_json::Value::Bool(unquoted.eq_ignore_ascii_case("TRUE")));
    }
    if let Ok(value) = unquoted.parse::<i64>() {
        return Ok(serde_json::Value::Number(value.into()));
    }
    if let Ok(value) = unquoted.parse::<f64>() {
        return number_json_value(value);
    }
    if let Some((prefix, column)) = unquoted.split_once('.') {
        if prefix.eq_ignore_ascii_case("EXCLUDED") {
            let incoming = incoming.ok_or_else(|| {
                RymeError::InvalidArgument(String::from("EXCLUDED outside conflict update"))
            })?;
            if let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(incoming) {
                return Ok(object
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(column.trim()))
                    .map(|(_, value)| value.clone())
                    .unwrap_or(serde_json::Value::Null));
            }
            return Ok(serde_json::Value::String(String::from_utf8_lossy(incoming).to_string()));
        }
        if let Some(source) = source {
            if let Some(value) = json_column_value(column.trim(), source) {
                return Ok(value.clone());
            }
        }
    }
    if let Some(value) = json_column_value(&unquoted, current) {
        return Ok(value.clone());
    }
    Err(RymeError::InvalidArgument(format!("unknown update expression {expression}")))
}

fn find_expression_operator<'a>(
    expression: &str,
    operators: &[&'a str],
) -> Option<(usize, &'a str)> {
    let mut quote = None;
    let mut depth = 0usize;
    let bytes = expression.as_bytes();
    for (position, ch) in expression.char_indices() {
        if let Some(open) = quote {
            if ch == open {
                quote = None;
            }
            continue;
        }
        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                for operator in operators {
                    if expression[position..].starts_with(*operator)
                        && !(*operator == "-"
                            && bytes[..position].iter().all(u8::is_ascii_whitespace))
                    {
                        return Some((position, *operator));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn apply_expression_operator(
    left: &serde_json::Value,
    right: &serde_json::Value,
    operator: &str,
) -> Result<serde_json::Value> {
    if left.is_null() || right.is_null() {
        return Ok(serde_json::Value::Null);
    }
    if operator == "||" {
        let mut result = String::from_utf8_lossy(&json_result_bytes(left)).to_string();
        result.push_str(&String::from_utf8_lossy(&json_result_bytes(right)));
        return Ok(serde_json::Value::String(result));
    }
    let left = left
        .as_f64()
        .ok_or_else(|| RymeError::InvalidArgument(String::from("numeric update expression")))?;
    let right = right
        .as_f64()
        .ok_or_else(|| RymeError::InvalidArgument(String::from("numeric update expression")))?;
    let result = match operator {
        "+" => left + right,
        "-" => left - right,
        "*" => left * right,
        "/" if right != 0.0 => left / right,
        "/" => return Err(RymeError::InvalidArgument(String::from("division by zero"))),
        _ => return Err(RymeError::InvalidArgument(String::from("update expression operator"))),
    };
    number_json_value(result)
}

fn number_json_value(value: f64) -> Result<serde_json::Value> {
    if !value.is_finite() {
        return Err(RymeError::InvalidArgument(String::from("non-finite update expression")));
    }
    if value.fract() == 0.0 && value.abs() < i64::MAX as f64 {
        Ok(serde_json::Value::Number((value as i64).into()))
    } else {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| RymeError::InvalidArgument(String::from("numeric update expression")))
    }
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

fn json_result_bytes_or_null(value: &serde_json::Value) -> Vec<u8> {
    if value.is_null() {
        SQL_NULL_SENTINEL.to_vec()
    } else {
        json_result_bytes(value)
    }
}

fn excluded_value_bytes(expression: &[u8], incoming: &[u8]) -> Option<Vec<u8>> {
    let expression = std::str::from_utf8(expression).ok()?.trim();
    let (prefix, column) = expression.split_once('.')?;
    if !prefix.eq_ignore_ascii_case("EXCLUDED") {
        return Some(expression.as_bytes().to_vec());
    }
    let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(incoming) else {
        return Some(incoming.to_vec());
    };
    object
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column.trim()))
        .and_then(|(_, value)| (!value.is_null()).then(|| json_result_bytes(value)))
}

fn substitute_excluded_predicate(predicate: &Predicate, incoming: &[u8]) -> Option<Predicate> {
    if predicate.column.as_deref().is_some_and(|column| {
        column.split_once('.').is_some_and(|(prefix, _)| prefix.eq_ignore_ascii_case("EXCLUDED"))
    }) {
        return None;
    }
    let mut substituted = predicate.clone();
    if let Some(value) = excluded_value_bytes(&predicate.operand, incoming) {
        substituted.operand = value;
    } else if std::str::from_utf8(&predicate.operand).ok().is_some_and(|operand| {
        operand.split_once('.').is_some_and(|(prefix, _)| prefix.eq_ignore_ascii_case("EXCLUDED"))
    }) {
        return None;
    }
    let mut operands = Vec::with_capacity(predicate.operands.len());
    for operand in &predicate.operands {
        let value = excluded_value_bytes(operand, incoming)?;
        operands.push(value);
    }
    substituted.operands = operands;
    substituted.alternatives = predicate
        .alternatives
        .iter()
        .map(|branch| {
            branch
                .iter()
                .map(|nested| substitute_excluded_predicate(nested, incoming))
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Option<Vec<_>>>()?;
    Some(substituted)
}

fn convert_json_column_value(
    value: &serde_json::Value,
    data_type: &str,
) -> Result<serde_json::Value> {
    if value.is_null() {
        return Ok(serde_json::Value::Null);
    }
    let converted = json_insert_value(Some(json_result_bytes(value)), data_type);
    let invalid = if data_type.contains("int")
        || data_type.contains("serial")
        || data_type.contains("numeric")
        || data_type.contains("decimal")
        || data_type.eq_ignore_ascii_case("real")
        || data_type.eq_ignore_ascii_case("double precision")
    {
        !converted.is_number()
    } else if data_type.eq_ignore_ascii_case("bool") || data_type.eq_ignore_ascii_case("boolean") {
        !converted.is_boolean()
    } else if data_type.eq_ignore_ascii_case("json") || data_type.eq_ignore_ascii_case("jsonb") {
        matches!(converted, serde_json::Value::String(_))
    } else if data_type.ends_with("[]") || data_type.eq_ignore_ascii_case("array") {
        !converted.is_array()
    } else {
        false
    };
    if invalid {
        return Err(RymeError::InvalidArgument(format!(
            "invalid input syntax for type {data_type}"
        )));
    }
    Ok(converted)
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

fn encode_key_parts(parts: &[Vec<u8>]) -> Option<Vec<u8>> {
    let mut encoded = Vec::new();
    for part in parts {
        if part.is_empty() {
            return None;
        }
        let length = u32::try_from(part.len()).ok()?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(part);
    }
    Some(encoded)
}

fn decode_key_parts(encoded: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut parts = Vec::new();
    let mut cursor = 0usize;
    while cursor < encoded.len() {
        let length_bytes = encoded.get(cursor..cursor + 4)?;
        let length = u32::from_be_bytes(length_bytes.try_into().ok()?) as usize;
        cursor = cursor.checked_add(4)?;
        let end = cursor.checked_add(length)?;
        parts.push(encoded.get(cursor..end)?.to_vec());
        cursor = end;
    }
    (!parts.is_empty()).then_some(parts)
}

fn referenced_columns_are_primary(
    referenced_columns: &[String],
    definitions: &[ColumnDefinition],
) -> bool {
    let primary = definitions
        .iter()
        .filter(|definition| definition.primary_key)
        .map(|definition| definition.name.as_str())
        .collect::<Vec<_>>();
    referenced_columns.len() == primary.len()
        && referenced_columns
            .iter()
            .zip(primary)
            .all(|(referenced, primary)| referenced.eq_ignore_ascii_case(primary))
}

fn index_value(definition: &IndexDefinition, pk: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    if !definition.columns.is_empty() {
        let serde_json::Value::Object(object) = serde_json::from_slice(value).ok()? else {
            return None;
        };
        let mut encoded = Vec::new();
        for column in &definition.columns {
            let selected = json_column_value(column, &object)?;
            if selected.is_null() {
                return None;
            }
            let bytes = json_result_bytes(selected);
            let length = u32::try_from(bytes.len()).ok()?;
            encoded.extend_from_slice(&length.to_be_bytes());
            encoded.extend_from_slice(&bytes);
        }
        return Some(encoded);
    }
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

fn index_filter_value(
    definition: &IndexDefinition,
    filter: &[Predicate],
) -> Option<(Vec<u8>, Vec<usize>)> {
    if definition.columns.is_empty() {
        return None;
    }
    let mut encoded = Vec::new();
    let mut covered = Vec::with_capacity(definition.columns.len());
    for column in &definition.columns {
        let (predicate_index, predicate) = filter.iter().enumerate().find(|(_, predicate)| {
            predicate.op == Cmp::Eq
                && predicate.field == Field::Value
                && predicate
                    .column
                    .as_deref()
                    .is_some_and(|indexed| indexed.eq_ignore_ascii_case(column))
        })?;
        if is_null_bytes(&predicate.operand) {
            return None;
        }
        let length = u32::try_from(predicate.operand.len()).ok()?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(&predicate.operand);
        covered.push(predicate_index);
    }
    Some((encoded, covered))
}

fn rename_index_column(indexed: &str, from: &str, to: &str) -> String {
    if indexed.eq_ignore_ascii_case(from) {
        return to.to_string();
    }
    let prefix = format!("{from}->");
    if indexed.len() >= prefix.len() && indexed[..prefix.len()].eq_ignore_ascii_case(&prefix) {
        return format!("{to}{}", &indexed[prefix.len()..]);
    }
    indexed.to_string()
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
    let from_or_to = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("FROM") || token.eq_ignore_ascii_case("TO"));
    let columns = from_or_to
        .filter(|position| *position > 2)
        .map(|position| {
            tokens[2..position]
                .iter()
                .map(|column| unquote(column))
                .filter(|column| !column.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if from_or_to.is_some_and(|position| tokens[position].eq_ignore_ascii_case("TO")) {
        Ok(Statement::CopyTo { table, columns })
    } else {
        Ok(Statement::CopyFrom { table, columns, rows: Vec::new() })
    }
}

fn parse_explain(input: &str) -> Result<Statement> {
    let trimmed = input.trim();
    let inner_sql = trimmed
        .get("EXPLAIN".len()..)
        .filter(|_| trimmed[.."EXPLAIN".len()].eq_ignore_ascii_case("EXPLAIN"))
        .unwrap_or("")
        .trim_start_matches(|character: char| {
            character.is_whitespace() || matches!(character, ':' | ';')
        });
    if inner_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("explain input")));
    }
    let (analyze, inner_sql) = if inner_sql
        .split_whitespace()
        .next()
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("ANALYZE"))
    {
        let offset = inner_sql.find(char::is_whitespace).unwrap_or(inner_sql.len());
        (true, inner_sql[offset..].trim_start())
    } else {
        (false, inner_sql)
    };
    if inner_sql.is_empty() {
        return Err(RymeError::InvalidArgument(String::from("explain input")));
    }
    let inner = parse(inner_sql)?;
    let plan = describe_plan(&inner);
    Ok(Statement::Explain { plan, inner: Box::new(inner), analyze })
}

pub fn describe_plan(statement: &Statement) -> String {
    match statement {
        Statement::CreateSchema { schema, .. } => format!("ddl create_schema({schema})"),
        Statement::CreateExtension { name, schema, .. } => {
            format!(
                "ddl create_extension({name}{})",
                schema.as_deref().map_or(String::new(), |schema| format!(" in {schema}"))
            )
        }
        Statement::CreateSequence { name, .. } => format!("ddl create_sequence({name})"),
        Statement::CreateFunction { name, language, .. } => {
            format!("ddl create_function({name}) language {language}")
        }
        Statement::CreateView { name, replace, .. } => {
            format!("ddl {}view({name})", if *replace { "replace_" } else { "create_" })
        }
        Statement::CreateMaterializedView { name, replace, .. } => format!(
            "ddl {}materialized_view({name})",
            if *replace { "replace_" } else { "create_" }
        ),
        Statement::CreateTrigger { name, table, timing, events, function } => format!(
            "ddl create_trigger({name} on {table} {timing} {} -> {function})",
            events.join("/")
        ),
        Statement::CreatePolicy { name, table, command, .. } => {
            format!("ddl create_policy({name}) on {table} for {command}")
        }
        Statement::DropPolicy { name, table, .. } => {
            format!("ddl drop_policy({name}) on {table}")
        }
        Statement::AlterTableRls { table, enabled, forced } => {
            format!("ddl alter_table({table}) row_security({enabled:?}, {forced:?})")
        }
        Statement::CreateTable { table, columns, .. } => {
            format!("ddl create_table({table}) columns {}", columns.len())
        }
        Statement::DropTable { table, .. } => format!("ddl drop_table({table})"),
        Statement::DropIndex { name, .. } => format!("ddl drop_index({name})"),
        Statement::DropTrigger { name, table, .. } => {
            format!("ddl drop_trigger({name}) on {table}")
        }
        Statement::DropView { name, .. } => format!("ddl drop_view({name})"),
        Statement::DropMaterializedView { name, .. } => {
            format!("ddl drop_materialized_view({name})")
        }
        Statement::RefreshMaterializedView { name, .. } => {
            format!("ddl refresh_materialized_view({name})")
        }
        Statement::DropSequence { name, .. } => format!("ddl drop_sequence({name})"),
        Statement::AlterSequence { name, restart, increment } => {
            format!("ddl alter_sequence({name}) restart {restart:?} increment {increment:?}")
        }
        Statement::CreateRole { name, .. } => format!("ddl create_role({name})"),
        Statement::AlterRoleInheritance { name, inherit } => {
            format!("ddl alter_role({name}) {}", if *inherit { "inherit" } else { "noinherit" })
        }
        Statement::DropRole { name, .. } => format!("ddl drop_role({name})"),
        Statement::RoleMembershipChange { grant, roles, members } => format!(
            "ddl {} roles {} to {}",
            if *grant { "grant" } else { "revoke" },
            roles.join(","),
            members.join(",")
        ),
        Statement::PrivilegeChange {
            grant, object_type, objects, privileges, grantees, ..
        } => {
            format!(
                "ddl {} {} {} on {} to {}",
                if *grant { "grant" } else { "revoke" },
                privileges.join(","),
                object_type,
                objects.join(","),
                grantees.join(",")
            )
        }
        Statement::DefaultPrivilegeChange {
            grant,
            object_type,
            schema,
            privileges,
            grantees,
            ..
        } => {
            format!(
                "ddl {} default {} {} on {:?} to {}",
                if *grant { "grant" } else { "revoke" },
                privileges.join(","),
                object_type,
                schema,
                grantees.join(",")
            )
        }
        Statement::TruncateTable { table, restart_identity, cascade } => {
            format!(
                "write truncate({table}) {} {}",
                if *restart_identity { "restart_identity" } else { "continue_identity" },
                if *cascade { "cascade" } else { "restrict" }
            )
        }
        Statement::AlterTableDropConstraint { table, constraint, .. } => {
            format!("ddl alter_table({table}) drop_constraint({constraint})")
        }
        Statement::AlterTableAddColumn { table, column, .. } => {
            format!("ddl alter_table({table}) add_column({})", column.name)
        }
        Statement::AlterTableAddConstraint { table, constraint } => {
            format!("ddl alter_table({table}) add_constraint({constraint:?})")
        }
        Statement::AlterTableDropColumn { table, column, .. } => {
            format!("ddl alter_table({table}) drop_column({column})")
        }
        Statement::AlterTableRenameColumn { table, from, to } => {
            format!("ddl alter_table({table}) rename_column({from}, {to})")
        }
        Statement::AlterTableColumn { table, column, alteration } => {
            format!("ddl alter_table({table}) alter_column({column}, {alteration:?})")
        }
        Statement::CreateIndex { name, table, field, column, columns, unique, .. } => {
            let field = if columns.is_empty() {
                column
                    .as_deref()
                    .unwrap_or(match field {
                        Field::Key => "key",
                        Field::Value => "value",
                    })
                    .to_string()
            } else {
                columns.join(", ")
            };
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
        Statement::InsertSelect { table, source_table, upsert, .. } => format!(
            "write {}({table}) from select({source_table})",
            if *upsert { "upsert" } else { "insert" }
        ),
        Statement::Upsert { table, .. } => format!("write upsert({table}) point"),
        Statement::InsertIgnore { table, .. } => format!("write insert({table}) ignore conflicts"),
        Statement::InsertConflict { table, do_nothing, .. } => format!(
            "write insert({table}) {} conflict target",
            if *do_nothing { "ignore" } else { "update" }
        ),
        Statement::SelectByKey { table, .. } => {
            format!("point_lookup({table}) using primary index")
        }
        Statement::SelectScan { table, limit, offset, order, filter } => {
            let direction = match order.direction {
                Direction::Asc => "asc",
                Direction::Desc => "desc",
            };
            let field = order.column.as_deref().unwrap_or(match order.field {
                Field::Key => "key",
                Field::Value => "value",
            });
            format!(
                "scan({table}) limit {limit} offset {offset} order {field} {direction} filters {} using ordered range",
                filter.len()
            )
        }
        Statement::SelectColumns { table, columns, limit, offset, .. } => {
            format!("project({table}) columns {} limit {limit} offset {offset}", columns.len())
        }
        Statement::SelectValues { columns, .. } => {
            format!("values select columns {}", columns.len())
        }
        Statement::Update { table, .. } => format!("write update({table}) point"),
        Statement::UpdateRow { table, assignments, .. } => {
            format!("write update({table}) columns {}", assignments.len())
        }
        Statement::UpdateWhere { table, assignments, filter } => {
            format!("write update({table}) columns {} filters {}", assignments.len(), filter.len())
        }
        Statement::UpdateFrom {
            table, assignments, source_table, filter, source_filter, ..
        } => {
            format!(
                "write update({table}) from {source_table} columns {} filters {}+{}",
                assignments.len(),
                filter.len(),
                source_filter.len()
            )
        }
        Statement::Delete { table, .. } => format!("write delete({table}) point"),
        Statement::DeleteWhere { table, filter } => {
            format!("write delete({table}) filters {}", filter.len())
        }
        Statement::DeleteUsing { table, source_table, filter, source_filter, .. } => {
            format!(
                "write delete({table}) using {source_table} filters {}+{}",
                filter.len(),
                source_filter.len()
            )
        }
        Statement::CopyFrom { table, .. } => format!("bulk ingest({table}) batched put"),
        Statement::CopyTo { table, columns } => {
            format!("bulk export({table}) {} columns", columns.len())
        }
        Statement::Returning { statement, fields } => {
            format!("{} returning {} fields", describe_plan(statement), fields.len())
        }
        Statement::Aggregate { table, func, filter, .. } => {
            format!("aggregate({table}) {} filters {}", func.label(), filter.len())
        }
        Statement::GroupBy { table, select, filter, .. } => {
            format!("group_by({table}) items {} filters {}", select.len(), filter.len())
        }
        Statement::Union { left, right, all, operation } => {
            let operation = match operation {
                SetOperation::Union => "union",
                SetOperation::Intersect => "intersect",
                SetOperation::Except => "except",
            };
            format!(
                "{operation}{}({}, {})",
                if *all { " all" } else { "" },
                describe_plan(left),
                describe_plan(right)
            )
        }
        Statement::Join { left, right, join_type, limit, offset, order, filter } => {
            let direction = match order.direction {
                Direction::Asc => "asc",
                Direction::Desc => "desc",
            };
            let join_name = match join_type {
                JoinType::Inner => "hash_join",
                JoinType::Left => "hash_left_join",
                JoinType::Right => "hash_right_join",
                JoinType::Full => "hash_full_join",
            };
            format!(
                "{join_name}({left},{right}) limit {limit} offset {offset} order {direction} filters {} using key index",
                filter.len()
            )
        }
        Statement::Explain { plan, analyze, .. } => {
            format!("explain{}({plan})", if *analyze { " analyze" } else { "" })
        }
        Statement::Distinct { statement, limit, offset } => {
            format!("distinct({}) limit {limit} offset {offset}", describe_plan(statement))
        }
        Statement::SequenceValue { operation, name, .. } => {
            format!("sequence {operation:?}({name})")
        }
    }
}

fn apply_distinct(result: QueryResult, offset: usize, limit: usize) -> QueryResult {
    match result {
        QueryResult::Rows { rows } => {
            let mut seen = std::collections::HashSet::new();
            let rows = rows
                .into_iter()
                .filter(|row| seen.insert(row.clone()))
                .skip(offset)
                .take(limit)
                .collect();
            QueryResult::Rows { rows }
        }
        QueryResult::Table { columns, rows } => {
            let mut seen = std::collections::HashSet::new();
            let rows = rows
                .into_iter()
                .filter(|row| seen.insert(row.clone()))
                .skip(offset)
                .take(limit)
                .collect();
            QueryResult::Table { columns, rows }
        }
        result => result,
    }
}

fn select_values_result(columns: Vec<String>, values: Vec<String>) -> Result<QueryResult> {
    let values =
        values.into_iter().map(|value| eval_select_value(&value)).collect::<Result<Vec<_>>>()?;
    Ok(QueryResult::Table { columns, rows: vec![values] })
}

fn apply_set_operation<T>(
    mut left: Vec<T>,
    right: Vec<T>,
    operation: SetOperation,
    all: bool,
) -> Vec<T>
where
    T: Clone + Eq + std::hash::Hash,
{
    match operation {
        SetOperation::Union => {
            left.extend(right);
            if !all {
                let mut seen = HashSet::new();
                left.retain(|row| seen.insert(row.clone()));
            }
            left
        }
        SetOperation::Intersect => {
            if all {
                let mut counts = right.into_iter().fold(HashMap::new(), |mut counts, row| {
                    *counts.entry(row).or_insert(0usize) += 1;
                    counts
                });
                left.into_iter()
                    .filter(|row| {
                        let Some(count) = counts.get_mut(row) else { return false };
                        if *count == 0 {
                            return false;
                        }
                        *count -= 1;
                        true
                    })
                    .collect()
            } else {
                let right = right.into_iter().collect::<HashSet<_>>();
                let mut seen = HashSet::new();
                left.into_iter()
                    .filter(|row| right.contains(row) && seen.insert(row.clone()))
                    .collect()
            }
        }
        SetOperation::Except => {
            if all {
                let mut counts = right.into_iter().fold(HashMap::new(), |mut counts, row| {
                    *counts.entry(row).or_insert(0usize) += 1;
                    counts
                });
                left.into_iter()
                    .filter(|row| {
                        let Some(count) = counts.get_mut(row) else { return true };
                        if *count == 0 {
                            return true;
                        }
                        *count -= 1;
                        false
                    })
                    .collect()
            } else {
                let right = right.into_iter().collect::<HashSet<_>>();
                let mut seen = HashSet::new();
                left.into_iter()
                    .filter(|row| !right.contains(row) && seen.insert(row.clone()))
                    .collect()
            }
        }
    }
}

fn merge_set_results(
    left: QueryResult,
    right: QueryResult,
    operation: SetOperation,
    all: bool,
) -> Result<QueryResult> {
    ensure_materialized_result(&left)?;
    ensure_materialized_result(&right)?;
    let result = match (left, right) {
        (
            QueryResult::Table { columns, rows: left_rows },
            QueryResult::Table { columns: right_columns, rows: right_rows },
        ) => {
            if columns.len() != right_columns.len() {
                return Err(RymeError::InvalidArgument(String::from(
                    "set operation column mismatch",
                )));
            }
            QueryResult::Table {
                columns,
                rows: apply_set_operation(left_rows, right_rows, operation, all),
            }
        }
        (left, right) => {
            let rows = |result| match result {
                QueryResult::Row { pk, value } => Some(vec![(pk, value)]),
                QueryResult::Rows { rows } => Some(rows),
                _ => None,
            };
            let Some(left) = rows(left) else {
                return Err(RymeError::InvalidArgument(String::from(
                    "set operation result shapes do not match",
                )));
            };
            let Some(right) = rows(right) else {
                return Err(RymeError::InvalidArgument(String::from(
                    "set operation result shapes do not match",
                )));
            };
            QueryResult::Rows { rows: apply_set_operation(left, right, operation, all) }
        }
    };
    ensure_materialized_result(&result)?;
    Ok(result)
}

fn join_rows(
    left_rows: Vec<Row>,
    right_rows: Vec<Row>,
    join_type: JoinType,
    filter: &[Predicate],
    order: &Order,
    offset: usize,
    limit: usize,
) -> Result<QueryResult> {
    let mut right_index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut index_budget = MaterializationBudget::default();
    for (pk, value) in &right_rows {
        index_budget.add_parts(pk, value)?;
        right_index.entry(pk.clone()).or_insert_with(|| value.clone());
    }
    let mut matched_right = HashSet::new();
    let mut rows = Vec::new();
    let mut output_budget = MaterializationBudget::default();
    for (pk, left_value) in left_rows {
        if let Some(right_value) = right_index.get(&pk) {
            matched_right.insert(pk.clone());
            let row = (
                pk,
                serde_json::json!({
                    "left": String::from_utf8_lossy(&left_value),
                    "right": String::from_utf8_lossy(right_value),
                })
                .to_string()
                .into_bytes(),
            );
            output_budget.add_row(&row)?;
            rows.push(row);
        } else if matches!(join_type, JoinType::Left | JoinType::Full) {
            let row = (
                pk,
                serde_json::json!({
                    "left": String::from_utf8_lossy(&left_value),
                    "right": serde_json::Value::Null,
                })
                .to_string()
                .into_bytes(),
            );
            output_budget.add_row(&row)?;
            rows.push(row);
        }
    }
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        for (pk, right_value) in right_rows {
            if matched_right.contains(&pk) {
                continue;
            }
            let row = (
                pk,
                serde_json::json!({
                    "left": serde_json::Value::Null,
                    "right": String::from_utf8_lossy(&right_value),
                })
                .to_string()
                .into_bytes(),
            );
            output_budget.add_row(&row)?;
            rows.push(row);
        }
    }
    ensure_materialized_rows(&rows)?;
    rows.retain(|(pk, value)| predicates_match(filter, pk, value));
    rows.sort_by(|left, right| compare_order(order, left, right));
    Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
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
                SQL_NULL_SENTINEL.to_vec()
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
                SQL_NULL_SENTINEL.to_vec()
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
            .unwrap_or_else(|| SQL_NULL_SENTINEL.to_vec()),
        AggFunc::Max => rows
            .iter()
            .map(|(pk, value)| match field {
                Field::Key => pk.clone(),
                Field::Value => value.clone(),
            })
            .max()
            .unwrap_or_else(|| SQL_NULL_SENTINEL.to_vec()),
    }
}

fn aggregate_column_rows(rows: &[(Vec<u8>, Vec<u8>)], func: AggFunc, column: &str) -> Vec<u8> {
    let values: Vec<Vec<u8>> = rows
        .iter()
        .filter_map(|(_, raw)| {
            let serde_json::Value::Object(object) = serde_json::from_slice(raw).ok()? else {
                return None;
            };
            let value = json_column_value(column, &object)?;
            (!value.is_null()).then(|| json_result_bytes(value))
        })
        .collect();
    match func {
        AggFunc::Count => values.len().to_string().into_bytes(),
        AggFunc::Sum => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for value in &values {
                if let Some(number) = parse_number(value) {
                    total += number;
                    count += 1;
                }
            }
            if count == 0 {
                SQL_NULL_SENTINEL.to_vec()
            } else {
                format_number(total).into_bytes()
            }
        }
        AggFunc::Avg => {
            let mut total = 0.0f64;
            let mut count = 0u64;
            for value in &values {
                if let Some(number) = parse_number(value) {
                    total += number;
                    count += 1;
                }
            }
            if count == 0 {
                SQL_NULL_SENTINEL.to_vec()
            } else {
                format_number(total / count as f64).into_bytes()
            }
        }
        AggFunc::Min => values.into_iter().min().unwrap_or_else(|| SQL_NULL_SENTINEL.to_vec()),
        AggFunc::Max => values.into_iter().max().unwrap_or_else(|| SQL_NULL_SENTINEL.to_vec()),
    }
}

fn aggregate_target_rows(
    rows: &[(Vec<u8>, Vec<u8>)],
    func: AggFunc,
    field: Field,
    column: Option<&str>,
) -> Vec<u8> {
    column.map_or_else(
        || aggregate_rows(rows, func, field),
        |column| aggregate_column_rows(rows, func, column),
    )
}

fn group_value(pk: &[u8], raw: &[u8], field: Field, column: Option<&str>) -> Vec<u8> {
    if let Some(column) = column {
        let Some(serde_json::Value::Object(object)) = serde_json::from_slice(raw).ok() else {
            return vec![0];
        };
        return json_column_value(column, &object)
            .filter(|value| !value.is_null())
            .map(json_result_bytes)
            .unwrap_or_else(|| vec![0]);
    }
    match field {
        Field::Key => pk.to_vec(),
        Field::Value => raw.to_vec(),
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

fn returning_changes(fields: &[ReturningField], changes: &[TransactionChange]) -> QueryResult {
    let columns = returning_columns(fields);
    let rows = changes
        .iter()
        .filter_map(|change| {
            let value = change.after.as_ref().or(change.before.as_ref())?;
            Some(
                fields
                    .iter()
                    .map(|field| returning_field_value(field, &change.pk, value))
                    .collect(),
            )
        })
        .collect();
    QueryResult::Returning { columns, rows }
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
            .map(|(_, value)| json_result_bytes_or_null(value))
            .unwrap_or_else(|| value.to_vec()),
        ReturningField::Column(column) => object
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, value)| json_result_bytes_or_null(value))
            .unwrap_or_default(),
    }
}

#[derive(Clone)]
pub struct Executor<B = TxnManager> {
    tenant: String,
    auth_uid: Option<String>,
    database: String,
    branch: String,
    role: String,
    manager: B,
    read_ts: Option<u64>,
    realtime: Option<Realtime>,
    read_only: bool,
    isolation: Isolation,
    catalog: Arc<Mutex<HashMap<String, Vec<ColumnDefinition>>>>,
    indexes: Arc<RwLock<HashMap<String, Vec<IndexState>>>>,
    pending_index_commits: Arc<RwLock<HashMap<String, usize>>>,
    rls_maps: Arc<RwLock<RlsPolicyMaps>>,
    rls_policy_version: Arc<AtomicU64>,
    rls_role_maps: RlsRoleMapCache,
    rls_enabled: Arc<RwLock<HashSet<String>>>,
    rls_forced: Arc<RwLock<HashSet<String>>>,
    platform_rls_tables: Arc<RwLock<HashSet<String>>>,
    rls_policies: Arc<RwLock<HashMap<String, RlsPolicy>>>,
    checks: Arc<Mutex<HashMap<String, Vec<String>>>>,
    foreign_keys: Arc<Mutex<HashMap<String, Vec<ForeignKeyConstraint>>>>,
    constraints: Arc<Mutex<HashMap<String, Vec<ConstraintMetadata>>>>,
    functions: Arc<Mutex<HashMap<String, FunctionDefinition>>>,
    triggers: Arc<Mutex<HashMap<String, TriggerDefinition>>>,
    views: Arc<Mutex<HashMap<String, ViewDefinition>>>,
    sequences: Arc<Mutex<HashMap<String, SequenceDefinition>>>,
    roles: Arc<Mutex<HashMap<String, RoleDefinition>>>,
    privileges: Arc<Mutex<Vec<PrivilegeGrant>>>,
    default_privileges: Arc<Mutex<Vec<DefaultPrivilegeGrant>>>,
    schema_path: Arc<Mutex<Option<PathBuf>>>,
    schema_persist_lock: Arc<Mutex<()>>,
    schema_dirty: Arc<AtomicBool>,
    schema_hook: Option<SchemaHook>,
    sequence_next: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl<B: std::fmt::Debug> std::fmt::Debug for Executor<B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Executor")
            .field("tenant", &self.tenant)
            .field("database", &self.database)
            .field("branch", &self.branch)
            .field("manager", &self.manager)
            .field("read_ts", &self.read_ts)
            .field("read_only", &self.read_only)
            .field("isolation", &self.isolation)
            .field("schema_hook_configured", &self.schema_hook.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
struct IndexState {
    definition: IndexDefinition,
    entries: BTreeMap<String, BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>>>,
    applied_ts: u64,
    valid: bool,
}

fn intersect_index_candidates<'a>(
    mut candidates: Vec<&'a Vec<u8>>,
    other: &BTreeSet<Vec<u8>>,
) -> Vec<&'a Vec<u8>> {
    let lookup_depth = usize::BITS.saturating_sub(other.len().max(1).leading_zeros());
    let lookup_work = candidates.len().saturating_mul(lookup_depth as usize);
    let merge_work = candidates.len().saturating_add(other.len());
    if merge_work <= lookup_work {
        let mut left = candidates.into_iter().peekable();
        let mut right = other.iter().peekable();
        let mut intersection = Vec::with_capacity(left.len().min(right.len()));
        while let (Some(candidate), Some(indexed)) = (left.peek().copied(), right.peek().copied()) {
            match candidate.as_slice().cmp(indexed.as_slice()) {
                std::cmp::Ordering::Less => {
                    left.next();
                }
                std::cmp::Ordering::Greater => {
                    right.next();
                }
                std::cmp::Ordering::Equal => {
                    intersection.push(candidate);
                    left.next();
                    right.next();
                }
            }
        }
        intersection
    } else {
        candidates.retain(|pk| other.contains(*pk));
        candidates
    }
}

struct PendingIndexCommit(Arc<RwLock<HashMap<String, usize>>>, Vec<String>);

impl Drop for PendingIndexCommit {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.0.write() {
            for table in &self.1 {
                if let Some(count) = pending.get_mut(table) {
                    *count -= 1;
                    if *count == 0 {
                        pending.remove(table);
                    }
                }
            }
        }
    }
}

impl Executor<TxnManager> {
    pub fn new(tenant: String, database: String) -> Self {
        Self {
            auth_uid: Some(tenant.clone()),
            tenant,
            database,
            branch: String::from("main"),
            role: String::from("postgres"),
            manager: TxnManager::new(),
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            pending_index_commits: Arc::new(RwLock::new(HashMap::new())),
            rls_maps: Arc::new(RwLock::new(RlsPolicyMaps::default())),
            rls_policy_version: Arc::new(AtomicU64::new(0)),
            rls_role_maps: Arc::new(RwLock::new(HashMap::new())),
            rls_enabled: Arc::new(RwLock::new(HashSet::new())),
            rls_forced: Arc::new(RwLock::new(HashSet::new())),
            platform_rls_tables: Arc::new(RwLock::new(HashSet::new())),
            rls_policies: Arc::new(RwLock::new(HashMap::new())),
            checks: Arc::new(Mutex::new(HashMap::new())),
            foreign_keys: Arc::new(Mutex::new(HashMap::new())),
            constraints: Arc::new(Mutex::new(HashMap::new())),
            functions: Arc::new(Mutex::new(HashMap::new())),
            triggers: Arc::new(Mutex::new(HashMap::new())),
            views: Arc::new(Mutex::new(HashMap::new())),
            sequences: Arc::new(Mutex::new(HashMap::new())),
            roles: Arc::new(Mutex::new(HashMap::new())),
            privileges: Arc::new(Mutex::new(Vec::new())),
            default_privileges: Arc::new(Mutex::new(Vec::new())),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            schema_hook: None,
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_manager(tenant: String, database: String, manager: TxnManager) -> Self {
        Self {
            auth_uid: Some(tenant.clone()),
            tenant,
            database,
            branch: String::from("main"),
            role: String::from("postgres"),
            manager,
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            pending_index_commits: Arc::new(RwLock::new(HashMap::new())),
            rls_maps: Arc::new(RwLock::new(RlsPolicyMaps::default())),
            rls_policy_version: Arc::new(AtomicU64::new(0)),
            rls_role_maps: Arc::new(RwLock::new(HashMap::new())),
            rls_enabled: Arc::new(RwLock::new(HashSet::new())),
            rls_forced: Arc::new(RwLock::new(HashSet::new())),
            platform_rls_tables: Arc::new(RwLock::new(HashSet::new())),
            rls_policies: Arc::new(RwLock::new(HashMap::new())),
            checks: Arc::new(Mutex::new(HashMap::new())),
            foreign_keys: Arc::new(Mutex::new(HashMap::new())),
            constraints: Arc::new(Mutex::new(HashMap::new())),
            functions: Arc::new(Mutex::new(HashMap::new())),
            triggers: Arc::new(Mutex::new(HashMap::new())),
            views: Arc::new(Mutex::new(HashMap::new())),
            sequences: Arc::new(Mutex::new(HashMap::new())),
            roles: Arc::new(Mutex::new(HashMap::new())),
            privileges: Arc::new(Mutex::new(Vec::new())),
            default_privileges: Arc::new(Mutex::new(Vec::new())),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            schema_hook: None,
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
            auth_uid: Some(tenant.clone()),
            tenant,
            database,
            branch: String::from("main"),
            role: String::from("postgres"),
            manager,
            read_ts: None,
            realtime: None,
            read_only: false,
            isolation: Isolation::Serializable,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            pending_index_commits: Arc::new(RwLock::new(HashMap::new())),
            rls_maps: Arc::new(RwLock::new(RlsPolicyMaps::default())),
            rls_policy_version: Arc::new(AtomicU64::new(0)),
            rls_role_maps: Arc::new(RwLock::new(HashMap::new())),
            rls_enabled: Arc::new(RwLock::new(HashSet::new())),
            rls_forced: Arc::new(RwLock::new(HashSet::new())),
            platform_rls_tables: Arc::new(RwLock::new(HashSet::new())),
            rls_policies: Arc::new(RwLock::new(HashMap::new())),
            checks: Arc::new(Mutex::new(HashMap::new())),
            foreign_keys: Arc::new(Mutex::new(HashMap::new())),
            constraints: Arc::new(Mutex::new(HashMap::new())),
            functions: Arc::new(Mutex::new(HashMap::new())),
            triggers: Arc::new(Mutex::new(HashMap::new())),
            views: Arc::new(Mutex::new(HashMap::new())),
            sequences: Arc::new(Mutex::new(HashMap::new())),
            roles: Arc::new(Mutex::new(HashMap::new())),
            privileges: Arc::new(Mutex::new(Vec::new())),
            default_privileges: Arc::new(Mutex::new(Vec::new())),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            schema_hook: None,
            sequence_next: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_realtime(mut self, realtime: Realtime) -> Self {
        self.realtime = Some(realtime);
        self
    }

    pub fn with_schema_hook(mut self, hook: SchemaHook) -> Self {
        self.schema_hook = Some(hook);
        self
    }

    pub fn schema_transaction_snapshot(&self) -> Option<SchemaSnapshot> {
        Some(self.schema_snapshot())
    }

    pub fn try_schema_transaction_snapshot(&self) -> Result<Option<SchemaSnapshot>> {
        Ok(Some(self.try_schema_snapshot()?))
    }

    pub fn rollback_schema_transaction(&self, snapshot: Option<SchemaSnapshot>) -> Result<()> {
        let Some(snapshot) = snapshot else { return Ok(()) };
        if self.try_schema_snapshot()? != snapshot {
            self.restore_schema_snapshot(snapshot)?;
        }
        Ok(())
    }

    fn validate_schema_mutation(&self, previous: &SchemaSnapshot, was_dirty: bool) -> Result<()> {
        if let Err(error) = validate_schema_snapshot(&self.try_schema_snapshot()?) {
            self.restore_schema_snapshot(previous.clone()).map_err(|rollback_error| {
                RymeError::Internal(format!(
                    "schema limit exceeded: {error}; rollback failed: {rollback_error}"
                ))
            })?;
            self.schema_dirty.store(was_dirty, Ordering::SeqCst);
            return Err(error);
        }
        Ok(())
    }

    pub fn with_rls_tables(mut self, rls_tables: HashMap<String, String>) -> Self {
        let policies = rls_tables
            .iter()
            .map(|(table, column)| {
                let policy = RlsPolicy {
                    name: format!("__config__{table}"),
                    table: table.clone(),
                    column: column.clone(),
                    command: String::from("ALL"),
                    using: None,
                    check: None,
                    roles: Vec::new(),
                    read: true,
                    write: true,
                };
                (format!("{table}\0{}", policy.name), policy)
            })
            .collect();
        let table_names: HashSet<String> = rls_tables.keys().cloned().collect();
        self.rls_maps = Arc::new(RwLock::new(RlsPolicyMaps {
            tenant_read: rls_tables.clone(),
            tenant_write: rls_tables.clone(),
            tenant_insert: rls_tables.clone(),
            tenant_update: rls_tables.clone(),
            tenant_delete: rls_tables,
            ..RlsPolicyMaps::default()
        }));
        self.rls_enabled = Arc::new(RwLock::new(table_names.clone()));
        self.platform_rls_tables = Arc::new(RwLock::new(table_names));
        self.rls_policies = Arc::new(RwLock::new(policies));
        self.rls_policy_version = Arc::new(AtomicU64::new(0));
        self.rls_role_maps = Arc::new(RwLock::new(HashMap::new()));
        self
    }

    pub fn set_rls_tables(&mut self, rls_tables: HashMap<String, String>) {
        if let Ok(mut platform_tables) = self.platform_rls_tables.write() {
            platform_tables.extend(rls_tables.keys().cloned());
        }
        if let Ok(mut maps) = self.rls_maps.write() {
            maps.tenant_read.extend(rls_tables.clone());
            maps.tenant_write.extend(rls_tables.clone());
            maps.tenant_insert.extend(rls_tables.clone());
            maps.tenant_update.extend(rls_tables.clone());
            maps.tenant_delete.extend(rls_tables.clone());
        }
        if let Ok(mut enabled) = self.rls_enabled.write() {
            enabled.extend(rls_tables.keys().cloned());
        }
        if let Ok(mut policies) = self.rls_policies.write() {
            for (table, column) in rls_tables {
                let policy = RlsPolicy {
                    name: format!("__config__{table}"),
                    table: table.clone(),
                    column,
                    command: String::from("ALL"),
                    using: None,
                    check: None,
                    roles: Vec::new(),
                    read: true,
                    write: true,
                };
                policies.insert(format!("{table}\0{}", policy.name), policy);
            }
        }
        self.invalidate_rls_role_maps();
    }

    fn set_rls_state(
        &self,
        table: &str,
        enabled: Option<bool>,
        forced: Option<bool>,
    ) -> Result<()> {
        let forced = forced.or_else(|| enabled.filter(|enabled| !enabled).map(|_| false));
        if let Some(enabled) = enabled {
            let mut tables = self
                .rls_enabled
                .write()
                .map_err(|_| RymeError::Internal(String::from("RLS state lock")))?;
            if enabled {
                tables.insert(table.to_string());
            } else if !self.platform_rls_applies(table) {
                tables.remove(table);
            }
        }
        if let Some(forced) = forced {
            let mut tables = self
                .rls_forced
                .write()
                .map_err(|_| RymeError::Internal(String::from("RLS force lock")))?;
            if forced {
                tables.insert(table.to_string());
            } else {
                tables.remove(table);
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn install_function(&self, definition: FunctionDefinition, replace: bool) -> Result<()> {
        let mut functions = self
            .functions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("function lock")))?;
        if !replace && functions.contains_key(&definition.name) {
            return Err(RymeError::Conflict(format!("function {}", definition.name)));
        }
        functions.insert(definition.name.clone(), definition);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn install_trigger(&self, definition: TriggerDefinition) -> Result<()> {
        let function_exists = self
            .functions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("function lock")))?
            .contains_key(&definition.function);
        if !function_exists {
            return Err(RymeError::NotFound(format!("function {}", definition.function)));
        }
        let key = format!("{}\0{}", definition.table, definition.name);
        let mut triggers =
            self.triggers.lock().map_err(|_| RymeError::Internal(String::from("trigger lock")))?;
        if triggers.contains_key(&key) {
            return Err(RymeError::Conflict(format!("trigger {}", definition.name)));
        }
        triggers.insert(key, definition);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn drop_trigger(&self, name: &str, table: &str, if_exists: bool) -> Result<()> {
        let key = format!("{table}\0{name}");
        let removed = self
            .triggers
            .lock()
            .map_err(|_| RymeError::Internal(String::from("trigger lock")))?
            .remove(&key)
            .is_some();
        if !removed && !if_exists {
            return Err(RymeError::NotFound(format!("trigger {name}")));
        }
        if removed {
            self.schema_dirty.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn install_view(&self, definition: ViewDefinition, replace: bool) -> Result<()> {
        self.install_view_definition(
            ViewDefinition { materialized: false, rows: Vec::new(), ..definition },
            replace,
        )
    }

    fn install_view_definition(&self, definition: ViewDefinition, replace: bool) -> Result<()> {
        let mut definition = definition;
        definition.dependencies = view_dependencies(&definition.query);
        for dependency in &definition.dependencies {
            let table_exists = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
                .keys()
                .any(|table| same_relation(table, dependency));
            let view_exists = self.view_definition(dependency)?.is_some();
            if !table_exists && !view_exists {
                return Err(RymeError::NotFound(format!("relation {dependency}")));
            }
            if same_relation(&definition.name, dependency) {
                return Err(RymeError::InvalidArgument(String::from("recursive view")));
            }
        }
        let mut views =
            self.views.lock().map_err(|_| RymeError::Internal(String::from("view lock")))?;
        let mut pending = definition.dependencies.clone();
        let mut visited = HashSet::new();
        while let Some(dependency) = pending.pop() {
            if !visited.insert(dependency.to_ascii_lowercase()) {
                continue;
            }
            if same_relation(&definition.name, &dependency) {
                return Err(RymeError::InvalidArgument(String::from("recursive view")));
            }
            if let Some(view) = views.values().find(|view| same_relation(&view.name, &dependency)) {
                pending.extend(view.dependencies.iter().cloned());
            }
        }
        if !replace && views.contains_key(&definition.name) {
            return Err(RymeError::Conflict(format!("view {}", definition.name)));
        }
        let object_name = definition.name.clone();
        views.insert(object_name.clone(), definition);
        drop(views);
        self.apply_default_privileges_for("TABLE", &object_name)?;
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn refresh_materialized_view(&self, txn: &mut Transaction, name: &str) -> Result<()> {
        let definition = self
            .view_definition(name)?
            .ok_or_else(|| RymeError::NotFound(format!("materialized view {name}")))?;
        if !definition.materialized {
            return Err(RymeError::InvalidArgument(String::from(
                "refresh target is not a materialized view",
            )));
        }
        let result = self.execute_read_in_transaction(txn, definition.query.clone())?;
        let mut refreshed = definition;
        refreshed.rows = query_result_rows(result);
        let mut views =
            self.views.lock().map_err(|_| RymeError::Internal(String::from("view lock")))?;
        let key = views
            .keys()
            .find(|candidate| candidate.eq_ignore_ascii_case(name))
            .cloned()
            .ok_or_else(|| RymeError::NotFound(format!("materialized view {name}")))?;
        views.insert(key, refreshed);
        drop(views);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn drop_view(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut views =
            self.views.lock().map_err(|_| RymeError::Internal(String::from("view lock")))?;
        let Some(key) = views.keys().find(|candidate| same_relation(candidate, name)).cloned()
        else {
            if if_exists {
                return Ok(());
            }
            return Err(RymeError::NotFound(format!("view {name}")));
        };
        if let Some(dependent) = views.values().find(|view| {
            !same_relation(&view.name, &key)
                && view.dependencies.iter().any(|dependency| same_relation(dependency, &key))
        }) {
            return Err(RymeError::Conflict(format!(
                "view {} is referenced by view {}",
                key, dependent.name
            )));
        }
        if views.remove(&key).is_none() {
            return Err(RymeError::NotFound(format!("view {name}")));
        }
        drop(views);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn view_definition(&self, name: &str) -> Result<Option<ViewDefinition>> {
        let views =
            self.views.lock().map_err(|_| RymeError::Internal(String::from("view lock")))?;
        Ok(views.get(name).cloned().or_else(|| {
            views.iter().find_map(|(view_name, definition)| {
                view_name
                    .rsplit('.')
                    .next()
                    .is_some_and(|short| short.eq_ignore_ascii_case(name))
                    .then(|| definition.clone())
            })
        }))
    }

    fn select_scan_options(
        &self,
        table: &str,
        order: &Order,
        limit: usize,
        offset: usize,
        filter_is_empty: bool,
    ) -> Result<(usize, bool)> {
        let primary_key_order = order == &Order::default();
        if primary_key_order {
            let views =
                self.views.lock().map_err(|_| RymeError::Internal(String::from("view lock")))?;
            let is_view = views.contains_key(table)
                || views.keys().any(|view_name| {
                    view_name
                        .rsplit('.')
                        .next()
                        .is_some_and(|short| short.eq_ignore_ascii_case(table))
                });
            let cap = if filter_is_empty && offset == 0 {
                limit.clamp(1, 10_000)
            } else if !is_view {
                limit.saturating_add(offset).clamp(1, 10_000)
            } else {
                10_000
            };
            return Ok((cap, is_view));
        }
        Ok((10_000, true))
    }

    fn install_sequence(&self, definition: SequenceDefinition, if_not_exists: bool) -> Result<()> {
        let mut sequences = self
            .sequences
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?;
        if sequences.contains_key(&definition.name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(RymeError::Conflict(format!("sequence {}", definition.name)));
        }
        let object_name = definition.name.clone();
        sequences.insert(object_name.clone(), definition);
        drop(sequences);
        self.apply_default_privileges_for("SEQUENCE", &object_name)?;
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn drop_sequence(&self, name: &str, if_exists: bool) -> Result<()> {
        let removed = self
            .sequences
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?
            .remove(name)
            .is_some();
        if !removed && !if_exists {
            return Err(RymeError::NotFound(format!("sequence {name}")));
        }
        if removed {
            self.schema_dirty.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn alter_sequence(
        &self,
        name: &str,
        restart: Option<i64>,
        increment: Option<i64>,
    ) -> Result<()> {
        let mut sequences = self
            .sequences
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?;
        let key = sequences
            .contains_key(name)
            .then(|| name.to_string())
            .or_else(|| {
                sequences
                    .keys()
                    .find(|key| {
                        key.rsplit('.').next().is_some_and(|short| short.eq_ignore_ascii_case(name))
                    })
                    .cloned()
            })
            .ok_or_else(|| RymeError::NotFound(format!("sequence {name}")))?;
        let sequence = sequences.get_mut(&key).expect("sequence key selected above");
        if let Some(increment) = increment {
            sequence.increment = increment;
        }
        if let Some(restart) = restart {
            if restart < sequence.min_value || restart > sequence.max_value {
                return Err(RymeError::InvalidArgument(String::from("sequence restart")));
            }
            sequence.start = restart;
            sequence.last_value = None;
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn sequence_mutation(
        &self,
        operation: SequenceOperation,
        name: &str,
        value: Option<i64>,
        is_called: bool,
    ) -> Result<i64> {
        self.enforce_sequence_privilege(operation, name)?;
        let mut sequences = self
            .sequences
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?;
        let key = sequences
            .contains_key(name)
            .then(|| name.to_string())
            .or_else(|| {
                sequences
                    .keys()
                    .find(|key| {
                        key.rsplit('.').next().is_some_and(|short| short.eq_ignore_ascii_case(name))
                    })
                    .cloned()
            })
            .ok_or_else(|| RymeError::NotFound(format!("sequence {name}")))?;
        let sequence = sequences.get_mut(&key).expect("sequence key selected above");
        match operation {
            SequenceOperation::Nextval => {
                let next = sequence.last_value.map_or(sequence.start, |last| {
                    last.checked_add(sequence.increment).unwrap_or({
                        if sequence.increment < 0 {
                            sequence.min_value
                        } else {
                            sequence.max_value
                        }
                    })
                });
                let next = if next < sequence.min_value || next > sequence.max_value {
                    if sequence.cycle {
                        if sequence.increment < 0 {
                            sequence.max_value
                        } else {
                            sequence.min_value
                        }
                    } else {
                        return Err(RymeError::Conflict(format!(
                            "sequence {} exhausted",
                            sequence.name
                        )));
                    }
                } else {
                    next
                };
                sequence.last_value = Some(next);
                self.schema_dirty.store(true, Ordering::SeqCst);
                Ok(next)
            }
            SequenceOperation::Currval => sequence.last_value.ok_or_else(|| {
                RymeError::InvalidArgument(format!(
                    "currval of sequence {} is not yet defined",
                    sequence.name
                ))
            }),
            SequenceOperation::Setval => {
                let value = value
                    .ok_or_else(|| RymeError::InvalidArgument(String::from("setval value")))?;
                if value < sequence.min_value || value > sequence.max_value {
                    return Err(RymeError::InvalidArgument(String::from("setval value")));
                }
                sequence.last_value = if is_called {
                    Some(value)
                } else {
                    Some(value.checked_sub(sequence.increment).unwrap_or(value))
                };
                self.schema_dirty.store(true, Ordering::SeqCst);
                Ok(value)
            }
        }
    }

    fn execute_sequence_value(
        &self,
        operation: SequenceOperation,
        name: String,
        value: Option<i64>,
        is_called: bool,
    ) -> Result<QueryResult> {
        let result = self.sequence_mutation(operation, &name, value, is_called)?;
        let label = match operation {
            SequenceOperation::Nextval => "nextval",
            SequenceOperation::Currval => "currval",
            SequenceOperation::Setval => "setval",
        };
        Ok(QueryResult::Table {
            columns: vec![label.to_string()],
            rows: vec![vec![result.to_string().into_bytes()]],
        })
    }

    fn install_role(&self, definition: RoleDefinition) -> Result<()> {
        if is_builtin_sql_role(&definition.name) {
            return Err(reserved_role_error(&definition.name));
        }
        let mut roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        if roles.keys().any(|name| name.eq_ignore_ascii_case(&definition.name)) {
            return Err(RymeError::Conflict(format!("role {}", definition.name)));
        }
        roles.insert(definition.name.clone(), definition);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn alter_role_inheritance(&self, name: &str, inherit: bool) -> Result<()> {
        if is_builtin_sql_role(name) {
            return Err(reserved_role_error(name));
        }
        let mut roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        let role = roles
            .values_mut()
            .find(|role| role.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| RymeError::NotFound(format!("role {name}")))?;
        if role.inherit != inherit {
            role.inherit = inherit;
            self.schema_dirty.store(true, Ordering::SeqCst);
            drop(roles);
            self.invalidate_rls_role_maps();
        }
        Ok(())
    }

    fn drop_role(&self, name: &str, if_exists: bool) -> Result<()> {
        if is_builtin_sql_role(name) {
            return Err(reserved_role_error(name));
        }
        let mut roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        if self
            .rls_policies
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS policy lock")))?
            .values()
            .any(|policy| policy.roles.iter().any(|target| target.eq_ignore_ascii_case(name)))
        {
            return Err(RymeError::Conflict(format!("role {name} is used by an RLS policy")));
        }
        let key = roles.keys().find(|key| key.eq_ignore_ascii_case(name)).cloned();
        let removed = key.and_then(|key| roles.remove(&key)).is_some();
        if !removed && !if_exists {
            return Err(RymeError::NotFound(format!("role {name}")));
        }
        if removed {
            let mut privileges = self
                .privileges
                .lock()
                .map_err(|_| RymeError::Internal(String::from("privilege lock")))?;
            privileges.retain(|grant| {
                !(grant.grantee.eq_ignore_ascii_case(name)
                    || grant.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT)
                        && grant.object.eq_ignore_ascii_case(name))
            });
            drop(privileges);
            let mut defaults = self
                .default_privileges
                .lock()
                .map_err(|_| RymeError::Internal(String::from("default privilege lock")))?;
            defaults.retain(|grant| {
                !grant.grantee.eq_ignore_ascii_case(name)
                    && !grant.owner.as_deref().is_some_and(|owner| owner.eq_ignore_ascii_case(name))
            });
            self.schema_dirty.store(true, Ordering::SeqCst);
        }
        drop(roles);
        if removed {
            self.invalidate_rls_role_maps();
        }
        Ok(())
    }

    fn apply_role_membership_change(
        &self,
        grant: bool,
        granted_roles: Vec<String>,
        members: Vec<String>,
    ) -> Result<()> {
        let roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        let mut stored = self
            .privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("privilege lock")))?;
        let edge_count = granted_roles
            .len()
            .checked_mul(members.len())
            .ok_or_else(|| RymeError::Overload(String::from("role membership change")))?;
        if edge_count > MAX_ROLE_MEMBERSHIP_CHANGE_EDGES {
            return Err(RymeError::Overload(String::from("role membership change")));
        }
        let mut known_roles =
            BUILTIN_SQL_ROLES.iter().map(|role| role.to_ascii_lowercase()).collect::<HashSet<_>>();
        known_roles.extend(roles.keys().map(|role| role.to_ascii_lowercase()));
        for role in &granted_roles {
            if !known_roles.contains(&role.to_ascii_lowercase()) {
                return Err(RymeError::NotFound(format!("role {role}")));
            }
        }
        for member in &members {
            if member.eq_ignore_ascii_case("PUBLIC")
                || !known_roles.contains(&member.to_ascii_lowercase())
            {
                return Err(RymeError::NotFound(format!("role {member}")));
            }
        }

        let memberships = stored
            .iter()
            .filter(|entry| {
                entry.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT)
                    && entry.privilege.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_PRIVILEGE)
            })
            .collect::<Vec<_>>();
        let mut edges = memberships
            .iter()
            .map(|entry| (entry.grantee.to_ascii_lowercase(), entry.object.to_ascii_lowercase()))
            .collect::<HashSet<_>>();
        let mut additions = Vec::new();
        for role in &granted_roles {
            for member in &members {
                if grant {
                    let edge = (member.to_ascii_lowercase(), role.to_ascii_lowercase());
                    if !edges.insert(edge) {
                        continue;
                    }
                    additions.push(PrivilegeGrant {
                        object_type: String::from(SQL_ROLE_MEMBERSHIP_OBJECT),
                        object: role.clone(),
                        privilege: String::from(SQL_ROLE_MEMBERSHIP_PRIVILEGE),
                        grantee: member.clone(),
                        grant_option: false,
                    });
                }
            }
        }
        if grant {
            if memberships.len().saturating_add(additions.len()) > MAX_SCHEMA_OBJECTS {
                return Err(RymeError::Overload(String::from("role memberships")));
            }
            let mut candidate =
                memberships.iter().map(|entry| (*entry).clone()).collect::<Vec<_>>();
            candidate.extend(additions.iter().cloned());
            if role_membership_has_cycle(&candidate) {
                return Err(RymeError::InvalidArgument(String::from("role membership cycle")));
            }
            stored.extend(additions);
        } else {
            let granted_roles =
                granted_roles.iter().map(|role| role.to_ascii_lowercase()).collect::<HashSet<_>>();
            let members =
                members.iter().map(|member| member.to_ascii_lowercase()).collect::<HashSet<_>>();
            stored.retain(|entry| {
                !entry.object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT)
                    || !entry.privilege.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_PRIVILEGE)
                    || !granted_roles.contains(&entry.object.to_ascii_lowercase())
                    || !members.contains(&entry.grantee.to_ascii_lowercase())
            });
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        drop(stored);
        drop(roles);
        self.invalidate_rls_role_maps();
        Ok(())
    }

    fn validate_grantees(
        roles: &HashMap<String, RoleDefinition>,
        grantees: &[String],
    ) -> Result<()> {
        for grantee in grantees {
            if grantee.eq_ignore_ascii_case("PUBLIC") || is_builtin_sql_role(grantee) {
                continue;
            }
            if !roles.keys().any(|role| role.eq_ignore_ascii_case(grantee)) {
                return Err(RymeError::NotFound(format!("role {grantee}")));
            }
        }
        Ok(())
    }

    fn apply_privilege_change(
        &self,
        grant: bool,
        object_type: String,
        objects: Vec<String>,
        privileges: Vec<String>,
        grantees: Vec<String>,
        grant_option: bool,
    ) -> Result<()> {
        if object_type.eq_ignore_ascii_case(SQL_ROLE_MEMBERSHIP_OBJECT) {
            return Err(RymeError::InvalidArgument(String::from("privilege object type")));
        }
        let roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        Self::validate_grantees(&roles, &grantees)?;
        let mut stored = self
            .privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("privilege lock")))?;
        for object in objects {
            for privilege in &privileges {
                for grantee in &grantees {
                    if grant {
                        if let Some(existing) = stored.iter_mut().find(|entry| {
                            entry.object_type.eq_ignore_ascii_case(&object_type)
                                && entry.object.eq_ignore_ascii_case(&object)
                                && entry.privilege.eq_ignore_ascii_case(privilege)
                                && entry.grantee.eq_ignore_ascii_case(grantee)
                        }) {
                            existing.grant_option |= grant_option;
                        } else {
                            stored.push(PrivilegeGrant {
                                object_type: object_type.clone(),
                                object: object.clone(),
                                privilege: privilege.clone(),
                                grantee: grantee.clone(),
                                grant_option,
                            });
                        }
                    } else {
                        stored.retain(|entry| {
                            !(entry.object_type.eq_ignore_ascii_case(&object_type)
                                && entry.object.eq_ignore_ascii_case(&object)
                                && entry.privilege.eq_ignore_ascii_case(privilege)
                                && entry.grantee.eq_ignore_ascii_case(grantee))
                        });
                    }
                }
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_default_privilege_change(
        &self,
        grant: bool,
        object_type: String,
        schema: Option<String>,
        owner: Option<String>,
        privileges: Vec<String>,
        grantees: Vec<String>,
        grant_option: bool,
    ) -> Result<()> {
        let roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        Self::validate_grantees(&roles, &grantees)?;
        let mut stored = self
            .default_privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("default privilege lock")))?;
        for privilege in &privileges {
            for grantee in &grantees {
                let matches = |entry: &DefaultPrivilegeGrant| {
                    entry.object_type.eq_ignore_ascii_case(&object_type)
                        && entry.schema == schema
                        && entry.owner == owner
                        && entry.privilege.eq_ignore_ascii_case(privilege)
                        && entry.grantee.eq_ignore_ascii_case(grantee)
                };
                if grant {
                    if let Some(existing) = stored.iter_mut().find(|entry| matches(entry)) {
                        existing.grant_option |= grant_option;
                    } else {
                        stored.push(DefaultPrivilegeGrant {
                            object_type: object_type.clone(),
                            schema: schema.clone(),
                            owner: owner.clone(),
                            privilege: privilege.clone(),
                            grantee: grantee.clone(),
                            grant_option,
                        });
                    }
                } else {
                    stored.retain(|entry| !matches(entry));
                }
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn apply_default_privileges_for(&self, object_type: &str, object: &str) -> Result<()> {
        let schema = object.rsplit_once('.').map(|(schema, _)| schema);
        let defaults = self
            .default_privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("default privilege lock")))?
            .iter()
            .filter(|entry| {
                entry.object_type.eq_ignore_ascii_case(object_type)
                    && entry.schema.as_deref().is_none_or(|expected| Some(expected) == schema)
            })
            .cloned()
            .collect::<Vec<_>>();
        for entry in defaults {
            self.apply_privilege_change(
                true,
                entry.object_type,
                vec![object.to_string()],
                vec![entry.privilege],
                vec![entry.grantee],
                entry.grant_option,
            )?;
        }
        Ok(())
    }

    fn apply_before_triggers(&self, table: &str, event: &str, value: &[u8]) -> Result<Vec<u8>> {
        let triggers = self
            .triggers
            .lock()
            .map_err(|_| RymeError::Internal(String::from("trigger lock")))?
            .values()
            .filter(|trigger| {
                trigger.table.eq_ignore_ascii_case(table)
                    && trigger.timing.eq_ignore_ascii_case("BEFORE")
                    && trigger.events.iter().any(|candidate| candidate.eq_ignore_ascii_case(event))
            })
            .cloned()
            .collect::<Vec<_>>();
        if triggers.is_empty() {
            return Ok(value.to_vec());
        }
        let functions = self
            .functions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("function lock")))?
            .clone();
        let mut row = serde_json::from_slice::<serde_json::Value>(value)
            .map_err(|_| RymeError::InvalidArgument(String::from("trigger row")))?;
        let Some(object) = row.as_object_mut() else { return Ok(value.to_vec()) };
        for trigger in triggers {
            let Some(function) = functions.get(&trigger.function) else { continue };
            for column in trigger_now_columns(&function.body) {
                let actual = object
                    .keys()
                    .find(|name| name.eq_ignore_ascii_case(&column))
                    .cloned()
                    .unwrap_or(column);
                object.insert(
                    actual,
                    serde_json::Value::Number(serde_json::Number::from(ryme_txn::now_unix())),
                );
            }
        }
        serde_json::to_vec(&row)
            .map_err(|_| RymeError::InvalidArgument(String::from("trigger row")))
    }

    fn rebuild_rls_policy_maps(&self) -> Result<()> {
        let policies = self
            .rls_policies
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS policy lock")))?
            .clone();
        let maps = rls_policy_maps_for_role(policies.values(), None);
        *self.rls_maps.write().map_err(|_| RymeError::Internal(String::from("RLS lock")))? = maps;
        self.invalidate_rls_role_maps();
        Ok(())
    }

    fn invalidate_rls_role_maps(&self) {
        self.rls_policy_version.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut maps) = self.rls_role_maps.write() {
            maps.clear();
        }
    }

    fn rls_maps_for_current_role(&self) -> Option<Arc<RlsPolicyMaps>> {
        loop {
            let version = self.rls_policy_version.load(Ordering::SeqCst);
            if let Some((cached_version, maps)) =
                self.rls_role_maps.read().ok()?.get(&self.role).cloned()
            {
                if cached_version == version {
                    return Some(maps);
                }
            }

            let effective_roles = self.effective_role_names().ok()?;
            let policies = self.rls_policies.read().ok()?;
            let maps =
                Arc::new(rls_policy_maps_for_role(policies.values(), Some(&effective_roles)));
            drop(policies);
            if self.rls_policy_version.load(Ordering::SeqCst) != version {
                continue;
            }
            let mut cache = self.rls_role_maps.write().ok()?;
            if self.rls_policy_version.load(Ordering::SeqCst) != version {
                continue;
            }
            if cache.len() < MAX_RLS_ROLE_MAP_CACHE_ENTRIES || cache.contains_key(&self.role) {
                cache.insert(self.role.clone(), (version, Arc::clone(&maps)));
            }
            return Some(maps);
        }
    }

    fn install_policy(
        &self,
        name: &str,
        table: &str,
        command: &str,
        roles: &[String],
        using: Option<&str>,
        check: Option<&str>,
    ) -> Result<()> {
        let defined_roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        for target in roles {
            let builtin =
                BUILTIN_SQL_ROLES.iter().any(|builtin| builtin.eq_ignore_ascii_case(target));
            if !builtin
                && !defined_roles.values().any(|role| role.name.eq_ignore_ascii_case(target))
            {
                return Err(RymeError::InvalidArgument(format!("policy role {target}")));
            }
        }
        let expressions = using.into_iter().chain(check);
        let mut tenant_column = None;
        let mut allow_all = false;
        for expression in expressions {
            if expression.trim().eq_ignore_ascii_case("true") {
                allow_all = true;
                continue;
            }
            let Some(column) = policy_tenant_column(expression) else {
                return Err(RymeError::InvalidArgument(String::from(
                    "unsupported RLS policy expression",
                )));
            };
            if tenant_column.as_deref().is_some_and(|current| current != column.as_str()) {
                return Err(RymeError::InvalidArgument(String::from(
                    "RLS policy columns do not match",
                )));
            }
            tenant_column = Some(column);
        }
        let command = command.to_ascii_uppercase();
        if !matches!(command.as_str(), "ALL" | "SELECT" | "INSERT" | "UPDATE" | "DELETE") {
            return Err(RymeError::InvalidArgument(String::from("unsupported RLS command")));
        }
        let column = tenant_column
            .or_else(|| allow_all.then(String::new))
            .ok_or_else(|| RymeError::InvalidArgument(String::from("create policy expression")))?;
        let read = matches!(command.as_str(), "ALL" | "SELECT" | "UPDATE" | "DELETE");
        let write = matches!(command.as_str(), "ALL" | "INSERT" | "UPDATE" | "DELETE");
        let policy = RlsPolicy {
            name: name.to_string(),
            table: table.to_string(),
            column,
            command,
            using: using.map(str::to_string),
            check: check.map(str::to_string),
            roles: roles.to_vec(),
            read,
            write,
        };
        let key = format!("{table}\0{name}");
        let mut policies = self
            .rls_policies
            .write()
            .map_err(|_| RymeError::Internal(String::from("RLS policy lock")))?;
        if policies.contains_key(&key) {
            return Err(RymeError::Conflict(format!("policy {name}")));
        }
        policies.insert(key, policy);
        drop(policies);
        drop(defined_roles);
        self.rebuild_rls_policy_maps()?;
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn drop_policy(&self, name: &str, table: &str, if_exists: bool) -> Result<()> {
        let key = format!("{table}\0{name}");
        let removed = self
            .rls_policies
            .write()
            .map_err(|_| RymeError::Internal(String::from("RLS policy lock")))?
            .remove(&key)
            .is_some();
        if !removed && !if_exists {
            return Err(RymeError::NotFound(format!("policy {name}")));
        }
        if removed {
            self.rebuild_rls_policy_maps()?;
            self.schema_dirty.store(true, Ordering::SeqCst);
        }
        Ok(())
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
            auth_uid: self.auth_uid,
            database: self.database,
            branch: self.branch,
            role: self.role,
            manager,
            read_ts: self.read_ts,
            realtime: self.realtime,
            read_only: self.read_only,
            isolation: self.isolation,
            catalog: self.catalog,
            indexes: self.indexes,
            pending_index_commits: self.pending_index_commits,
            rls_maps: self.rls_maps,
            rls_policy_version: self.rls_policy_version,
            rls_role_maps: self.rls_role_maps,
            rls_enabled: self.rls_enabled,
            rls_forced: self.rls_forced,
            platform_rls_tables: self.platform_rls_tables,
            rls_policies: self.rls_policies,
            checks: self.checks,
            foreign_keys: self.foreign_keys,
            constraints: self.constraints,
            functions: self.functions,
            triggers: self.triggers,
            views: self.views,
            sequences: self.sequences,
            roles: self.roles,
            privileges: self.privileges,
            default_privileges: self.default_privileges,
            schema_path: self.schema_path,
            schema_persist_lock: self.schema_persist_lock,
            schema_dirty: self.schema_dirty,
            schema_hook: self.schema_hook,
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

    fn begin_read_only(&self, isolation: Isolation) -> Result<Transaction> {
        let mut txn = self.begin_with(isolation);
        txn.set_read_only()?;
        Ok(txn)
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
        if self.auth_uid.as_deref() == Some(self.tenant.as_str()) {
            self.auth_uid = Some(tenant.clone());
        }
        self.tenant = tenant;
        self
    }

    /// Set the authenticated user identity used by PostgreSQL-compatible `auth.uid()` policies.
    pub fn with_auth_uid(mut self, auth_uid: String) -> Self {
        self.auth_uid = Some(auth_uid);
        self
    }

    /// Clear authenticated-user identity for operator or anonymous contexts.
    pub fn without_auth_uid(mut self) -> Self {
        self.auth_uid = None;
        self
    }

    /// Returns an executor that evaluates table privileges as the named SQL role.
    pub fn with_role(mut self, role: String) -> Self {
        self.role = role;
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

    /// Sets the durable schema snapshot path.
    pub fn set_schema_path(&mut self, path: PathBuf) -> Result<()> {
        *self
            .schema_path
            .lock()
            .map_err(|_| RymeError::Internal(String::from("schema path lock")))? = Some(path);
        Ok(())
    }

    /// Fallible schema snapshot used by persistence and transaction paths.
    ///
    /// Any poisoned schema lock is an error: serializing a defaulted snapshot
    /// could replace durable schema state with an incomplete catalog.
    pub fn try_schema_snapshot(&self) -> Result<SchemaSnapshot> {
        let tables = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .iter()
            .map(|(table, columns)| (table.clone(), columns.clone()))
            .collect();
        let mut indexes = self
            .indexes
            .read()
            .map_err(|_| RymeError::Internal(String::from("index lock")))?
            .values()
            .flat_map(|states| states.iter().map(|state| state.definition.clone()))
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| {
            left.table.cmp(&right.table).then_with(|| left.name.cmp(&right.name))
        });
        let checks = self
            .checks
            .lock()
            .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?
            .iter()
            .map(|(table, expressions)| (table.clone(), expressions.clone()))
            .collect();
        let foreign_keys = self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .iter()
            .map(|(table, constraints)| (table.clone(), constraints.clone()))
            .collect();
        let constraints = self
            .constraints
            .lock()
            .map_err(|_| RymeError::Internal(String::from("constraint lock")))?
            .iter()
            .map(|(table, entries)| (table.clone(), entries.clone()))
            .collect();
        let rls_maps = self
            .rls_maps
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS map lock")))?
            .clone();
        let rls_tables = rls_maps.tenant_read.into_iter().collect();
        let rls_write_tables = rls_maps.tenant_write.into_iter().collect();
        let rls_enabled = self
            .rls_enabled
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS enabled lock")))?
            .iter()
            .cloned()
            .collect();
        let rls_forced = self
            .rls_forced
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS forced lock")))?
            .iter()
            .cloned()
            .collect();
        let rls_policies = self
            .rls_policies
            .read()
            .map_err(|_| RymeError::Internal(String::from("RLS policy lock")))?
            .iter()
            .map(|(key, policy)| (key.clone(), policy.clone()))
            .collect();
        let functions = self
            .functions
            .lock()
            .map_err(|_| RymeError::Internal(String::from("function lock")))?
            .iter()
            .map(|(name, definition)| (name.clone(), definition.clone()))
            .collect();
        let triggers = self
            .triggers
            .lock()
            .map_err(|_| RymeError::Internal(String::from("trigger lock")))?
            .iter()
            .map(|key| (key.0.clone(), key.1.clone()))
            .collect();
        let views = self
            .views
            .lock()
            .map_err(|_| RymeError::Internal(String::from("view lock")))?
            .iter()
            .map(|(name, definition)| (name.clone(), definition.clone()))
            .collect();
        let sequences = self
            .sequences
            .lock()
            .map_err(|_| RymeError::Internal(String::from("sequence lock")))?
            .iter()
            .map(|(name, definition)| (name.clone(), definition.clone()))
            .collect();
        let roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?.clone();
        let privileges = self
            .privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("privilege lock")))?
            .clone();
        let default_privileges = self
            .default_privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("default privilege lock")))?
            .clone();
        Ok(SchemaSnapshot {
            tables,
            indexes,
            checks,
            foreign_keys,
            constraints,
            functions,
            triggers,
            views,
            sequences,
            roles: roles.into_iter().collect(),
            privileges,
            default_privileges,
            rls_tables,
            rls_write_tables,
            rls_enabled,
            rls_forced,
            rls_policies,
        })
    }

    /// Legacy infallible accessor. Durable paths must use `try_schema_snapshot`.
    pub fn schema_snapshot(&self) -> SchemaSnapshot {
        self.try_schema_snapshot().unwrap_or_default()
    }

    pub fn restore_schema_snapshot(&self, snapshot: SchemaSnapshot) -> Result<()> {
        let previous = self.try_schema_snapshot()?;
        let was_dirty = self.schema_dirty.load(Ordering::SeqCst);
        match self.restore_schema_snapshot_inner(snapshot) {
            Ok(()) => Ok(()),
            Err(error) => {
                let rollback = self.restore_schema_snapshot_inner(previous);
                self.schema_dirty.store(was_dirty, Ordering::SeqCst);
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(RymeError::Internal(format!(
                        "schema restore failed: {error}; rollback failed: {rollback_error}"
                    ))),
                }
            }
        }
    }

    fn restore_schema_snapshot_inner(&self, snapshot: SchemaSnapshot) -> Result<()> {
        validate_schema_snapshot(&snapshot)?;
        let functions = snapshot.functions;
        let triggers = snapshot.triggers;
        let mut views = snapshot.views;
        for definition in views.values_mut() {
            definition.dependencies = view_dependencies(&definition.query);
        }
        let sequences = snapshot.sequences;
        let roles = snapshot.roles;
        let privileges = snapshot.privileges;
        let default_privileges = snapshot.default_privileges;
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            *catalog = snapshot.tables.into_iter().collect();
        }
        if let Ok(mut checks) = self.checks.lock() {
            *checks = snapshot.checks.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("check constraint lock")));
        }
        if let Ok(mut foreign_keys) = self.foreign_keys.lock() {
            *foreign_keys = snapshot.foreign_keys.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("foreign key lock")));
        }
        if let Ok(mut constraints) = self.constraints.lock() {
            *constraints = snapshot.constraints.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("constraint lock")));
        }
        if let Ok(mut stored) = self.functions.lock() {
            *stored = functions.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("function lock")));
        }
        if let Ok(mut stored) = self.triggers.lock() {
            *stored = triggers.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("trigger lock")));
        }
        if let Ok(mut stored) = self.views.lock() {
            *stored = views.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("view lock")));
        }
        if let Ok(mut stored) = self.sequences.lock() {
            *stored = sequences.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("sequence lock")));
        }
        if let Ok(mut stored) = self.roles.lock() {
            *stored = roles.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("role lock")));
        }
        if let Ok(mut stored) = self.privileges.lock() {
            *stored = privileges;
        } else {
            return Err(RymeError::Internal(String::from("privilege lock")));
        }
        if let Ok(mut stored) = self.default_privileges.lock() {
            *stored = default_privileges;
        } else {
            return Err(RymeError::Internal(String::from("default privilege lock")));
        }
        let legacy_rls_enabled: HashSet<String> = snapshot.rls_tables.keys().cloned().collect();
        let rls_enabled: HashSet<String> = if snapshot.rls_enabled.is_empty() {
            legacy_rls_enabled
        } else {
            snapshot.rls_enabled.into_iter().collect()
        };
        let rls_forced: HashSet<String> = snapshot.rls_forced.into_iter().collect();
        let mut legacy_policies = BTreeMap::new();
        for (table, column) in &snapshot.rls_tables {
            let policy = RlsPolicy {
                name: format!("__config__{table}"),
                table: table.clone(),
                column: column.clone(),
                command: String::from("ALL"),
                using: None,
                check: None,
                roles: Vec::new(),
                read: true,
                write: snapshot.rls_write_tables.contains_key(table),
            };
            legacy_policies.insert(format!("{table}\0{}", policy.name), policy);
        }
        for (table, column) in &snapshot.rls_write_tables {
            legacy_policies.entry(format!("{table}\0__config__{table}")).or_insert_with(|| {
                RlsPolicy {
                    name: format!("__config__{table}"),
                    table: table.clone(),
                    column: column.clone(),
                    command: String::from("ALL"),
                    using: None,
                    check: None,
                    roles: Vec::new(),
                    read: false,
                    write: true,
                }
            });
        }
        let rls_policies =
            if snapshot.rls_policies.is_empty() { legacy_policies } else { snapshot.rls_policies };
        if let Ok(mut maps) = self.rls_maps.write() {
            maps.tenant_read = snapshot.rls_tables.into_iter().collect();
            maps.tenant_write = snapshot.rls_write_tables.into_iter().collect();
            maps.uid_read.clear();
            maps.uid_write.clear();
        } else {
            return Err(RymeError::Internal(String::from("RLS lock")));
        }
        if let Ok(mut tables) = self.rls_enabled.write() {
            *tables = rls_enabled;
        } else {
            return Err(RymeError::Internal(String::from("RLS state lock")));
        }
        if let Ok(mut tables) = self.rls_forced.write() {
            *tables = rls_forced;
        } else {
            return Err(RymeError::Internal(String::from("RLS force lock")));
        }
        if let Ok(mut policies) = self.rls_policies.write() {
            *policies = rls_policies.into_iter().collect();
        } else {
            return Err(RymeError::Internal(String::from("RLS policy lock")));
        }
        self.rebuild_rls_policy_maps()?;
        if let Ok(mut indexes) = self.indexes.write() {
            indexes.clear();
        } else {
            return Err(RymeError::Internal(String::from("index lock")));
        }
        for definition in snapshot.indexes {
            self.create_index(definition, false)?;
        }
        self.schema_dirty.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn with_isolated_schema<C>(self, manager: C) -> Executor<C>
    where
        C: TxnBackend,
    {
        let catalog = self.catalog.lock().map(|catalog| catalog.clone()).unwrap_or_default();
        let indexes = self.indexes.read().map(|indexes| indexes.clone()).unwrap_or_default();
        let checks = self.checks.lock().map(|checks| checks.clone()).unwrap_or_default();
        let foreign_keys =
            self.foreign_keys.lock().map(|foreign_keys| foreign_keys.clone()).unwrap_or_default();
        let constraints =
            self.constraints.lock().map(|constraints| constraints.clone()).unwrap_or_default();
        let functions =
            self.functions.lock().map(|functions| functions.clone()).unwrap_or_default();
        let triggers = self.triggers.lock().map(|triggers| triggers.clone()).unwrap_or_default();
        let views = self.views.lock().map(|views| views.clone()).unwrap_or_default();
        let sequences =
            self.sequences.lock().map(|sequences| sequences.clone()).unwrap_or_default();
        let roles = self.roles.lock().map(|roles| roles.clone()).unwrap_or_default();
        let privileges =
            self.privileges.lock().map(|privileges| privileges.clone()).unwrap_or_default();
        let default_privileges =
            self.default_privileges.lock().map(|privileges| privileges.clone()).unwrap_or_default();
        let rls_maps = self.rls_maps.read().map(|maps| maps.clone()).unwrap_or_default();
        let rls_enabled = self.rls_enabled.read().map(|tables| tables.clone()).unwrap_or_default();
        let rls_forced = self.rls_forced.read().map(|tables| tables.clone()).unwrap_or_default();
        let platform_rls_tables =
            self.platform_rls_tables.read().map(|tables| tables.clone()).unwrap_or_default();
        let rls_policies =
            self.rls_policies.read().map(|policies| policies.clone()).unwrap_or_default();
        Executor {
            tenant: self.tenant,
            auth_uid: self.auth_uid,
            database: self.database,
            branch: self.branch,
            role: self.role,
            manager,
            read_ts: self.read_ts,
            realtime: self.realtime,
            read_only: self.read_only,
            isolation: self.isolation,
            catalog: Arc::new(Mutex::new(catalog)),
            indexes: Arc::new(RwLock::new(indexes)),
            pending_index_commits: Arc::new(RwLock::new(HashMap::new())),
            rls_maps: Arc::new(RwLock::new(rls_maps)),
            rls_policy_version: Arc::new(AtomicU64::new(0)),
            rls_role_maps: Arc::new(RwLock::new(HashMap::new())),
            rls_enabled: Arc::new(RwLock::new(rls_enabled)),
            rls_forced: Arc::new(RwLock::new(rls_forced)),
            platform_rls_tables: Arc::new(RwLock::new(platform_rls_tables)),
            rls_policies: Arc::new(RwLock::new(rls_policies)),
            checks: Arc::new(Mutex::new(checks)),
            foreign_keys: Arc::new(Mutex::new(foreign_keys)),
            constraints: Arc::new(Mutex::new(constraints)),
            functions: Arc::new(Mutex::new(functions)),
            triggers: Arc::new(Mutex::new(triggers)),
            views: Arc::new(Mutex::new(views)),
            sequences: Arc::new(Mutex::new(sequences)),
            roles: Arc::new(Mutex::new(roles)),
            privileges: Arc::new(Mutex::new(privileges)),
            default_privileges: Arc::new(Mutex::new(default_privileges)),
            schema_path: Arc::new(Mutex::new(None)),
            schema_persist_lock: Arc::new(Mutex::new(())),
            schema_dirty: Arc::new(AtomicBool::new(false)),
            schema_hook: None,
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

    pub fn catalog_views(&self) -> Vec<String> {
        let Ok(views) = self.views.lock() else { return Vec::new() };
        let mut names: Vec<String> = views.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn view_dependencies(&self, name: &str) -> Result<Option<Vec<String>>> {
        let Some(view) = self.view_definition(name)? else {
            return Ok(None);
        };
        let mut pending = view.dependencies;
        let mut leaves = BTreeSet::new();
        let mut visited = HashSet::new();
        while let Some(dependency) = pending.pop() {
            let visit_key = dependency.to_ascii_lowercase();
            if !visited.insert(visit_key) {
                continue;
            }
            if let Some(dependency_view) = self.view_definition(&dependency)? {
                pending.extend(dependency_view.dependencies);
            } else {
                leaves.insert(dependency);
            }
        }
        Ok(Some(leaves.into_iter().collect()))
    }

    pub async fn realtime_view_rows(
        &self,
        name: &str,
        read_ts: Option<u64>,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        if self.view_definition(name)?.is_none() {
            return Ok(None);
        }
        let mut txn = self.begin_with(self.isolation);
        if let Some(read_ts) = read_ts {
            txn.restamp(read_ts);
        }
        Ok(Some(self.scan_rows_async(&mut txn, name, &[], usize::MAX).await?))
    }

    pub fn catalog_sequences(&self) -> Vec<SequenceDefinition> {
        let Ok(sequences) = self.sequences.lock() else { return Vec::new() };
        let mut definitions: Vec<SequenceDefinition> = sequences.values().cloned().collect();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        definitions
    }

    pub fn catalog_roles(&self) -> Vec<RoleDefinition> {
        let Ok(roles) = self.roles.lock() else { return Vec::new() };
        let mut definitions: Vec<RoleDefinition> = roles.values().cloned().collect();
        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        definitions
    }

    pub fn catalog_privileges(&self) -> Vec<PrivilegeGrant> {
        self.privileges.lock().map(|privileges| privileges.clone()).unwrap_or_default()
    }

    pub fn catalog_default_privileges(&self) -> Vec<DefaultPrivilegeGrant> {
        self.default_privileges.lock().map(|privileges| privileges.clone()).unwrap_or_default()
    }

    fn effective_role_names(&self) -> Result<Vec<String>> {
        let roles =
            self.roles.lock().map_err(|_| RymeError::Internal(String::from("role lock")))?;
        let privileges = self
            .privileges
            .lock()
            .map_err(|_| RymeError::Internal(String::from("privilege lock")))?;
        Ok(effective_sql_roles(&self.role, &roles, &privileges))
    }

    fn role_is_superuser(&self) -> bool {
        self.role.eq_ignore_ascii_case("postgres")
            || self
                .roles
                .lock()
                .ok()
                .and_then(|roles| {
                    roles.values().find(|role| role.name.eq_ignore_ascii_case(&self.role)).cloned()
                })
                .is_some_and(|role| role.superuser)
    }

    fn has_object_privilege(&self, object_type: &str, object: &str, privilege: &str) -> bool {
        let canonical = match object_type {
            "TABLE" => self.canonical_table_name(object).unwrap_or_else(|| object.to_string()),
            "SEQUENCE" => {
                self.canonical_sequence_name(object).unwrap_or_else(|| object.to_string())
            }
            _ => object.to_string(),
        };
        let canonical_schema = canonical.rsplit_once('.').map(|(schema, _)| schema);
        let Ok(roles) = self.roles.lock() else { return false };
        let Ok(privileges) = self.privileges.lock() else { return false };
        let effective_roles = effective_sql_roles(&self.role, &roles, &privileges);
        privileges.iter().any(|grant| {
            if !grant.object_type.eq_ignore_ascii_case(object_type)
                || !(effective_roles.iter().any(|role| grant.grantee.eq_ignore_ascii_case(role))
                    || grant.grantee.eq_ignore_ascii_case("PUBLIC"))
            {
                return false;
            }
            let object_matches = if let Some(schema) = grant.object.strip_suffix(".*") {
                canonical_schema.is_some_and(|actual| schema.eq_ignore_ascii_case(actual))
            } else {
                same_relation(&grant.object, &canonical)
            };
            object_matches
                && (grant.privilege.eq_ignore_ascii_case("ALL")
                    || grant.privilege.eq_ignore_ascii_case(privilege))
        })
    }

    fn has_table_privilege(&self, table: &str, privilege: &str) -> bool {
        self.has_object_privilege("TABLE", table, privilege)
    }

    pub fn can_select_table(&self, table: &str) -> bool {
        self.role_is_superuser() || self.has_table_privilege(table, "SELECT")
    }

    pub fn can_table_privilege(&self, table: &str, privilege: &str) -> bool {
        self.role_is_superuser() || self.has_table_privilege(table, privilege)
    }

    pub fn can_select_all_tables(&self) -> bool {
        self.role_is_superuser()
    }

    fn canonical_sequence_name(&self, sequence: &str) -> Option<String> {
        self.sequences.lock().ok().and_then(|sequences| {
            sequences.get_key_value(sequence).map(|(name, _)| name.clone()).or_else(|| {
                sequences
                    .keys()
                    .find_map(|name| same_relation(name, sequence).then(|| name.clone()))
            })
        })
    }

    fn sequence_privileges(operation: SequenceOperation) -> &'static [&'static str] {
        match operation {
            SequenceOperation::Nextval => &["USAGE", "UPDATE"],
            SequenceOperation::Currval => &["USAGE", "SELECT"],
            SequenceOperation::Setval => &["UPDATE"],
        }
    }

    fn enforce_sequence_privilege(
        &self,
        operation: SequenceOperation,
        sequence: &str,
    ) -> Result<()> {
        if self.role_is_superuser()
            || Self::sequence_privileges(operation)
                .iter()
                .any(|privilege| self.has_object_privilege("SEQUENCE", sequence, privilege))
        {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
    }

    fn enforce_statement_privileges(&self, statement: &Statement) -> Result<()> {
        if self.role_is_superuser() {
            return Ok(());
        }
        if let Statement::SequenceValue { operation, name, .. } = statement {
            self.enforce_sequence_privilege(*operation, name)?;
        }
        if statement.is_schema_change() {
            return Err(RymeError::Forbidden);
        }
        let mut required = Vec::new();
        collect_table_privilege_requirements(statement, &mut required);
        if required
            .into_iter()
            .all(|(table, privilege)| self.has_table_privilege(&table, privilege))
        {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
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

    fn canonical_table_name(&self, table: &str) -> Option<String> {
        self.catalog.lock().ok().and_then(|catalog| {
            catalog.get_key_value(table).map(|(name, _)| name.clone()).or_else(|| {
                catalog
                    .keys()
                    .find_map(|name| (name.rsplit('.').next() == Some(table)).then(|| name.clone()))
            })
        })
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
            let composite_primary = self
                .catalog_columns(table)
                .iter()
                .filter(|definition| definition.primary_key)
                .count()
                > 1;
            for (pk, value) in self.scan_all_rows(table)? {
                let candidate = if !composite_primary
                    && (definition.primary_key
                        || definition.name.eq_ignore_ascii_case("id")
                        || definition.name.eq_ignore_ascii_case("pk")
                        || definition.name.eq_ignore_ascii_case("key"))
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
            .read()
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

    pub fn catalog_foreign_keys(&self, table: &str) -> Vec<ForeignKeyConstraint> {
        self.foreign_keys
            .lock()
            .ok()
            .and_then(|foreign_keys| {
                foreign_keys.get(table).cloned().or_else(|| {
                    foreign_keys.iter().find_map(|(name, constraints)| {
                        (name.rsplit('.').next() == Some(table)).then(|| constraints.clone())
                    })
                })
            })
            .unwrap_or_default()
    }

    fn rls_is_enabled(&self, table: &str) -> bool {
        self.platform_rls_applies(table)
            || self.rls_enabled.read().map_or(true, |tables| relation_set_contains(&tables, table))
    }

    fn platform_rls_applies(&self, table: &str) -> bool {
        self.platform_rls_tables.read().map_or(true, |tables| relation_set_contains(&tables, table))
    }

    fn rls_applies_to_role(&self, table: &str) -> bool {
        self.rls_is_enabled(table)
            && (self.platform_rls_applies(table)
                || !self.role_is_superuser()
                || self
                    .rls_forced
                    .read()
                    .map_or(true, |tables| relation_set_contains(&tables, table)))
    }

    fn rls_policy_allows(&self, table: &str, value: &[u8]) -> bool {
        self.rls_policy_allows_for_command(table, value, RlsCommand::Select)
    }

    fn rls_policy_allows_for_command(
        &self,
        table: &str,
        value: &[u8],
        command: RlsCommand,
    ) -> bool {
        let Some(maps) = self.rls_maps_for_current_role() else { return false };
        let (tenant_map, uid_map) = match command {
            RlsCommand::Select => (&maps.tenant_read, &maps.uid_read),
            RlsCommand::Insert => (&maps.tenant_insert, &maps.uid_insert),
            RlsCommand::Update => (&maps.tenant_update, &maps.uid_update),
            RlsCommand::Delete => (&maps.tenant_delete, &maps.uid_delete),
        };
        let tenant_column = relation_map_get(tenant_map, table);
        let uid_column = relation_map_get(uid_map, table);
        if tenant_column.is_none() && uid_column.is_none() {
            return false;
        }
        let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
            return false;
        };
        rls_tenant_column_allows(&object, tenant_column, &self.tenant)
            && rls_uid_policies_allow(
                &object,
                uid_column.map(Vec::as_slice),
                self.auth_uid.as_deref(),
            )
    }

    fn rls_allows(&self, table: &str, value: &[u8]) -> bool {
        self.rls_allows_with_context(table, value, self.rls_applies_to_role(table))
    }

    #[inline]
    fn rls_allows_with_context(&self, table: &str, value: &[u8], applies: bool) -> bool {
        !applies || self.rls_policy_allows(table, value)
    }

    pub fn row_allowed_by_rls(&self, table: &str, value: &[u8]) -> bool {
        // Realtime and branch diff consumers are platform trust boundaries. They
        // must keep filtering rows even when their internal executor uses the
        // postgres role to inspect committed storage.
        !self.rls_is_enabled(table) || self.rls_policy_allows(table, value)
    }

    pub fn row_security_enabled(&self, table: &str) -> bool {
        self.rls_is_enabled(table)
    }

    pub fn row_allowed_for_write(&self, table: &str, value: &[u8]) -> bool {
        self.row_allowed_for_command(table, value, RlsCommand::Update)
    }

    /// Checks a row against policies for one SQL command and the current role.
    pub fn row_allowed_for_command(&self, table: &str, value: &[u8], command: RlsCommand) -> bool {
        !self.rls_applies_to_role(table)
            || self.rls_policy_allows_for_command(table, value, command)
    }

    fn enforce_rls(&self, table: &str, value: &[u8], command: RlsCommand) -> Result<()> {
        if !self.rls_applies_to_role(table) {
            return Ok(());
        }
        if self.rls_policy_allows_for_command(table, value, command) {
            Ok(())
        } else {
            Err(RymeError::Forbidden)
        }
    }

    fn enforce_checks(&self, table: &str, pk: &[u8], value: &[u8]) -> Result<()> {
        let checks = self
            .checks
            .lock()
            .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?
            .get(table)
            .cloned()
            .unwrap_or_default();
        for expression in checks {
            let predicates = parse_filter(&tokenize(&expression))?;
            let mut valid = true;
            for predicate in predicates {
                match check_predicate_result(&predicate, pk, value) {
                    Some(true) => {}
                    Some(false) => {
                        valid = false;
                        break;
                    }
                    None => {}
                }
            }
            if !valid {
                return Err(RymeError::Conflict(format!("check constraint failed: {expression}")));
            }
        }
        Ok(())
    }

    fn row_column_value(
        &self,
        table: &str,
        pk: &[u8],
        value: &[u8],
        column: &str,
    ) -> Option<Vec<u8>> {
        if let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) {
            if let Some(selected) = json_column_value(column, &object) {
                return (!selected.is_null()).then(|| json_result_bytes(selected));
            }
        }
        if column.eq_ignore_ascii_case("key")
            || column.eq_ignore_ascii_case("pk")
            || column.eq_ignore_ascii_case("id")
        {
            return Some(pk.to_vec());
        }
        if column.eq_ignore_ascii_case("value")
            || column.eq_ignore_ascii_case("val")
            || column.eq_ignore_ascii_case("data")
        {
            return Some(value.to_vec());
        }
        let primary_keys = self
            .catalog_columns(table)
            .into_iter()
            .filter(|definition| definition.primary_key)
            .collect::<Vec<_>>();
        if let Some(index) =
            primary_keys.iter().position(|definition| definition.name.eq_ignore_ascii_case(column))
        {
            if primary_keys.len() == 1 {
                return Some(pk.to_vec());
            }
            return decode_key_parts(pk).and_then(|parts| parts.get(index).cloned());
        }
        None
    }

    fn scan_all_rows_in_transaction(&self, txn: &mut Transaction, table: &str) -> Result<Vec<Row>> {
        const PAGE: usize = 10_000;
        let mut rows = self.manager.scan(&mut *txn, &self.tenant, &self.database, table, PAGE)?;
        let mut budget = MaterializationBudget::default();
        budget.add_rows(&rows)?;
        loop {
            if rows.len() < PAGE {
                break;
            }
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next = self.manager.scan_after(
                &mut *txn,
                &self.tenant,
                &self.database,
                table,
                &last,
                PAGE,
            )?;
            if next.is_empty() {
                break;
            }
            budget.add_rows(&next)?;
            rows.extend(next);
        }
        if !txn.writes().is_empty() {
            let mut merged = rows.into_iter().collect::<BTreeMap<_, _>>();
            for (key, write) in txn.writes() {
                if key.tenant != self.tenant || key.database != self.database || key.table != table
                {
                    continue;
                }
                match write.value.as_ref() {
                    Some(value) => {
                        budget.add_parts(&key.pk, value)?;
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            let rows: Vec<Row> = merged.into_iter().collect();
            ensure_materialized_rows(&rows)?;
            return Ok(rows);
        }
        ensure_materialized_rows(&rows)?;
        Ok(rows)
    }

    async fn scan_all_rows_in_transaction_async(
        &self,
        txn: &mut Transaction,
        table: &str,
    ) -> Result<Vec<Row>> {
        const PAGE: usize = 10_000;
        let mut rows =
            self.manager.scan_async(&mut *txn, &self.tenant, &self.database, table, PAGE).await?;
        let mut budget = MaterializationBudget::default();
        budget.add_rows(&rows)?;
        loop {
            if rows.len() < PAGE {
                break;
            }
            let Some(last) = rows.last().map(|(pk, _)| pk.clone()) else { break };
            let next = self
                .manager
                .scan_after_async(&mut *txn, &self.tenant, &self.database, table, &last, PAGE)
                .await?;
            if next.is_empty() {
                break;
            }
            budget.add_rows(&next)?;
            rows.extend(next);
        }
        if !txn.writes().is_empty() {
            let mut merged = rows.into_iter().collect::<BTreeMap<_, _>>();
            for (key, write) in txn.writes() {
                if key.tenant != self.tenant || key.database != self.database || key.table != table
                {
                    continue;
                }
                match write.value.as_ref() {
                    Some(value) => {
                        budget.add_parts(&key.pk, value)?;
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            let rows: Vec<Row> = merged.into_iter().collect();
            ensure_materialized_rows(&rows)?;
            return Ok(rows);
        }
        ensure_materialized_rows(&rows)?;
        Ok(rows)
    }

    async fn enforce_foreign_keys_async(
        &self,
        txn: &mut Transaction,
        table: &str,
        pk: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let constraints = self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .get(table)
            .cloned()
            .unwrap_or_default();
        for constraint in constraints {
            let Some(local_values) = constraint
                .columns
                .iter()
                .map(|column| self.row_column_value(table, pk, value, column))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let referenced_columns = if constraint.referenced_columns.is_empty() {
                self.catalog_columns(&constraint.referenced_table)
                    .into_iter()
                    .filter(|definition| definition.primary_key)
                    .map(|definition| definition.name)
                    .collect::<Vec<_>>()
            } else {
                constraint.referenced_columns.clone()
            };
            if referenced_columns.len() != local_values.len() || referenced_columns.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("foreign key column count")));
            }
            let parent_columns = self.catalog_columns(&constraint.referenced_table);
            let parent_primary = parent_columns
                .iter()
                .filter(|definition| definition.primary_key)
                .map(|definition| definition.name.clone())
                .collect::<Vec<_>>();
            let references_primary = referenced_columns.len() == parent_primary.len()
                && referenced_columns
                    .iter()
                    .zip(&parent_primary)
                    .all(|(referenced, primary)| referenced.eq_ignore_ascii_case(primary));
            let exists = if references_primary {
                let parent_pk = if local_values.len() == 1 {
                    local_values[0].clone()
                } else {
                    encode_key_parts(&local_values).ok_or_else(|| {
                        RymeError::InvalidArgument(String::from("foreign key value"))
                    })?
                };
                self.manager
                    .get_async(
                        txn,
                        &RecordKey::new(
                            &self.tenant,
                            &self.database,
                            &constraint.referenced_table,
                            &parent_pk,
                        ),
                    )
                    .await?
                    .is_some()
            } else {
                self.scan_all_rows_in_transaction_async(txn, &constraint.referenced_table)
                    .await?
                    .iter()
                    .any(|(parent_pk, parent_value)| {
                        referenced_columns.iter().zip(&local_values).all(|(column, expected)| {
                            self.row_column_value(
                                &constraint.referenced_table,
                                parent_pk,
                                parent_value,
                                column,
                            )
                            .is_some_and(|actual| actual == *expected)
                        })
                    })
            };
            if !exists {
                return Err(RymeError::Conflict(format!(
                    "foreign key constraint failed: {} references {}",
                    constraint.columns.join(", "),
                    constraint.referenced_table
                )));
            }
        }
        Ok(())
    }

    fn enforce_foreign_keys(
        &self,
        txn: &mut Transaction,
        table: &str,
        pk: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let constraints = self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .get(table)
            .cloned()
            .unwrap_or_default();
        for constraint in constraints {
            let Some(local_values) = constraint
                .columns
                .iter()
                .map(|column| self.row_column_value(table, pk, value, column))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            let referenced_columns = if constraint.referenced_columns.is_empty() {
                self.catalog_columns(&constraint.referenced_table)
                    .into_iter()
                    .filter(|definition| definition.primary_key)
                    .map(|definition| definition.name)
                    .collect::<Vec<_>>()
            } else {
                constraint.referenced_columns.clone()
            };
            if referenced_columns.len() != local_values.len() || referenced_columns.is_empty() {
                return Err(RymeError::InvalidArgument(String::from("foreign key column count")));
            }
            let parent_columns = self.catalog_columns(&constraint.referenced_table);
            let parent_primary = parent_columns
                .iter()
                .filter(|definition| definition.primary_key)
                .map(|definition| definition.name.clone())
                .collect::<Vec<_>>();
            let references_primary = referenced_columns.len() == parent_primary.len()
                && referenced_columns
                    .iter()
                    .zip(&parent_primary)
                    .all(|(referenced, primary)| referenced.eq_ignore_ascii_case(primary));
            let exists = if references_primary {
                let parent_pk = if local_values.len() == 1 {
                    local_values[0].clone()
                } else {
                    encode_key_parts(&local_values).ok_or_else(|| {
                        RymeError::InvalidArgument(String::from("foreign key value"))
                    })?
                };
                self.manager
                    .get(
                        txn,
                        &RecordKey::new(
                            &self.tenant,
                            &self.database,
                            &constraint.referenced_table,
                            &parent_pk,
                        ),
                    )?
                    .is_some()
            } else {
                self.scan_all_rows_in_transaction(txn, &constraint.referenced_table)?.iter().any(
                    |(parent_pk, parent_value)| {
                        referenced_columns.iter().zip(&local_values).all(|(column, expected)| {
                            self.row_column_value(
                                &constraint.referenced_table,
                                parent_pk,
                                parent_value,
                                column,
                            )
                            .is_some_and(|actual| actual == *expected)
                        })
                    },
                )
            };
            if !exists {
                return Err(RymeError::Conflict(format!(
                    "foreign key constraint failed: {} references {}",
                    constraint.columns.join(", "),
                    constraint.referenced_table
                )));
            }
        }
        Ok(())
    }

    fn delete_row_with_references_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        table: &'a str,
        pk: &'a [u8],
        value: &'a [u8],
        changes: &'a mut Vec<TransactionChange>,
        visited: &'a mut BTreeSet<(String, Vec<u8>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let table = self.canonical_table_name(table).unwrap_or_else(|| table.to_string());
            if !visited.insert((table.clone(), pk.to_vec())) {
                return Ok(());
            }
            let constraints = self
                .foreign_keys
                .lock()
                .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
                .iter()
                .flat_map(|(child_table, constraints)| {
                    constraints
                        .iter()
                        .map(move |constraint| (child_table.clone(), constraint.clone()))
                })
                .filter(|(_, constraint)| constraint.referenced_table.eq_ignore_ascii_case(&table))
                .collect::<Vec<_>>();
            for (child_table, constraint) in constraints {
                let referenced_columns = if constraint.referenced_columns.is_empty() {
                    self.catalog_columns(&table)
                        .into_iter()
                        .filter(|definition| definition.primary_key)
                        .map(|definition| definition.name)
                        .collect::<Vec<_>>()
                } else {
                    constraint.referenced_columns.clone()
                };
                let Some(parent_values) = referenced_columns
                    .iter()
                    .map(|column| self.row_column_value(&table, pk, value, column))
                    .collect::<Option<Vec<_>>>()
                else {
                    continue;
                };
                let child_rows = self.scan_all_rows_in_transaction_async(txn, &child_table).await?;
                for (child_pk, child_value) in child_rows {
                    let matches_parent =
                        constraint.columns.iter().zip(&parent_values).all(|(column, expected)| {
                            self.row_column_value(&child_table, &child_pk, &child_value, column)
                                .is_some_and(|actual| actual == *expected)
                        });
                    if !matches_parent {
                        continue;
                    }
                    match constraint.on_delete {
                        ForeignKeyAction::Restrict => {
                            return Err(RymeError::Conflict(format!(
                                "foreign key constraint failed: referenced row in {table} is still used"
                            )));
                        }
                        ForeignKeyAction::Cascade => {
                            let child_identity = (child_table.clone(), child_pk.clone());
                            if visited.contains(&child_identity) {
                                continue;
                            }
                            self.delete_row_with_references_async(
                                txn,
                                &child_table,
                                &child_pk,
                                &child_value,
                                changes,
                                visited,
                            )
                            .await?;
                            self.manager.delete(
                                txn,
                                RecordKey::new(
                                    &self.tenant,
                                    &self.database,
                                    &child_table,
                                    &child_pk,
                                ),
                            );
                            changes.push(TransactionChange {
                                table: child_table.clone(),
                                pk: child_pk,
                                previous_pk: None,
                                op: Operation::Delete,
                                before: Some(child_value),
                                after: None,
                            });
                        }
                        ForeignKeyAction::SetNull | ForeignKeyAction::SetDefault => {
                            let assignment = match constraint.on_delete {
                                ForeignKeyAction::SetNull => InsertValue::Null,
                                ForeignKeyAction::SetDefault => InsertValue::Default,
                                ForeignKeyAction::Restrict | ForeignKeyAction::Cascade => {
                                    unreachable!()
                                }
                            };
                            let assignments = constraint
                                .columns
                                .iter()
                                .cloned()
                                .map(|column| (column, assignment.clone()))
                                .collect();
                            let after = self.materialize_update_row(
                                &child_table,
                                &child_pk,
                                assignments,
                                &child_value,
                            )?;
                            self.enforce_checks(&child_table, &child_pk, &after)?;
                            self.enforce_foreign_keys_async(txn, &child_table, &child_pk, &after)
                                .await?;
                            self.check_unique(txn, &child_table, &child_pk, &after)?;
                            self.manager.put(
                                txn,
                                RecordKey::new(
                                    &self.tenant,
                                    &self.database,
                                    &child_table,
                                    &child_pk,
                                ),
                                after.clone(),
                            );
                            changes.push(TransactionChange {
                                table: child_table.clone(),
                                pk: child_pk,
                                previous_pk: None,
                                op: Operation::Update,
                                before: Some(child_value),
                                after: Some(after),
                            });
                        }
                    }
                }
            }
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn update_referencing_rows_async<'a>(
        &'a self,
        txn: &'a mut Transaction,
        table: &'a str,
        pk: &'a [u8],
        before: &'a [u8],
        after: &'a [u8],
        changes: &'a mut Vec<TransactionChange>,
        visited: &'a mut BTreeSet<(String, Vec<u8>)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let table = self.canonical_table_name(table).unwrap_or_else(|| table.to_string());
            let identity = (table.clone(), pk.to_vec());
            if !visited.insert(identity.clone()) {
                return Ok(());
            }
            let constraints = self
                .foreign_keys
                .lock()
                .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
                .iter()
                .flat_map(|(child_table, constraints)| {
                    constraints
                        .iter()
                        .map(move |constraint| (child_table.clone(), constraint.clone()))
                })
                .filter(|(_, constraint)| constraint.referenced_table.eq_ignore_ascii_case(&table))
                .collect::<Vec<_>>();
            for (child_table, constraint) in constraints {
                let referenced_columns = if constraint.referenced_columns.is_empty() {
                    self.catalog_columns(&table)
                        .into_iter()
                        .filter(|definition| definition.primary_key)
                        .map(|definition| definition.name)
                        .collect::<Vec<_>>()
                } else {
                    constraint.referenced_columns.clone()
                };
                let old_values = referenced_columns
                    .iter()
                    .map(|column| self.row_column_value(&table, pk, before, column))
                    .collect::<Vec<_>>();
                let new_values = referenced_columns
                    .iter()
                    .map(|column| self.row_column_value(&table, pk, after, column))
                    .collect::<Vec<_>>();
                if old_values == new_values || old_values.iter().any(Option::is_none) {
                    continue;
                }
                let child_rows = self.scan_all_rows_in_transaction_async(txn, &child_table).await?;
                for (child_pk, child_value) in child_rows {
                    let matches_old =
                        constraint.columns.iter().zip(&old_values).all(|(column, expected)| {
                            self.row_column_value(&child_table, &child_pk, &child_value, column)
                                .is_some_and(|actual| {
                                    expected.as_ref().is_some_and(|expected| actual == *expected)
                                })
                        });
                    if !matches_old {
                        continue;
                    }
                    if constraint.on_update == ForeignKeyAction::Restrict {
                        return Err(RymeError::Conflict(format!(
                            "foreign key constraint failed: referenced key in {table} is still used"
                        )));
                    }
                    let child_identity = (child_table.clone(), child_pk.clone());
                    if visited.contains(&child_identity) {
                        continue;
                    }
                    let assignment = match constraint.on_update {
                        ForeignKeyAction::Cascade => None,
                        ForeignKeyAction::SetNull => Some(InsertValue::Null),
                        ForeignKeyAction::SetDefault => Some(InsertValue::Default),
                        ForeignKeyAction::Restrict => unreachable!(),
                    };
                    let assignments = constraint
                        .columns
                        .iter()
                        .zip(&new_values)
                        .map(|(column, value)| {
                            let value = assignment.clone().unwrap_or_else(|| {
                                value.clone().map(InsertValue::Value).unwrap_or(InsertValue::Null)
                            });
                            (column.clone(), value)
                        })
                        .collect();
                    let child_after = self.materialize_update_row(
                        &child_table,
                        &child_pk,
                        assignments,
                        &child_value,
                    )?;
                    self.enforce_checks(&child_table, &child_pk, &child_after)?;
                    self.enforce_foreign_keys_async(txn, &child_table, &child_pk, &child_after)
                        .await?;
                    self.check_unique(txn, &child_table, &child_pk, &child_after)?;
                    self.manager.put(
                        txn,
                        RecordKey::new(&self.tenant, &self.database, &child_table, &child_pk),
                        child_after.clone(),
                    );
                    self.update_referencing_rows_async(
                        txn,
                        &child_table,
                        &child_pk,
                        &child_value,
                        &child_after,
                        changes,
                        visited,
                    )
                    .await?;
                    changes.push(TransactionChange {
                        table: child_table.clone(),
                        pk: child_pk,
                        previous_pk: None,
                        op: Operation::Update,
                        before: Some(child_value),
                        after: Some(child_after),
                    });
                }
            }
            visited.remove(&identity);
            Ok(())
        })
    }

    fn filter_rls_rows(&self, table: &str, rows: impl IntoIterator<Item = Row>) -> Vec<Row> {
        self.filter_rls_rows_with_context(table, rows, self.rls_applies_to_role(table))
    }

    fn filter_rls_rows_with_context(
        &self,
        table: &str,
        rows: impl IntoIterator<Item = Row>,
        applies: bool,
    ) -> Vec<Row> {
        rows.into_iter()
            .filter(|(_, value)| self.rls_allows_with_context(table, value, applies))
            .collect()
    }

    fn materialize_insert_row(
        &self,
        table: &str,
        columns: Vec<String>,
        values: Vec<InsertValue>,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let definitions = self.catalog_columns(table);
        let (columns, values) = if columns.is_empty() && values.is_empty() {
            if definitions.is_empty() {
                return Err(RymeError::InvalidArgument(String::from(
                    "DEFAULT VALUES requires a table schema",
                )));
            }
            (
                definitions.iter().map(|definition| definition.name.clone()).collect(),
                definitions.iter().map(|_| InsertValue::Default).collect(),
            )
        } else {
            (columns, values)
        };
        if columns.is_empty() || columns.len() != values.len() {
            return Err(RymeError::InvalidArgument(String::from("insert column/value count")));
        }
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
                            if let Some(sequence) = definition
                                .column_default
                                .as_deref()
                                .and_then(sequence_name_from_default)
                            {
                                Some(
                                    self.sequence_mutation(
                                        SequenceOperation::Nextval,
                                        &sequence,
                                        None,
                                        true,
                                    )?
                                    .to_string()
                                    .into_bytes(),
                                )
                            } else {
                                Some(self.next_sequence_value(table, definition)?)
                            }
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
                InsertValue::Excluded(_) => {
                    return Err(RymeError::InvalidArgument(String::from(
                        "EXCLUDED is only valid in conflict updates",
                    )));
                }
                InsertValue::Expression(_) => {
                    return Err(RymeError::InvalidArgument(String::from(
                        "expressions are only valid in updates",
                    )));
                }
            };
            if resolved.is_none() && definition.is_some_and(|definition| !definition.nullable) {
                return Err(RymeError::InvalidArgument(format!(
                    "null value in column {column} violates not-null constraint"
                )));
            }
            supplied.insert(name, resolved);
        }

        let mut row = Vec::new();
        if definitions.is_empty() {
            for (column, value) in &supplied {
                row.push((column.clone(), value.clone(), String::new()));
            }
        } else {
            for definition in &definitions {
                let value = if let Some(value) =
                    supplied.remove(&definition.name.to_ascii_lowercase())
                {
                    value
                } else if definition.auto_increment {
                    if let Some(sequence) =
                        definition.column_default.as_deref().and_then(sequence_name_from_default)
                    {
                        Some(
                            self.sequence_mutation(
                                SequenceOperation::Nextval,
                                &sequence,
                                None,
                                true,
                            )?
                            .to_string()
                            .into_bytes(),
                        )
                    } else {
                        Some(self.next_sequence_value(table, definition)?)
                    }
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

        let primary_keys = definitions
            .iter()
            .filter(|definition| definition.primary_key)
            .map(|definition| definition.name.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let pk = if primary_keys.len() > 1 {
            let parts = primary_keys
                .iter()
                .map(|primary_key| {
                    row.iter()
                        .find(|(name, _, _)| name.eq_ignore_ascii_case(primary_key))
                        .and_then(|(_, value, _)| value.clone())
                        .ok_or_else(|| {
                            RymeError::InvalidArgument(String::from("null value in primary key"))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            encode_key_parts(&parts).ok_or_else(|| {
                RymeError::InvalidArgument(String::from("null value in primary key"))
            })?
        } else {
            let primary_key = primary_keys
                .first()
                .cloned()
                .or_else(|| {
                    row.iter()
                        .find(|(name, _, _)| {
                            name.eq_ignore_ascii_case("id") || name.eq_ignore_ascii_case("pk")
                        })
                        .map(|(name, _, _)| name.to_ascii_lowercase())
                })
                .or_else(|| row.first().map(|(name, _, _)| name.to_ascii_lowercase()))
                .ok_or_else(|| RymeError::InvalidArgument(String::from("insert primary key")))?;
            row.iter()
                .find(|(name, _, _)| name.eq_ignore_ascii_case(&primary_key))
                .and_then(|(_, value, _)| value.clone())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    RymeError::InvalidArgument(String::from("null value in primary key"))
                })?
        };

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
        self.materialize_update_row_with_incoming(table, pk, assignments, current, None)
    }

    fn materialize_update_row_with_incoming(
        &self,
        table: &str,
        pk: &[u8],
        assignments: Vec<(String, InsertValue)>,
        current: &[u8],
        incoming: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        self.materialize_update_row_with_context(table, pk, assignments, current, incoming, None)
    }

    fn materialize_update_row_with_source(
        &self,
        table: &str,
        pk: &[u8],
        assignments: Vec<(String, InsertValue)>,
        current: &[u8],
        source: &[u8],
    ) -> Result<Vec<u8>> {
        self.materialize_update_row_with_context(
            table,
            pk,
            assignments,
            current,
            None,
            Some(source),
        )
    }

    fn materialize_update_row_with_context(
        &self,
        table: &str,
        pk: &[u8],
        assignments: Vec<(String, InsertValue)>,
        current: &[u8],
        incoming: Option<&[u8]>,
        source: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let source_object = source
            .and_then(|value| serde_json::from_slice::<serde_json::Value>(value).ok())
            .and_then(|value| value.as_object().cloned());
        let definitions = self.catalog_columns(table);
        if definitions.is_empty() {
            if let Ok(serde_json::Value::Object(mut object)) =
                serde_json::from_slice::<serde_json::Value>(current)
            {
                for (column, value) in assignments {
                    let previous = object.get(&column);
                    let resolved = match value {
                        InsertValue::Value(value) => json_update_value(Some(value), previous),
                        InsertValue::Null => serde_json::Value::Null,
                        InsertValue::Default => {
                            return Err(RymeError::InvalidArgument(String::from(
                                "default update value requires table schema",
                            )));
                        }
                        InsertValue::Excluded(_) => {
                            return Err(RymeError::InvalidArgument(String::from(
                                "EXCLUDED is only valid in conflict updates",
                            )));
                        }
                        InsertValue::Expression(expression) => expression_value_with_source(
                            &expression,
                            &object,
                            incoming,
                            source_object.as_ref(),
                        )?,
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
                    InsertValue::Excluded(_) => Err(RymeError::InvalidArgument(String::from(
                        "EXCLUDED is only valid in conflict updates",
                    ))),
                    InsertValue::Expression(_) => Err(RymeError::InvalidArgument(String::from(
                        "structured row required for update expression",
                    ))),
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
        let primary_keys = definitions
            .iter()
            .filter(|definition| definition.primary_key)
            .map(|definition| definition.name.clone())
            .collect::<Vec<_>>();
        for (column, value) in assignments {
            let definition = definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(&column))
                .ok_or_else(|| RymeError::InvalidArgument(format!("unknown column {column}")))?;
            let resolved = match value {
                InsertValue::Value(value) => Some(value),
                InsertValue::Null => None,
                InsertValue::Default => {
                    definition.column_default.as_deref().map(eval_default).transpose()?.flatten()
                }
                InsertValue::Excluded(_) => {
                    return Err(RymeError::InvalidArgument(String::from(
                        "EXCLUDED is only valid in conflict updates",
                    )));
                }
                InsertValue::Expression(expression) => {
                    let value = expression_value_with_source(
                        &expression,
                        &object,
                        incoming,
                        source_object.as_ref(),
                    )?;
                    (!value.is_null()).then(|| json_result_bytes(&value))
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
        if primary_keys.is_empty() {
            object.insert(
                String::from("id"),
                serde_json::Value::String(String::from_utf8_lossy(pk).to_string()),
            );
        }
        serde_json::to_vec(&serde_json::Value::Object(object))
            .map_err(|error| RymeError::Internal(error.to_string()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_update_from_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        assignments: Vec<(String, InsertValue)>,
        source_table: String,
        target_column: String,
        source_column: String,
        filter: Vec<Predicate>,
        source_filter: Vec<Predicate>,
    ) -> Result<Vec<TransactionChange>> {
        self.reject_if_read_only()?;
        let targets = self.scan_rows_async(txn, &table, &filter, usize::MAX).await?;
        let sources = self.scan_rows_async(txn, &source_table, &source_filter, usize::MAX).await?;
        let mut changes = Vec::new();
        for (pk, before) in targets {
            self.enforce_rls(&table, &before, RlsCommand::Update)?;
            let Some((_, source_value)) = sources.iter().find(|(source_pk, source_value)| {
                self.row_column_value(&table, &pk, &before, &target_column)
                    .zip(self.row_column_value(
                        &source_table,
                        source_pk,
                        source_value,
                        &source_column,
                    ))
                    .is_some_and(|(target, source)| target == source)
            }) else {
                continue;
            };
            let after = self.materialize_update_row_with_source(
                &table,
                &pk,
                assignments.clone(),
                &before,
                source_value,
            )?;
            let after = self.apply_before_triggers(&table, "UPDATE", &after)?;
            let new_pk = self.primary_key_for_row(&table, &pk, &after)?;
            self.enforce_rls(&table, &after, RlsCommand::Update)?;
            self.enforce_checks(&table, &new_pk, &after)?;
            self.enforce_foreign_keys_async(txn, &table, &new_pk, &after).await?;
            self.check_unique_excluding(txn, &table, &new_pk, &after, Some(&pk))?;
            if new_pk != pk
                && self
                    .manager
                    .get_async(txn, &RecordKey::new(&self.tenant, &self.database, &table, &new_pk))
                    .await?
                    .is_some()
            {
                return Err(RymeError::Conflict(String::from("primary key exists")));
            }
            if new_pk != pk {
                self.manager.delete(txn, RecordKey::new(&self.tenant, &self.database, &table, &pk));
            }
            self.manager.put(
                txn,
                RecordKey::new(&self.tenant, &self.database, &table, &new_pk),
                after.clone(),
            );
            self.update_referencing_rows_async(
                txn,
                &table,
                &pk,
                &before,
                &after,
                &mut changes,
                &mut BTreeSet::new(),
            )
            .await?;
            changes.push(TransactionChange {
                table: table.clone(),
                previous_pk: (new_pk != pk).then_some(pk),
                pk: new_pk,
                op: Operation::Update,
                before: Some(before),
                after: Some(after),
            });
        }
        Ok(changes)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_delete_using_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        source_table: String,
        target_column: String,
        source_column: String,
        filter: Vec<Predicate>,
        source_filter: Vec<Predicate>,
    ) -> Result<Vec<TransactionChange>> {
        self.reject_if_read_only()?;
        let targets = self.scan_rows_async(txn, &table, &filter, usize::MAX).await?;
        let sources = self.scan_rows_async(txn, &source_table, &source_filter, usize::MAX).await?;
        let mut changes = Vec::new();
        for (pk, value) in targets {
            self.enforce_rls(&table, &value, RlsCommand::Delete)?;
            let matched = sources.iter().any(|(source_pk, source_value)| {
                self.row_column_value(&table, &pk, &value, &target_column)
                    .zip(self.row_column_value(
                        &source_table,
                        source_pk,
                        source_value,
                        &source_column,
                    ))
                    .is_some_and(|(target, source)| target == source)
            });
            if !matched {
                continue;
            }
            self.delete_row_with_references_async(
                txn,
                &table,
                &pk,
                &value,
                &mut changes,
                &mut BTreeSet::new(),
            )
            .await?;
            self.manager.delete(txn, RecordKey::new(&self.tenant, &self.database, &table, &pk));
            changes.push(TransactionChange {
                table: table.clone(),
                pk,
                previous_pk: None,
                op: Operation::Delete,
                before: Some(value),
                after: None,
            });
        }
        Ok(changes)
    }

    fn primary_key_for_row(&self, table: &str, fallback: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        let definitions = self.catalog_columns(table);
        let primary_keys =
            definitions.iter().filter(|definition| definition.primary_key).collect::<Vec<_>>();
        if primary_keys.is_empty() {
            return Ok(fallback.to_vec());
        }
        let serde_json::Value::Object(object) = serde_json::from_slice(value)
            .map_err(|_| RymeError::InvalidArgument(String::from("row is not a schema record")))?
        else {
            return Err(RymeError::InvalidArgument(String::from("row is not a schema record")));
        };
        let parts = primary_keys
            .iter()
            .map(|definition| {
                let selected = object
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&definition.name))
                    .map(|(_, selected)| selected)
                    .filter(|selected| !selected.is_null())
                    .ok_or_else(|| {
                        RymeError::InvalidArgument(format!(
                            "null value in column {} violates not-null constraint",
                            definition.name
                        ))
                    })?;
                let bytes = json_result_bytes(selected);
                if bytes.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from(
                        "null value in primary key",
                    )));
                }
                Ok(bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        if parts.len() == 1 {
            Ok(parts.into_iter().next().unwrap_or_default())
        } else {
            encode_key_parts(&parts).ok_or_else(|| {
                RymeError::InvalidArgument(String::from("null value in primary key"))
            })
        }
    }

    fn project_rows(
        &self,
        table: &str,
        columns: &[String],
        rows: impl Iterator<Item = Row>,
    ) -> Vec<Vec<Vec<u8>>> {
        let definitions = self.catalog_columns(table);
        let primary_keys =
            definitions.iter().filter(|definition| definition.primary_key).collect::<Vec<_>>();
        let projection = columns
            .iter()
            .map(|column| {
                if let Some(index) = primary_keys
                    .iter()
                    .position(|definition| definition.name.eq_ignore_ascii_case(column))
                {
                    ProjectionSource::PrimaryKey(index)
                } else if column.eq_ignore_ascii_case("key")
                    || column.eq_ignore_ascii_case("pk")
                    || column.eq_ignore_ascii_case("id")
                {
                    ProjectionSource::Key
                } else if !definitions
                    .iter()
                    .any(|definition| definition.name.eq_ignore_ascii_case(column))
                    && (column.eq_ignore_ascii_case("value")
                        || column.eq_ignore_ascii_case("val")
                        || column.eq_ignore_ascii_case("data"))
                {
                    ProjectionSource::RawValue
                } else {
                    ProjectionSource::Json(column.clone())
                }
            })
            .collect::<Vec<_>>();
        let needs_json =
            projection.iter().any(|source| matches!(source, ProjectionSource::Json(_)));
        rows.map(|(pk, value)| {
            Self::project_row(&primary_keys, &projection, needs_json, &pk, &value)
        })
        .collect()
    }

    fn project_row(
        primary_keys: &[&ColumnDefinition],
        projection: &[ProjectionSource],
        needs_json: bool,
        pk: &[u8],
        value: &[u8],
    ) -> Vec<Vec<u8>> {
        let composite_parts = (primary_keys.len() > 1).then(|| decode_key_parts(pk)).flatten();
        let object = needs_json.then(|| projected_json_object(value, projection)).flatten();
        projection
            .iter()
            .map(|source| match source {
                ProjectionSource::PrimaryKey(index) => composite_parts
                    .as_ref()
                    .map(|parts| parts.get(*index).cloned().unwrap_or_default())
                    .unwrap_or_else(|| pk.to_vec()),
                ProjectionSource::Key => pk.to_vec(),
                ProjectionSource::RawValue => value.to_vec(),
                ProjectionSource::Json(column) => {
                    let Some(object) = object.as_ref() else {
                        return value.to_vec();
                    };
                    if let Some((projected, text_result)) = json_projection_value(column, object) {
                        return if projected.is_null() {
                            SQL_NULL_SENTINEL.to_vec()
                        } else if text_result {
                            json_result_bytes(projected)
                        } else {
                            serde_json::to_vec(projected).unwrap_or_default()
                        };
                    }
                    object
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(column))
                        .map(|(_, value)| json_result_bytes_or_null(value))
                        .unwrap_or_default()
                }
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn select_columns_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        aliases: Vec<Option<String>>,
        limit: usize,
        offset: usize,
        order: Order,
        filter: Vec<Predicate>,
    ) -> Result<QueryResult> {
        let exact_key = filter.len() == 1
            && filter[0].column.is_none()
            && filter[0].field == Field::Key
            && filter[0].op == Cmp::Eq;
        let (rows, sort_rows) = if exact_key && self.view_definition(&table)?.is_none() {
            let pk = filter[0].operand.clone();
            let rows = self
                .manager
                .get(txn, &RecordKey::new(&self.tenant, &self.database, &table, &pk))?
                .filter(|value| self.rls_allows(&table, value))
                .map(|value| vec![(pk, value)])
                .unwrap_or_default();
            (rows, false)
        } else {
            let (cap, sort_rows) =
                self.select_scan_options(&table, &order, limit, offset, filter.is_empty())?;
            (self.scan_rows(txn, &table, &filter, cap)?, sort_rows)
        };
        let mut rows = rows;
        if sort_rows {
            rows.sort_by(|left, right| compare_order(&order, left, right));
        }
        let rows = self.project_rows(&table, &columns, rows.into_iter().skip(offset).take(limit));
        let output_columns = columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                aliases.get(index).and_then(|alias| alias.clone()).unwrap_or_else(|| column.clone())
            })
            .collect();
        Ok(QueryResult::Table { columns: output_columns, rows })
    }

    fn scan_all_rows(&self, table: &str) -> Result<Vec<Row>> {
        self.scan_all_rows_with_ts(table).map(|(rows, _)| rows)
    }

    fn scan_all_rows_with_ts(&self, table: &str) -> Result<(Vec<Row>, u64)> {
        const PAGE: usize = 10_000;
        let mut txn = self.begin_with(self.isolation);
        let read_ts = txn.read_ts;
        let mut rows = self.manager.scan(&mut txn, &self.tenant, &self.database, table, PAGE)?;
        let mut budget = MaterializationBudget::default();
        budget.add_rows(&rows)?;
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
            budget.add_rows(&next)?;
            rows.extend(next);
        }
        ensure_materialized_rows(&rows)?;
        Ok((rows, read_ts))
    }

    fn copy_to_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        requested_columns: Vec<String>,
    ) -> Result<QueryResult> {
        let columns = if requested_columns.is_empty() {
            let columns = self
                .catalog_columns(&table)
                .into_iter()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            if columns.is_empty() {
                vec![String::from("key"), String::from("value")]
            } else {
                columns
            }
        } else {
            requested_columns
        };
        if columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("COPY table columns")));
        }
        let source_rows = self.scan_all_rows_in_transaction(txn, &table)?;
        let rows = self.project_rows(
            &table,
            &columns,
            source_rows.into_iter().filter(|(_, value)| self.rls_allows(&table, value)),
        );
        Ok(QueryResult::Table { columns, rows })
    }

    fn create_index(&self, definition: IndexDefinition, if_not_exists: bool) -> Result<()> {
        self.reject_if_read_only()?;
        if !definition.columns.is_empty() {
            let definitions = self.catalog_columns(&definition.table);
            for column in &definition.columns {
                if !definitions.iter().any(|entry| entry.name.eq_ignore_ascii_case(column)) {
                    return Err(RymeError::InvalidArgument(format!(
                        "unknown index column {column}"
                    )));
                }
            }
        }
        {
            let indexes =
                self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
            if indexes.values().flatten().any(|state| state.definition.name == definition.name) {
                if if_not_exists {
                    return Ok(());
                }
                return Err(RymeError::Conflict(String::from("index exists")));
            }
        }
        let (rows, applied_ts) = self.scan_all_rows_with_ts(&definition.table)?;
        let mut tenant_entries: BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>> =
            BTreeMap::new();
        for (pk, value) in rows {
            let Some(indexed) = index_value(&definition, &pk, &value) else { continue };
            let pks = tenant_entries.entry(indexed).or_default();
            if definition.unique && !pks.is_empty() && !pks.contains(&pk) {
                return Err(RymeError::Conflict(format!("unique index {}", definition.name)));
            }
            pks.insert(pk);
        }
        let entries = [(self.tenant.clone(), tenant_entries)].into_iter().collect();
        let mut indexes =
            self.indexes.write().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let table_indexes = indexes.entry(definition.table.clone()).or_default();
        let valid = !self.has_pending_index_commit(&definition.table)
            && self.manager.begin().read_ts == applied_ts;
        table_indexes.push(IndexState { definition, entries, applied_ts, valid });
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn drop_index(&self, name: String, if_exists: bool) -> Result<()> {
        self.reject_if_read_only()?;
        let mut indexes =
            self.indexes.write().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let Some(table) = indexes.iter().find_map(|(table, states)| {
            states.iter().any(|state| state.definition.name == name).then(|| table.clone())
        }) else {
            if if_exists {
                return Ok(());
            }
            return Err(RymeError::NotFound(String::from("index")));
        };
        if let Some(states) = indexes.get_mut(&table) {
            states.retain(|state| state.definition.name != name);
            if states.is_empty() {
                indexes.remove(&table);
            }
        }
        if let Ok(mut constraints) = self.constraints.lock() {
            for entries in constraints.values_mut() {
                entries.retain(|entry| {
                    !matches!(&entry.kind, ConstraintKind::Unique { index_name } if index_name == &name)
                });
            }
            constraints.retain(|_, entries| !entries.is_empty());
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn indexed_candidates(
        &self,
        table: &str,
        filter: &[Predicate],
        read_ts: u64,
    ) -> Result<Option<IndexedCandidates>> {
        self.ensure_index_entries_for_tenant(table)?;
        let has_pending_commit = self.has_pending_index_commit(table);
        let indexes =
            self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let mut matching_indexes = Vec::new();
        for state in indexes.get(table).into_iter().flatten() {
            let key = if state.definition.columns.is_empty() {
                filter
                    .iter()
                    .enumerate()
                    .find(|predicate| {
                        let (_, predicate) = predicate;
                        predicate.op == Cmp::Eq
                            && state.definition.field == predicate.field
                            && state.definition.column == predicate.column
                    })
                    .map(|(index, predicate)| (predicate.operand.clone(), vec![index]))
            } else {
                index_filter_value(&state.definition, filter)
            };
            let Some((key, covered)) = key else { continue };
            if !state.valid || has_pending_commit || read_ts < state.applied_ts {
                return Ok(None);
            }
            let Some(pks) = state.entries.get(&self.tenant).and_then(|entries| entries.get(&key))
            else {
                return Ok(Some(IndexedCandidates {
                    pks: Vec::new(),
                    covered_predicates: covered,
                }));
            };
            matching_indexes.push((pks, covered));
        }
        let Some((smallest_index, smallest)) =
            matching_indexes.iter().enumerate().min_by_key(|(_, (pks, _))| pks.len())
        else {
            return Ok(None);
        };
        if smallest.0.len() > MAX_QUERY_MATERIALIZED_ROWS {
            return Err(materialization_overload());
        }
        let covered_predicates =
            matching_indexes.iter().flat_map(|(_, covered)| covered.iter().copied()).collect();
        if matching_indexes.len() == 1 {
            return Ok(Some(IndexedCandidates {
                pks: smallest.0.iter().cloned().collect(),
                covered_predicates,
            }));
        }
        let mut candidates: Vec<_> = smallest.0.iter().collect();
        for (index, (other, _)) in matching_indexes.iter().enumerate() {
            if index == smallest_index || candidates.is_empty() {
                continue;
            }
            candidates = intersect_index_candidates(candidates, other);
        }
        Ok(Some(IndexedCandidates {
            pks: candidates.into_iter().cloned().collect(),
            covered_predicates,
        }))
    }

    fn indexed_candidates_for_value(
        &self,
        table: &str,
        definition: &IndexDefinition,
        indexed: &[u8],
        read_ts: u64,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        self.ensure_index_entries_for_tenant(table)?;
        let has_pending_commit = self.has_pending_index_commit(table);
        let indexes =
            self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let Some(state) = indexes.get(table).and_then(|states| {
            states.iter().find(|state| state.definition.name == definition.name)
        }) else {
            return Ok(None);
        };
        if !state.valid || has_pending_commit || read_ts < state.applied_ts {
            return Ok(None);
        }
        Ok(Some(
            state
                .entries
                .get(&self.tenant)
                .and_then(|entries| entries.get(indexed))
                .map(|pks| pks.iter().cloned().collect())
                .unwrap_or_default(),
        ))
    }

    fn ensure_index_entries_for_tenant(&self, table: &str) -> Result<()> {
        let needs_rebuild = !self.has_pending_index_commit(table)
            && self
                .indexes
                .read()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?
                .get(table)
                .is_some_and(|states| {
                    states
                        .iter()
                        .any(|state| !state.valid || !state.entries.contains_key(&self.tenant))
                });
        if needs_rebuild {
            self.rebuild_index_entries(table)?;
        }
        Ok(())
    }

    fn check_unique(
        &self,
        txn: &mut Transaction,
        table: &str,
        pk: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.check_unique_excluding(txn, table, pk, value, None)
    }

    fn check_unique_excluding(
        &self,
        txn: &mut Transaction,
        table: &str,
        pk: &[u8],
        value: &[u8],
        excluded_pk: Option<&[u8]>,
    ) -> Result<()> {
        self.ensure_index_entries_for_tenant(table)?;
        let has_pending_commit = self.has_pending_index_commit(table);
        let indexes =
            self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        let Some(table_indexes) = indexes.get(table) else { return Ok(()) };
        if table_indexes.iter().any(|state| state.definition.unique) {
            txn.record_constraint_scan(&self.tenant, &self.database, table)?;
        }
        for state in table_indexes.iter().filter(|state| state.definition.unique) {
            let Some(indexed) = index_value(&state.definition, pk, value) else { continue };
            let conflict = if !state.valid || has_pending_commit || txn.read_ts < state.applied_ts {
                self.scan_all_rows_in_transaction(txn, table)?.iter().any(
                    |(existing_pk, existing_value)| {
                        existing_pk.as_slice() != pk
                            && excluded_pk.is_none_or(|excluded| existing_pk.as_slice() != excluded)
                            && index_value(&state.definition, existing_pk, existing_value)
                                .is_some_and(|current| current == indexed)
                    },
                )
            } else {
                self.unique_index_conflicts_with_transaction(txn, state, &indexed, pk, excluded_pk)
            };
            if conflict {
                return Err(RymeError::Conflict(format!("unique index {}", state.definition.name)));
            }
        }
        Ok(())
    }

    fn unique_index_conflicts_with_transaction(
        &self,
        txn: &Transaction,
        state: &IndexState,
        indexed: &[u8],
        pk: &[u8],
        excluded_pk: Option<&[u8]>,
    ) -> bool {
        let is_other_pk = |candidate: &[u8]| {
            candidate != pk && excluded_pk.is_none_or(|excluded| candidate != excluded)
        };
        if state.entries.get(&self.tenant).and_then(|entries| entries.get(indexed)).is_some_and(
            |pks| {
                pks.iter().any(|existing| {
                    if !is_other_pk(existing) {
                        return false;
                    }
                    let key = RecordKey::new(
                        &self.tenant,
                        &self.database,
                        state.definition.table.as_str(),
                        existing,
                    );
                    match txn.writes().get(&key) {
                        Some(write) => write
                            .value
                            .as_ref()
                            .and_then(|value| index_value(&state.definition, existing, value))
                            .is_some_and(|current| current == indexed),
                        None => true,
                    }
                })
            },
        ) {
            return true;
        }
        txn.writes().iter().any(|(key, write)| {
            key.tenant == self.tenant
                && key.database == self.database
                && key.table == state.definition.table
                && is_other_pk(&key.pk)
                && write
                    .value
                    .as_ref()
                    .and_then(|value| index_value(&state.definition, &key.pk, value))
                    .is_some_and(|current| current == indexed)
        })
    }

    fn has_pending_index_commit(&self, table: &str) -> bool {
        self.pending_index_commits
            .read()
            .map(|pending| pending.get(table).copied().unwrap_or_default() > 0)
            .unwrap_or(true)
    }

    fn begin_index_commit(&self, mut tables: Vec<String>) -> PendingIndexCommit {
        tables.sort_unstable();
        tables.dedup();
        if let Ok(mut pending) = self.pending_index_commits.write() {
            for table in &tables {
                *pending.entry(table.clone()).or_default() += 1;
            }
        }
        PendingIndexCommit(Arc::clone(&self.pending_index_commits), tables)
    }

    fn apply_index_changes(&self, changes: &[TransactionChange], commit_ts: u64) {
        let Ok(mut indexes) = self.indexes.write() else { return };
        let mut changes_by_table: HashMap<&str, Vec<&TransactionChange>> = HashMap::new();
        for change in changes {
            changes_by_table.entry(change.table.as_str()).or_default().push(change);
        }
        for (table, table_changes) in changes_by_table {
            let Some(table_indexes) = indexes.get_mut(table) else { continue };
            for state in table_indexes {
                if !state.valid {
                    continue;
                }
                if commit_ts <= state.applied_ts {
                    state.valid = false;
                    continue;
                }
                if let Some(tenant_entries) = state.entries.get_mut(&self.tenant) {
                    for change in &table_changes {
                        let previous_pk = change.previous_pk.as_deref().unwrap_or(&change.pk);
                        if let Some(before) = change.before.as_ref() {
                            if let Some(indexed) =
                                index_value(&state.definition, previous_pk, before)
                            {
                                if let Some(pks) = tenant_entries.get_mut(&indexed) {
                                    pks.remove(previous_pk);
                                    if pks.is_empty() {
                                        tenant_entries.remove(&indexed);
                                    }
                                }
                            }
                        }
                        if let Some(after) = change.after.as_ref() {
                            if let Some(indexed) = index_value(&state.definition, &change.pk, after)
                            {
                                tenant_entries
                                    .entry(indexed)
                                    .or_default()
                                    .insert(change.pk.clone());
                            }
                        }
                    }
                }
                state.applied_ts = commit_ts;
            }
        }
    }

    async fn scan_rows_async(
        &self,
        txn: &mut Transaction,
        table: &str,
        filter: &[Predicate],
        limit: usize,
    ) -> Result<Vec<Row>> {
        let rls_applies = self.rls_applies_to_role(table);
        if let Some(view) = self.view_definition(table)? {
            if view.materialized && view.rows.len() > MAX_QUERY_MATERIALIZED_ROWS {
                return Err(materialization_overload());
            }
            let mut rows = if view.materialized {
                view.rows
            } else {
                query_result_rows(self.execute_read_in_transaction(txn, view.query)?)
            };
            ensure_materialized_rows(&rows)?;
            rows = self.filter_rls_rows_with_context(table, rows, rls_applies);
            rows.retain(|(pk, value)| predicates_match(filter, pk, value));
            rows.truncate(limit);
            return Ok(rows);
        }
        if limit == 0 {
            let _ = self.manager.scan_async(txn, &self.tenant, &self.database, table, 0).await?;
            return Ok(Vec::new());
        }
        if txn.writes().is_empty() {
            if let Some(indexed) = self.indexed_candidates(table, filter, txn.read_ts)? {
                let candidates = indexed.pks;
                let covered_predicates = indexed.covered_predicates;
                let mut rows = Vec::with_capacity(candidates.len().min(limit));
                if let [pk] = candidates.as_slice() {
                    let key = RecordKey::new(&self.tenant, &self.database, table, pk);
                    if let Some(value) = self.manager.get_async(txn, &key).await? {
                        if self.rls_allows_with_context(table, &value, rls_applies)
                            && predicates_match_except(filter, &covered_predicates, pk, &value)
                        {
                            rows.push((pk.clone(), value));
                        }
                    }
                } else {
                    for candidate_batch in candidates.chunks(INDEX_READ_BATCH_SIZE) {
                        let keys = candidate_batch
                            .iter()
                            .map(|pk| RecordKey::new(&self.tenant, &self.database, table, pk))
                            .collect::<Vec<_>>();
                        let values = self.manager.get_many_async(txn, &keys).await?;
                        let batch_rows = keys
                            .into_iter()
                            .zip(values)
                            .filter_map(|(key, value)| value.map(|value| (key.pk, value)))
                            .collect::<Vec<_>>();
                        ensure_materialized_rows(&batch_rows)?;
                        rows.extend(batch_rows.into_iter().filter(|(pk, value)| {
                            self.rls_allows_with_context(table, value, rls_applies)
                                && predicates_match_except(filter, &covered_predicates, pk, value)
                        }));
                        if rows.len() >= limit {
                            rows.truncate(limit);
                            break;
                        }
                    }
                }
                ensure_materialized_rows(&rows)?;
                return Ok(rows);
            }
        }
        if txn.writes().is_empty() && filter.is_empty() && !rls_applies {
            let rows = self
                .manager
                .scan_async(txn, &self.tenant, &self.database, table, bounded_scan_limit(limit))
                .await?;
            ensure_materialized_rows(&rows)?;
            return Ok(rows);
        }
        if txn.writes().is_empty() && !filter.is_empty() {
            const PAGE: usize = 1024;
            let mut matched = Vec::new();
            let mut budget = MaterializationBudget::default();
            let mut after = None;
            loop {
                let page = match after.as_deref() {
                    Some(after) => {
                        self.manager
                            .scan_after_async(txn, &self.tenant, &self.database, table, after, PAGE)
                            .await?
                    }
                    None => {
                        self.manager
                            .scan_async(txn, &self.tenant, &self.database, table, PAGE)
                            .await?
                    }
                };
                if page.is_empty() {
                    break;
                }
                ensure_materialized_rows(&page)?;
                let page_len = page.len();
                let last = page.last().map(|(pk, _)| pk.clone());
                let matching: Vec<Row> = page
                    .into_iter()
                    .filter(|(pk, value)| {
                        self.rls_allows_with_context(table, value, rls_applies)
                            && predicates_match(filter, pk, value)
                    })
                    .collect();
                budget.add_rows(&matching)?;
                matched.extend(matching);
                if matched.len() >= limit || page_len < PAGE {
                    break;
                }
                after = last;
            }
            matched.truncate(limit);
            return Ok(matched);
        }
        const PAGE: usize = 1024;
        let mut base = BTreeMap::new();
        let mut budget = MaterializationBudget::default();
        let mut after = None;
        loop {
            let page = match after.as_deref() {
                Some(after) => {
                    self.manager
                        .scan_after_async(txn, &self.tenant, &self.database, table, after, PAGE)
                        .await?
                }
                None => {
                    self.manager.scan_async(txn, &self.tenant, &self.database, table, PAGE).await?
                }
            };
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            let last = page.last().map(|(pk, _)| pk.clone());
            budget.add_rows(&page)?;
            base.extend(page);
            if page_len < PAGE {
                break;
            }
            after = last;
        }
        for (key, write) in txn.writes() {
            if key.tenant != self.tenant || key.database != self.database || key.table != table {
                continue;
            }
            match write.value.as_ref() {
                Some(value) if write.expires_at == 0 || write.expires_at > ryme_txn::now_unix() => {
                    budget.add_parts(&key.pk, value)?;
                    base.insert(key.pk.clone(), value.clone());
                }
                _ => {
                    base.remove(&key.pk);
                }
            }
        }
        let rows: Vec<Row> = base.into_iter().collect();
        ensure_materialized_rows(&rows)?;
        Ok(rows
            .into_iter()
            .filter(|(pk, value)| {
                self.rls_allows_with_context(table, value, rls_applies)
                    && predicates_match(filter, pk, value)
            })
            .take(limit)
            .collect())
    }

    fn scan_rows(
        &self,
        txn: &mut Transaction,
        table: &str,
        filter: &[Predicate],
        limit: usize,
    ) -> Result<Vec<Row>> {
        let rls_applies = self.rls_applies_to_role(table);
        if let Some(view) = self.view_definition(table)? {
            if view.materialized && view.rows.len() > MAX_QUERY_MATERIALIZED_ROWS {
                return Err(materialization_overload());
            }
            let mut rows = if view.materialized {
                view.rows
            } else {
                query_result_rows(self.execute_read_in_transaction(txn, view.query)?)
            };
            ensure_materialized_rows(&rows)?;
            rows = self.filter_rls_rows_with_context(table, rows, rls_applies);
            rows.retain(|(pk, value)| predicates_match(filter, pk, value));
            rows.truncate(limit);
            return Ok(rows);
        }
        if rls_applies {
            if limit == 0 {
                return Ok(Vec::new());
            }
            const PAGE: usize = 1024;
            if txn.writes().is_empty() {
                let mut visible = Vec::new();
                let mut budget = MaterializationBudget::default();
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
                    ensure_materialized_rows(&page)?;
                    let page_len = page.len();
                    let last = page.last().map(|(pk, _)| pk.clone());
                    let matching: Vec<Row> = page
                        .into_iter()
                        .filter(|(pk, value)| {
                            self.rls_allows_with_context(table, value, rls_applies)
                                && predicates_match(filter, pk, value)
                        })
                        .collect();
                    budget.add_rows(&matching)?;
                    visible.extend(matching);
                    if visible.len() >= limit || page_len < PAGE {
                        break;
                    }
                    after = last;
                }
                visible.truncate(limit);
                return Ok(visible);
            }

            let mut merged = BTreeMap::new();
            let mut budget = MaterializationBudget::default();
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
                budget.add_rows(&page)?;
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
                        budget.add_parts(&key.pk, value)?;
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            let rows: Vec<Row> = merged.into_iter().collect();
            ensure_materialized_rows(&rows)?;
            return Ok(rows
                .into_iter()
                .filter(|(pk, value)| {
                    self.rls_allows_with_context(table, value, rls_applies)
                        && predicates_match(filter, pk, value)
                })
                .take(limit)
                .collect());
        }
        if txn.writes().is_empty() {
            if let Some(indexed) = self.indexed_candidates(table, filter, txn.read_ts)? {
                let candidates = indexed.pks;
                let covered_predicates = indexed.covered_predicates;
                let mut rows = Vec::with_capacity(candidates.len().min(limit));
                if let [pk] = candidates.as_slice() {
                    let key = RecordKey::new(&self.tenant, &self.database, table, pk);
                    if let Some(value) = self.manager.get(txn, &key)? {
                        if self.rls_allows_with_context(table, &value, rls_applies)
                            && predicates_match_except(filter, &covered_predicates, pk, &value)
                        {
                            rows.push((pk.clone(), value));
                        }
                    }
                } else {
                    for candidate_batch in candidates.chunks(INDEX_READ_BATCH_SIZE) {
                        let keys = candidate_batch
                            .iter()
                            .map(|pk| RecordKey::new(&self.tenant, &self.database, table, pk))
                            .collect::<Vec<_>>();
                        let values = self.manager.get_many(txn, &keys)?;
                        let batch_rows = keys
                            .into_iter()
                            .zip(values)
                            .filter_map(|(key, value)| value.map(|value| (key.pk, value)))
                            .collect::<Vec<_>>();
                        ensure_materialized_rows(&batch_rows)?;
                        rows.extend(batch_rows.into_iter().filter(|(pk, value)| {
                            self.rls_allows_with_context(table, value, rls_applies)
                                && predicates_match_except(filter, &covered_predicates, pk, value)
                        }));
                        if rows.len() >= limit {
                            rows.truncate(limit);
                            break;
                        }
                    }
                }
                ensure_materialized_rows(&rows)?;
                return Ok(rows);
            }

            if !filter.is_empty() {
                const PAGE: usize = 1024;
                let mut matched = Vec::new();
                let mut budget = MaterializationBudget::default();
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
                    ensure_materialized_rows(&page)?;
                    let page_len = page.len();
                    let last = page.last().map(|(pk, _)| pk.clone());
                    let matching: Vec<Row> = page
                        .into_iter()
                        .filter(|(pk, value)| predicates_match(filter, pk, value))
                        .collect();
                    budget.add_rows(&matching)?;
                    matched.extend(matching);
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
            let mut budget = MaterializationBudget::default();
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
                budget.add_rows(&page)?;
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
                        budget.add_parts(&key.pk, value)?;
                        merged.insert(key.pk.clone(), value.clone());
                    }
                    None => {
                        merged.remove(&key.pk);
                    }
                }
            }
            let rows: Vec<Row> = merged.into_iter().collect();
            ensure_materialized_rows(&rows)?;
            return Ok(rows
                .into_iter()
                .filter(|(pk, value)| predicates_match(filter, pk, value))
                .take(limit)
                .collect());
        }
        let mut rows = self.manager.scan(
            txn,
            &self.tenant,
            &self.database,
            table,
            bounded_scan_limit(limit),
        )?;
        ensure_materialized_rows(&rows)?;
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
        let rows = self.filter_rls_rows(table, rows);
        ensure_materialized_rows(&rows)?;
        Ok(rows)
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
                    if let Ok(mut indexes) = self.indexes.write() {
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
                                    columns: Vec::new(),
                                    unique: true,
                                },
                                entries: [(self.tenant.clone(), BTreeMap::new())]
                                    .into_iter()
                                    .collect(),
                                applied_ts: self.manager.begin().read_ts,
                                valid: !self.has_pending_index_commit(&table_name),
                            });
                        }
                    }
                }
                self.schema_dirty.store(true, Ordering::SeqCst);
            }
        }
    }

    fn normalize_foreign_key(
        &self,
        table: &str,
        mut constraint: ForeignKeyConstraint,
    ) -> Result<ForeignKeyConstraint> {
        let table_columns = self.catalog_columns(table);
        if constraint.columns.is_empty()
            || constraint.columns.iter().any(|column| {
                !table_columns.iter().any(|definition| definition.name.eq_ignore_ascii_case(column))
            })
        {
            return Err(RymeError::InvalidArgument(String::from("unknown foreign key column")));
        }
        constraint.referenced_table =
            self.canonical_table_name(&constraint.referenced_table).ok_or_else(|| {
                RymeError::InvalidArgument(format!(
                    "unknown referenced table {}",
                    constraint.referenced_table
                ))
            })?;
        let referenced_definitions = self.catalog_columns(&constraint.referenced_table);
        if constraint.referenced_columns.is_empty() {
            constraint.referenced_columns = referenced_definitions
                .iter()
                .filter(|definition| definition.primary_key)
                .map(|definition| definition.name.clone())
                .collect();
        }
        if constraint.referenced_columns.len() != constraint.columns.len()
            || constraint.referenced_columns.iter().any(|column| {
                !referenced_definitions
                    .iter()
                    .any(|definition| definition.name.eq_ignore_ascii_case(column))
            })
        {
            return Err(RymeError::InvalidArgument(String::from("unknown referenced column")));
        }
        let references_primary =
            referenced_columns_are_primary(&constraint.referenced_columns, &referenced_definitions);
        let references_unique_column = constraint.referenced_columns.len() == 1
            && referenced_definitions.iter().any(|definition| {
                definition.name.eq_ignore_ascii_case(&constraint.referenced_columns[0])
                    && definition.unique
            });
        let references_unique_index =
            self.catalog_indexes(&constraint.referenced_table).iter().any(|index| {
                if !index.unique {
                    return false;
                }
                if constraint.referenced_columns.len() == 1 && index.columns.is_empty() {
                    return index.column.as_deref().is_some_and(|indexed| {
                        indexed.eq_ignore_ascii_case(&constraint.referenced_columns[0])
                    });
                }
                index.columns.len() == constraint.referenced_columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(&constraint.referenced_columns)
                        .all(|(indexed, referenced)| indexed.eq_ignore_ascii_case(referenced))
            });
        if !references_primary && !references_unique_column && !references_unique_index {
            return Err(RymeError::InvalidArgument(String::from(
                "referenced columns are not unique",
            )));
        }
        Ok(constraint)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_table(
        &self,
        table: String,
        columns: Vec<ColumnDefinition>,
        unique_constraints: Vec<Vec<String>>,
        checks: Vec<String>,
        foreign_keys: Vec<ForeignKeyConstraint>,
        named_constraints: Vec<TableConstraint>,
        if_not_exists: bool,
    ) -> Result<()> {
        let exists = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .contains_key(&table);
        if exists {
            if if_not_exists {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("table exists")));
        }
        self.register_table(table.clone(), columns);
        if !checks.is_empty() {
            self.checks
                .lock()
                .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?
                .insert(table.clone(), checks);
        }
        if !foreign_keys.is_empty() {
            let normalized = foreign_keys
                .into_iter()
                .map(|constraint| self.normalize_foreign_key(&table, constraint))
                .collect::<Result<Vec<_>>>();
            let normalized = match normalized {
                Ok(normalized) => normalized,
                Err(error) => {
                    if let Ok(mut catalog) = self.catalog.lock() {
                        catalog.remove(&table);
                    }
                    if let Ok(mut indexes) = self.indexes.write() {
                        indexes.remove(&table);
                    }
                    if let Ok(mut checks) = self.checks.lock() {
                        checks.remove(&table);
                    }
                    self.schema_dirty.store(true, Ordering::SeqCst);
                    return Err(error);
                }
            };
            self.foreign_keys
                .lock()
                .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
                .insert(table.clone(), normalized);
        }
        for columns in unique_constraints {
            if columns.len() < 2 {
                continue;
            }
            let name = format!("{}_{}_unique", table, columns.join("_"));
            self.create_index(
                IndexDefinition {
                    name,
                    table: table.clone(),
                    field: Field::Value,
                    column: None,
                    columns,
                    unique: true,
                },
                false,
            )?;
        }
        for constraint in named_constraints {
            let (TableConstraint::PrimaryKey { name: Some(name), columns }
            | TableConstraint::Unique { name: Some(name), columns }) = &constraint
            else {
                if let TableConstraint::Check { name: Some(name), expression } = &constraint {
                    self.remember_constraint(
                        &table,
                        ConstraintMetadata {
                            name: name.clone(),
                            kind: ConstraintKind::Check { expression: expression.clone() },
                        },
                    )?;
                } else if let TableConstraint::ForeignKey { name: Some(name), constraint } =
                    &constraint
                {
                    let normalized = self.normalize_foreign_key(&table, constraint.clone())?;
                    self.remember_constraint(
                        &table,
                        ConstraintMetadata {
                            name: name.clone(),
                            kind: ConstraintKind::ForeignKey { constraint: normalized },
                        },
                    )?;
                }
                continue;
            };
            let kind = match &constraint {
                TableConstraint::PrimaryKey { .. } => {
                    ConstraintKind::PrimaryKey { columns: columns.clone() }
                }
                TableConstraint::Unique { .. } => {
                    let index_name = self
                        .catalog_indexes(&table)
                        .into_iter()
                        .find(|index| {
                            index.unique
                                && if columns.len() == 1 && index.columns.is_empty() {
                                    index.column.as_deref().is_some_and(|column| {
                                        column.eq_ignore_ascii_case(&columns[0])
                                    })
                                } else {
                                    index.columns.len() == columns.len()
                                        && index
                                            .columns
                                            .iter()
                                            .zip(columns)
                                            .all(|(left, right)| left.eq_ignore_ascii_case(right))
                                }
                        })
                        .map(|index| index.name)
                        .ok_or_else(|| RymeError::NotFound(String::from("unique constraint")))?;
                    ConstraintKind::Unique { index_name }
                }
                _ => unreachable!("handled named constraint branch"),
            };
            self.remember_constraint(&table, ConstraintMetadata { name: name.clone(), kind })?;
        }
        self.apply_default_privileges_for("TABLE", &table)?;
        Ok(())
    }

    fn add_unique_constraint(
        &self,
        table: String,
        name: Option<String>,
        columns: Vec<String>,
    ) -> Result<()> {
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("unique constraint")));
        }
        let index_name = name.unwrap_or_else(|| format!("{}_{}_unique", table, columns.join("_")));
        let (field, column, index_columns) = if columns.len() == 1 {
            (Field::Value, Some(columns[0].clone()), Vec::new())
        } else {
            (Field::Value, None, columns)
        };
        self.create_index(
            IndexDefinition {
                name: index_name,
                table,
                field,
                column,
                columns: index_columns,
                unique: true,
            },
            false,
        )
    }

    async fn add_primary_key_constraint(&self, table: String, columns: Vec<String>) -> Result<()> {
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if columns.is_empty()
            || columns.iter().any(|column| {
                columns.iter().filter(|other| other.eq_ignore_ascii_case(column)).count() > 1
            })
        {
            return Err(RymeError::InvalidArgument(String::from("primary key constraint")));
        }
        let definitions = self.catalog_columns(&table);
        if definitions.is_empty() {
            return Err(RymeError::NotFound(String::from("table columns")));
        }
        if definitions.iter().any(|definition| definition.primary_key) {
            return Err(RymeError::Conflict(String::from("primary key already exists")));
        }
        let primary_definitions = columns
            .iter()
            .map(|column| {
                definitions
                    .iter()
                    .find(|definition| definition.name.eq_ignore_ascii_case(column))
                    .cloned()
                    .ok_or_else(|| {
                        RymeError::InvalidArgument(format!("unknown primary key column {column}"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let rows = self.scan_all_rows(&table)?;
        let mut rekeyed = Vec::with_capacity(rows.len());
        let mut new_keys = BTreeSet::new();
        for (old_pk, value) in rows {
            let serde_json::Value::Object(object) = serde_json::from_slice(&value)
                .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
            else {
                return Err(RymeError::InvalidArgument(String::from("schema row")));
            };
            let parts = primary_definitions
                .iter()
                .map(|definition| {
                    let selected = object
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(&definition.name))
                        .map(|(_, selected)| selected)
                        .filter(|selected| !selected.is_null())
                        .ok_or_else(|| {
                            RymeError::InvalidArgument(format!(
                                "null value in column {} violates not-null constraint",
                                definition.name
                            ))
                        })?;
                    let part = json_result_bytes(selected);
                    if part.is_empty() {
                        return Err(RymeError::InvalidArgument(String::from(
                            "null value in primary key",
                        )));
                    }
                    Ok(part)
                })
                .collect::<Result<Vec<_>>>()?;
            let new_pk = if parts.len() == 1 {
                parts.into_iter().next().unwrap_or_default()
            } else {
                encode_key_parts(&parts).ok_or_else(|| {
                    RymeError::InvalidArgument(String::from("null value in primary key"))
                })?
            };
            if !new_keys.insert(new_pk.clone()) {
                return Err(RymeError::Conflict(String::from("duplicate primary key")));
            }
            rekeyed.push((old_pk, new_pk, value));
        }

        if rekeyed.iter().any(|(old_pk, new_pk, _)| old_pk != new_pk) {
            let mut txn = self.begin_with(self.isolation);
            for (old_pk, new_pk, _) in &rekeyed {
                if old_pk != new_pk {
                    self.manager.delete(
                        &mut txn,
                        RecordKey::new(&self.tenant, &self.database, &table, old_pk),
                    );
                }
            }
            for (_, new_pk, value) in &rekeyed {
                self.manager.put(
                    &mut txn,
                    RecordKey::new(&self.tenant, &self.database, &table, new_pk),
                    value.clone(),
                );
            }
            self.manager.commit(txn).await?;
        }

        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            let definitions = catalog
                .get_mut(&table)
                .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
            for definition in definitions {
                if columns.iter().any(|column| column.eq_ignore_ascii_case(&definition.name)) {
                    definition.primary_key = true;
                    definition.nullable = false;
                }
            }
        }
        self.rebuild_index_entries(&table)?;
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn rebuild_index_entries(&self, table: &str) -> Result<()> {
        let (rows, applied_ts) = self.scan_all_rows_with_ts(table)?;
        let definitions = {
            let indexes =
                self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
            indexes
                .get(table)
                .map(|states| {
                    states.iter().map(|state| state.definition.clone()).collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        let mut entries = definitions
            .iter()
            .map(|definition| (definition.name.clone(), BTreeMap::new()))
            .collect::<HashMap<_, BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>>>();
        for (pk, value) in rows {
            for definition in &definitions {
                let Some(indexed) = index_value(definition, &pk, &value) else { continue };
                entries
                    .get_mut(&definition.name)
                    .expect("index definition was initialized")
                    .entry(indexed)
                    .or_default()
                    .insert(pk.clone());
            }
        }
        let mut indexes =
            self.indexes.write().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        if let Some(states) = indexes.get_mut(table) {
            for state in states {
                state.entries.insert(
                    self.tenant.clone(),
                    entries.remove(&state.definition.name).unwrap_or_default(),
                );
                state.applied_ts = applied_ts;
                state.valid = !self.has_pending_index_commit(table)
                    && self.manager.begin().read_ts == applied_ts;
            }
        }
        Ok(())
    }

    fn add_check_constraint(&self, table: String, expression: String) -> Result<()> {
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if expression.trim().is_empty() {
            return Err(RymeError::InvalidArgument(String::from("check constraint")));
        }
        parse_filter(&tokenize(&expression))?;
        {
            self.checks
                .lock()
                .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?
                .entry(table.clone())
                .or_default()
                .push(expression.clone());
        }
        let result = self
            .scan_all_rows(&table)?
            .into_iter()
            .try_for_each(|(pk, value)| self.enforce_checks(&table, &pk, &value));
        if let Err(error) = result {
            if let Ok(mut checks) = self.checks.lock() {
                if let Some(expressions) = checks.get_mut(&table) {
                    if expressions.last().is_some_and(|last| last == &expression) {
                        expressions.pop();
                    }
                    if expressions.is_empty() {
                        checks.remove(&table);
                    }
                }
            }
            return Err(error);
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn add_foreign_key_constraint(
        &self,
        table: String,
        constraint: ForeignKeyConstraint,
    ) -> Result<()> {
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        let constraint = self.normalize_foreign_key(&table, constraint)?;
        {
            self.foreign_keys
                .lock()
                .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
                .entry(table.clone())
                .or_default()
                .push(constraint.clone());
        }
        let mut txn = self.begin_with(self.isolation);
        let result = self
            .scan_all_rows_in_transaction(&mut txn, &table)?
            .into_iter()
            .try_for_each(|(pk, value)| self.enforce_foreign_keys(&mut txn, &table, &pk, &value));
        if let Err(error) = result {
            if let Ok(mut foreign_keys) = self.foreign_keys.lock() {
                if let Some(constraints) = foreign_keys.get_mut(&table) {
                    if constraints.last().is_some_and(|last| last == &constraint) {
                        constraints.pop();
                    }
                    if constraints.is_empty() {
                        foreign_keys.remove(&table);
                    }
                }
            }
            return Err(error);
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn add_table_constraint(&self, table: String, constraint: TableConstraint) -> Result<()> {
        self.reject_if_read_only()?;
        let table_name = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        match constraint {
            TableConstraint::PrimaryKey { name, columns } => {
                self.add_primary_key_constraint(table, columns.clone()).await?;
                let name = name.unwrap_or_else(|| format!("{table_name}_pkey"));
                self.remember_constraint(
                    &table_name,
                    ConstraintMetadata { name, kind: ConstraintKind::PrimaryKey { columns } },
                )
            }
            TableConstraint::Unique { name, columns } => {
                let index_name = name
                    .clone()
                    .unwrap_or_else(|| format!("{table_name}_{}_unique", columns.join("_")));
                self.add_unique_constraint(table, name, columns.clone())?;
                self.remember_constraint(
                    &table_name,
                    ConstraintMetadata {
                        name: index_name.clone(),
                        kind: ConstraintKind::Unique { index_name },
                    },
                )
            }
            TableConstraint::Check { name, expression } => {
                self.add_check_constraint(table, expression.clone())?;
                if let Some(name) = name {
                    self.remember_constraint(
                        &table_name,
                        ConstraintMetadata { name, kind: ConstraintKind::Check { expression } },
                    )?;
                }
                Ok(())
            }
            TableConstraint::ForeignKey { name, constraint } => {
                self.add_foreign_key_constraint(table, constraint.clone())?;
                if let Some(name) = name {
                    self.remember_constraint(
                        &table_name,
                        ConstraintMetadata {
                            name,
                            kind: ConstraintKind::ForeignKey { constraint },
                        },
                    )?;
                }
                Ok(())
            }
        }
    }

    fn remember_constraint(&self, table: &str, metadata: ConstraintMetadata) -> Result<()> {
        let mut constraints = self
            .constraints
            .lock()
            .map_err(|_| RymeError::Internal(String::from("constraint lock")))?;
        let entries = constraints.entry(table.to_string()).or_default();
        if entries.iter().any(|entry| entry.name.eq_ignore_ascii_case(&metadata.name)) {
            return Err(RymeError::Conflict(String::from("constraint exists")));
        }
        entries.push(metadata);
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn find_constraint(&self, table: &str, name: &str) -> Option<ConstraintMetadata> {
        self.constraints.lock().ok().and_then(|constraints| {
            constraints.get(table).and_then(|entries| {
                entries.iter().find(|entry| entry.name.eq_ignore_ascii_case(name)).cloned()
            })
        })
    }

    fn forget_constraint(&self, table: &str, name: &str) {
        if let Ok(mut constraints) = self.constraints.lock() {
            if let Some(entries) = constraints.get_mut(table) {
                entries.retain(|entry| !entry.name.eq_ignore_ascii_case(name));
                if entries.is_empty() {
                    constraints.remove(table);
                }
            }
        }
    }

    fn drop_constraint(&self, table: String, name: String, if_exists: bool) -> Result<()> {
        self.reject_if_read_only()?;
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        let metadata = self.find_constraint(&table, &name);
        if let Some(metadata) = metadata {
            match &metadata.kind {
                ConstraintKind::Unique { index_name } => {
                    self.drop_index(index_name.clone(), if_exists)?;
                }
                ConstraintKind::Check { expression } => {
                    let mut checks = self
                        .checks
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?;
                    let Some(expressions) = checks.get_mut(&table) else {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(RymeError::NotFound(String::from("constraint")));
                    };
                    let Some(position) =
                        expressions.iter().position(|candidate| candidate == expression)
                    else {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(RymeError::NotFound(String::from("constraint")));
                    };
                    expressions.remove(position);
                    if expressions.is_empty() {
                        checks.remove(&table);
                    }
                    self.schema_dirty.store(true, Ordering::SeqCst);
                }
                ConstraintKind::ForeignKey { constraint } => {
                    let mut foreign_keys = self
                        .foreign_keys
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?;
                    let Some(entries) = foreign_keys.get_mut(&table) else {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(RymeError::NotFound(String::from("constraint")));
                    };
                    let Some(position) =
                        entries.iter().position(|candidate| candidate == constraint)
                    else {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(RymeError::NotFound(String::from("constraint")));
                    };
                    entries.remove(position);
                    if entries.is_empty() {
                        foreign_keys.remove(&table);
                    }
                    self.schema_dirty.store(true, Ordering::SeqCst);
                }
                ConstraintKind::PrimaryKey { columns } => {
                    if columns.len() != 1 {
                        return Err(RymeError::InvalidArgument(String::from(
                            "dropping a composite primary key is not supported",
                        )));
                    }
                    let mut catalog = self
                        .catalog
                        .lock()
                        .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
                    let definitions = catalog
                        .get_mut(&table)
                        .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
                    let Some(definition) = definitions
                        .iter_mut()
                        .find(|definition| definition.name.eq_ignore_ascii_case(&columns[0]))
                    else {
                        if if_exists {
                            return Ok(());
                        }
                        return Err(RymeError::NotFound(String::from("constraint")));
                    };
                    definition.primary_key = false;
                    self.schema_dirty.store(true, Ordering::SeqCst);
                }
            }
            self.forget_constraint(&table, &metadata.name);
            return Ok(());
        }

        let index_name = self
            .catalog_indexes(&table)
            .into_iter()
            .find(|index| index.name.eq_ignore_ascii_case(&name))
            .map(|index| index.name);
        if let Some(index_name) = index_name {
            self.drop_index(index_name, false)?;
            return Ok(());
        }
        if if_exists {
            Ok(())
        } else {
            Err(RymeError::NotFound(String::from("constraint")))
        }
    }

    async fn drop_table(&self, table: String, if_exists: bool) -> Result<()> {
        self.reject_if_read_only()?;
        let present = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .contains_key(&table);
        if !present {
            if if_exists {
                return Ok(());
            }
            return Err(RymeError::NotFound(String::from("table")));
        }
        if let Some(dependent) = self
            .views
            .lock()
            .map_err(|_| RymeError::Internal(String::from("view lock")))?
            .values()
            .find(|view| {
                view.dependencies.iter().any(|dependency| same_relation(dependency, &table))
            })
        {
            return Err(RymeError::Conflict(format!(
                "table {} is referenced by view {}",
                table, dependent.name
            )));
        }
        if self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .iter()
            .any(|(child_table, constraints)| {
                child_table != &table
                    && constraints
                        .iter()
                        .any(|constraint| constraint.referenced_table.eq_ignore_ascii_case(&table))
            })
        {
            return Err(RymeError::Conflict(String::from("table is referenced by a foreign key")));
        }

        let rows = self.scan_all_rows(&table)?;
        if !rows.is_empty() {
            let mut txn = self.begin_with(self.isolation);
            for (pk, _) in &rows {
                self.manager
                    .delete(&mut txn, RecordKey::new(&self.tenant, &self.database, &table, pk));
            }
            self.manager.commit(txn).await?;
        }
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            catalog.remove(&table);
        }
        {
            let mut indexes = self
                .indexes
                .write()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?;
            indexes.remove(&table);
        }
        if let Ok(mut checks) = self.checks.lock() {
            checks.remove(&table);
        }
        if let Ok(mut foreign_keys) = self.foreign_keys.lock() {
            foreign_keys.remove(&table);
        }
        if let Ok(mut constraints) = self.constraints.lock() {
            constraints.remove(&table);
        }
        if let Ok(mut triggers) = self.triggers.lock() {
            triggers.retain(|_, trigger| !trigger.table.eq_ignore_ascii_case(&table));
        }
        let prefix = format!("{}\0{}\0{}\0", self.tenant, self.database, table);
        if let Ok(mut sequences) = self.sequence_next.lock() {
            sequences.retain(|key, _| !key.starts_with(&prefix));
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn truncate_table(
        &self,
        table: String,
        restart_identity: bool,
        cascade: bool,
    ) -> Result<()> {
        self.reject_if_read_only()?;
        let table = self
            .canonical_table_name(&table)
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        let foreign_keys = self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .clone();
        let mut tables = vec![table.clone()];
        if cascade {
            let mut position = 0;
            while position < tables.len() {
                let parent = tables[position].clone();
                for (child_table, constraints) in &foreign_keys {
                    if child_table.eq_ignore_ascii_case(&parent)
                        || tables.iter().any(|table| table.eq_ignore_ascii_case(child_table))
                        || !constraints.iter().any(|constraint| {
                            constraint.referenced_table.eq_ignore_ascii_case(&parent)
                        })
                    {
                        continue;
                    }
                    tables.push(child_table.clone());
                }
                position += 1;
            }
        } else if foreign_keys.iter().any(|(child_table, constraints)| {
            !child_table.eq_ignore_ascii_case(&table)
                && constraints
                    .iter()
                    .any(|constraint| constraint.referenced_table.eq_ignore_ascii_case(&table))
        }) {
            return Err(RymeError::Conflict(String::from("table is referenced by a foreign key")));
        }

        let mut rows_by_table = Vec::with_capacity(tables.len());
        let mut total_rows = 0;
        for table in &tables {
            let rows = self.scan_all_rows(table)?;
            total_rows += rows.len();
            rows_by_table.push((table, rows));
        }
        let _pending_index_commit =
            (total_rows > 0).then(|| self.begin_index_commit(tables.clone()));
        let mut truncate_commit_ts = None;
        if total_rows > 0 {
            let mut txn = self.begin_with(self.isolation);
            for (table, rows) in &rows_by_table {
                for (pk, _) in rows {
                    self.manager
                        .delete(&mut txn, RecordKey::new(&self.tenant, &self.database, table, pk));
                }
            }
            truncate_commit_ts = Some(self.manager.commit(txn).await?);
        }
        {
            let mut indexes = self
                .indexes
                .write()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?;
            for table in &tables {
                if let Some(table_indexes) = indexes.get_mut(table) {
                    for state in table_indexes {
                        state.entries.remove(&self.tenant);
                        if let Some(commit_ts) = truncate_commit_ts {
                            state.applied_ts = commit_ts;
                            state.valid = false;
                        }
                    }
                }
            }
        }
        if restart_identity {
            let prefixes = tables
                .iter()
                .map(|table| format!("{}\0{}\0{}\0", self.tenant, self.database, table))
                .collect::<Vec<_>>();
            let mut sequences = self
                .sequence_next
                .lock()
                .map_err(|_| RymeError::Internal(String::from("sequence lock")))?;
            sequences.retain(|key, _| !prefixes.iter().any(|prefix| key.starts_with(prefix)));
        }
        Ok(())
    }

    async fn alter_table_add_column(
        &self,
        table: String,
        column: ColumnDefinition,
        if_not_exists: bool,
    ) -> Result<()> {
        self.reject_if_read_only()?;
        if column.primary_key {
            return Err(RymeError::InvalidArgument(String::from(
                "adding a primary key column is not supported",
            )));
        }
        let existing = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .get(&table)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if existing.iter().any(|definition| definition.name.eq_ignore_ascii_case(&column.name)) {
            if if_not_exists {
                return Ok(());
            }
            return Err(RymeError::Conflict(String::from("column")));
        }

        let (rows, rows_read_ts) = self.scan_all_rows_with_ts(&table)?;
        let index_definition = IndexDefinition {
            name: format!("{}_{}_unique", table, column.name),
            table: table.clone(),
            field: Field::Value,
            column: Some(column.name.clone()),
            columns: Vec::new(),
            unique: true,
        };
        let mut index_entries: BTreeMap<Vec<u8>, std::collections::BTreeSet<Vec<u8>>> =
            BTreeMap::new();
        let mut next_sequence = 1u64;
        let mut updates = Vec::with_capacity(rows.len());
        for (pk, before) in rows {
            let serde_json::Value::Object(mut object) = serde_json::from_slice(&before)
                .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
            else {
                return Err(RymeError::InvalidArgument(String::from("schema row")));
            };
            let resolved = if column.auto_increment {
                let value = next_sequence.to_string().into_bytes();
                next_sequence = next_sequence.saturating_add(1);
                Some(value)
            } else if let Some(default) = column.column_default.as_deref() {
                eval_default(default)?
            } else if column.nullable {
                None
            } else {
                return Err(RymeError::InvalidArgument(format!(
                    "column {} contains null values",
                    column.name
                )));
            };
            object.insert(column.name.clone(), json_insert_value(resolved, &column.data_type));
            let after = serde_json::to_vec(&serde_json::Value::Object(object))
                .map_err(|error| RymeError::Internal(error.to_string()))?;
            if column.unique {
                if let Some(indexed) = index_value(&index_definition, &pk, &after) {
                    let pks = index_entries.entry(indexed).or_default();
                    if !pks.is_empty() && !pks.contains(&pk) {
                        return Err(RymeError::Conflict(format!(
                            "unique index {}",
                            index_definition.name
                        )));
                    }
                    pks.insert(pk.clone());
                }
            }
            updates.push((pk, before, after));
        }

        let mut applied_ts = rows_read_ts;
        if !updates.is_empty() {
            let mut txn = self.begin_with(self.isolation);
            for (pk, _, after) in &updates {
                self.manager.put(
                    &mut txn,
                    RecordKey::new(&self.tenant, &self.database, &table, pk),
                    after.clone(),
                );
            }
            applied_ts = self.manager.commit(txn).await?;
        }
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            let definitions = catalog
                .get_mut(&table)
                .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
            definitions.push(column.clone());
        }
        if column.unique {
            let mut indexes = self
                .indexes
                .write()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?;
            let table_indexes = indexes.entry(table.clone()).or_default();
            if !table_indexes.iter().any(|state| state.definition.name == index_definition.name) {
                table_indexes.push(IndexState {
                    definition: index_definition,
                    entries: [(self.tenant.clone(), index_entries)].into_iter().collect(),
                    applied_ts,
                    valid: !self.has_pending_index_commit(&table)
                        && self.manager.begin().read_ts == applied_ts,
                });
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn alter_table_drop_column(
        &self,
        table: String,
        column: String,
        if_exists: bool,
    ) -> Result<()> {
        self.reject_if_read_only()?;
        let existing = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .get(&table)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        let dropped = existing
            .iter()
            .find(|definition| definition.name.eq_ignore_ascii_case(&column))
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("column")));
        let Ok(dropped) = dropped else {
            if if_exists {
                return Ok(());
            }
            return Err(RymeError::NotFound(String::from("column")));
        };
        if dropped.primary_key {
            return Err(RymeError::InvalidArgument(String::from(
                "dropping a primary key column is not supported",
            )));
        }
        if self
            .checks
            .lock()
            .map_err(|_| RymeError::Internal(String::from("check constraint lock")))?
            .get(&table)
            .is_some_and(|checks| {
                checks.iter().any(|check| check_references_column(check, &column))
            })
        {
            return Err(RymeError::InvalidArgument(String::from(
                "dropping a column referenced by a check constraint is not supported",
            )));
        }
        if self
            .foreign_keys
            .lock()
            .map_err(|_| RymeError::Internal(String::from("foreign key lock")))?
            .iter()
            .any(|(child_table, constraints)| {
                constraints.iter().any(|constraint| {
                    (child_table.eq_ignore_ascii_case(&table)
                        && constraint.columns.iter().any(|name| name.eq_ignore_ascii_case(&column)))
                        || (constraint.referenced_table.eq_ignore_ascii_case(&table)
                            && constraint
                                .referenced_columns
                                .iter()
                                .any(|name| name.eq_ignore_ascii_case(&column)))
                })
            })
        {
            return Err(RymeError::InvalidArgument(String::from(
                "dropping a column referenced by a foreign key is not supported",
            )));
        }

        let rows = self.scan_all_rows(&table)?;
        let mut updates = Vec::with_capacity(rows.len());
        for (pk, before) in rows {
            let serde_json::Value::Object(mut object) = serde_json::from_slice(&before)
                .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
            else {
                return Err(RymeError::InvalidArgument(String::from("schema row")));
            };
            if let Some(actual_name) =
                object.keys().find(|name| name.eq_ignore_ascii_case(&column)).cloned()
            {
                object.remove(&actual_name);
            }
            let after = serde_json::to_vec(&serde_json::Value::Object(object))
                .map_err(|error| RymeError::Internal(error.to_string()))?;
            updates.push((pk, before, after));
        }

        if !updates.is_empty() {
            let mut txn = self.begin_with(self.isolation);
            for (pk, _, after) in &updates {
                self.manager.put(
                    &mut txn,
                    RecordKey::new(&self.tenant, &self.database, &table, pk),
                    after.clone(),
                );
            }
            self.manager.commit(txn).await?;
        }
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            let definitions = catalog
                .get_mut(&table)
                .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
            definitions.retain(|definition| !definition.name.eq_ignore_ascii_case(&column));
        }
        {
            let mut indexes = self
                .indexes
                .write()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?;
            if let Some(table_indexes) = indexes.get_mut(&table) {
                let column_prefix = format!("{}->", column.to_ascii_lowercase());
                table_indexes.retain(|state| {
                    let in_composite = state.definition.columns.iter().any(|indexed| {
                        indexed.eq_ignore_ascii_case(&column)
                            || indexed.to_ascii_lowercase().starts_with(&column_prefix)
                    });
                    !in_composite
                        && state.definition.column.as_deref().is_none_or(|indexed| {
                            let indexed = indexed.to_ascii_lowercase();
                            indexed != column.to_ascii_lowercase()
                                && !indexed.starts_with(&column_prefix)
                        })
                });
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn alter_table_rename_column(
        &self,
        table: String,
        from: String,
        to: String,
    ) -> Result<()> {
        self.reject_if_read_only()?;
        if from.eq_ignore_ascii_case(&to) {
            return Err(RymeError::InvalidArgument(String::from("column rename")));
        }
        let existing = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .get(&table)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if !existing.iter().any(|definition| definition.name.eq_ignore_ascii_case(&from)) {
            return Err(RymeError::NotFound(String::from("column")));
        }
        if existing.iter().any(|definition| definition.name.eq_ignore_ascii_case(&to)) {
            return Err(RymeError::Conflict(String::from("column")));
        }

        let (rows, rows_read_ts) = self.scan_all_rows_with_ts(&table)?;
        let mut updates = Vec::with_capacity(rows.len());
        for (pk, before) in rows {
            let serde_json::Value::Object(mut object) = serde_json::from_slice(&before)
                .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
            else {
                return Err(RymeError::InvalidArgument(String::from("schema row")));
            };
            if let Some(actual_name) =
                object.keys().find(|name| name.eq_ignore_ascii_case(&from)).cloned()
            {
                if let Some(value) = object.remove(&actual_name) {
                    object.insert(to.clone(), value);
                }
            }
            let after = serde_json::to_vec(&serde_json::Value::Object(object))
                .map_err(|error| RymeError::Internal(error.to_string()))?;
            updates.push((pk, before, after));
        }

        let mut applied_ts = rows_read_ts;
        if !updates.is_empty() {
            let mut txn = self.begin_with(self.isolation);
            for (pk, _, after) in &updates {
                self.manager.put(
                    &mut txn,
                    RecordKey::new(&self.tenant, &self.database, &table, pk),
                    after.clone(),
                );
            }
            applied_ts = self.manager.commit(txn).await?;
        }
        {
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            let definitions = catalog
                .get_mut(&table)
                .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
            if let Some(definition) = definitions
                .iter_mut()
                .find(|definition| definition.name.eq_ignore_ascii_case(&from))
            {
                definition.name = to.clone();
            }
        }
        {
            let mut indexes = self
                .indexes
                .write()
                .map_err(|_| RymeError::Internal(String::from("index lock")))?;
            if let Some(table_indexes) = indexes.get_mut(&table) {
                for state in table_indexes {
                    if !state.definition.columns.is_empty() {
                        let renamed = state
                            .definition
                            .columns
                            .iter()
                            .map(|indexed| rename_index_column(indexed, &from, &to))
                            .collect::<Vec<_>>();
                        if renamed != state.definition.columns {
                            state.definition.columns = renamed;
                            let mut entries = BTreeMap::new();
                            for (pk, _, after) in &updates {
                                if let Some(value) = index_value(&state.definition, pk, after) {
                                    entries
                                        .entry(value)
                                        .or_insert_with(std::collections::BTreeSet::new)
                                        .insert(pk.clone());
                                }
                            }
                            state.entries.insert(self.tenant.clone(), entries);
                            state.applied_ts = applied_ts;
                        }
                    }
                    if let Some(indexed) = state.definition.column.as_deref() {
                        let renamed = rename_index_column(indexed, &from, &to);
                        if renamed != indexed {
                            state.definition.column = Some(renamed);
                            let mut entries = BTreeMap::new();
                            for (pk, _, after) in &updates {
                                if let Some(value) = index_value(&state.definition, pk, after) {
                                    entries
                                        .entry(value)
                                        .or_insert_with(std::collections::BTreeSet::new)
                                        .insert(pk.clone());
                                }
                            }
                            state.entries.insert(self.tenant.clone(), entries);
                            state.applied_ts = applied_ts;
                        }
                    }
                }
            }
        }
        if let Ok(mut checks) = self.checks.lock() {
            if let Some(expressions) = checks.get_mut(&table) {
                for expression in expressions {
                    *expression = rename_check_column(expression, &from, &to);
                }
            }
        }
        if let Ok(mut foreign_keys) = self.foreign_keys.lock() {
            for (child_table, constraints) in foreign_keys.iter_mut() {
                for constraint in constraints {
                    if child_table.eq_ignore_ascii_case(&table) {
                        for name in &mut constraint.columns {
                            if name.eq_ignore_ascii_case(&from) {
                                *name = to.clone();
                            }
                        }
                    }
                    if constraint.referenced_table.eq_ignore_ascii_case(&table) {
                        for name in &mut constraint.referenced_columns {
                            if name.eq_ignore_ascii_case(&from) {
                                *name = to.clone();
                            }
                        }
                    }
                }
            }
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn alter_table_column(
        &self,
        table: String,
        column: String,
        alteration: ColumnAlteration,
    ) -> Result<()> {
        self.reject_if_read_only()?;
        let existing = self
            .catalog
            .lock()
            .map_err(|_| RymeError::Internal(String::from("catalog lock")))?
            .get(&table)
            .cloned()
            .ok_or_else(|| RymeError::NotFound(String::from("table")))?;
        if !existing.iter().any(|definition| definition.name.eq_ignore_ascii_case(&column)) {
            return Err(RymeError::NotFound(String::from("column")));
        }
        if let ColumnAlteration::SetType(data_type) = &alteration {
            let definition = existing
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(&column))
                .cloned()
                .ok_or_else(|| RymeError::NotFound(String::from("column")))?;
            let rows = self.scan_all_rows(&table)?;
            if definition.primary_key && !rows.is_empty() {
                return Err(RymeError::InvalidArgument(String::from(
                    "changing the type of a populated primary key column is not supported",
                )));
            }
            let mut updates = Vec::with_capacity(rows.len());
            for (pk, before) in rows {
                let serde_json::Value::Object(mut object) = serde_json::from_slice(&before)
                    .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
                else {
                    return Err(RymeError::InvalidArgument(String::from("schema row")));
                };
                let actual_name = object
                    .keys()
                    .find(|name| name.eq_ignore_ascii_case(&column))
                    .cloned()
                    .ok_or_else(|| {
                        RymeError::InvalidArgument(format!("unknown column {column}"))
                    })?;
                let current = object
                    .get(&actual_name)
                    .ok_or_else(|| RymeError::InvalidArgument(String::from("schema row")))?;
                let converted = convert_json_column_value(current, data_type)?;
                object.insert(actual_name, converted);
                let after = serde_json::to_vec(&serde_json::Value::Object(object))
                    .map_err(|error| RymeError::Internal(error.to_string()))?;
                updates.push((pk, before, after));
            }

            let index_definitions = self.catalog_indexes(&table);
            for index_definition in index_definitions.iter().filter(|definition| definition.unique)
            {
                let mut entries: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
                for (pk, _, after) in &updates {
                    let Some(indexed) = index_value(index_definition, pk, after) else { continue };
                    if entries.insert(indexed, pk.clone()).is_some() {
                        return Err(RymeError::Conflict(format!(
                            "unique index {}",
                            index_definition.name
                        )));
                    }
                }
            }
            if !updates.is_empty() {
                let mut txn = self.begin_with(self.isolation);
                for (pk, _, after) in &updates {
                    self.enforce_checks(&table, pk, after)?;
                    self.enforce_foreign_keys(&mut txn, &table, pk, after)?;
                }
                for (pk, _, after) in &updates {
                    self.manager.put(
                        &mut txn,
                        RecordKey::new(&self.tenant, &self.database, &table, pk),
                        after.clone(),
                    );
                }
                self.manager.commit(txn).await?;
            }
            let mut catalog = self
                .catalog
                .lock()
                .map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
            let updated = catalog
                .get_mut(&table)
                .and_then(|definitions| {
                    definitions
                        .iter_mut()
                        .find(|definition| definition.name.eq_ignore_ascii_case(&column))
                })
                .ok_or_else(|| RymeError::NotFound(String::from("column")))?;
            updated.data_type = data_type.clone();
            drop(catalog);
            self.rebuild_index_entries(&table)?;
            self.schema_dirty.store(true, Ordering::SeqCst);
            return Ok(());
        }
        if matches!(&alteration, ColumnAlteration::SetNotNull) {
            for (_, value) in self.scan_all_rows(&table)? {
                let serde_json::Value::Object(object) = serde_json::from_slice(&value)
                    .map_err(|_| RymeError::InvalidArgument(String::from("schema row")))?
                else {
                    return Err(RymeError::InvalidArgument(String::from("schema row")));
                };
                let present = object
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&column))
                    .map(|(_, value)| !value.is_null())
                    .unwrap_or(false);
                if !present {
                    return Err(RymeError::InvalidArgument(format!(
                        "column {} contains null values",
                        column
                    )));
                }
            }
        }
        let mut catalog =
            self.catalog.lock().map_err(|_| RymeError::Internal(String::from("catalog lock")))?;
        let definition = catalog
            .get_mut(&table)
            .and_then(|definitions| {
                definitions
                    .iter_mut()
                    .find(|definition| definition.name.eq_ignore_ascii_case(&column))
            })
            .ok_or_else(|| RymeError::NotFound(String::from("column")))?;
        match alteration {
            ColumnAlteration::SetDefault(default) => definition.column_default = Some(default),
            ColumnAlteration::DropDefault => definition.column_default = None,
            ColumnAlteration::SetNotNull => definition.nullable = false,
            ColumnAlteration::DropNotNull => definition.nullable = true,
            ColumnAlteration::SetType(_) => unreachable!("column type handled before catalog lock"),
        }
        self.schema_dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn ensure_table(&self, table: &str) -> Result<()> {
        let was_dirty = self.schema_dirty.load(Ordering::SeqCst);
        let previous = self.try_schema_snapshot()?;
        if previous.tables.contains_key(table) {
            return Ok(());
        }
        self.register_table(table.to_string(), Vec::new());
        self.validate_schema_mutation(&previous, was_dirty)
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
        persist_schema_snapshot(&path, &self.try_schema_snapshot()?)?;
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
        self.enforce_statement_privileges(&statement)?;
        let schema_statement = statement.is_schema_change();
        let previous_schema =
            if schema_statement { Some(self.try_schema_snapshot()?) } else { None };
        let previous_schema_dirty = self.schema_dirty.load(Ordering::SeqCst);
        match &statement {
            Statement::CreateSchema { .. }
            | Statement::CreateExtension { .. }
            | Statement::CreateSequence { .. }
            | Statement::CreateFunction { .. }
            | Statement::CreateView { .. }
            | Statement::CreateMaterializedView { .. }
            | Statement::CreateTrigger { .. }
            | Statement::CreatePolicy { .. }
            | Statement::DropPolicy { .. }
            | Statement::DropTrigger { .. }
            | Statement::DropView { .. }
            | Statement::DropMaterializedView { .. }
            | Statement::RefreshMaterializedView { .. }
            | Statement::DropSequence { .. }
            | Statement::AlterSequence { .. }
            | Statement::CreateRole { .. }
            | Statement::AlterRoleInheritance { .. }
            | Statement::DropRole { .. }
            | Statement::RoleMembershipChange { .. }
            | Statement::PrivilegeChange { .. }
            | Statement::DefaultPrivilegeChange { .. }
            | Statement::SequenceValue { .. }
            | Statement::AlterTableRls { .. } => {}
            Statement::CreateTable {
                table,
                columns,
                unique_constraints,
                checks,
                foreign_keys,
                named_constraints,
                if_not_exists,
            } => {
                self.create_table(
                    table.clone(),
                    columns.clone(),
                    unique_constraints.clone(),
                    checks.clone(),
                    foreign_keys.clone(),
                    named_constraints.clone(),
                    *if_not_exists,
                )?;
            }
            Statement::DropTable { .. } => {}
            Statement::DropIndex { .. } => {}
            Statement::TruncateTable { .. } => {}
            Statement::AlterTableDropConstraint { .. } => {}
            Statement::AlterTableAddColumn { .. } => {}
            Statement::AlterTableAddConstraint { .. } => {}
            Statement::AlterTableDropColumn { .. } => {}
            Statement::AlterTableRenameColumn { .. } => {}
            Statement::AlterTableColumn { .. } => {}
            Statement::CreateIndex { .. } => {}
            statement if statement.is_write() => self.ensure_table(statement.table())?,
            _ => {}
        }
        let result = match statement {
            Statement::Returning { statement, fields } => {
                let checkpoint = txn.checkpoint();
                let returning_table = returning_target_table(&statement).map(str::to_string);
                match self.execute_returning_in_transaction(txn, *statement, fields).await {
                    Ok((result, changes)) => {
                        let denied = returning_table.as_deref().is_some_and(|table| {
                            changes.iter().filter(|change| change.table == table).any(|change| {
                                let value = if change.op == Operation::Delete {
                                    change.before.as_deref()
                                } else {
                                    change.after.as_deref()
                                };
                                value.is_some_and(|value| !self.row_allowed_by_rls(table, value))
                            })
                        });
                        if denied {
                            txn.restore_checkpoint(&checkpoint);
                            Err(RymeError::Forbidden)
                        } else {
                            Ok((result, changes))
                        }
                    }
                    Err(error) => {
                        txn.restore_checkpoint(&checkpoint);
                        Err(error)
                    }
                }
            }
            statement => self.execute_in_transaction_base(txn, statement).await,
        }?;
        txn.ensure_healthy()?;
        if let Some(previous) = previous_schema.as_ref() {
            self.validate_schema_mutation(previous, previous_schema_dirty)?;
        }
        Ok(result)
    }

    async fn execute_in_transaction_base(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        match statement {
            Statement::Distinct { statement, limit, offset } => {
                let (result, changes) =
                    Box::pin(self.execute_in_transaction_base(txn, *statement)).await?;
                Ok((apply_distinct(result, offset, limit), changes))
            }
            Statement::CreateSchema { .. } | Statement::CreateExtension { .. } => {
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateSequence {
                name,
                if_not_exists,
                data_type,
                start,
                increment,
                min_value,
                max_value,
                cache,
                cycle,
            } => {
                self.install_sequence(
                    SequenceDefinition {
                        name,
                        data_type,
                        start,
                        increment,
                        min_value,
                        max_value,
                        cache,
                        cycle,
                        last_value: None,
                    },
                    if_not_exists,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateRole { name, login, superuser, createdb, createrole, inherit } => {
                self.install_role(RoleDefinition {
                    name,
                    login,
                    superuser,
                    createdb,
                    createrole,
                    inherit,
                })?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterRoleInheritance { name, inherit } => {
                self.alter_role_inheritance(&name, inherit)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropRole { name, if_exists } => {
                self.drop_role(&name, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::RoleMembershipChange { grant, roles, members } => {
                self.apply_role_membership_change(grant, roles, members)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::PrivilegeChange {
                grant,
                object_type,
                objects,
                privileges,
                grantees,
                grant_option,
            } => {
                self.apply_privilege_change(
                    grant,
                    object_type,
                    objects,
                    privileges,
                    grantees,
                    grant_option,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DefaultPrivilegeChange {
                grant,
                object_type,
                schema,
                owner,
                privileges,
                grantees,
                grant_option,
            } => {
                self.apply_default_privilege_change(
                    grant,
                    object_type,
                    schema,
                    owner,
                    privileges,
                    grantees,
                    grant_option,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateFunction { name, body, language, replace } => {
                self.install_function(FunctionDefinition { name, body, language }, replace)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateView { name, query, replace } => {
                self.install_view(
                    ViewDefinition {
                        name,
                        query: *query,
                        dependencies: Vec::new(),
                        materialized: false,
                        rows: Vec::new(),
                    },
                    replace,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateMaterializedView { name, query, replace, with_data } => {
                let query = *query;
                let rows = if with_data {
                    query_result_rows(self.execute_read_in_transaction(txn, query.clone())?)
                } else {
                    Vec::new()
                };
                self.install_view_definition(
                    ViewDefinition {
                        name,
                        query,
                        dependencies: Vec::new(),
                        materialized: true,
                        rows,
                    },
                    replace,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateTrigger { name, table, timing, events, function } => {
                self.install_trigger(TriggerDefinition { name, table, timing, events, function })?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropTrigger { name, table, if_exists } => {
                self.drop_trigger(&name, &table, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropView { name, if_exists } => {
                self.drop_view(&name, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropMaterializedView { name, if_exists } => {
                self.drop_view(&name, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::RefreshMaterializedView { name, concurrently: _ } => {
                self.refresh_materialized_view(txn, &name)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropSequence { name, if_exists } => {
                self.drop_sequence(&name, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterSequence { name, restart, increment } => {
                self.alter_sequence(&name, restart, increment)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::SequenceValue { operation, name, value, is_called } => {
                Ok((self.execute_sequence_value(operation, name, value, is_called)?, Vec::new()))
            }
            Statement::CreatePolicy { name, table, command, roles, using, check } => {
                self.install_policy(
                    &name,
                    &table,
                    &command,
                    &roles,
                    using.as_deref(),
                    check.as_deref(),
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropPolicy { name, table, if_exists } => {
                self.drop_policy(&name, &table, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableRls { table, enabled, forced } => {
                self.set_rls_state(&table, enabled, forced)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateTable { .. } => Ok((QueryResult::Ok, Vec::new())),
            Statement::DropTable { table, if_exists } => {
                self.drop_table(table, if_exists).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::TruncateTable { table, restart_identity, cascade } => {
                self.truncate_table(table, restart_identity, cascade).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableDropConstraint { table, constraint, if_exists } => {
                self.drop_constraint(table, constraint, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableAddColumn { table, column, if_not_exists } => {
                self.alter_table_add_column(table, column, if_not_exists).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableAddConstraint { table, constraint } => {
                self.add_table_constraint(table, constraint).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableDropColumn { table, column, if_exists } => {
                self.alter_table_drop_column(table, column, if_exists).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableRenameColumn { table, from, to } => {
                self.alter_table_rename_column(table, from, to).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::AlterTableColumn { table, column, alteration } => {
                self.alter_table_column(table, column, alteration).await?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::DropIndex { name, if_exists } => {
                self.drop_index(name, if_exists)?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::CreateIndex {
                name,
                table,
                field,
                column,
                columns,
                unique,
                if_not_exists,
            } => {
                self.create_index(
                    IndexDefinition { name, table, field, column, columns, unique },
                    if_not_exists,
                )?;
                Ok((QueryResult::Ok, Vec::new()))
            }
            Statement::Explain { plan, inner, analyze } => {
                if !analyze {
                    return Ok((
                        QueryResult::Row { pk: b"plan".to_vec(), value: plan.into_bytes() },
                        Vec::new(),
                    ));
                }
                let started = std::time::Instant::now();
                let (result, changes) = Box::pin(self.execute_in_transaction(txn, *inner)).await?;
                let change_count = changes.len();
                Ok((
                    explain_analyze_result(plan, &result, change_count, started.elapsed()),
                    changes,
                ))
            }
            Statement::CopyFrom { table, columns, rows } => {
                self.reject_if_read_only()?;
                if rows.len() > 10000 {
                    return Err(RymeError::InvalidArgument(String::from("batch too large")));
                }
                if !columns.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from(
                        "COPY column rows require the wire COPY path",
                    )));
                }
                let mut changes = Vec::with_capacity(rows.len());
                for (pk, value) in rows {
                    if pk.is_empty() || pk.len() > 1024 {
                        return Err(RymeError::InvalidArgument(String::from("key")));
                    }
                    if value.len() > 4 * 1024 * 1024 {
                        return Err(RymeError::Overload(String::from("value")));
                    }
                    let (_, mut row_changes) = Box::pin(self.execute_in_transaction_base(
                        txn,
                        Statement::Upsert { table: table.clone(), pk, value },
                    ))
                    .await?;
                    changes.append(&mut row_changes);
                }
                Ok((QueryResult::Ok, changes))
            }
            Statement::InsertRow {
                table,
                columns,
                values,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                    Statement::InsertConflict {
                        table,
                        pk,
                        value,
                        target_columns: conflict_target,
                        assignments: conflict_update,
                        conflict_filter,
                        do_nothing: on_conflict_do_nothing,
                    }
                } else if on_conflict_do_nothing {
                    Statement::InsertIgnore { table, pk, value }
                } else if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_in_transaction_base(txn, statement)).await
            }
            Statement::InsertSelect {
                table,
                columns,
                source_table,
                source_columns,
                filter,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (columns, rows) = self
                    .materialize_insert_select_rows(
                        txn,
                        table.clone(),
                        columns,
                        source_table,
                        source_columns,
                        filter,
                    )
                    .await?;
                let changes = self
                    .execute_insert_rows_in_transaction(
                        txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                    )
                    .await?;
                Ok((QueryResult::Ok, changes))
            }
            Statement::InsertRows {
                table,
                columns,
                rows,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let changes = self
                    .execute_insert_rows_in_transaction(
                        txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                    )
                    .await?;
                Ok((QueryResult::Ok, changes))
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                let value = self.apply_before_triggers(&table, "INSERT", &value)?;
                self.enforce_rls(&table, &value, RlsCommand::Insert)?;
                self.enforce_checks(&table, &pk, &value)?;
                self.enforce_foreign_keys_async(txn, &table, &pk, &value).await?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(txn, &key).await?;
                if before.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                self.check_unique(txn, &table, &pk, &value)?;
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        previous_pk: None,
                        op: Operation::Insert,
                        before,
                        after: Some(value),
                    }],
                ))
            }
            Statement::InsertIgnore { table, pk, value } => {
                self.reject_if_read_only()?;
                let value = self.apply_before_triggers(&table, "INSERT", &value)?;
                self.enforce_rls(&table, &value, RlsCommand::Insert)?;
                self.enforce_checks(&table, &pk, &value)?;
                self.enforce_foreign_keys_async(txn, &table, &pk, &value).await?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                if self.manager.get_async(txn, &key).await?.is_some() {
                    return Ok((QueryResult::Ok, Vec::new()));
                }
                if let Err(error) = self.check_unique(txn, &table, &pk, &value) {
                    if matches!(error, RymeError::Conflict(_)) {
                        return Ok((QueryResult::Ok, Vec::new()));
                    }
                    return Err(error);
                }
                self.manager.put(txn, key, value.clone());
                Ok((
                    QueryResult::Ok,
                    vec![TransactionChange {
                        table,
                        pk,
                        previous_pk: None,
                        op: Operation::Insert,
                        before: None,
                        after: Some(value),
                    }],
                ))
            }
            Statement::InsertConflict {
                table,
                pk,
                value,
                target_columns,
                assignments,
                conflict_filter,
                do_nothing,
            } => {
                let value = self.apply_before_triggers(&table, "INSERT", &value)?;
                self.execute_insert_conflict_in_transaction(
                    txn,
                    table,
                    pk,
                    value,
                    target_columns,
                    assignments,
                    conflict_filter,
                    do_nothing,
                )
                .await
            }
            Statement::Upsert { table, pk, value } => {
                self.reject_if_read_only()?;
                let value = self.apply_before_triggers(&table, "INSERT", &value)?;
                self.enforce_rls(&table, &value, RlsCommand::Insert)?;
                self.enforce_checks(&table, &pk, &value)?;
                self.enforce_foreign_keys_async(txn, &table, &pk, &value).await?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(txn, &key).await?;
                if let Some(before) = before.as_deref() {
                    self.enforce_rls(&table, before, RlsCommand::Update)?;
                    self.enforce_rls(&table, &value, RlsCommand::Update)?;
                }
                self.check_unique(txn, &table, &pk, &value)?;
                self.manager.put(txn, key, value.clone());
                let mut changes = Vec::new();
                if let Some(before_value) = before.as_deref() {
                    self.update_referencing_rows_async(
                        txn,
                        &table,
                        &pk,
                        before_value,
                        &value,
                        &mut changes,
                        &mut BTreeSet::new(),
                    )
                    .await?;
                }
                changes.push(TransactionChange {
                    table,
                    pk,
                    previous_pk: None,
                    op: if before.is_some() { Operation::Update } else { Operation::Insert },
                    before,
                    after: Some(value),
                });
                Ok((QueryResult::Ok, changes))
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get_async(txn, &key)
                    .await?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                Box::pin(
                    self.execute_in_transaction_base(txn, Statement::Update { table, pk, value }),
                )
                .await
            }
            Statement::UpdateWhere { table, assignments, filter } => {
                self.reject_if_read_only()?;
                let rows = self.scan_rows_async(txn, &table, &filter, usize::MAX).await?;
                let mut changes = Vec::with_capacity(rows.len());
                for (pk, before) in rows {
                    self.enforce_rls(&table, &before, RlsCommand::Update)?;
                    let after =
                        self.materialize_update_row(&table, &pk, assignments.clone(), &before)?;
                    let after = self.apply_before_triggers(&table, "UPDATE", &after)?;
                    let new_pk = self.primary_key_for_row(&table, &pk, &after)?;
                    self.enforce_rls(&table, &after, RlsCommand::Update)?;
                    self.enforce_checks(&table, &new_pk, &after)?;
                    self.enforce_foreign_keys_async(txn, &table, &new_pk, &after).await?;
                    self.check_unique_excluding(txn, &table, &new_pk, &after, Some(&pk))?;
                    if new_pk != pk
                        && self
                            .manager
                            .get_async(
                                txn,
                                &RecordKey::new(&self.tenant, &self.database, &table, &new_pk),
                            )
                            .await?
                            .is_some()
                    {
                        return Err(RymeError::Conflict(String::from("primary key exists")));
                    }
                    if new_pk != pk {
                        self.manager
                            .delete(txn, RecordKey::new(&self.tenant, &self.database, &table, &pk));
                    }
                    self.manager.put(
                        txn,
                        RecordKey::new(&self.tenant, &self.database, &table, &new_pk),
                        after.clone(),
                    );
                    self.update_referencing_rows_async(
                        txn,
                        &table,
                        &pk,
                        &before,
                        &after,
                        &mut changes,
                        &mut BTreeSet::new(),
                    )
                    .await?;
                    changes.push(TransactionChange {
                        table: table.clone(),
                        previous_pk: (new_pk != pk).then_some(pk),
                        pk: new_pk,
                        op: Operation::Update,
                        before: Some(before),
                        after: Some(after),
                    });
                }
                Ok((QueryResult::Ok, changes))
            }
            Statement::UpdateFrom {
                table,
                assignments,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let changes = self
                    .execute_update_from_in_transaction(
                        txn,
                        table,
                        assignments,
                        source_table,
                        target_column,
                        source_column,
                        filter,
                        source_filter,
                    )
                    .await?;
                Ok((QueryResult::Ok, changes))
            }
            Statement::Update { table, pk, value } => {
                self.reject_if_read_only()?;
                let value = self.apply_before_triggers(&table, "UPDATE", &value)?;
                let before_key = pk.clone();
                let new_pk = self.primary_key_for_row(&table, &pk, &value)?;
                self.enforce_rls(&table, &value, RlsCommand::Update)?;
                self.enforce_checks(&table, &new_pk, &value)?;
                self.enforce_foreign_keys_async(txn, &table, &new_pk, &value).await?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(txn, &key).await?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(
                    &table,
                    before.as_deref().unwrap_or_default(),
                    RlsCommand::Update,
                )?;
                self.check_unique_excluding(txn, &table, &new_pk, &value, Some(&pk))?;
                if new_pk != pk
                    && self
                        .manager
                        .get_async(
                            txn,
                            &RecordKey::new(&self.tenant, &self.database, &table, &new_pk),
                        )
                        .await?
                        .is_some()
                {
                    return Err(RymeError::Conflict(String::from("primary key exists")));
                }
                if new_pk != pk {
                    self.manager.delete(txn, key);
                }
                self.manager.put(
                    txn,
                    RecordKey::new(&self.tenant, &self.database, &table, &new_pk),
                    value.clone(),
                );
                let mut changes = Vec::new();
                if let Some(before_value) = before.as_deref() {
                    self.update_referencing_rows_async(
                        txn,
                        &table,
                        &pk,
                        before_value,
                        &value,
                        &mut changes,
                        &mut BTreeSet::new(),
                    )
                    .await?;
                }
                changes.push(TransactionChange {
                    table,
                    previous_pk: (new_pk != before_key).then_some(before_key),
                    pk: new_pk,
                    op: Operation::Update,
                    before,
                    after: Some(value),
                });
                Ok((QueryResult::Ok, changes))
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(txn, &key).await?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(
                    &table,
                    before.as_deref().unwrap_or_default(),
                    RlsCommand::Delete,
                )?;
                let mut changes = Vec::new();
                self.delete_row_with_references_async(
                    txn,
                    &table,
                    &pk,
                    before.as_deref().unwrap_or_default(),
                    &mut changes,
                    &mut BTreeSet::new(),
                )
                .await?;
                self.manager.delete(txn, key);
                changes.push(TransactionChange {
                    table,
                    pk,
                    previous_pk: None,
                    op: Operation::Delete,
                    before,
                    after: None,
                });
                Ok((QueryResult::Ok, changes))
            }
            Statement::DeleteWhere { table, filter } => {
                self.reject_if_read_only()?;
                let rows = self.scan_rows_async(txn, &table, &filter, usize::MAX).await?;
                let mut changes = Vec::with_capacity(rows.len());
                for (pk, before) in rows {
                    self.enforce_rls(&table, &before, RlsCommand::Delete)?;
                    self.delete_row_with_references_async(
                        txn,
                        &table,
                        &pk,
                        &before,
                        &mut changes,
                        &mut BTreeSet::new(),
                    )
                    .await?;
                    self.manager
                        .delete(txn, RecordKey::new(&self.tenant, &self.database, &table, &pk));
                    changes.push(TransactionChange {
                        table: table.clone(),
                        pk,
                        previous_pk: None,
                        op: Operation::Delete,
                        before: Some(before),
                        after: None,
                    });
                }
                Ok((QueryResult::Ok, changes))
            }
            Statement::DeleteUsing {
                table,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let changes = self
                    .execute_delete_using_in_transaction(
                        txn,
                        table,
                        source_table,
                        target_column,
                        source_column,
                        filter,
                        source_filter,
                    )
                    .await?;
                Ok((QueryResult::Ok, changes))
            }
            statement => {
                Ok((self.execute_read_in_transaction_async(txn, statement).await?, Vec::new()))
            }
        }
    }

    fn conflict_target_matches(
        &self,
        table: &str,
        target_columns: &[String],
        incoming: &[u8],
        existing: &[u8],
    ) -> Result<bool> {
        let incoming = serde_json::from_slice::<serde_json::Value>(incoming)
            .map_err(|_| RymeError::InvalidArgument(String::from("conflict row")))?;
        let existing = serde_json::from_slice::<serde_json::Value>(existing)
            .map_err(|_| RymeError::InvalidArgument(String::from("conflict row")))?;
        let (serde_json::Value::Object(incoming), serde_json::Value::Object(existing)) =
            (incoming, existing)
        else {
            return Err(RymeError::InvalidArgument(String::from("conflict row")));
        };
        for column in target_columns {
            let incoming = incoming
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, value)| value)
                .filter(|value| !value.is_null());
            let existing = existing
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, value)| value)
                .filter(|value| !value.is_null());
            let (Some(incoming), Some(existing)) = (incoming, existing) else {
                return Ok(false);
            };
            if json_result_bytes(incoming) != json_result_bytes(existing) {
                return Ok(false);
            }
        }
        let _ = table;
        Ok(true)
    }

    fn find_conflict_row(
        &self,
        txn: &mut Transaction,
        table: &str,
        pk: &[u8],
        value: &[u8],
        target_columns: &[String],
    ) -> Result<Option<Row>> {
        if !target_columns.is_empty() {
            let unique_index = self.catalog_indexes(table).into_iter().find(|index| {
                if !index.unique {
                    return false;
                }
                let indexed = if index.columns.is_empty() {
                    index.column.iter().collect::<Vec<_>>()
                } else {
                    index.columns.iter().collect::<Vec<_>>()
                };
                indexed.len() == target_columns.len()
                    && indexed
                        .iter()
                        .zip(target_columns)
                        .all(|(left, right)| left.eq_ignore_ascii_case(right))
            });
            if let Some(unique_index) = unique_index {
                if let Some(incoming_index) = index_value(&unique_index, pk, value) {
                    if let Some(candidates) = self.indexed_candidates_for_value(
                        table,
                        &unique_index,
                        &incoming_index,
                        txn.read_ts,
                    )? {
                        txn.record_constraint_scan(&self.tenant, &self.database, table)?;
                        let mut candidate_pks = candidates;
                        candidate_pks.extend(
                            txn.writes()
                                .iter()
                                .filter(|(key, write)| {
                                    key.tenant == self.tenant
                                        && key.database == self.database
                                        && key.table == table
                                        && write.value.as_ref().is_some_and(|after| {
                                            index_value(&unique_index, &key.pk, after)
                                                .is_some_and(|indexed| indexed == incoming_index)
                                        })
                                })
                                .map(|(key, _)| key.pk.clone()),
                        );
                        candidate_pks.sort();
                        candidate_pks.dedup();
                        for existing_pk in candidate_pks {
                            let key =
                                RecordKey::new(&self.tenant, &self.database, table, &existing_pk);
                            if let Some(existing_value) = self.manager.get(txn, &key)? {
                                if self.conflict_target_matches(
                                    table,
                                    target_columns,
                                    value,
                                    &existing_value,
                                )? {
                                    return Ok(Some((existing_pk, existing_value)));
                                }
                            }
                        }
                        return Ok(None);
                    }
                }
            }
        }
        let rows = self.scan_all_rows_in_transaction(txn, table)?;
        if target_columns.is_empty() {
            if let Some(existing) = rows.iter().find(|(existing_pk, _)| existing_pk == pk) {
                return Ok(Some(existing.clone()));
            }
            for definition in
                self.catalog_columns(table).into_iter().filter(|definition| definition.unique)
            {
                let target = [definition.name];
                if let Some((existing_pk, existing_value)) =
                    rows.iter().find(|(existing_pk, existing_value)| {
                        self.conflict_target_matches(table, &target, value, existing_value)
                            .unwrap_or(false)
                            || (target[0].eq_ignore_ascii_case("id") && existing_pk == pk)
                    })
                {
                    return Ok(Some((existing_pk.clone(), existing_value.clone())));
                }
            }
            for index in self.catalog_indexes(table).into_iter().filter(|index| index.unique) {
                let Some(incoming_index) = index_value(&index, pk, value) else { continue };
                if let Some((existing_pk, existing_value)) =
                    rows.iter().find(|(existing_pk, existing_value)| {
                        index_value(&index, existing_pk, existing_value)
                            .is_some_and(|indexed| indexed == incoming_index)
                    })
                {
                    return Ok(Some((existing_pk.clone(), existing_value.clone())));
                }
            }
            return Ok(None);
        }
        let definitions = self.catalog_columns(table);
        if definitions.is_empty()
            && target_columns.len() == 1
            && target_columns[0].eq_ignore_ascii_case("id")
        {
            return Ok(rows.into_iter().find(|(existing_pk, _)| existing_pk == pk));
        }
        if target_columns.iter().any(|column| {
            !definitions.iter().any(|definition| definition.name.eq_ignore_ascii_case(column))
        }) {
            return Err(RymeError::InvalidArgument(String::from("unknown conflict target")));
        }
        let primary = definitions
            .iter()
            .filter(|definition| definition.primary_key)
            .map(|definition| definition.name.clone())
            .collect::<Vec<_>>();
        let is_primary = primary.len() == target_columns.len()
            && primary
                .iter()
                .zip(target_columns)
                .all(|(left, right)| left.eq_ignore_ascii_case(right));
        let is_unique = self.catalog_indexes(table).into_iter().any(|index| {
            if !index.unique {
                return false;
            }
            let indexed = if index.columns.is_empty() {
                index.column.into_iter().collect::<Vec<_>>()
            } else {
                index.columns
            };
            indexed.len() == target_columns.len()
                && indexed
                    .iter()
                    .zip(target_columns)
                    .all(|(left, right)| left.eq_ignore_ascii_case(right))
        });
        let is_inline_unique = target_columns.len() == 1
            && definitions.iter().any(|definition| {
                definition.unique && definition.name.eq_ignore_ascii_case(&target_columns[0])
            });
        if !is_primary && !is_unique && !is_inline_unique {
            return Err(RymeError::InvalidArgument(String::from("conflict target is not unique")));
        }
        for (existing_pk, existing_value) in rows {
            if self.conflict_target_matches(table, target_columns, value, &existing_value)? {
                return Ok(Some((existing_pk, existing_value)));
            }
        }
        Ok(None)
    }

    fn conflict_filter_matches(
        &self,
        predicates: &[Predicate],
        pk: &[u8],
        existing: &[u8],
        incoming: &[u8],
    ) -> bool {
        predicates.iter().all(|predicate| {
            let Some(predicate) = substitute_excluded_predicate(predicate, incoming) else {
                return false;
            };
            predicate.matches(pk, existing)
        })
    }

    fn resolve_conflict_assignments(
        &self,
        table: &str,
        assignments: Vec<(String, InsertValue)>,
        incoming: &[u8],
    ) -> Result<Vec<(String, InsertValue)>> {
        let incoming_object = serde_json::from_slice::<serde_json::Value>(incoming)
            .ok()
            .and_then(|value| value.as_object().cloned());
        assignments
            .into_iter()
            .map(|(column, value)| {
                let value = match value {
                    InsertValue::Excluded(excluded) => {
                        if let Some(incoming) = incoming_object.as_ref() {
                            let selected = incoming
                                .iter()
                                .find(|(name, _)| name.eq_ignore_ascii_case(&excluded))
                                .map(|(_, value)| value)
                                .ok_or_else(|| {
                                    RymeError::InvalidArgument(format!(
                                        "unknown EXCLUDED column {excluded}"
                                    ))
                                })?;
                            if selected.is_null() {
                                InsertValue::Null
                            } else {
                                InsertValue::Value(json_result_bytes(selected))
                            }
                        } else {
                            InsertValue::Value(incoming.to_vec())
                        }
                    }
                    other => other,
                };
                let _ = table;
                Ok((column, value))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_insert_conflict_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        pk: Vec<u8>,
        value: Vec<u8>,
        target_columns: Vec<String>,
        assignments: Vec<(String, InsertValue)>,
        conflict_filter: Vec<Predicate>,
        do_nothing: bool,
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        self.reject_if_read_only()?;
        self.enforce_rls(&table, &value, RlsCommand::Insert)?;
        let conflict = self.find_conflict_row(txn, &table, &pk, &value, &target_columns)?;
        let Some((existing_pk, existing_value)) = conflict else {
            let statement = Statement::Insert { table, pk, value };
            return Box::pin(self.execute_in_transaction_base(txn, statement)).await;
        };
        if do_nothing {
            return Ok((QueryResult::Ok, Vec::new()));
        }
        if !conflict_filter.is_empty()
            && !self.conflict_filter_matches(
                &conflict_filter,
                &existing_pk,
                &existing_value,
                &value,
            )
        {
            return Ok((QueryResult::Ok, Vec::new()));
        }
        if assignments.is_empty() {
            return Err(RymeError::InvalidArgument(String::from("conflict update")));
        }
        let assignments = self.resolve_conflict_assignments(&table, assignments, &value)?;
        let after = self.materialize_update_row_with_incoming(
            &table,
            &existing_pk,
            assignments,
            &existing_value,
            Some(&value),
        )?;
        let statement = Statement::Update { table, pk: existing_pk, value: after };
        Box::pin(self.execute_in_transaction_base(txn, statement)).await
    }

    fn insert_select_value(
        &self,
        source_table: &str,
        column: &str,
        pk: &[u8],
        value: &[u8],
    ) -> InsertValue {
        let definitions = self.catalog_columns(source_table);
        let primary_keys =
            definitions.iter().filter(|definition| definition.primary_key).collect::<Vec<_>>();
        if let Some(index) =
            primary_keys.iter().position(|definition| definition.name.eq_ignore_ascii_case(column))
        {
            if primary_keys.len() == 1 {
                return InsertValue::Value(pk.to_vec());
            }
            if let Some(parts) = decode_key_parts(pk) {
                return parts
                    .get(index)
                    .cloned()
                    .map(InsertValue::Value)
                    .unwrap_or(InsertValue::Null);
            }
        }
        let schema_column =
            definitions.iter().any(|definition| definition.name.eq_ignore_ascii_case(column));
        if !schema_column
            && (column.eq_ignore_ascii_case("id")
                || column.eq_ignore_ascii_case("pk")
                || column.eq_ignore_ascii_case("key"))
        {
            return InsertValue::Value(pk.to_vec());
        }
        if !schema_column
            && (column.eq_ignore_ascii_case("value")
                || column.eq_ignore_ascii_case("val")
                || column.eq_ignore_ascii_case("data"))
        {
            return InsertValue::Value(value.to_vec());
        }
        let Ok(serde_json::Value::Object(object)) = serde_json::from_slice(value) else {
            return InsertValue::Value(value.to_vec());
        };
        json_column_value(column, &object)
            .map(|selected| {
                if selected.is_null() {
                    InsertValue::Null
                } else {
                    InsertValue::Value(json_result_bytes(selected))
                }
            })
            .unwrap_or(InsertValue::Null)
    }

    async fn materialize_insert_select_rows(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        source_table: String,
        source_columns: Vec<String>,
        filter: Vec<Predicate>,
    ) -> Result<(Vec<String>, Vec<Vec<InsertValue>>)> {
        let target_columns = if columns.is_empty() {
            self.catalog_columns(&table)
                .into_iter()
                .map(|definition| definition.name)
                .collect::<Vec<_>>()
        } else {
            columns
        };
        if target_columns.is_empty() {
            return Err(RymeError::InvalidArgument(String::from(
                "INSERT SELECT requires target columns",
            )));
        }
        let source_columns = if source_columns.is_empty() {
            let definitions = self.catalog_columns(&source_table);
            if definitions.is_empty() {
                vec![String::from("id"), String::from("value")]
            } else {
                definitions.into_iter().map(|definition| definition.name).collect()
            }
        } else {
            source_columns
        };
        if source_columns.len() != target_columns.len() {
            return Err(RymeError::InvalidArgument(String::from("INSERT SELECT column count")));
        }
        let rows = self.scan_rows_async(txn, &source_table, &filter, usize::MAX).await?;
        let values = rows
            .into_iter()
            .filter(|(pk, value)| predicates_match(&filter, pk, value))
            .map(|(pk, value)| {
                source_columns
                    .iter()
                    .map(|column| self.insert_select_value(&source_table, column, &pk, &value))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok((target_columns, values))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_insert_rows_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
        on_conflict_do_nothing: bool,
        conflict_target: Vec<String>,
        conflict_update: Vec<(String, InsertValue)>,
        conflict_filter: Vec<Predicate>,
    ) -> Result<Vec<TransactionChange>> {
        let mut changes = Vec::new();
        for values in rows {
            let (pk, value) = self.materialize_insert_row(&table, columns.clone(), values)?;
            let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                Statement::InsertConflict {
                    table: table.clone(),
                    pk,
                    value,
                    target_columns: conflict_target.clone(),
                    assignments: conflict_update.clone(),
                    conflict_filter: conflict_filter.clone(),
                    do_nothing: on_conflict_do_nothing,
                }
            } else if on_conflict_do_nothing {
                Statement::InsertIgnore { table: table.clone(), pk, value }
            } else if upsert {
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

    #[allow(clippy::too_many_arguments)]
    async fn execute_returning_rows_in_transaction(
        &self,
        txn: &mut Transaction,
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<InsertValue>>,
        upsert: bool,
        on_conflict_do_nothing: bool,
        conflict_target: Vec<String>,
        conflict_update: Vec<(String, InsertValue)>,
        conflict_filter: Vec<Predicate>,
        fields: &[ReturningField],
    ) -> Result<(QueryResult, Vec<TransactionChange>)> {
        let result_columns = returning_columns(fields);
        let mut result_rows = Vec::new();
        let mut changes = Vec::new();
        for values in rows {
            let (pk, value) = self.materialize_insert_row(&table, columns.clone(), values)?;
            let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                Statement::InsertConflict {
                    table: table.clone(),
                    pk: pk.clone(),
                    value: value.clone(),
                    target_columns: conflict_target.clone(),
                    assignments: conflict_update.clone(),
                    conflict_filter: conflict_filter.clone(),
                    do_nothing: on_conflict_do_nothing,
                }
            } else if on_conflict_do_nothing {
                Statement::InsertIgnore {
                    table: table.clone(),
                    pk: pk.clone(),
                    value: value.clone(),
                }
            } else if upsert {
                Statement::Upsert { table: table.clone(), pk: pk.clone(), value: value.clone() }
            } else {
                Statement::Insert { table: table.clone(), pk: pk.clone(), value: value.clone() }
            };
            let (_, mut row_changes) =
                Box::pin(self.execute_in_transaction_base(txn, statement)).await?;
            let returned_row = row_changes.last().and_then(|change| {
                change
                    .after
                    .as_ref()
                    .map(|after| returning_result(fields, change.pk.clone(), after.clone()))
            });
            changes.append(&mut row_changes);
            if let Some(returned_row) = returned_row {
                let QueryResult::Returning { rows, .. } = returned_row else {
                    unreachable!("returning result always contains rows");
                };
                result_rows.extend(rows);
            }
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
            Statement::InsertRow {
                table,
                columns,
                values,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                    Statement::InsertConflict {
                        table,
                        pk,
                        value,
                        target_columns: conflict_target,
                        assignments: conflict_update,
                        conflict_filter,
                        do_nothing: on_conflict_do_nothing,
                    }
                } else if on_conflict_do_nothing {
                    Statement::InsertIgnore { table, pk, value }
                } else if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_returning_in_transaction(txn, statement, fields)).await
            }
            Statement::InsertSelect {
                table,
                columns,
                source_table,
                source_columns,
                filter,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (columns, rows) = self
                    .materialize_insert_select_rows(
                        txn,
                        table.clone(),
                        columns,
                        source_table,
                        source_columns,
                        filter,
                    )
                    .await?;
                self.execute_returning_rows_in_transaction(
                    txn,
                    table,
                    columns,
                    rows,
                    upsert,
                    on_conflict_do_nothing,
                    conflict_target,
                    conflict_update,
                    conflict_filter,
                    &fields,
                )
                .await
            }
            Statement::InsertRows {
                table,
                columns,
                rows,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                self.execute_returning_rows_in_transaction(
                    txn,
                    table,
                    columns,
                    rows,
                    upsert,
                    on_conflict_do_nothing,
                    conflict_target,
                    conflict_update,
                    conflict_filter,
                    &fields,
                )
                .await
            }
            Statement::Insert { table, pk, value } => {
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Insert { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                Ok((result, changes))
            }
            Statement::Upsert { table, pk, value } => {
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Upsert { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                Ok((result, changes))
            }
            Statement::InsertIgnore { table, pk, value } => {
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::InsertIgnore { table, pk, value })
                    .await?;
                let result = if changes.is_empty() {
                    QueryResult::Returning { columns: returning_columns(&fields), rows: Vec::new() }
                } else {
                    changes
                        .last()
                        .and_then(|change| {
                            change.after.as_ref().map(|after| {
                                returning_result(&fields, change.pk.clone(), after.clone())
                            })
                        })
                        .unwrap_or_else(|| {
                            returning_result(&fields, pk_for_result, value_for_result)
                        })
                };
                Ok((result, changes))
            }
            Statement::InsertConflict {
                table,
                pk,
                value,
                target_columns,
                assignments,
                conflict_filter,
                do_nothing,
            } => {
                let (_, changes) = self
                    .execute_in_transaction_base(
                        txn,
                        Statement::InsertConflict {
                            table,
                            pk,
                            value,
                            target_columns,
                            assignments,
                            conflict_filter,
                            do_nothing,
                        },
                    )
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                Ok((result, changes))
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get_async(txn, &key)
                    .await?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Update { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                Ok((result, changes))
            }
            Statement::Update { table, pk, value } => {
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Update { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                Ok((result, changes))
            }
            Statement::UpdateWhere { table, assignments, filter } => {
                let statement = Statement::UpdateWhere { table, assignments, filter };
                let (_, changes) = self.execute_in_transaction_base(txn, statement).await?;
                Ok((returning_changes(&fields, &changes), changes))
            }
            Statement::UpdateFrom {
                table,
                assignments,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let statement = Statement::UpdateFrom {
                    table,
                    assignments,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(txn, statement).await?;
                Ok((returning_changes(&fields, &changes), changes))
            }
            Statement::Delete { table, pk } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let value = self
                    .manager
                    .get_async(txn, &key)
                    .await?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let (_, changes) = self
                    .execute_in_transaction_base(txn, Statement::Delete { table, pk: pk.clone() })
                    .await?;
                Ok((returning_result(&fields, pk, value), changes))
            }
            Statement::DeleteWhere { table, filter } => {
                let statement = Statement::DeleteWhere { table, filter };
                let (_, changes) = self.execute_in_transaction_base(txn, statement).await?;
                Ok((returning_changes(&fields, &changes), changes))
            }
            Statement::DeleteUsing {
                table,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let statement = Statement::DeleteUsing {
                    table,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(txn, statement).await?;
                Ok((returning_changes(&fields, &changes), changes))
            }
            _ => Err(RymeError::InvalidArgument(String::from("RETURNING requires a row mutation"))),
        }
    }

    pub async fn commit_transaction(
        &self,
        txn: Transaction,
        changes: Vec<TransactionChange>,
    ) -> Result<u64> {
        self.commit_transaction_with_schema(txn, changes, None).await
    }

    pub async fn commit_transaction_with_schema(
        &self,
        txn: Transaction,
        changes: Vec<TransactionChange>,
        schema_before: Option<SchemaSnapshot>,
    ) -> Result<u64> {
        self.check_transaction_uniqueness(&changes)?;
        let previous_schema_dirty = self.schema_dirty.load(Ordering::SeqCst);
        let previous_schema = schema_before.clone();
        let schema_after =
            if schema_before.is_some() { Some(self.try_schema_snapshot()?) } else { None };
        let schema_changed = schema_after
            .as_ref()
            .is_some_and(|after| schema_before.as_ref().is_some_and(|before| before != after));
        if schema_changed {
            if let (Some(before), Some(after)) = (schema_before.as_ref(), schema_after.as_ref()) {
                if let Err(error) = validate_schema_snapshot(after) {
                    self.restore_schema_snapshot(before.clone())?;
                    self.schema_dirty.store(previous_schema_dirty, Ordering::SeqCst);
                    return Err(error);
                }
            }
        }
        if schema_changed && !changes.is_empty() {
            self.rollback_schema_transaction(previous_schema.clone())?;
            return Err(RymeError::InvalidArgument(String::from(
                "schema changes and row changes cannot share a transaction",
            )));
        }
        let _pending_index_commit =
            self.begin_index_commit(changes.iter().map(|change| change.table.clone()).collect());
        let commit_ts = match self.manager.commit(txn).await {
            Ok(commit_ts) => commit_ts,
            Err(error) => {
                if schema_changed {
                    self.rollback_schema_transaction(previous_schema.clone())?;
                }
                return Err(error);
            }
        };
        if schema_changed {
            if let (Some(hook), Some(before), Some(after)) =
                (self.schema_hook.clone(), schema_before, schema_after)
            {
                if let Err(error) = hook(before.clone(), after).await {
                    self.rollback_schema_transaction(Some(before))?;
                    return Err(error);
                }
            }
        }
        self.apply_index_changes(&changes, commit_ts);
        for change in changes {
            self.emit(&change.table, change.pk, change.op, change.before, change.after, commit_ts)?;
        }
        if let Err(error) = self.persist_schema_if_configured() {
            if schema_changed {
                let rollback = self.rollback_schema_transaction(previous_schema).and_then(|_| {
                    self.schema_dirty.store(true, Ordering::SeqCst);
                    self.persist_schema_if_configured()
                });
                if let Err(rollback_error) = rollback {
                    return Err(RymeError::Internal(format!(
                        "schema persistence failed: {error}; rollback failed: {rollback_error}"
                    )));
                }
            }
            return Err(error);
        }
        Ok(commit_ts)
    }

    fn check_transaction_uniqueness(&self, changes: &[TransactionChange]) -> Result<()> {
        let tables: BTreeSet<String> = changes.iter().map(|change| change.table.clone()).collect();
        for table in &tables {
            self.ensure_index_entries_for_tenant(table)?;
        }
        let indexes =
            self.indexes.read().map_err(|_| RymeError::Internal(String::from("index lock")))?;
        for (table, table_indexes) in indexes.iter() {
            let table_changes: Vec<&TransactionChange> =
                changes.iter().filter(|change| &change.table == table).collect();
            if table_changes.is_empty() {
                continue;
            }
            for state in table_indexes.iter().filter(|state| state.definition.unique) {
                let touched: std::collections::BTreeSet<Vec<u8>> = table_changes
                    .iter()
                    .flat_map(|change| {
                        std::iter::once(change.pk.clone()).chain(change.previous_pk.iter().cloned())
                    })
                    .collect();
                let mut occupied: HashMap<Vec<u8>, Vec<u8>> = state
                    .entries
                    .get(&self.tenant)
                    .into_iter()
                    .flat_map(|entries| entries.iter())
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

    async fn execute_read_in_transaction_async(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<QueryResult> {
        match statement {
            Statement::SelectByKey { table, pk } => {
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                match self.manager.get_async(txn, &key).await? {
                    Some(value) if self.rls_allows(&table, &value) => {
                        Ok(QueryResult::Row { pk, value })
                    }
                    None | Some(_) => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter }
                if filter.len() == 1
                    && filter[0].column.is_none()
                    && filter[0].field == Field::Key
                    && filter[0].op == Cmp::Eq
                    && self.view_definition(&table)?.is_none() =>
            {
                let pk = filter[0].operand.clone();
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let rows = self
                    .manager
                    .get_async(txn, &key)
                    .await?
                    .filter(|value| self.rls_allows(&table, value))
                    .map(|value| vec![(pk, value)])
                    .unwrap_or_default();
                let mut rows: Vec<Row> = rows
                    .into_iter()
                    .filter(|(pk, value)| predicates_match(&filter, pk, value))
                    .collect();
                rows.sort_by(|left, right| compare_order(&order, left, right));
                let rows =
                    self.project_rows(&table, &columns, rows.into_iter().skip(offset).take(limit));
                let output_columns = columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        aliases
                            .get(index)
                            .and_then(|alias| alias.clone())
                            .unwrap_or_else(|| column.clone())
                    })
                    .collect();
                Ok(QueryResult::Table { columns: output_columns, rows })
            }
            Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter } => {
                let (cap, sort_rows) =
                    self.select_scan_options(&table, &order, limit, offset, filter.is_empty())?;
                let rows = self.scan_rows_async(txn, &table, &filter, cap).await?;
                let mut rows = rows;
                if sort_rows {
                    rows.sort_by(|left, right| compare_order(&order, left, right));
                }
                let rows =
                    self.project_rows(&table, &columns, rows.into_iter().skip(offset).take(limit));
                let output_columns = columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        aliases
                            .get(index)
                            .and_then(|alias| alias.clone())
                            .unwrap_or_else(|| column.clone())
                    })
                    .collect();
                Ok(QueryResult::Table { columns: output_columns, rows })
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let (cap, sort_rows) =
                    self.select_scan_options(&table, &order, limit, offset, filter.is_empty())?;
                let rows = self.scan_rows_async(txn, &table, &filter, cap).await?;
                let mut rows = rows;
                if sort_rows {
                    rows.sort_by(|left, right| compare_order(&order, left, right));
                }
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Aggregate { table, func, field, column, filter } => {
                let rows = self.scan_rows_async(txn, &table, &filter, usize::MAX).await?;
                Ok(QueryResult::Scalar {
                    label: func.label().to_string(),
                    value: aggregate_target_rows(&rows, func, field, column.as_deref()),
                })
            }
            Statement::Join { left, right, join_type, limit, offset, order, filter } => {
                let left_rows = self.scan_rows_async(txn, &left, &[], usize::MAX).await?;
                let right_rows = self.scan_rows_async(txn, &right, &[], usize::MAX).await?;
                Ok(join_rows(left_rows, right_rows, join_type, &filter, &order, offset, limit)?)
            }
            statement => self.execute_read_in_transaction(txn, statement),
        }
    }

    fn execute_read_in_transaction(
        &self,
        txn: &mut Transaction,
        statement: Statement,
    ) -> Result<QueryResult> {
        match statement {
            Statement::Union { left, right, all, operation } => {
                let left = self.execute_read_in_transaction(txn, *left)?;
                let right = self.execute_read_in_transaction(txn, *right)?;
                merge_set_results(left, right, operation, all)
            }
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
            Statement::SelectValues { columns, values } => select_values_result(columns, values),
            Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter } => {
                self.select_columns_in_transaction(
                    txn, table, columns, aliases, limit, offset, order, filter,
                )
            }
            Statement::CopyTo { table, columns } => {
                self.copy_to_in_transaction(txn, table, columns)
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let (cap, sort_rows) =
                    self.select_scan_options(&table, &order, limit, offset, filter.is_empty())?;
                let rows = self.scan_rows(txn, &table, &filter, cap)?;
                let mut rows = rows;
                if sort_rows {
                    rows.sort_by(|a, b| compare_order(&order, a, b));
                }
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Aggregate { table, func, field, column, filter } => {
                let rows = self.scan_rows(txn, &table, &filter, usize::MAX)?;
                Ok(QueryResult::Scalar {
                    label: func.label().to_string(),
                    value: aggregate_target_rows(&rows, func, field, column.as_deref()),
                })
            }
            Statement::GroupBy {
                table,
                select,
                group,
                group_column,
                filter,
                having,
                limit,
                offset,
                order,
            } => {
                let rows = self.scan_rows(txn, &table, &filter, usize::MAX)?;
                let mut groups: BTreeMap<Vec<u8>, Vec<Row>> = BTreeMap::new();
                for (pk, value) in rows {
                    let key = group_value(&pk, &value, group, group_column.as_deref());
                    groups.entry(key).or_default().push((pk, value));
                }
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut output_budget = MaterializationBudget::default();
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
                            SelectItem::Column(column) => {
                                record.insert(
                                    column.clone(),
                                    if group_key == &[0] {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::String(
                                            String::from_utf8_lossy(group_key).to_string(),
                                        )
                                    },
                                );
                            }
                            SelectItem::Agg(func, field, column) => {
                                record.insert(
                                    func.label().to_string(),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(&aggregate_target_rows(
                                            members,
                                            *func,
                                            *field,
                                            column.as_deref(),
                                        ))
                                        .to_string(),
                                    ),
                                );
                            }
                        }
                    }
                    let record = serde_json::Value::Object(record);
                    let encoded = record.to_string().into_bytes();
                    if predicates_match(&having, group_key, &encoded) {
                        let row = (group_key.clone(), encoded);
                        output_budget.add_row(&row)?;
                        out.push(row);
                    }
                }
                out.sort_by(|a, b| compare_order(&order, a, b));
                Ok(QueryResult::Rows { rows: out.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Join { left, right, join_type, limit, offset, order, filter } => {
                let left_rows = self.scan_rows(txn, &left, &[], usize::MAX)?;
                let right_rows = self.scan_rows(txn, &right, &[], usize::MAX)?;
                Ok(join_rows(left_rows, right_rows, join_type, &filter, &order, offset, limit)?)
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
        self.enforce_statement_privileges(&statement)?;
        let schema_statement = statement.is_schema_change();
        let previous_schema =
            if schema_statement { Some(self.try_schema_snapshot()?) } else { None };
        let previous_schema_dirty = self.schema_dirty.load(Ordering::SeqCst);
        match &statement {
            Statement::CreateSchema { .. }
            | Statement::CreateExtension { .. }
            | Statement::CreateSequence { .. }
            | Statement::CreateFunction { .. }
            | Statement::CreateView { .. }
            | Statement::CreateMaterializedView { .. }
            | Statement::CreateTrigger { .. }
            | Statement::CreatePolicy { .. }
            | Statement::DropPolicy { .. }
            | Statement::DropTrigger { .. }
            | Statement::DropView { .. }
            | Statement::DropMaterializedView { .. }
            | Statement::RefreshMaterializedView { .. }
            | Statement::DropSequence { .. }
            | Statement::AlterSequence { .. }
            | Statement::CreateRole { .. }
            | Statement::AlterRoleInheritance { .. }
            | Statement::DropRole { .. }
            | Statement::RoleMembershipChange { .. }
            | Statement::PrivilegeChange { .. }
            | Statement::DefaultPrivilegeChange { .. }
            | Statement::SequenceValue { .. }
            | Statement::AlterTableRls { .. } => {}
            Statement::CreateTable {
                table,
                columns,
                unique_constraints,
                checks,
                foreign_keys,
                named_constraints,
                if_not_exists,
            } => {
                self.create_table(
                    table.clone(),
                    columns.clone(),
                    unique_constraints.clone(),
                    checks.clone(),
                    foreign_keys.clone(),
                    named_constraints.clone(),
                    *if_not_exists,
                )?;
            }
            Statement::DropTable { .. } => {}
            Statement::DropIndex { .. } => {}
            Statement::TruncateTable { .. } => {}
            Statement::AlterTableDropConstraint { .. } => {}
            Statement::AlterTableAddColumn { .. } => {}
            Statement::AlterTableAddConstraint { .. } => {}
            Statement::AlterTableDropColumn { .. } => {}
            Statement::AlterTableRenameColumn { .. } => {}
            Statement::AlterTableColumn { .. } => {}
            Statement::CreateIndex { .. } => {}
            statement if statement.is_write() => self.ensure_table(statement.table())?,
            _ => {}
        }
        let sequence_state_statement = matches!(
            &statement,
            Statement::SequenceValue {
                operation: SequenceOperation::Nextval | SequenceOperation::Setval,
                ..
            }
        );
        let result = match statement {
            Statement::Returning { statement, fields } => {
                if returning_target_table(&statement).is_none() {
                    self.execute_returning(*statement, fields, isolation).await
                } else {
                    let mut txn = self.begin_with(isolation);
                    let (result, changes) = self
                        .execute_in_transaction(
                            &mut txn,
                            Statement::Returning { statement, fields },
                        )
                        .await?;
                    self.commit_transaction(txn, changes).await?;
                    Ok(result)
                }
            }
            statement => self.execute_with_base(statement, isolation).await,
        };
        if result.is_ok() && schema_statement {
            if let Some(previous) = previous_schema.as_ref() {
                self.validate_schema_mutation(previous, previous_schema_dirty)?;
            }
            if let (Some(hook), Some(previous)) =
                (self.schema_hook.clone(), previous_schema.clone())
            {
                let next = self.try_schema_snapshot()?;
                if next != previous {
                    if let Err(error) = hook(previous.clone(), next).await {
                        self.restore_schema_snapshot(previous)?;
                        self.persist_schema_if_configured()?;
                        return Err(error);
                    }
                }
            }
        }
        if result.is_ok() && (schema_statement || sequence_state_statement) {
            if let Err(error) = self.persist_schema_if_configured() {
                if let Some(previous) = previous_schema {
                    let rollback = self.restore_schema_snapshot(previous).and_then(|_| {
                        self.schema_dirty.store(true, Ordering::SeqCst);
                        self.persist_schema_if_configured()
                    });
                    if let Err(rollback_error) = rollback {
                        return Err(RymeError::Internal(format!(
                            "schema persistence failed: {error}; rollback failed: {rollback_error}"
                        )));
                    }
                }
                return Err(error);
            }
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
            Statement::InsertRow {
                table,
                columns,
                values,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                    Statement::InsertConflict {
                        table,
                        pk,
                        value,
                        target_columns: conflict_target,
                        assignments: conflict_update,
                        conflict_filter,
                        do_nothing: on_conflict_do_nothing,
                    }
                } else if on_conflict_do_nothing {
                    Statement::InsertIgnore { table, pk, value }
                } else if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_returning(statement, fields, isolation)).await
            }
            Statement::InsertSelect {
                table,
                columns,
                source_table,
                source_columns,
                filter,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let mut txn = self.begin_with(isolation);
                let (columns, rows) = self
                    .materialize_insert_select_rows(
                        &mut txn,
                        table.clone(),
                        columns,
                        source_table,
                        source_columns,
                        filter,
                    )
                    .await?;
                let (result, changes) = self
                    .execute_returning_rows_in_transaction(
                        &mut txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                        &fields,
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::InsertRows {
                table,
                columns,
                rows,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let mut txn = self.begin_with(isolation);
                let (result, changes) = self
                    .execute_returning_rows_in_transaction(
                        &mut txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                        &fields,
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Insert { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Insert { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Upsert { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Upsert { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::InsertIgnore { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let pk_for_result = pk.clone();
                let value_for_result = value.clone();
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::InsertIgnore { table, pk, value },
                    )
                    .await?;
                let result = if changes.is_empty() {
                    QueryResult::Returning { columns: returning_columns(&fields), rows: Vec::new() }
                } else {
                    changes
                        .last()
                        .and_then(|change| {
                            change.after.as_ref().map(|after| {
                                returning_result(&fields, change.pk.clone(), after.clone())
                            })
                        })
                        .unwrap_or_else(|| {
                            returning_result(&fields, pk_for_result, value_for_result)
                        })
                };
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::InsertConflict {
                table,
                pk,
                value,
                target_columns,
                assignments,
                conflict_filter,
                do_nothing,
            } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::InsertConflict {
                            table,
                            pk,
                            value,
                            target_columns,
                            assignments,
                            conflict_filter,
                            do_nothing,
                        },
                    )
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::UpdateRow { table, pk, assignments } => {
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let current = self
                    .manager
                    .get(&mut txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let value = self.materialize_update_row(&table, &pk, assignments, &current)?;
                let (_result, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Update { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Update { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Update { table, pk, value })
                    .await?;
                let result = changes
                    .last()
                    .and_then(|change| {
                        change.after.as_ref().map(|after| {
                            returning_result(&fields, change.pk.clone(), after.clone())
                        })
                    })
                    .unwrap_or_else(|| QueryResult::Returning {
                        columns: returning_columns(&fields),
                        rows: Vec::new(),
                    });
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::UpdateWhere { table, assignments, filter } => {
                let mut txn = self.begin_with(isolation);
                let statement = Statement::UpdateWhere { table, assignments, filter };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                let result = returning_changes(&fields, &changes);
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::UpdateFrom {
                table,
                assignments,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let mut txn = self.begin_with(isolation);
                let statement = Statement::UpdateFrom {
                    table,
                    assignments,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                let result = returning_changes(&fields, &changes);
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::Delete { table, pk } => {
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let value = self
                    .manager
                    .get(&mut txn, &key)?
                    .ok_or_else(|| RymeError::NotFound(String::from("row")))?;
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::Delete { table, pk: pk.clone() },
                    )
                    .await?;
                let result = returning_result(&fields, pk, value);
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::DeleteWhere { table, filter } => {
                let mut txn = self.begin_with(isolation);
                let statement = Statement::DeleteWhere { table, filter };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                let result = returning_changes(&fields, &changes);
                self.commit_transaction(txn, changes).await?;
                Ok(result)
            }
            Statement::DeleteUsing {
                table,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                let mut txn = self.begin_with(isolation);
                let statement = Statement::DeleteUsing {
                    table,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                let result = returning_changes(&fields, &changes);
                self.commit_transaction(txn, changes).await?;
                Ok(result)
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
            Statement::Distinct { statement, limit, offset } => {
                let result = Box::pin(self.execute_with_base(*statement, isolation)).await?;
                Ok(apply_distinct(result, offset, limit))
            }
            Statement::Union { left, right, all, operation } => {
                let mut txn = self.begin_read_only(isolation)?;
                let left = self.execute_read_in_transaction(&mut txn, *left)?;
                let right = self.execute_read_in_transaction(&mut txn, *right)?;
                merge_set_results(left, right, operation, all)
            }
            Statement::CreateSchema { .. } | Statement::CreateExtension { .. } => {
                Ok(QueryResult::Ok)
            }
            Statement::CreateSequence {
                name,
                if_not_exists,
                data_type,
                start,
                increment,
                min_value,
                max_value,
                cache,
                cycle,
            } => {
                self.install_sequence(
                    SequenceDefinition {
                        name,
                        data_type,
                        start,
                        increment,
                        min_value,
                        max_value,
                        cache,
                        cycle,
                        last_value: None,
                    },
                    if_not_exists,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateRole { name, login, superuser, createdb, createrole, inherit } => {
                self.install_role(RoleDefinition {
                    name,
                    login,
                    superuser,
                    createdb,
                    createrole,
                    inherit,
                })?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterRoleInheritance { name, inherit } => {
                self.alter_role_inheritance(&name, inherit)?;
                Ok(QueryResult::Ok)
            }
            Statement::DropRole { name, if_exists } => {
                self.drop_role(&name, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::RoleMembershipChange { grant, roles, members } => {
                self.apply_role_membership_change(grant, roles, members)?;
                Ok(QueryResult::Ok)
            }
            Statement::PrivilegeChange {
                grant,
                object_type,
                objects,
                privileges,
                grantees,
                grant_option,
            } => {
                self.apply_privilege_change(
                    grant,
                    object_type,
                    objects,
                    privileges,
                    grantees,
                    grant_option,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::DefaultPrivilegeChange {
                grant,
                object_type,
                schema,
                owner,
                privileges,
                grantees,
                grant_option,
            } => {
                self.apply_default_privilege_change(
                    grant,
                    object_type,
                    schema,
                    owner,
                    privileges,
                    grantees,
                    grant_option,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateFunction { name, body, language, replace } => {
                self.install_function(FunctionDefinition { name, body, language }, replace)?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateView { name, query, replace } => {
                self.install_view(
                    ViewDefinition {
                        name,
                        query: *query,
                        dependencies: Vec::new(),
                        materialized: false,
                        rows: Vec::new(),
                    },
                    replace,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateMaterializedView { name, query, replace, with_data } => {
                let query = *query;
                let mut txn = self.begin_read_only(isolation)?;
                let rows = if with_data {
                    query_result_rows(self.execute_read_in_transaction(&mut txn, query.clone())?)
                } else {
                    Vec::new()
                };
                self.install_view_definition(
                    ViewDefinition {
                        name,
                        query,
                        dependencies: Vec::new(),
                        materialized: true,
                        rows,
                    },
                    replace,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateTrigger { name, table, timing, events, function } => {
                self.install_trigger(TriggerDefinition { name, table, timing, events, function })?;
                Ok(QueryResult::Ok)
            }
            Statement::DropTrigger { name, table, if_exists } => {
                self.drop_trigger(&name, &table, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::DropView { name, if_exists } => {
                self.drop_view(&name, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::DropMaterializedView { name, if_exists } => {
                self.drop_view(&name, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::RefreshMaterializedView { name, concurrently: _ } => {
                let mut txn = self.begin_read_only(isolation)?;
                self.refresh_materialized_view(&mut txn, &name)?;
                Ok(QueryResult::Ok)
            }
            Statement::DropSequence { name, if_exists } => {
                self.drop_sequence(&name, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterSequence { name, restart, increment } => {
                self.alter_sequence(&name, restart, increment)?;
                Ok(QueryResult::Ok)
            }
            Statement::SequenceValue { operation, name, value, is_called } => {
                self.execute_sequence_value(operation, name, value, is_called)
            }
            Statement::CreatePolicy { name, table, command, roles, using, check } => {
                self.install_policy(
                    &name,
                    &table,
                    &command,
                    &roles,
                    using.as_deref(),
                    check.as_deref(),
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::DropPolicy { name, table, if_exists } => {
                self.drop_policy(&name, &table, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableRls { table, enabled, forced } => {
                self.set_rls_state(&table, enabled, forced)?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateTable { .. } => Ok(QueryResult::Ok),
            Statement::DropTable { table, if_exists } => {
                self.drop_table(table, if_exists).await?;
                Ok(QueryResult::Ok)
            }
            Statement::DropIndex { name, if_exists } => {
                self.drop_index(name, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::TruncateTable { table, restart_identity, cascade } => {
                self.truncate_table(table, restart_identity, cascade).await?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableDropConstraint { table, constraint, if_exists } => {
                self.drop_constraint(table, constraint, if_exists)?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableAddColumn { table, column, if_not_exists } => {
                self.alter_table_add_column(table, column, if_not_exists).await?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableAddConstraint { table, constraint } => {
                self.add_table_constraint(table, constraint).await?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableDropColumn { table, column, if_exists } => {
                self.alter_table_drop_column(table, column, if_exists).await?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableRenameColumn { table, from, to } => {
                self.alter_table_rename_column(table, from, to).await?;
                Ok(QueryResult::Ok)
            }
            Statement::AlterTableColumn { table, column, alteration } => {
                self.alter_table_column(table, column, alteration).await?;
                Ok(QueryResult::Ok)
            }
            Statement::CreateIndex {
                name,
                table,
                field,
                column,
                columns,
                unique,
                if_not_exists,
            } => {
                self.create_index(
                    IndexDefinition { name, table, field, column, columns, unique },
                    if_not_exists,
                )?;
                Ok(QueryResult::Ok)
            }
            Statement::CopyFrom { table, columns, rows } => {
                self.reject_if_read_only()?;
                if !columns.is_empty() {
                    return Err(RymeError::InvalidArgument(String::from(
                        "COPY column rows require the wire COPY path",
                    )));
                }
                self.bulk_upsert(table, rows).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Explain { plan, inner, analyze } => {
                if !analyze {
                    return Ok(QueryResult::Row { pk: b"plan".to_vec(), value: plan.into_bytes() });
                }
                let started = std::time::Instant::now();
                let result = Box::pin(self.execute_with(*inner, isolation)).await?;
                Ok(explain_analyze_result(plan, &result, 0, started.elapsed()))
            }
            Statement::InsertRow {
                table,
                columns,
                values,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                let (pk, value) = self.materialize_insert_row(&table, columns, values)?;
                let statement = if !conflict_target.is_empty() || !conflict_update.is_empty() {
                    Statement::InsertConflict {
                        table,
                        pk,
                        value,
                        target_columns: conflict_target,
                        assignments: conflict_update,
                        conflict_filter,
                        do_nothing: on_conflict_do_nothing,
                    }
                } else if on_conflict_do_nothing {
                    Statement::InsertIgnore { table, pk, value }
                } else if upsert {
                    Statement::Upsert { table, pk, value }
                } else {
                    Statement::Insert { table, pk, value }
                };
                Box::pin(self.execute_with_base(statement, isolation)).await
            }
            Statement::InsertSelect {
                table,
                columns,
                source_table,
                source_columns,
                filter,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let (columns, rows) = self
                    .materialize_insert_select_rows(
                        &mut txn,
                        table.clone(),
                        columns,
                        source_table,
                        source_columns,
                        filter,
                    )
                    .await?;
                let changes = self
                    .execute_insert_rows_in_transaction(
                        &mut txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::InsertRows {
                table,
                columns,
                rows,
                upsert,
                on_conflict_do_nothing,
                conflict_target,
                conflict_update,
                conflict_filter,
            } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let changes = self
                    .execute_insert_rows_in_transaction(
                        &mut txn,
                        table,
                        columns,
                        rows,
                        upsert,
                        on_conflict_do_nothing,
                        conflict_target,
                        conflict_update,
                        conflict_filter,
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::InsertIgnore { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::InsertIgnore { table, pk, value },
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::InsertConflict {
                table,
                pk,
                value,
                target_columns,
                assignments,
                conflict_filter,
                do_nothing,
            } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::InsertConflict {
                            table,
                            pk,
                            value,
                            target_columns,
                            assignments,
                            conflict_filter,
                            do_nothing,
                        },
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Insert { table, pk, value } => {
                self.reject_if_read_only()?;
                self.enforce_rls(&table, &value, RlsCommand::Insert)?;
                self.enforce_checks(&table, &pk, &value)?;
                let mut txn = self.begin_with(isolation);
                self.enforce_foreign_keys_async(&mut txn, &table, &pk, &value).await?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(&mut txn, &key).await?;
                if before.is_some() {
                    return Err(RymeError::Conflict(String::from("exists")));
                }
                self.check_unique(&mut txn, &table, &pk, &value)?;
                let after = self.realtime.as_ref().map(|_| value.clone());
                self.manager.put(&mut txn, key, value.clone());
                let _pending_index_commit = self.begin_index_commit(vec![table.clone()]);
                let commit_ts = self.manager.commit(txn).await?;
                self.apply_index_changes(
                    &[TransactionChange {
                        table: table.clone(),
                        pk: pk.clone(),
                        previous_pk: None,
                        op: Operation::Insert,
                        before: before.clone(),
                        after: Some(value.clone()),
                    }],
                    commit_ts,
                );
                if let Some(after) = after {
                    self.emit(&table, pk, Operation::Insert, None, Some(after), commit_ts)?;
                }
                Ok(QueryResult::Ok)
            }
            Statement::Upsert { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Upsert { table, pk, value })
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::SelectByKey { table, pk } => {
                let mut txn = self.begin_read_only(isolation)?;
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                match self.manager.get(&mut txn, &key)? {
                    Some(value) if self.rls_allows(&table, &value) => {
                        Ok(QueryResult::Row { pk, value })
                    }
                    None => Ok(QueryResult::Rows { rows: Vec::new() }),
                    Some(_) => Ok(QueryResult::Rows { rows: Vec::new() }),
                }
            }
            Statement::SelectValues { columns, values } => select_values_result(columns, values),
            Statement::SelectColumns { table, columns, aliases, limit, offset, order, filter } => {
                let mut txn = self.begin_read_only(isolation)?;
                self.select_columns_in_transaction(
                    &mut txn, table, columns, aliases, limit, offset, order, filter,
                )
            }
            Statement::CopyTo { table, columns } => {
                let mut txn = self.begin_read_only(isolation)?;
                self.copy_to_in_transaction(&mut txn, table, columns)
            }
            Statement::SelectScan { table, limit, offset, order, filter } => {
                let mut txn = self.begin_read_only(isolation)?;
                let (cap, sort_rows) =
                    self.select_scan_options(&table, &order, limit, offset, filter.is_empty())?;
                let rows = self.scan_rows(&mut txn, &table, &filter, cap)?;
                let mut rows = rows;
                if sort_rows {
                    rows.sort_by(|a, b| compare_order(&order, a, b));
                }
                Ok(QueryResult::Rows { rows: rows.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Aggregate { table, func, field, column, filter } => {
                let mut txn = self.begin_read_only(isolation)?;
                let rows = self.scan_rows(&mut txn, &table, &filter, usize::MAX)?;
                Ok(QueryResult::Scalar {
                    label: func.label().to_string(),
                    value: aggregate_target_rows(&rows, func, field, column.as_deref()),
                })
            }
            Statement::GroupBy {
                table,
                select,
                group,
                group_column,
                filter,
                having,
                limit,
                offset,
                order,
            } => {
                let mut txn = self.begin_read_only(isolation)?;
                let rows = self.scan_rows(&mut txn, &table, &filter, usize::MAX)?;
                let mut groups: BTreeMap<Vec<u8>, Vec<Row>> = BTreeMap::new();
                for (pk, value) in rows {
                    let key = group_value(&pk, &value, group, group_column.as_deref());
                    groups.entry(key).or_default().push((pk, value));
                }
                let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                let mut output_budget = MaterializationBudget::default();
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
                            SelectItem::Column(column) => {
                                record.insert(
                                    column.clone(),
                                    if group_key == &[0] {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::String(
                                            String::from_utf8_lossy(group_key).to_string(),
                                        )
                                    },
                                );
                            }
                            SelectItem::Agg(func, field, column) => {
                                record.insert(
                                    func.label().to_string(),
                                    serde_json::Value::String(
                                        String::from_utf8_lossy(&aggregate_target_rows(
                                            members,
                                            *func,
                                            *field,
                                            column.as_deref(),
                                        ))
                                        .to_string(),
                                    ),
                                );
                            }
                        }
                    }
                    let record = serde_json::Value::Object(record);
                    let encoded = record.to_string().into_bytes();
                    if predicates_match(&having, group_key, &encoded) {
                        let row = (group_key.clone(), encoded);
                        output_budget.add_row(&row)?;
                        out.push(row);
                    }
                }
                out.sort_by(|a, b| compare_order(&order, a, b));
                Ok(QueryResult::Rows { rows: out.into_iter().skip(offset).take(limit).collect() })
            }
            Statement::Join { left, right, join_type, limit, offset, order, filter } => {
                let mut txn = self.begin_read_only(isolation)?;
                let left_rows = self.scan_rows(&mut txn, &left, &[], usize::MAX)?;
                let right_rows = self.scan_rows(&mut txn, &right, &[], usize::MAX)?;
                Ok(join_rows(left_rows, right_rows, join_type, &filter, &order, offset, limit)?)
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
            Statement::UpdateWhere { table, assignments, filter } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::UpdateWhere { table, assignments, filter },
                    )
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::UpdateFrom {
                table,
                assignments,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let statement = Statement::UpdateFrom {
                    table,
                    assignments,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Update { table, pk, value } => {
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::Update { table, pk, value })
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Delete { table, pk } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let key = RecordKey::new(&self.tenant, &self.database, &table, &pk);
                let before = self.manager.get_async(&mut txn, &key).await?;
                if before.is_none() {
                    return Err(RymeError::NotFound(String::from("row")));
                }
                self.enforce_rls(
                    &table,
                    before.as_deref().unwrap_or_default(),
                    RlsCommand::Delete,
                )?;
                let mut changes = Vec::new();
                self.delete_row_with_references_async(
                    &mut txn,
                    &table,
                    &pk,
                    before.as_deref().unwrap_or_default(),
                    &mut changes,
                    &mut BTreeSet::new(),
                )
                .await?;
                self.manager.delete(&mut txn, key);
                changes.push(TransactionChange {
                    table,
                    pk,
                    previous_pk: None,
                    op: Operation::Delete,
                    before,
                    after: None,
                });
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::DeleteWhere { table, filter } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let (_, changes) = self
                    .execute_in_transaction_base(&mut txn, Statement::DeleteWhere { table, filter })
                    .await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::DeleteUsing {
                table,
                source_table,
                target_column,
                source_column,
                filter,
                source_filter,
            } => {
                self.reject_if_read_only()?;
                let mut txn = self.begin_with(isolation);
                let statement = Statement::DeleteUsing {
                    table,
                    source_table,
                    target_column,
                    source_column,
                    filter,
                    source_filter,
                };
                let (_, changes) = self.execute_in_transaction_base(&mut txn, statement).await?;
                self.commit_transaction(txn, changes).await?;
                Ok(QueryResult::Ok)
            }
            Statement::Returning { .. } => {
                Err(RymeError::InvalidArgument(String::from("nested RETURNING statement")))
            }
        }
    }

    pub async fn bulk_upsert(&self, table: String, rows: Vec<(Vec<u8>, Vec<u8>)>) -> Result<usize> {
        self.reject_if_read_only()?;
        if !self.role_is_superuser()
            && (!self.has_table_privilege(&table, "INSERT")
                || !self.has_table_privilege(&table, "UPDATE"))
        {
            return Err(RymeError::Forbidden);
        }
        if rows.len() > 10000 {
            return Err(RymeError::InvalidArgument(String::from("batch too large")));
        }
        self.ensure_table(&table)?;
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
                let (_, mut changes) = self
                    .execute_in_transaction_base(
                        &mut txn,
                        Statement::Upsert {
                            table: table.clone(),
                            pk: pk.clone(),
                            value: value.clone(),
                        },
                    )
                    .await?;
                staged.append(&mut changes);
            }
            self.check_transaction_uniqueness(&staged)?;
            let _pending_index_commit =
                self.begin_index_commit(staged.iter().map(|change| change.table.clone()).collect());
            let commit_ts = self.manager.commit(txn).await?;
            self.apply_index_changes(&staged, commit_ts);
            for change in staged {
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
        self.persist_schema_if_configured()?;
        Ok(rows.len())
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        let statement = parse(sql)?;
        self.enforce_statement_privileges(&statement)?;
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
    fn json_projection_parses_only_selected_roots_and_keeps_projection_semantics() {
        let projection = vec![
            ProjectionSource::Json(String::from("payload->'meta'->>'role'")),
            ProjectionSource::Json(String::from("STATE")),
            ProjectionSource::Json(String::from("field name")),
        ];
        let value = br#"{"discard":{"nested":[1,2,3]},"payload":{"meta":{"role":"first"}},"payload":{"meta":{"role":"admin"}},"state":"ready","field\u0020name":"escaped"}"#;
        let selected = projected_json_object(value, &projection).unwrap();
        assert_eq!(selected.len(), 3);
        assert!(!selected.contains_key("discard"));

        let row = Executor::<TxnManager>::project_row(&[], &projection, true, b"pk", value);
        assert_eq!(row, vec![b"admin".to_vec(), b"ready".to_vec(), b"escaped".to_vec()]);
        assert_eq!(
            Executor::<TxnManager>::project_row(&[], &projection, true, b"pk", b"not-json"),
            vec![b"not-json".to_vec(); projection.len()]
        );
        assert!(projected_json_object(b"[]", &projection).is_none());
    }

    #[test]
    fn sql_parser_bounds_bytes_tokens_and_recursive_nesting() {
        let oversized = "x".repeat(MAX_SQL_STATEMENT_BYTES + 1);
        assert!(matches!(
            parse(&oversized),
            Err(RymeError::Overload(message)) if message == "SQL statement bytes"
        ));

        let mut token_heavy = String::from("SELECT");
        for _ in 0..MAX_SQL_TOKENS {
            token_heavy.push_str(" x");
        }
        assert!(matches!(
            parse(&token_heavy),
            Err(RymeError::Overload(message)) if message == "SQL statement tokens"
        ));

        let within_depth = (0..MAX_SQL_PARSE_DEPTH - 1)
            .fold(String::from("SELECT * FROM source"), |query, level| {
                format!("WITH c{level} AS ({query}) SELECT * FROM c{level}")
            });
        assert!(parse(&within_depth).is_ok());

        let nested = (0..MAX_SQL_PARSE_DEPTH + 1)
            .fold(String::from("SELECT * FROM source"), |query, level| {
                format!("WITH c{level} AS ({query}) SELECT * FROM c{level}")
            });
        assert!(matches!(
            parse(&nested),
            Err(RymeError::Overload(message)) if message == "SQL parse nesting"
        ));
        assert!(parse("SELECT 1").is_ok());
    }

    #[test]
    fn query_materialization_budget_is_bounded_without_partial_accounting() {
        let row = (b"key".to_vec(), b"value".to_vec());
        let mut budget = MaterializationBudget::default();
        for _ in 0..MAX_QUERY_MATERIALIZED_ROWS {
            budget.add_row(&row).unwrap();
        }
        assert!(
            matches!(budget.add_row(&row), Err(RymeError::Overload(message)) if message == "query materialization")
        );
        assert_eq!(budget.rows, MAX_QUERY_MATERIALIZED_ROWS);

        let mut byte_budget =
            MaterializationBudget { rows: 0, bytes: MAX_QUERY_MATERIALIZED_BYTES - 1 };
        assert!(byte_budget.add_row(&row).is_err());
        assert_eq!(byte_budget.bytes, MAX_QUERY_MATERIALIZED_BYTES - 1);

        let small = vec![(b"key".to_vec(), b"value".to_vec())];
        assert!(ensure_materialized_rows(&small).is_ok());
    }

    #[tokio::test]
    async fn schema_hook_coordinates_autocommit_ddl_and_rolls_back_on_failure() {
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let captured = snapshots.clone();
        let executor = Executor::new(String::from("tenant"), String::from("database"))
            .with_schema_hook(Arc::new(move |before, after| {
                let captured = captured.clone();
                Box::pin(async move {
                    assert_ne!(before, after);
                    captured.lock().unwrap().push(after);
                    Ok(())
                })
            }));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        assert_eq!(snapshots.lock().unwrap().len(), 1);
        assert_eq!(executor.catalog_tables(), vec![String::from("messages")]);

        let failing = Executor::new(String::from("tenant"), String::from("database"))
            .with_schema_hook(Arc::new(|_, _| {
                Box::pin(async { Err(RymeError::Unavailable(String::from("schema quorum"))) })
            }));
        assert!(matches!(
            failing
                .execute(parse("CREATE TABLE rejected (id TEXT PRIMARY KEY)").unwrap())
                .await,
            Err(RymeError::Unavailable(message)) if message == "schema quorum"
        ));
        assert!(failing.catalog_tables().is_empty());
    }

    #[test]
    fn explain_without_analyze_does_not_coordinate_schema_ddl() {
        assert!(!parse("EXPLAIN CREATE TABLE messages (id TEXT PRIMARY KEY)")
            .unwrap()
            .is_schema_change());
        assert!(parse("EXPLAIN ANALYZE CREATE TABLE messages (id TEXT PRIMARY KEY)")
            .unwrap()
            .is_schema_change());
    }

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
            matches!(insert, Statement::InsertRow { ref table, ref columns, ref values, upsert: false, .. }
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

        let ignore =
            parse("INSERT INTO users (id, value) VALUES ('1', 'ignored') ON CONFLICT DO NOTHING")
                .unwrap();
        assert!(matches!(
            ignore,
            Statement::InsertRow { upsert: false, on_conflict_do_nothing: true, .. }
        ));
        executor.execute(ignore).await.unwrap();
        let row = executor.execute(parse("SELECT * FROM users KEY '1'").unwrap()).await.unwrap();
        assert!(matches!(row, QueryResult::Row { value, .. } if value == b"grace"));

        let returned = executor
            .execute(
                parse(
                    "INSERT INTO users (id, value) VALUES ('1', 'ignored-again') ON CONFLICT DO NOTHING RETURNING *",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(returned, QueryResult::Returning { rows, .. } if rows.is_empty()));
        executor
            .execute(
                parse(
                    "INSERT INTO users (id, value) VALUES ('1', 'ignored'), ('2', 'ada') ON CONFLICT DO NOTHING",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(executor.execute(parse("SELECT * FROM users KEY '2'").unwrap()).await.unwrap(), QueryResult::Row { value, .. } if value == b"ada")
        );
    }

    #[tokio::test]
    async fn conflict_targets_use_unique_columns_and_return_updated_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO accounts (id, email, name) VALUES ('1', 'a@example.com', 'Ada')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let returned = executor
            .execute(
                parse(
                    "INSERT INTO accounts (id, email, name) VALUES ('2', 'a@example.com', 'Grace') ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id, name",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            returned,
            QueryResult::Returning { ref columns, ref rows }
                if columns == &[String::from("id"), String::from("name")]
                    && rows == &vec![vec![b"1".to_vec(), b"Grace".to_vec()]]
        ));
        match executor.execute(parse("SELECT * FROM accounts KEY '1'").unwrap()).await.unwrap() {
            QueryResult::Row { value, .. } => {
                let row: serde_json::Value = serde_json::from_slice(&value).unwrap();
                assert_eq!(
                    row.get("name"),
                    Some(&serde_json::Value::String(String::from("Grace")))
                );
            }
            _ => panic!("expected updated account"),
        }
        assert!(matches!(
            executor.execute(parse("SELECT * FROM accounts KEY '2'").unwrap()).await.unwrap(),
            QueryResult::Rows { rows } if rows.is_empty()
        ));

        executor
            .execute(
                parse(
                    "INSERT INTO accounts (id, email, name) VALUES ('3', 'a@example.com', 'Ignored') ON CONFLICT (email) DO NOTHING",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        match executor.execute(parse("SELECT * FROM accounts KEY '1'").unwrap()).await.unwrap() {
            QueryResult::Row { value, .. } => {
                let row: serde_json::Value = serde_json::from_slice(&value).unwrap();
                assert_eq!(
                    row.get("name"),
                    Some(&serde_json::Value::String(String::from("Grace")))
                );
            }
            _ => panic!("expected preserved account"),
        }
    }

    #[tokio::test]
    async fn conflict_updates_honor_where_and_excluded_predicates() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE versions (id TEXT PRIMARY KEY, email TEXT UNIQUE, version INTEGER, name TEXT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO versions (id, email, version, name) VALUES ('1', 'a@example.com', 1, 'Ada')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let stale = executor
            .execute(
                parse(
                    "INSERT INTO versions (id, email, version, name) VALUES ('2', 'a@example.com', 1, 'Stale') ON CONFLICT (email) DO UPDATE SET version = EXCLUDED.version, name = EXCLUDED.name WHERE version < EXCLUDED.version RETURNING *",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(stale, QueryResult::Returning { rows, .. } if rows.is_empty()));

        let fresh = executor
            .execute(
                parse(
                    "INSERT INTO versions (id, email, version, name) VALUES ('3', 'a@example.com', 2, 'Grace') ON CONFLICT (email) DO UPDATE SET version = EXCLUDED.version, name = EXCLUDED.name WHERE version < EXCLUDED.version RETURNING id, version, name",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            fresh,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"2".to_vec(), b"Grace".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn update_and_conflict_assignments_support_atomic_expressions() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE counters (id TEXT PRIMARY KEY, name TEXT UNIQUE, count INTEGER)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO counters (id, name, count) VALUES ('1', 'hits', 1)").unwrap(),
            )
            .await
            .unwrap();

        let updated = executor
            .execute(
                parse("UPDATE counters SET count = count + 1 WHERE id = '1' RETURNING count")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            updated,
            QueryResult::Returning { ref rows, .. } if rows == &vec![vec![b"2".to_vec()]]
        ));

        executor
            .execute(
                parse("INSERT INTO counters (id, name, count) VALUES ('2', 'hits', 3) ON CONFLICT (name) DO UPDATE SET count = count + EXCLUDED.count")
                    .unwrap(),
            )
            .await
            .unwrap();
        match executor.execute(parse("SELECT * FROM counters KEY '1'").unwrap()).await.unwrap() {
            QueryResult::Row { value, .. } => {
                let row: serde_json::Value = serde_json::from_slice(&value).unwrap();
                assert_eq!(row.get("count"), Some(&serde_json::Value::Number(5.into())));
            }
            _ => panic!("expected counter row"),
        }
    }

    #[tokio::test]
    async fn default_values_use_identity_and_column_defaults() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let statement = parse("INSERT INTO events DEFAULT VALUES RETURNING id, state").unwrap();
        assert!(matches!(
            statement,
            Statement::Returning { ref statement, .. }
                if matches!(statement.as_ref(), Statement::InsertRow { columns, values, .. } if columns.is_empty() && values.is_empty())
        ));
        executor
            .execute(
                parse("CREATE TABLE events (id SERIAL PRIMARY KEY, state TEXT DEFAULT 'queued')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let first = executor.execute(statement).await.unwrap();
        assert!(matches!(
            first,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"queued".to_vec()]]
        ));
        executor.execute(parse("INSERT INTO events DEFAULT VALUES").unwrap()).await.unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT * FROM events KEY '2'").unwrap()).await.unwrap(),
            QueryResult::Row { .. }
        ));
    }

    #[tokio::test]
    async fn insert_select_copies_projected_rows_transactionally() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE source (id TEXT PRIMARY KEY, payload TEXT, active BOOLEAN)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE archive (id TEXT PRIMARY KEY, payload TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO source (id, payload, active) VALUES ('1', 'keep', true), ('2', 'skip', false)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(
                parse("INSERT INTO archive (id, payload) SELECT id, payload FROM source WHERE active = true RETURNING id, payload")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"keep".to_vec()]]
        ));
        assert!(matches!(
            executor.execute(parse("SELECT * FROM archive KEY '2'").unwrap()).await.unwrap(),
            QueryResult::Rows { rows } if rows.is_empty()
        ));
    }

    #[tokio::test]
    async fn simple_ctes_feed_selects_and_insert_selects() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT, score INTEGER)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE archive (id TEXT PRIMARY KEY, score INTEGER)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO accounts (id, status, score) VALUES ('1', 'active', 4), ('2', 'inactive', 9), ('3', 'active', 1)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let selected = executor
            .execute(
                parse("WITH active AS (SELECT * FROM accounts WHERE status = 'active') SELECT id, score FROM active WHERE score > 1")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            selected,
            QueryResult::Table { rows, .. }
                if rows == vec![vec![b"1".to_vec(), b"4".to_vec()]]
        ));

        executor
            .execute(
                parse("WITH active AS (SELECT * FROM accounts WHERE status = 'active') INSERT INTO archive (id, score) SELECT id, score FROM active WHERE score > 1 RETURNING id, score")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT id, score FROM archive").unwrap()).await.unwrap(),
            QueryResult::Table { rows, .. }
                if rows == vec![vec![b"1".to_vec(), b"4".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn multiple_simple_ctes_resolve_in_dependency_order() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE events (id TEXT PRIMARY KEY, state TEXT, score INTEGER)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO events (id, state, score) VALUES ('1', 'ready', 8), ('2', 'ready', 3), ('3', 'done', 9)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let selected = executor
            .execute(
                parse("WITH ready AS (SELECT * FROM events WHERE state = 'ready'), ranked AS (SELECT * FROM ready WHERE score > 5) SELECT id, score FROM ranked")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            selected,
            QueryResult::Table { rows, .. }
                if rows == vec![vec![b"1".to_vec(), b"8".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn insert_select_accepts_qualified_source_columns() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE source (id TEXT PRIMARY KEY, payload TEXT, active BOOLEAN)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE archive (id TEXT PRIMARY KEY, payload TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO source (id, payload, active) VALUES ('1', 'keep', true), ('2', 'skip', false)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let statement = parse(
            "INSERT INTO archive (id, payload) SELECT s.id, s.payload FROM source AS s WHERE s.active = true RETURNING archive.id, archive.payload",
        )
        .unwrap();
        let result = executor.execute(statement).await.unwrap();
        assert!(matches!(
            result,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"keep".to_vec()]]
        ));
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

        let statement =
            parse("SELECT payload AS body, count AS total FROM events WHERE id = 'e1'").unwrap();
        assert!(matches!(statement, Statement::SelectColumns { ref columns, ref aliases, .. }
            if columns == &[String::from("payload"), String::from("count")]
                && aliases == &[Some(String::from("body")), Some(String::from("total"))]));
        let result = executor.execute(statement).await.unwrap();
        assert!(matches!(result, QueryResult::Table { ref columns, ref rows }
            if columns == &[String::from("body"), String::from("total")]
                && rows == &vec![vec![b"hello".to_vec(), b"3".to_vec()]]));

        executor
            .execute(
                parse("INSERT INTO events (id, payload, count) VALUES ('e2', NULL, 4)").unwrap(),
            )
            .await
            .unwrap();
        let result = executor
            .execute(parse("SELECT payload FROM events WHERE id = 'e2'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { ref rows, .. }
            if rows == &vec![vec![SQL_NULL_SENTINEL.to_vec()]]));
        let returned = executor
            .execute(
                parse("UPDATE events SET payload = NULL WHERE id = 'e2' RETURNING payload")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(returned, QueryResult::Returning { ref rows, .. }
            if rows == &vec![vec![SQL_NULL_SENTINEL.to_vec()]]));

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
    async fn views_are_readable_and_persist_in_schema_snapshots() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT, active BOOLEAN)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, body, active) VALUES ('1', 'hello', true), ('2', 'hidden', false)").unwrap())
            .await
            .unwrap();

        let create =
            parse("CREATE VIEW active_messages AS SELECT * FROM messages WHERE active = true")
                .unwrap();
        assert!(
            matches!(create, Statement::CreateView { ref name, .. } if name == "active_messages")
        );
        executor.execute(create).await.unwrap();

        let rows = executor.execute(parse("SELECT * FROM active_messages").unwrap()).await.unwrap();
        assert!(matches!(rows, QueryResult::Rows { ref rows } if rows.len() == 1));
        let projected = executor
            .execute(parse("SELECT body FROM active_messages WHERE id = '1'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(projected, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"hello".to_vec()]])
        );

        let snapshot = executor.schema_snapshot();
        assert!(snapshot.views.contains_key("active_messages"));
        executor.execute(parse("DROP VIEW active_messages").unwrap()).await.unwrap();
        executor.restore_schema_snapshot(snapshot).unwrap();
        let restored =
            executor.execute(parse("SELECT body FROM active_messages").unwrap()).await.unwrap();
        assert!(
            matches!(restored, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"hello".to_vec()]])
        );
    }

    #[tokio::test]
    async fn views_validate_dependencies_and_protect_referenced_objects() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        assert!(matches!(
            executor.execute(parse("CREATE VIEW missing_view AS SELECT * FROM missing_table").unwrap()).await,
            Err(RymeError::NotFound(message)) if message.contains("missing_table")
        ));

        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE VIEW message_view AS SELECT * FROM messages").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE VIEW nested_view AS SELECT * FROM message_view").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            executor
                .execute(parse("CREATE OR REPLACE VIEW message_view AS SELECT * FROM nested_view").unwrap())
                .await,
            Err(RymeError::InvalidArgument(message)) if message.contains("recursive view")
        ));

        let snapshot = executor.schema_snapshot();
        assert_eq!(snapshot.views["message_view"].dependencies, vec![String::from("messages")]);
        assert_eq!(snapshot.views["nested_view"].dependencies, vec![String::from("message_view")]);
        assert_eq!(
            executor.view_dependencies("nested_view"),
            Ok(Some(vec![String::from("messages")]))
        );
        assert!(matches!(
            executor.execute(parse("DROP TABLE messages").unwrap()).await,
            Err(RymeError::Conflict(message)) if message.contains("message_view")
        ));
        assert!(matches!(
            executor.execute(parse("DROP VIEW message_view").unwrap()).await,
            Err(RymeError::Conflict(message)) if message.contains("nested_view")
        ));

        executor.execute(parse("DROP VIEW nested_view").unwrap()).await.unwrap();
        executor.execute(parse("DROP VIEW message_view").unwrap()).await.unwrap();
        executor.execute(parse("DROP TABLE messages").unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn materialized_views_refresh_and_restore_cached_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, body) VALUES ('1', 'hello')").unwrap())
            .await
            .unwrap();

        let create =
            parse("CREATE MATERIALIZED VIEW message_snapshot AS SELECT * FROM messages WITH DATA")
                .unwrap();
        assert!(matches!(
            create,
            Statement::CreateMaterializedView { ref name, with_data: true, .. }
                if name == "message_snapshot"
        ));
        assert!(matches!(
            parse("CREATE MATERIALIZED VIEW empty_snapshot AS SELECT * FROM messages WITH NO DATA")
                .unwrap(),
            Statement::CreateMaterializedView { with_data: false, .. }
        ));
        executor.execute(create).await.unwrap();
        assert!(matches!(
            executor
                .execute(parse("SELECT * FROM message_snapshot").unwrap())
                .await
                .unwrap(),
            QueryResult::Rows { rows } if rows.len() == 1
        ));

        executor
            .execute(parse("INSERT INTO messages (id, body) VALUES ('2', 'world')").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            executor
                .execute(parse("SELECT * FROM message_snapshot").unwrap())
                .await
                .unwrap(),
            QueryResult::Rows { rows } if rows.len() == 1
        ));

        executor
            .execute(parse("REFRESH MATERIALIZED VIEW CONCURRENTLY message_snapshot").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            executor
                .execute(parse("SELECT * FROM message_snapshot").unwrap())
                .await
                .unwrap(),
            QueryResult::Rows { rows } if rows.len() == 2
        ));

        let snapshot = executor.schema_snapshot();
        executor.execute(parse("DROP MATERIALIZED VIEW message_snapshot").unwrap()).await.unwrap();
        executor.restore_schema_snapshot(snapshot).unwrap();
        assert!(matches!(
            executor
                .execute(parse("SELECT * FROM message_snapshot").unwrap())
                .await
                .unwrap(),
            QueryResult::Rows { rows } if rows.len() == 2
        ));
    }

    #[tokio::test]
    async fn sequences_support_nextval_currval_setval_and_restore() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE SEQUENCE public.events_id_seq AS bigint START WITH 10 INCREMENT BY 2 CACHE 1").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE SEQUENCE public.default_id_seq START WITH 100").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE events (id BIGINT PRIMARY KEY DEFAULT nextval('public.default_id_seq'::regclass), body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (body) VALUES ('hello')").unwrap())
            .await
            .unwrap();
        let generated =
            executor.execute(parse("SELECT id FROM events WHERE id = 100").unwrap()).await.unwrap();
        assert!(
            matches!(generated, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"100".to_vec()]])
        );
        let next = executor
            .execute(parse("SELECT nextval('public.events_id_seq'::regclass)").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(next, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"10".to_vec()]])
        );
        let current = executor
            .execute(parse("SELECT currval('public.events_id_seq')").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(current, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"10".to_vec()]])
        );

        let set = executor
            .execute(parse("SELECT setval('public.events_id_seq', 50, false)").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(set, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"50".to_vec()]])
        );
        let next =
            executor.execute(parse("SELECT nextval('events_id_seq')").unwrap()).await.unwrap();
        assert!(
            matches!(next, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"50".to_vec()]])
        );

        executor
            .execute(parse("ALTER SEQUENCE public.events_id_seq RESTART WITH 7").unwrap())
            .await
            .unwrap();
        let restarted =
            executor.execute(parse("SELECT nextval('events_id_seq')").unwrap()).await.unwrap();
        assert!(
            matches!(restarted, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"7".to_vec()]])
        );

        let snapshot = executor.schema_snapshot();
        assert_eq!(snapshot.sequences["public.events_id_seq"].last_value, Some(7));
        executor.execute(parse("DROP SEQUENCE public.events_id_seq").unwrap()).await.unwrap();
        executor.restore_schema_snapshot(snapshot).unwrap();
        let restored =
            executor.execute(parse("SELECT currval('events_id_seq')").unwrap()).await.unwrap();
        assert!(
            matches!(restored, QueryResult::Table { ref rows, .. } if rows == &vec![vec![b"7".to_vec()]])
        );
    }

    #[tokio::test]
    async fn roles_and_privileges_persist_and_revoke_cleanly() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE ROLE app_reader LOGIN").unwrap()).await.unwrap();
        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE public.messages TO app_reader, authenticated WITH GRANT OPTION").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT USAGE ON SCHEMA public TO authenticated").unwrap())
            .await
            .unwrap();
        let snapshot = executor.schema_snapshot();
        assert!(snapshot.roles.contains_key("app_reader"));
        assert!(snapshot.privileges.iter().any(|grant| {
            grant.object == "public.messages"
                && grant.privilege == "SELECT"
                && grant.grantee == "app_reader"
                && grant.grant_option
        }));
        assert!(snapshot.privileges.iter().any(|grant| {
            grant.object == "public"
                && grant.object_type == "SCHEMA"
                && grant.grantee == "authenticated"
        }));

        executor
            .execute(parse("REVOKE SELECT ON TABLE public.messages FROM app_reader").unwrap())
            .await
            .unwrap();
        assert!(!executor.schema_snapshot().privileges.iter().any(|grant| {
            grant.object == "public.messages"
                && grant.privilege == "SELECT"
                && grant.grantee == "app_reader"
        }));
        executor.execute(parse("DROP ROLE app_reader").unwrap()).await.unwrap();
        executor.restore_schema_snapshot(snapshot).unwrap();
        assert!(executor.schema_snapshot().roles.contains_key("app_reader"));
    }

    #[tokio::test]
    async fn builtin_roles_cannot_be_shadowed_dropped_or_restored() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for name in BUILTIN_SQL_ROLES {
            let create = format!("CREATE ROLE {name}");
            assert!(matches!(
                executor.execute(parse(&create).unwrap()).await,
                Err(RymeError::Conflict(message)) if message.contains("reserved")
            ));
            let drop = format!("DROP ROLE {name}");
            assert!(matches!(
                executor.execute(parse(&drop).unwrap()).await,
                Err(RymeError::Conflict(message)) if message.contains("reserved")
            ));
        }

        executor.execute(parse("CREATE ROLE AppReader").unwrap()).await.unwrap();
        assert!(matches!(
            executor.execute(parse("CREATE ROLE appreader").unwrap()).await,
            Err(RymeError::Conflict(message)) if message.contains("role appreader")
        ));
        executor.execute(parse("DROP ROLE appreader").unwrap()).await.unwrap();

        let mut snapshot = SchemaSnapshot::default();
        snapshot.roles.insert(
            String::from("postgres"),
            RoleDefinition {
                name: String::from("postgres"),
                login: true,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
        );
        assert!(matches!(
            executor.restore_schema_snapshot(snapshot),
            Err(RymeError::Corrupt(message)) if message.contains("reserved role")
        ));
    }

    #[tokio::test]
    async fn table_privileges_gate_role_queries_and_mutations() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE public.messages (id TEXT PRIMARY KEY, body TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO public.messages (id, body) VALUES ('one', 'hello')").unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("CREATE ROLE app_reader LOGIN").unwrap()).await.unwrap();
        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE public.messages TO app_reader").unwrap())
            .await
            .unwrap();

        let reader = executor.clone().with_role(String::from("app_reader"));
        assert!(reader.can_table_privilege("public.messages", "SELECT"));
        assert!(!reader.can_table_privilege("public.messages", "UPDATE"));
        assert!(reader
            .execute(parse("SELECT body FROM public.messages WHERE id = 'one'").unwrap())
            .await
            .is_ok());
        reader
            .execute(
                parse("INSERT INTO public.messages (id, body) VALUES ('two', 'world')").unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            reader
                .bulk_upsert(
                    String::from("public.messages"),
                    vec![(b"one".to_vec(), b"blocked".to_vec())],
                )
                .await,
            Err(RymeError::Forbidden)
        ));
        assert!(matches!(
            reader
                .execute(parse("SELECT body FROM public.messages WHERE id = 'one'").unwrap())
                .await,
            Ok(QueryResult::Table { ref rows, .. }) if rows == &vec![vec![b"hello".to_vec()]]
        ));
        assert!(reader
            .execute(parse("UPDATE public.messages SET body = 'blocked' WHERE id = 'one'").unwrap())
            .await
            .is_err());
        assert!(matches!(
            reader
                .execute(parse("CREATE INDEX messages_body_idx ON public.messages (body)").unwrap())
                .await,
            Err(RymeError::Forbidden)
        ));
        assert!(matches!(
            reader
                .execute(parse("CREATE TABLE public.private_data (id TEXT PRIMARY KEY)").unwrap())
                .await,
            Err(RymeError::Forbidden)
        ));

        executor
            .execute(parse("REVOKE SELECT ON TABLE public.messages FROM app_reader").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            reader.execute(parse("SELECT * FROM public.messages").unwrap()).await,
            Err(RymeError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn returning_requires_select_privilege_on_mutated_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE public.secrets (id TEXT PRIMARY KEY, value TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO public.secrets (id, value) VALUES ('one', 'hidden')").unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("CREATE ROLE updater LOGIN").unwrap()).await.unwrap();
        executor
            .execute(parse("GRANT UPDATE ON TABLE public.secrets TO updater").unwrap())
            .await
            .unwrap();

        let updater = executor.clone().with_role(String::from("updater"));
        assert!(matches!(
            updater
                .execute(
                    parse("UPDATE public.secrets SET id = 'one' WHERE id = 'one' RETURNING value")
                        .unwrap(),
                )
                .await,
            Err(RymeError::Forbidden)
        ));
        assert!(matches!(
            executor.execute(parse("SELECT value FROM public.secrets WHERE id = 'one'").unwrap()).await,
            Ok(QueryResult::Table { ref rows, .. }) if rows == &vec![vec![b"hidden".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn returning_obeys_select_rls_and_restores_transaction_on_denial() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        for sql in [
            "CREATE TABLE public.scoped_rows (id TEXT PRIMARY KEY, user_id TEXT, visible_to TEXT, secret TEXT)",
            "GRANT SELECT, INSERT ON TABLE public.scoped_rows TO authenticated",
            "CREATE POLICY scoped_insert ON public.scoped_rows FOR INSERT TO authenticated WITH CHECK (auth.uid() = user_id)",
            "CREATE POLICY scoped_select ON public.scoped_rows FOR SELECT TO authenticated USING (auth.uid() = visible_to)",
            "ALTER TABLE public.scoped_rows ENABLE ROW LEVEL SECURITY",
        ] {
            executor.execute(parse(sql).unwrap()).await.unwrap();
        }

        let user = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-a"));
        let mut txn = user.begin_transaction(Isolation::Serializable);
        assert!(matches!(
            user.execute_in_transaction(
                &mut txn,
                parse("INSERT INTO public.scoped_rows (id, user_id, visible_to, secret) VALUES ('hidden', 'user-a', 'user-b', 'secret') RETURNING secret").unwrap(),
            )
            .await,
            Err(RymeError::Forbidden)
        ));
        assert!(txn.writes().is_empty());

        assert!(matches!(
            user.execute(
                parse("INSERT INTO public.scoped_rows (id, user_id, visible_to, secret) VALUES ('hidden-autocommit', 'user-a', 'user-b', 'secret') RETURNING secret").unwrap(),
            )
            .await,
            Err(RymeError::Forbidden)
        ));

        let admin = executor.clone().with_role(String::from("postgres"));
        assert!(matches!(
            admin.execute(parse("SELECT * FROM public.scoped_rows").unwrap()).await,
            Ok(QueryResult::Rows { ref rows }) if rows.is_empty()
        ));

        assert!(user.row_allowed_by_rls(
            "public.scoped_rows",
            br#"{"id":"visible","user_id":"user-a","visible_to":"user-a","secret":"shown"}"#,
        ));
        let visible = user
            .execute(parse("INSERT INTO public.scoped_rows (id, user_id, visible_to, secret) VALUES ('visible', 'user-a', 'user-a', 'shown') RETURNING secret").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            visible,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"shown".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn rls_policies_are_scoped_to_their_sql_command() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        for sql in [
            "CREATE TABLE command_scoped (id TEXT PRIMARY KEY, owner_id TEXT, value TEXT)",
            "INSERT INTO command_scoped (id, owner_id, value) VALUES ('one', 'user-a', 'before')",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE command_scoped TO authenticated",
            "CREATE POLICY update_owner ON command_scoped FOR UPDATE TO authenticated USING (auth.uid() = owner_id) WITH CHECK (auth.uid() = owner_id)",
            "ALTER TABLE command_scoped ENABLE ROW LEVEL SECURITY",
        ] {
            executor.execute(parse(sql).unwrap()).await.unwrap();
        }

        let user = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-a"));
        let row = br#"{"id":"one","owner_id":"user-a","value":"before"}"#;
        assert!(!user.row_allowed_by_rls("command_scoped", row));
        assert!(matches!(
            user.execute(parse("SELECT * FROM command_scoped").unwrap()).await,
            Ok(QueryResult::Rows { ref rows }) if rows.is_empty()
        ));
        assert!(!user.row_allowed_for_command("command_scoped", row, RlsCommand::Insert));
        assert!(user.row_allowed_for_command("command_scoped", row, RlsCommand::Update));
        assert!(!user.row_allowed_for_command("command_scoped", row, RlsCommand::Delete));

        let insert = user
            .execute(parse("INSERT INTO command_scoped (id, owner_id, value) VALUES ('two', 'user-a', 'inserted')").unwrap())
            .await;
        assert!(matches!(insert, Err(RymeError::Forbidden)));

        executor
            .execute(
                parse("CREATE POLICY insert_owner ON command_scoped FOR INSERT TO authenticated WITH CHECK (auth.uid() = owner_id)").unwrap(),
            )
            .await
            .unwrap();
        assert!(user.row_allowed_for_command("command_scoped", row, RlsCommand::Insert));
        assert!(!user.row_allowed_for_command("command_scoped", row, RlsCommand::Delete));
        assert_eq!(
            user.execute(parse("INSERT INTO command_scoped (id, owner_id, value) VALUES ('two', 'user-a', 'inserted')").unwrap())
                .await
                .unwrap(),
            QueryResult::Ok
        );

        let mut txn = user.begin_transaction(Isolation::Serializable);
        assert!(matches!(
            user.execute_in_transaction(
                &mut txn,
                Statement::Delete { table: String::from("command_scoped"), pk: b"one".to_vec() },
            )
            .await,
            Err(RymeError::Forbidden)
        ));

        let mut txn = user.begin_transaction(Isolation::Serializable);
        let update = user
            .execute_in_transaction(
                &mut txn,
                Statement::Update {
                    table: String::from("command_scoped"),
                    pk: b"one".to_vec(),
                    value: br#"{"id":"one","owner_id":"user-a","value":"updated"}"#.to_vec(),
                },
            )
            .await;
        let (_, changes) = update.unwrap();
        user.commit_transaction(txn, changes).await.unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT value FROM command_scoped WHERE id = 'one'").unwrap()).await,
            Ok(QueryResult::Table { ref rows, .. }) if rows == &vec![vec![b"updated".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn default_privileges_apply_to_new_objects_and_restore() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO authenticated",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO authenticated")
                    .unwrap(),
            )
            .await
            .unwrap();

        let table_change = parse(
            "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO authenticated",
        )
        .unwrap();
        assert!(matches!(
            table_change,
            Statement::DefaultPrivilegeChange { ref object_type, ref schema, ref owner, .. }
                if object_type == "TABLE"
                    && schema.as_deref() == Some("public")
                    && owner.as_deref() == Some("postgres")
        ));

        executor
            .execute(parse("CREATE TABLE public.messages (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        executor.execute(parse("CREATE SEQUENCE public.messages_id_seq").unwrap()).await.unwrap();

        let snapshot = executor.schema_snapshot();
        assert_eq!(snapshot.default_privileges.len(), 2);
        assert!(snapshot.privileges.iter().any(|grant| {
            grant.object == "public.messages"
                && grant.object_type == "TABLE"
                && grant.privilege == "SELECT"
                && grant.grantee == "authenticated"
        }));
        assert!(snapshot.privileges.iter().any(|grant| {
            grant.object == "public.messages_id_seq"
                && grant.object_type == "SEQUENCE"
                && grant.privilege == "USAGE"
                && grant.grantee == "authenticated"
        }));

        executor.restore_schema_snapshot(snapshot.clone()).unwrap();
        let restored = executor.schema_snapshot();
        assert_eq!(restored.default_privileges, snapshot.default_privileges);
        assert!(restored.privileges.iter().any(|grant| {
            grant.object == "public.messages"
                && grant.privilege == "SELECT"
                && grant.grantee == "authenticated"
        }));
    }

    #[tokio::test]
    async fn sequence_operations_require_sequence_privileges() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE SEQUENCE public.events_id_seq START WITH 10").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE public.events (id BIGINT PRIMARY KEY DEFAULT nextval('public.events_id_seq'::regclass), body TEXT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("CREATE ROLE app_writer LOGIN").unwrap()).await.unwrap();
        executor
            .execute(parse("GRANT INSERT ON TABLE public.events TO app_writer").unwrap())
            .await
            .unwrap();

        let writer = executor.clone().with_role(String::from("app_writer"));
        assert!(matches!(
            writer
                .execute(parse("SELECT nextval('public.events_id_seq'::regclass)").unwrap())
                .await,
            Err(RymeError::Forbidden)
        ));
        assert!(matches!(
            writer
                .execute(parse("INSERT INTO public.events (body) VALUES ('blocked')").unwrap())
                .await,
            Err(RymeError::Forbidden)
        ));

        executor
            .execute(parse("GRANT USAGE ON SEQUENCE public.events_id_seq TO app_writer").unwrap())
            .await
            .unwrap();
        writer
            .execute(parse("INSERT INTO public.events (body) VALUES ('allowed')").unwrap())
            .await
            .unwrap();
        assert!(writer
            .execute(parse("SELECT currval('public.events_id_seq')").unwrap())
            .await
            .is_ok());
        assert!(matches!(
            writer.execute(parse("SELECT setval('public.events_id_seq', 99)").unwrap()).await,
            Err(RymeError::Forbidden)
        ));

        executor
            .execute(parse("GRANT UPDATE ON SEQUENCE public.events_id_seq TO app_writer").unwrap())
            .await
            .unwrap();
        assert!(writer
            .execute(parse("SELECT setval('public.events_id_seq', 99)").unwrap())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn sql_rls_filters_reads_and_rejects_hidden_mutations() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d")).with_rls_tables(
            HashMap::from([(String::from("messages"), String::from("tenant_id"))]),
        );
        let ambiguous_tenant = br#"{"tenant_id":"tenant-a","Tenant_id":"tenant-b"}"#;
        assert!(!executor.row_allowed_by_rls("messages", ambiguous_tenant));
        assert!(!executor.row_allowed_for_write("messages", ambiguous_tenant));
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

        executor
            .execute(parse("ALTER TABLE messages DISABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        let still_hidden =
            executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(still_hidden, QueryResult::Rows { ref rows } if rows.len() == 1));
        assert!(executor.schema_snapshot().rls_enabled.contains("messages"));

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
    async fn sql_rls_filters_regular_and_materialized_views() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
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
                    "INSERT INTO messages (id, tenant_id, body) VALUES ('visible', 'tenant-a', 'hello'), ('hidden', 'tenant-b', 'secret')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE VIEW message_view AS SELECT * FROM messages").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE MATERIALIZED VIEW message_snapshot AS SELECT * FROM messages WITH DATA",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        for view in ["message_view", "message_snapshot"] {
            executor
                .execute(
                    parse(&format!(
                        "CREATE POLICY own_{view} ON {view} FOR SELECT USING (auth.uid() = tenant_id)"
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
            executor
                .execute(parse(&format!("ALTER TABLE {view} ENABLE ROW LEVEL SECURITY")).unwrap())
                .await
                .unwrap();
            executor
                .execute(parse(&format!("GRANT SELECT ON TABLE {view} TO authenticated")).unwrap())
                .await
                .unwrap();
            let user = executor.clone().with_role(String::from("authenticated"));
            let result =
                user.execute(parse(&format!("SELECT * FROM {view}")).unwrap()).await.unwrap();
            assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1));
            let realtime_rows = user.realtime_view_rows(view, None).await.unwrap().unwrap();
            assert_eq!(realtime_rows.len(), 1);
        }
    }

    #[tokio::test]
    async fn sql_create_policy_installs_persisted_auth_uid_rls() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
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
                    "INSERT INTO messages (id, tenant_id, body) VALUES ('visible', 'tenant-a', 'hello'), ('hidden', 'tenant-b', 'secret')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let policy = parse(
            "CREATE POLICY own_messages ON public.messages FOR ALL USING (auth.uid() = tenant_id) WITH CHECK (auth.uid() = tenant_id)",
        )
        .unwrap();
        assert!(
            matches!(policy, Statement::CreatePolicy { ref table, ref command, ref using, ref check, .. }
            if table == "messages"
                && command == "ALL"
                && using.as_deref() == Some("auth.uid() = tenant_id")
                && check.as_deref() == Some("auth.uid() = tenant_id"))
        );
        executor.execute(policy).await.unwrap();
        let result = executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 2));
        let enable = parse("ALTER TABLE messages ENABLE ROW LEVEL SECURITY").unwrap();
        assert!(matches!(
            enable,
            Statement::AlterTableRls { enabled: Some(true), forced: None, .. }
        ));
        executor.execute(enable).await.unwrap();
        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE messages TO authenticated").unwrap())
            .await
            .unwrap();
        let user = executor.clone().with_role(String::from("authenticated"));
        let result = user.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1));
        let force = parse("ALTER TABLE messages FORCE ROW LEVEL SECURITY").unwrap();
        assert!(matches!(
            force,
            Statement::AlterTableRls { enabled: None, forced: Some(true), .. }
        ));
        executor.execute(force).await.unwrap();
        let no_force = parse("ALTER TABLE messages NO FORCE ROW LEVEL SECURITY").unwrap();
        assert!(matches!(
            no_force,
            Statement::AlterTableRls { enabled: None, forced: Some(false), .. }
        ));
        executor.execute(no_force).await.unwrap();
        let rejected = user
            .execute(
                parse(
                    "INSERT INTO messages (id, tenant_id, body) VALUES ('blocked', 'tenant-b', 'nope')",
                )
                .unwrap(),
            )
            .await;
        assert!(matches!(rejected, Err(RymeError::Forbidden)));

        let snapshot = executor.schema_snapshot();
        assert!(!snapshot.rls_tables.contains_key("messages"));
        assert!(!snapshot.rls_write_tables.contains_key("messages"));
        assert!(snapshot.rls_policies.values().any(|policy| {
            policy.table == "messages"
                && policy.using.as_deref() == Some("auth.uid() = tenant_id")
                && policy.check.as_deref() == Some("auth.uid() = tenant_id")
        }));
        assert!(snapshot.rls_enabled.contains("messages"));
        executor.restore_schema_snapshot(snapshot).unwrap();
        let user = executor.with_role(String::from("authenticated"));
        let result = user.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1));
    }

    #[tokio::test]
    async fn auth_uid_rls_uses_authenticated_user_not_tenant() {
        let executor = Executor::new(String::from("project-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE profiles (id TEXT PRIMARY KEY, user_id TEXT, body TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO profiles (id, user_id, body) VALUES ('own', 'user-a', 'visible'), ('other', 'user-b', 'hidden')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE POLICY profile_owner ON profiles FOR ALL USING ((select auth.uid()) = user_id) WITH CHECK ((select auth.uid()) = user_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE profiles ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE profiles TO authenticated").unwrap())
            .await
            .unwrap();

        let user = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-a"));
        let rows = user.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(
            matches!(rows, QueryResult::Rows { ref rows } if rows.len() == 1 && rows[0].0 == b"own")
        );
        let write = user
            .execute(
                parse(
                    "INSERT INTO profiles (id, user_id, body) VALUES ('blocked', 'user-b', 'no')",
                )
                .unwrap(),
            )
            .await;
        assert!(matches!(write, Err(RymeError::Forbidden)));

        let other_user = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-b"));
        let other_rows =
            other_user.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(matches!(
            other_rows,
            QueryResult::Rows { ref rows } if rows.len() == 1 && rows[0].0 == b"other"
        ));

        let snapshot = executor.schema_snapshot();
        executor.restore_schema_snapshot(snapshot).unwrap();
        let restored_user = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-a"));
        let restored_rows =
            restored_user.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(matches!(
            restored_rows,
            QueryResult::Rows { ref rows } if rows.len() == 1 && rows[0].0 == b"own"
        ));
        let no_user = executor.with_role(String::from("authenticated")).without_auth_uid();
        let no_user_rows = no_user.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(matches!(no_user_rows, QueryResult::Rows { ref rows } if rows.is_empty()));
    }

    #[tokio::test]
    async fn sql_rls_superuser_bypasses_unforced_policy_but_force_restores_it() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, tenant_id TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO messages (id, tenant_id) VALUES ('mine', 'tenant-a'), ('other', 'tenant-b')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE POLICY own_messages ON messages FOR ALL USING (auth.uid() = tenant_id) WITH CHECK (auth.uid() = tenant_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE messages ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE messages TO authenticated").unwrap())
            .await
            .unwrap();

        let ordinary = executor.clone().with_role(String::from("authenticated"));
        let ordinary_rows =
            ordinary.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(ordinary_rows, QueryResult::Rows { ref rows } if rows.len() == 1));

        let superuser_rows =
            executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(superuser_rows, QueryResult::Rows { ref rows } if rows.len() == 2));

        executor
            .execute(parse("ALTER TABLE messages FORCE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        let forced_rows = executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(forced_rows, QueryResult::Rows { ref rows } if rows.len() == 1));
        let forced_write = executor
            .execute(
                parse("INSERT INTO messages (id, tenant_id) VALUES ('blocked', 'tenant-b')")
                    .unwrap(),
            )
            .await;
        assert!(matches!(forced_write, Err(RymeError::Forbidden)));

        executor
            .execute(parse("ALTER TABLE messages NO FORCE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        let unforced_rows =
            executor.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(unforced_rows, QueryResult::Rows { ref rows } if rows.len() == 2));
        executor
            .execute(
                parse("INSERT INTO messages (id, tenant_id) VALUES ('allowed', 'tenant-b')")
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sql_functions_and_triggers_update_rows_and_survive_restore() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE profiles (id TEXT PRIMARY KEY, updated_at BIGINT, body TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        let function = parse(
            "CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql",
        )
        .unwrap();
        assert!(
            matches!(function, Statement::CreateFunction { ref name, ref body, replace: true, .. }
            if name == "set_updated_at" && body.contains("NEW.updated_at"))
        );
        executor.execute(function).await.unwrap();
        let trigger = parse(
            "CREATE TRIGGER set_updated_at BEFORE UPDATE ON public.profiles FOR EACH ROW EXECUTE FUNCTION public.set_updated_at()",
        )
        .unwrap();
        assert!(
            matches!(trigger, Statement::CreateTrigger { ref table, ref events, ref function, .. }
            if table == "profiles" && events == &[String::from("UPDATE")] && function == "set_updated_at")
        );
        executor.execute(trigger).await.unwrap();
        executor
            .execute(
                parse("INSERT INTO profiles (id, updated_at, body) VALUES ('p1', 0, 'hello')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let updated = executor
            .execute(
                parse("UPDATE profiles SET updated_at = 1 WHERE id = 'p1' RETURNING updated_at")
                    .unwrap(),
            )
            .await
            .unwrap();
        let timestamp = match updated {
            QueryResult::Returning { rows, .. } => rows[0][0].clone(),
            result => panic!("expected returning row, got {result:?}"),
        };
        assert!(String::from_utf8(timestamp).unwrap().parse::<u64>().unwrap() > 1);

        let snapshot = executor.schema_snapshot();
        assert!(snapshot.functions.contains_key("set_updated_at"));
        assert!(snapshot.triggers.contains_key("profiles\0set_updated_at"));
        executor.restore_schema_snapshot(snapshot).unwrap();
        executor
            .execute(parse("UPDATE profiles SET updated_at = 2 WHERE id = 'p1'").unwrap())
            .await
            .unwrap();
        let updated = executor
            .execute(parse("SELECT updated_at FROM profiles WHERE id = 'p1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(updated, QueryResult::Table { ref rows, .. }
            if rows[0][0].as_slice() != b"2"));
        let drop = parse("DROP TRIGGER IF EXISTS set_updated_at ON profiles").unwrap();
        assert!(matches!(drop, Statement::DropTrigger { ref name, ref table, if_exists: true }
            if name == "set_updated_at" && table == "profiles"));
        executor.execute(drop).await.unwrap();
        assert!(!executor.schema_snapshot().triggers.contains_key("profiles\0set_updated_at"));
    }

    #[tokio::test]
    async fn sql_rls_defaults_to_deny_without_matching_policy_command() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE private_rows (id TEXT PRIMARY KEY, tenant_id TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO private_rows (id, tenant_id) VALUES ('one', 'tenant-a')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE private_rows ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();

        executor
            .execute(parse("GRANT SELECT, INSERT ON TABLE private_rows TO authenticated").unwrap())
            .await
            .unwrap();
        let user = executor.clone().with_role(String::from("authenticated"));
        let result = user.execute(parse("SELECT * FROM private_rows").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.is_empty()));
        let rejected = user
            .execute(
                parse("INSERT INTO private_rows (id, tenant_id) VALUES ('two', 'tenant-a')")
                    .unwrap(),
            )
            .await;
        assert!(matches!(rejected, Err(RymeError::Forbidden)));

        executor
            .execute(
                parse("CREATE POLICY read_private ON private_rows FOR SELECT USING (auth.uid() = tenant_id)")
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = user.execute(parse("SELECT * FROM private_rows").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1));
        let rejected = user
            .execute(
                parse("INSERT INTO private_rows (id, tenant_id) VALUES ('three', 'tenant-a')")
                    .unwrap(),
            )
            .await;
        assert!(matches!(rejected, Err(RymeError::Forbidden)));
    }

    #[tokio::test]
    async fn permissive_sql_policy_cannot_disable_configured_tenant_isolation() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"))
            .with_rls_tables(HashMap::from([(String::from("docs"), String::from("tenant_id"))]));
        executor
            .execute(
                parse("CREATE POLICY public_docs ON docs FOR ALL USING (true) WITH CHECK (true)")
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(
            executor.row_allowed_by_rls("docs", br#"{"tenant_id":"tenant-a","body":"visible"}"#,)
        );
        assert!(!executor
            .row_allowed_by_rls("docs", br#"{"tenant_id":"tenant-b","body":"must stay hidden"}"#,));
        assert!(!executor.row_allowed_for_write(
            "docs",
            br#"{"tenant_id":"tenant-b","body":"must stay hidden"}"#,
        ));

        let restored = Executor::new(String::from("tenant-a"), String::from("d"))
            .with_role(String::from("authenticated"));
        restored.restore_schema_snapshot(executor.schema_snapshot()).unwrap();
        assert!(!restored
            .row_allowed_by_rls("docs", br#"{"tenant_id":"tenant-b","body":"must stay hidden"}"#,));
        assert!(!restored.row_allowed_for_write(
            "docs",
            br#"{"tenant_id":"tenant-b","body":"must stay hidden"}"#,
        ));
    }

    #[tokio::test]
    async fn rls_rejects_boolean_composition_it_cannot_evaluate_safely() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE docs (id TEXT PRIMARY KEY, owner_id TEXT, other INT)").unwrap(),
            )
            .await
            .unwrap();

        let policy = executor
            .execute(
                parse(
                    "CREATE POLICY unsafe ON docs FOR SELECT USING (auth.uid() = owner_id OR other = 0)",
                )
                .unwrap(),
            )
            .await;
        assert!(matches!(policy, Err(RymeError::InvalidArgument(_))));
    }

    #[test]
    fn policy_parser_rejects_only_unimplemented_restrictive_scopes() {
        assert!(parse("CREATE POLICY ON docs USING (true)").is_err());
        assert!(parse(
            "CREATE POLICY admin_rows ON docs AS RESTRICTIVE FOR SELECT TO admin USING (true)"
        )
        .is_err());
        assert!(parse("CREATE POLICY bad_mode ON docs AS RESTRICTIVE USING (true)").is_err());
        assert!(parse(
            "CREATE POLICY app_rows ON docs FOR SELECT TO authenticated USING (auth.uid() = user_id)"
        )
        .is_ok());
        assert!(parse(
            "CREATE POLICY app_rows ON docs FOR SELECT TO authenticated, anon USING (auth.uid() = user_id)"
        )
        .is_ok());
        assert!(parse(
            "CREATE POLICY bad_scope ON docs FOR SELECT TO PUBLIC, authenticated USING (true)"
        )
        .is_err());
        assert!(parse(
            "CREATE POLICY public_rows ON docs AS PERMISSIVE FOR SELECT TO PUBLIC USING (true)"
        )
        .is_ok());
    }

    #[test]
    fn schema_snapshot_rejects_invalid_rls_policy_scopes() {
        let mut snapshot = SchemaSnapshot::default();
        snapshot.roles.insert(
            String::from("app_reader"),
            RoleDefinition {
                name: String::from("app_reader"),
                login: false,
                superuser: false,
                createdb: false,
                createrole: false,
                inherit: true,
            },
        );
        snapshot.rls_policies.insert(
            String::from("docs\0owner"),
            RlsPolicy {
                name: String::from("owner"),
                table: String::from("docs"),
                column: String::from("owner_id"),
                command: String::from("SELECT"),
                using: Some(String::from("auth.uid() = owner_id")),
                check: None,
                roles: vec![String::from("app_reader")],
                read: true,
                write: false,
            },
        );
        assert!(validate_schema_snapshot(&snapshot).is_ok());

        let mut malformed = snapshot.clone();
        malformed.rls_policies.get_mut("docs\0owner").unwrap().roles =
            vec![String::from("PUBLIC"), String::from("app_reader")];
        assert!(matches!(validate_schema_snapshot(&malformed), Err(RymeError::Corrupt(_))));

        let mut malformed = snapshot.clone();
        malformed.rls_policies.get_mut("docs\0owner").unwrap().write = true;
        assert!(matches!(validate_schema_snapshot(&malformed), Err(RymeError::Corrupt(_))));

        let mut malformed = snapshot;
        malformed.rls_policies.get_mut("docs\0owner").unwrap().roles =
            vec![String::from("missing_role")];
        assert!(matches!(validate_schema_snapshot(&malformed), Err(RymeError::Corrupt(_))));
    }

    #[tokio::test]
    async fn rls_policy_role_scope_does_not_leak_to_other_roles() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE scoped_docs (id TEXT PRIMARY KEY, owner_id TEXT, body TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO scoped_docs (id, owner_id, body) VALUES ('owned', 'user-a', 'visible'), ('other', 'user-b', 'hidden')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE POLICY authenticated_docs ON scoped_docs FOR SELECT TO authenticated USING (auth.uid() = owner_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE scoped_docs ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT ON TABLE scoped_docs TO authenticated, anon").unwrap())
            .await
            .unwrap();

        let authenticated = executor
            .clone()
            .with_role(String::from("authenticated"))
            .with_auth_uid(String::from("user-a"));
        let rows =
            authenticated.execute(parse("SELECT * FROM scoped_docs").unwrap()).await.unwrap();
        assert!(
            matches!(rows, QueryResult::Rows { ref rows } if rows.len() == 1 && rows[0].0 == b"owned")
        );
        let mut authenticated_txn = authenticated.begin_transaction(Isolation::Serializable);
        let (transaction_rows, _) = authenticated
            .execute_in_transaction(
                &mut authenticated_txn,
                parse("SELECT * FROM scoped_docs").unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(transaction_rows, QueryResult::Rows { ref rows }
            if rows.len() == 1 && rows[0].0 == b"owned"));

        let anonymous = executor.clone().with_role(String::from("anon"));
        let rows = anonymous.execute(parse("SELECT * FROM scoped_docs").unwrap()).await.unwrap();
        assert!(matches!(rows, QueryResult::Rows { ref rows } if rows.is_empty()));
        let mut anonymous_txn = anonymous.begin_transaction(Isolation::Serializable);
        let (transaction_rows, _) = anonymous
            .execute_in_transaction(&mut anonymous_txn, parse("SELECT * FROM scoped_docs").unwrap())
            .await
            .unwrap();
        assert!(matches!(transaction_rows, QueryResult::Rows { ref rows } if rows.is_empty()));

        executor
            .execute(
                parse(
                    "CREATE POLICY anonymous_docs ON scoped_docs FOR SELECT TO anon USING (auth.uid() = owner_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let anonymous_user = anonymous.clone().with_auth_uid(String::from("user-a"));
        let rows =
            anonymous_user.execute(parse("SELECT * FROM scoped_docs").unwrap()).await.unwrap();
        assert!(matches!(rows, QueryResult::Rows { ref rows } if rows.len() == 1));
        executor
            .execute(parse("DROP POLICY anonymous_docs ON scoped_docs").unwrap())
            .await
            .unwrap();
        let rows =
            anonymous_user.execute(parse("SELECT * FROM scoped_docs").unwrap()).await.unwrap();
        assert!(matches!(rows, QueryResult::Rows { ref rows } if rows.is_empty()));

        let unrelated = executor.with_role(String::from("authenticated"));
        let rows = unrelated.execute(parse("SELECT * FROM scoped_docs").unwrap()).await.unwrap();
        assert!(matches!(rows, QueryResult::Rows { ref rows } if rows.is_empty()));
    }

    #[tokio::test]
    async fn role_cannot_be_dropped_while_targeted_by_rls_policy() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor.execute(parse("CREATE ROLE app_reader").unwrap()).await.unwrap();
        executor
            .execute(
                parse("CREATE TABLE scoped_docs (id TEXT PRIMARY KEY, owner_id TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE POLICY app_reader_docs ON scoped_docs FOR SELECT TO app_reader USING (auth.uid() = owner_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let result = executor.execute(parse("DROP ROLE app_reader").unwrap()).await;
        assert!(
            matches!(result, Err(RymeError::Conflict(message)) if message.contains("RLS policy"))
        );

        executor
            .execute(parse("DROP POLICY app_reader_docs ON scoped_docs").unwrap())
            .await
            .unwrap();
        executor.execute(parse("DROP ROLE app_reader").unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn permissive_auth_uid_policies_combine_with_or() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE profiles (id TEXT PRIMARY KEY, owner_id TEXT, reviewer_id TEXT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO profiles (id, owner_id, reviewer_id) VALUES ('owned', 'user-a', 'user-b'), ('reviewed', 'user-b', 'user-a'), ('hidden', 'user-b', 'user-c')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE POLICY profile_owner ON profiles FOR SELECT USING (auth.uid() = owner_id)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE POLICY profile_reviewer ON profiles FOR SELECT USING (auth.uid() = reviewer_id)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE profiles ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT ON TABLE profiles TO authenticated").unwrap())
            .await
            .unwrap();

        let user =
            executor.with_role(String::from("authenticated")).with_auth_uid(String::from("user-a"));
        let rows = user.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(matches!(
            rows,
            QueryResult::Rows { ref rows }
                if rows.len() == 2
                    && rows.iter().any(|row| row.0 == b"owned")
                    && rows.iter().any(|row| row.0 == b"reviewed")
        ));
    }

    #[tokio::test]
    async fn sql_drop_policy_removes_named_policy_and_persists_state() {
        let executor = Executor::new(String::from("tenant-a"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE policy_rows (id TEXT PRIMARY KEY, tenant_id TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO policy_rows (id, tenant_id) VALUES ('one', 'tenant-a')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE POLICY own_rows ON policy_rows FOR SELECT USING (auth.uid() = tenant_id)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE policy_rows ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT ON TABLE policy_rows TO authenticated").unwrap())
            .await
            .unwrap();
        let user = executor.clone().with_role(String::from("authenticated"));
        let visible = user.execute(parse("SELECT * FROM policy_rows").unwrap()).await.unwrap();
        assert!(matches!(visible, QueryResult::Rows { ref rows } if rows.len() == 1));

        let drop = parse("DROP POLICY IF EXISTS own_rows ON policy_rows").unwrap();
        assert!(matches!(drop, Statement::DropPolicy { ref name, ref table, if_exists: true }
            if name == "own_rows" && table == "policy_rows"));
        executor.execute(drop).await.unwrap();
        let hidden = user.execute(parse("SELECT * FROM policy_rows").unwrap()).await.unwrap();
        assert!(matches!(hidden, QueryResult::Rows { ref rows } if rows.is_empty()));
        let snapshot = executor.schema_snapshot();
        assert!(!snapshot.rls_policies.keys().any(|key| key.ends_with("\0own_rows")));
        executor.restore_schema_snapshot(snapshot).unwrap();
        let hidden = user.execute(parse("SELECT * FROM policy_rows").unwrap()).await.unwrap();
        assert!(matches!(hidden, QueryResult::Rows { ref rows } if rows.is_empty()));
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
    async fn predicate_mutations_update_and_delete_multiple_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT, score INTEGER)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO accounts (id, status, score) VALUES ('a1', 'active', 1), ('a2', 'active', 2), ('a3', 'closed', 3)",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let updated = executor
            .execute(parse("UPDATE accounts SET score = 10 WHERE status = 'active'").unwrap())
            .await
            .unwrap();
        assert_eq!(updated, QueryResult::Ok);
        let returned = executor
            .execute(
                parse("UPDATE accounts SET score = 11 WHERE status = 'active' RETURNING id, score")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            returned,
            QueryResult::Returning { rows, .. }
                if rows == vec![
                    vec![b"a1".to_vec(), b"11".to_vec()],
                    vec![b"a2".to_vec(), b"11".to_vec()]
                ]
        ));
        let result = executor
            .execute(parse("SELECT id, score FROM accounts ORDER BY id ASC").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"a1".to_vec(), b"11".to_vec()],
                    vec![b"a2".to_vec(), b"11".to_vec()],
                    vec![b"a3".to_vec(), b"3".to_vec()]
                ]
        ));

        let deleted = executor
            .execute(
                parse("DELETE FROM accounts WHERE status = 'closed' RETURNING id, status").unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            deleted,
            QueryResult::Returning { rows, .. }
                if rows == vec![vec![b"a3".to_vec(), b"closed".to_vec()]]
        ));
        let remaining =
            executor.execute(parse("SELECT COUNT(*) FROM accounts").unwrap()).await.unwrap();
        assert!(matches!(remaining, QueryResult::Scalar { value, .. } if value == b"2"));
    }

    #[tokio::test]
    async fn update_from_applies_source_values_and_filters_atomically() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT, score INTEGER)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE TABLE patches (id TEXT PRIMARY KEY, status TEXT, delta INTEGER, active BOOLEAN)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO accounts (id, status, score) VALUES ('1', 'old', 10), ('2', 'old', 20)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO patches (id, status, delta, active) VALUES ('1', 'ready', 3, true), ('2', 'ignored', 5, false)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(
                parse("UPDATE accounts AS a SET score = score + p.delta, status = p.status FROM patches AS p WHERE a.id = p.id AND p.active = true RETURNING a.id, a.score, a.status")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"13".to_vec(), b"ready".to_vec()]]
        ));
        assert!(matches!(
            executor
                .execute(parse("SELECT id, status, score FROM accounts ORDER BY id").unwrap())
                .await
                .unwrap(),
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"1".to_vec(), b"ready".to_vec(), b"13".to_vec()],
                    vec![b"2".to_vec(), b"old".to_vec(), b"20".to_vec()],
                ]
        ));
    }

    #[tokio::test]
    async fn delete_using_applies_source_values_and_filters_atomically() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE TABLE tombstones (id TEXT PRIMARY KEY, active BOOLEAN)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO accounts (id, status) VALUES ('1', 'expired'), ('2', 'open'), ('3', 'open')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO tombstones (id, active) VALUES ('1', true), ('2', false)")
                    .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(
                parse("DELETE FROM accounts AS a USING tombstones AS t WHERE a.id = t.id AND t.active = true RETURNING a.id, a.status")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Returning { ref rows, .. }
                if rows == &vec![vec![b"1".to_vec(), b"expired".to_vec()]]
        ));
        assert!(matches!(
            executor
                .execute(parse("SELECT id, status FROM accounts ORDER BY id").unwrap())
                .await
                .unwrap(),
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"2".to_vec(), b"open".to_vec()],
                    vec![b"3".to_vec(), b"open".to_vec()],
                ]
        ));
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
    async fn migration_schema_and_extension_declarations_are_accepted() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let schema = parse("CREATE SCHEMA IF NOT EXISTS extensions").unwrap();
        assert!(matches!(schema, Statement::CreateSchema { ref schema, if_not_exists: true }
            if schema == "extensions"));
        assert!(schema.is_write());
        assert!(matches!(executor.execute(schema).await, Ok(QueryResult::Ok)));

        let extension =
            parse("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions").unwrap();
        assert!(
            matches!(extension, Statement::CreateExtension { ref name, ref schema, if_not_exists: true }
            if name == "pgcrypto" && schema.as_deref() == Some("extensions"))
        );
        assert!(extension.is_write());
        assert!(matches!(executor.execute(extension).await, Ok(QueryResult::Ok)));
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
    async fn backend_wrapping_preserves_historical_reads() {
        let manager = TxnManager::new();
        let executor =
            Executor::with_manager(String::from("t"), String::from("d"), manager.clone());
        executor.execute(parse("INSERT INTO docs KEY 'k' VALUE 'old'").unwrap()).await.unwrap();
        let first_commit = manager.latest_commit();
        executor.execute(parse("UPSERT INTO docs KEY 'k' VALUE 'new'").unwrap()).await.unwrap();

        let historical = executor.clone().with_read_ts(first_commit).with_backend_manager(manager);
        assert!(matches!(
            historical.execute(parse("SELECT * FROM docs KEY 'k'").unwrap()).await,
            Ok(QueryResult::Row { value, .. }) if value == b"old"
        ));
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
            Statement::CreateTable { table, columns, .. } => (table.clone(), columns.clone()),
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
    async fn create_table_if_not_exists_is_idempotent_but_duplicate_create_fails() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();

        assert!(executor
            .execute(parse("CREATE TABLE users (id INTEGER PRIMARY KEY)").unwrap())
            .await
            .is_err());
        let statement = parse("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)").unwrap();
        assert!(matches!(statement, Statement::CreateTable { if_not_exists: true, .. }));
        executor.execute(statement).await.unwrap();
        assert_eq!(executor.catalog_columns("users")[0].data_type, "text");
    }

    #[test]
    fn schema_snapshot_rejects_excessive_object_count() {
        let mut snapshot = SchemaSnapshot::default();
        for index in 0..=MAX_SCHEMA_OBJECTS {
            snapshot.tables.insert(format!("table_{index}"), Vec::new());
        }
        assert!(matches!(
            validate_schema_snapshot(&snapshot),
            Err(RymeError::Overload(message)) if message == "schema snapshot objects"
        ));
    }

    #[test]
    fn schema_snapshot_serialized_size_matches_json_size() {
        let snapshot = SchemaSnapshot::default();
        assert_eq!(
            schema_snapshot_serialized_size(&snapshot).unwrap(),
            serde_json::to_vec(&snapshot).unwrap().len()
        );
    }

    #[tokio::test]
    async fn runtime_schema_mutations_enforce_object_limit_and_rollback() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let mut snapshot = SchemaSnapshot::default();
        for index in 0..MAX_SCHEMA_OBJECTS {
            snapshot.tables.insert(format!("table_{index}"), Vec::new());
        }
        executor.restore_schema_snapshot(snapshot).unwrap();

        let result = executor.execute(parse("CREATE ROLE overflow_role").unwrap()).await;
        assert!(matches!(
            result,
            Err(RymeError::Overload(message)) if message == "schema snapshot objects"
        ));
        assert!(!executor.catalog_roles().iter().any(|role| role.name == "overflow_role"));

        let result = executor
            .execute(parse("INSERT INTO implicit_overflow KEY 'k' VALUE 'v'").unwrap())
            .await;
        assert!(matches!(
            result,
            Err(RymeError::Overload(message)) if message == "schema snapshot objects"
        ));
        assert!(!executor.catalog_tables().iter().any(|table| table == "implicit_overflow"));
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
        executor.set_schema_path(path.clone()).unwrap();
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
        isolated
            .execute(parse("ALTER TABLE messages ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        assert!(isolated.row_security_enabled("messages"));
        assert!(!restored.row_security_enabled("messages"));
        isolated.execute(parse("CREATE TABLE branch_only").unwrap()).await.unwrap();
        assert!(!restored.catalog_tables().contains(&String::from("branch_only")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn failed_schema_snapshot_restore_rolls_back_all_changes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE docs (id TEXT PRIMARY KEY, value TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO docs (id, value) VALUES ('one', 'same'), ('two', 'same')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let before = executor.schema_snapshot();
        let mut invalid = before.clone();
        invalid.indexes.push(IndexDefinition {
            name: String::from("docs_value_unique"),
            table: String::from("docs"),
            field: Field::Value,
            column: Some(String::from("value")),
            columns: Vec::new(),
            unique: true,
        });

        assert!(matches!(
            executor.restore_schema_snapshot(invalid),
            Err(RymeError::Conflict(message)) if message.contains("unique index")
        ));
        assert_eq!(executor.schema_snapshot(), before);
        assert!(executor.catalog_indexes("docs").is_empty());
    }

    #[tokio::test]
    async fn failed_schema_persistence_rolls_back_ddl() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-schema-persist-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut executor = Executor::new(String::from("t"), String::from("d"));
        executor.set_schema_path(dir.clone()).unwrap();
        assert!(executor
            .execute(parse("CREATE TABLE should_rollback (id TEXT PRIMARY KEY)").unwrap())
            .await
            .is_err());
        assert!(!executor.catalog_tables().contains(&String::from("should_rollback")));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(dir.with_extension("tmp"));
    }

    #[tokio::test]
    async fn explicit_transaction_schema_persistence_failure_rolls_back_ddl() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-schema-transaction-persist-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut executor = Executor::new(String::from("t"), String::from("d"));
        executor.set_schema_path(dir.clone()).unwrap();
        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let before = executor.schema_transaction_snapshot();
        let (_, changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("CREATE TABLE should_rollback (id TEXT PRIMARY KEY)").unwrap(),
            )
            .await
            .unwrap();

        assert!(executor.commit_transaction_with_schema(txn, changes, before).await.is_err());
        assert!(!executor.catalog_tables().contains(&String::from("should_rollback")));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(dir.with_extension("tmp"));
    }

    #[tokio::test]
    async fn standalone_transactions_reject_mixed_schema_and_row_changes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();

        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let before = executor.schema_transaction_snapshot();
        let (_, mut changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("CREATE TABLE migration (id TEXT PRIMARY KEY)").unwrap(),
            )
            .await
            .unwrap();
        let (_, row_changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO messages (id) VALUES ('one')").unwrap(),
            )
            .await
            .unwrap();
        changes.extend(row_changes);

        let result = executor.commit_transaction_with_schema(txn, changes, before).await;
        assert!(matches!(
            result,
            Err(RymeError::InvalidArgument(message))
                if message.contains("schema changes and row changes")
        ));
        assert!(!executor.catalog_tables().contains(&String::from("migration")));
        assert!(matches!(
            executor.execute(parse("SELECT * FROM messages").unwrap()).await,
            Ok(QueryResult::Rows { ref rows }) if rows.is_empty()
        ));
    }

    #[tokio::test]
    async fn rls_policy_state_survives_executor_restart() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-rls-restart-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("schema.json");
        let manager = TxnManager::new();
        let mut executor =
            Executor::with_manager(String::from("tenant-a"), String::from("d"), manager.clone());
        executor.set_schema_path(path.clone()).unwrap();
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, tenant_id TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO messages (id, tenant_id) VALUES ('visible', 'tenant-a'), ('hidden', 'tenant-b')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE POLICY own_messages ON messages FOR ALL TO authenticated USING (auth.uid() = tenant_id) WITH CHECK (auth.uid() = tenant_id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE messages ENABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE messages FORCE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("GRANT SELECT ON TABLE messages TO authenticated").unwrap())
            .await
            .unwrap();

        let snapshot: SchemaSnapshot =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(snapshot.rls_enabled.contains("messages"));
        assert!(snapshot.rls_forced.contains("messages"));
        assert!(snapshot.rls_policies.keys().any(|key| key.ends_with("\0own_messages")));
        assert!(snapshot
            .rls_policies
            .values()
            .any(|policy| policy.name == "own_messages" && policy.roles == ["authenticated"]));

        let mut restarted =
            Executor::with_manager(String::from("tenant-a"), String::from("d"), manager);
        restarted.set_schema_path(path.clone()).unwrap();
        restarted.restore_schema_snapshot(snapshot).unwrap();
        let user = restarted.clone().with_role(String::from("authenticated"));
        let visible = user.execute(parse("SELECT * FROM messages").unwrap()).await.unwrap();
        assert!(matches!(visible, QueryResult::Rows { ref rows } if rows.len() == 1));
        assert!(restarted.schema_snapshot().rls_forced.contains("messages"));

        restarted.execute(parse("DROP POLICY own_messages ON messages").unwrap()).await.unwrap();
        restarted
            .execute(parse("ALTER TABLE messages DISABLE ROW LEVEL SECURITY").unwrap())
            .await
            .unwrap();
        let after: SchemaSnapshot = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(!after.rls_enabled.contains("messages"));
        assert!(!after.rls_forced.contains("messages"));
        assert!(!after.rls_policies.keys().any(|key| key.ends_with("\0own_messages")));
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

        let result = executor
            .execute(parse("SELECT id, count FROM events ORDER BY count DESC").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"e2".to_vec(), b"4".to_vec()],
                    vec![b"e1".to_vec(), b"2".to_vec()]
                ]
        ));

        let result = executor
            .execute(
                parse("SELECT id, payload->>'name' FROM events ORDER BY payload->>'name' ASC")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"e1".to_vec(), b"Ada".to_vec()],
                    vec![b"e2".to_vec(), b"Grace".to_vec()]
                ]
        ));
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
        let Statement::CreateTable { columns, .. } = parse(
            "CREATE TABLE users (left_id BIGINT, right_id BIGINT, PRIMARY KEY (left_id, right_id))",
        )
        .unwrap() else {
            panic!("expected create table")
        };
        assert!(columns.iter().all(|column| column.primary_key));
        assert!(columns.iter().all(|column| !column.nullable));
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
                columns: Vec::new(),
                unique: false,
                if_not_exists: false,
            }
        );
        assert!(matches!(
            parse("CREATE UNIQUE INDEX messages_value_unique ON messages (value)").unwrap(),
            Statement::CreateIndex { unique: true, .. }
        ));
        assert!(matches!(
            parse("CREATE INDEX IF NOT EXISTS messages_value_idx ON messages (value)").unwrap(),
            Statement::CreateIndex { name, if_not_exists: true, .. } if name == "messages_value_idx"
        ));
        assert!(matches!(
            parse("CREATE INDEX CONCURRENTLY messages_value_idx ON messages (value)").unwrap(),
            Statement::CreateIndex { name, if_not_exists: false, .. } if name == "messages_value_idx"
        ));
        assert!(matches!(
            parse("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS messages_value_unique ON messages (value)").unwrap(),
            Statement::CreateIndex { name, unique: true, if_not_exists: true, .. }
                if name == "messages_value_unique"
        ));
        assert!(matches!(
            parse("DROP INDEX IF EXISTS messages_value_idx").unwrap(),
            Statement::DropIndex { name, if_exists: true } if name == "messages_value_idx"
        ));
        assert!(matches!(
            parse("DROP INDEX CONCURRENTLY IF EXISTS messages_value_idx").unwrap(),
            Statement::DropIndex { name, if_exists: true } if name == "messages_value_idx"
        ));
    }

    #[test]
    fn parses_composite_create_index_and_unique_constraint() {
        assert_eq!(
            parse(
                "CREATE UNIQUE INDEX memberships_pair_unique ON memberships (tenant_id, user_id)"
            )
            .unwrap(),
            Statement::CreateIndex {
                name: String::from("memberships_pair_unique"),
                table: String::from("memberships"),
                field: Field::Value,
                column: None,
                columns: vec![String::from("tenant_id"), String::from("user_id")],
                unique: true,
                if_not_exists: false,
            }
        );
        let Statement::CreateTable { unique_constraints, .. } = parse(
            "CREATE TABLE memberships (id TEXT PRIMARY KEY, tenant_id TEXT, user_id TEXT, UNIQUE (tenant_id, user_id))",
        )
        .unwrap()
        else {
            panic!("expected create table")
        };
        assert_eq!(
            unique_constraints,
            vec![vec![String::from("tenant_id"), String::from("user_id")]]
        );
    }

    #[test]
    fn parses_column_and_table_check_constraints() {
        let Statement::CreateTable { checks, .. } = parse(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, age INTEGER CHECK (age >= 18), CHECK (status = 'active' OR status = 'pending'))",
        )
        .unwrap()
        else {
            panic!("expected create table")
        };
        assert_eq!(
            checks,
            vec![
                String::from("age >= 18"),
                String::from("status = 'active' OR status = 'pending'"),
            ]
        );
    }

    #[test]
    fn parses_column_and_table_foreign_keys() {
        let Statement::CreateTable { foreign_keys, .. } = parse(
            "CREATE TABLE messages (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id), room_id TEXT, FOREIGN KEY (room_id) REFERENCES rooms (id) ON DELETE CASCADE ON UPDATE CASCADE)",
        )
        .unwrap()
        else {
            panic!("expected create table")
        };
        assert_eq!(
            foreign_keys,
            vec![
                ForeignKeyConstraint {
                    columns: vec![String::from("user_id")],
                    referenced_table: String::from("users"),
                    referenced_columns: vec![String::from("id")],
                    on_delete: ForeignKeyAction::Restrict,
                    on_update: ForeignKeyAction::Restrict,
                },
                ForeignKeyConstraint {
                    columns: vec![String::from("room_id")],
                    referenced_table: String::from("rooms"),
                    referenced_columns: vec![String::from("id")],
                    on_delete: ForeignKeyAction::Cascade,
                    on_update: ForeignKeyAction::Cascade,
                },
            ]
        );
    }

    #[test]
    fn parses_alter_table_constraints() {
        assert!(matches!(
            parse("ALTER TABLE messages ADD CONSTRAINT messages_pk PRIMARY KEY (tenant_id, id)").unwrap(),
            Statement::AlterTableAddConstraint {
                constraint: TableConstraint::PrimaryKey { name: Some(name), columns },
                ..
            } if name == "messages_pk"
                && columns == vec![String::from("tenant_id"), String::from("id")]
        ));
        assert!(matches!(
            parse("ALTER TABLE messages ADD CONSTRAINT messages_user_fk FOREIGN KEY (user_id) REFERENCES users (id)").unwrap(),
            Statement::AlterTableAddConstraint {
                constraint: TableConstraint::ForeignKey { name: Some(name), .. },
                ..
            } if name == "messages_user_fk"
        ));
        assert!(matches!(
            parse("ALTER TABLE messages ADD CONSTRAINT messages_room_unique UNIQUE (room_id)").unwrap(),
            Statement::AlterTableAddConstraint {
                constraint: TableConstraint::Unique { name: Some(name), columns },
                ..
            } if name == "messages_room_unique" && columns == vec![String::from("room_id")]
        ));
        assert!(matches!(
            parse("ALTER TABLE messages ADD CHECK (length > 0)").unwrap(),
            Statement::AlterTableAddConstraint {
                constraint: TableConstraint::Check { expression, .. },
                ..
            } if expression == "length > 0"
        ));
        assert!(matches!(
            parse("ALTER TABLE messages ALTER COLUMN length TYPE BIGINT").unwrap(),
            Statement::AlterTableColumn {
                column,
                alteration: ColumnAlteration::SetType(data_type),
                ..
            } if column == "length" && data_type == "bigint"
        ));
        assert!(matches!(
            parse("ALTER TABLE messages ALTER COLUMN length TYPE BIGINT USING length::BIGINT")
                .unwrap(),
            Statement::AlterTableColumn {
                column,
                alteration: ColumnAlteration::SetType(data_type),
                ..
            } if column == "length" && data_type == "bigint"
        ));
        assert!(parse("ALTER TABLE messages ALTER COLUMN length TYPE BIGINT USING other::BIGINT")
            .is_err());
    }

    #[tokio::test]
    async fn alter_table_add_primary_key_rekeys_existing_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE records (legacy TEXT, account TEXT, sequence INTEGER, body TEXT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO records (legacy, account, sequence, body) VALUES ('legacy-1', 'room', 1, 'hello')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE records ADD PRIMARY KEY (account, sequence)").unwrap())
            .await
            .unwrap();

        let old_key = executor
            .execute(parse("SELECT * FROM records WHERE key = 'legacy-1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(old_key, QueryResult::Rows { ref rows } if rows.is_empty()));
        let row = executor
            .execute(parse("SELECT body FROM records WHERE account = 'room'").unwrap())
            .await
            .unwrap();
        assert!(matches!(row, QueryResult::Table { ref rows, .. }
            if rows == &vec![vec![b"hello".to_vec()]]));

        let duplicate = executor
            .execute(
                parse("INSERT INTO records (legacy, account, sequence, body) VALUES ('legacy-2', 'room', 1, 'again')")
                    .unwrap(),
            )
            .await;
        assert!(matches!(duplicate, Err(RymeError::Conflict(_))));
    }

    #[tokio::test]
    async fn index_ddl_enforces_existence_and_can_remove_index_metadata() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE messages").unwrap()).await.unwrap();
        executor
            .execute(parse("CREATE INDEX messages_value_idx ON messages (value)").unwrap())
            .await
            .unwrap();
        assert!(executor
            .execute(parse("CREATE INDEX messages_value_idx ON messages (key)").unwrap())
            .await
            .is_err());
        executor
            .execute(
                parse("CREATE INDEX IF NOT EXISTS messages_value_idx ON messages (key)").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executor.catalog_indexes("messages")[0].field, Field::Value);
        executor
            .execute(parse("CREATE INDEX CONCURRENTLY messages_key_idx ON messages (key)").unwrap())
            .await
            .unwrap();
        assert_eq!(executor.catalog_indexes("messages").len(), 2);

        executor.execute(parse("DROP INDEX messages_value_idx").unwrap()).await.unwrap();
        executor.execute(parse("DROP INDEX CONCURRENTLY messages_key_idx").unwrap()).await.unwrap();
        assert!(executor.catalog_indexes("messages").is_empty());
        assert!(executor.execute(parse("DROP INDEX messages_value_idx").unwrap()).await.is_err());
        executor.execute(parse("DROP INDEX IF EXISTS messages_value_idx").unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn composite_unique_constraints_enforce_column_tuples() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse(
                "CREATE TABLE memberships (id TEXT PRIMARY KEY, tenant_id TEXT, user_id TEXT, UNIQUE (tenant_id, user_id))",
            )
            .unwrap())
            .await
            .unwrap();
        executor
            .execute(parse(
                "INSERT INTO memberships (id, tenant_id, user_id) VALUES ('1', 'tenant-a', 'user-a')",
            )
            .unwrap())
            .await
            .unwrap();
        executor
            .execute(parse(
                "INSERT INTO memberships (id, tenant_id, user_id) VALUES ('2', 'tenant-a', 'user-b')",
            )
            .unwrap())
            .await
            .unwrap();
        let duplicate = executor
            .execute(parse(
                "INSERT INTO memberships (id, tenant_id, user_id) VALUES ('3', 'tenant-a', 'user-a')",
            )
            .unwrap())
            .await;
        assert!(
            matches!(duplicate, Err(RymeError::Conflict(message)) if message.contains("tenant_id_user_id"))
        );

        let null_tuple = executor
            .execute(parse(
                "INSERT INTO memberships (id, tenant_id, user_id) VALUES ('4', 'tenant-a', NULL)",
            )
            .unwrap())
            .await;
        assert!(null_tuple.is_ok());
    }

    #[tokio::test]
    async fn composite_primary_keys_encode_and_project_each_component() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse(
                "CREATE TABLE memberships (tenant_id TEXT, user_id TEXT, role TEXT, PRIMARY KEY (tenant_id, user_id))",
            )
            .unwrap())
            .await
            .unwrap();
        executor
            .execute(parse(
                "INSERT INTO memberships (tenant_id, user_id, role) VALUES ('tenant-a', 'user-a', 'admin')",
            )
            .unwrap())
            .await
            .unwrap();
        let duplicate = executor
            .execute(parse(
                "INSERT INTO memberships (tenant_id, user_id, role) VALUES ('tenant-a', 'user-a', 'member')",
            )
            .unwrap())
            .await;
        assert!(duplicate.is_err());

        let result = executor
            .execute(parse(
                "SELECT tenant_id, user_id, role FROM memberships WHERE tenant_id = 'tenant-a' AND user_id = 'user-a'",
            )
            .unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { rows, .. }
                if rows == vec![
                    vec![b"tenant-a".to_vec(), b"user-a".to_vec(), b"admin".to_vec()]
                ]
        ));
    }

    #[tokio::test]
    async fn check_constraints_reject_false_rows_and_allow_sql_null() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse(
                "CREATE TABLE accounts (id TEXT PRIMARY KEY, age INTEGER CHECK (age >= 18), status TEXT, CHECK (status = 'active' OR status = 'pending'))",
            )
            .unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO accounts (id, age, status) VALUES ('1', 21, 'active')").unwrap(),
            )
            .await
            .unwrap();
        let too_young = executor
            .execute(
                parse("INSERT INTO accounts (id, age, status) VALUES ('2', 17, 'active')").unwrap(),
            )
            .await;
        assert!(
            matches!(too_young, Err(RymeError::Conflict(message)) if message.contains("age >= 18"))
        );
        let bad_status = executor
            .execute(
                parse("INSERT INTO accounts (id, age, status) VALUES ('3', 21, 'disabled')")
                    .unwrap(),
            )
            .await;
        assert!(
            matches!(bad_status, Err(RymeError::Conflict(message)) if message.contains("status ="))
        );
        executor
            .execute(
                parse("INSERT INTO accounts (id, age, status) VALUES ('4', NULL, NULL)").unwrap(),
            )
            .await
            .unwrap();

        let invalid_update =
            executor.execute(parse("UPDATE accounts SET age = 12 WHERE id = '1'").unwrap()).await;
        assert!(invalid_update.is_err());

        let restored = Executor::with_manager(
            String::from("t"),
            String::from("d"),
            executor.manager().clone(),
        );
        restored.restore_schema_snapshot(executor.schema_snapshot()).unwrap();
        let restored_invalid = restored
            .execute(
                parse("INSERT INTO accounts (id, age, status) VALUES ('5', 17, 'active')").unwrap(),
            )
            .await;
        assert!(restored_invalid.is_err());
    }

    #[tokio::test]
    async fn foreign_keys_enforce_writes_and_restrict_parent_deletes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE messages (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id))",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO users (id) VALUES ('u1')").unwrap()).await.unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m1', 'u1')").unwrap())
            .await
            .unwrap();
        let missing_parent = executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m2', 'missing')").unwrap())
            .await;
        assert!(
            matches!(missing_parent, Err(RymeError::Conflict(message)) if message.contains("foreign key"))
        );
        executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m3', NULL)").unwrap())
            .await
            .unwrap();
        let invalid_update = executor
            .execute(parse("UPDATE messages SET user_id = 'missing' WHERE id = 'm1'").unwrap())
            .await;
        assert!(invalid_update.is_err());
        let blocked_delete =
            executor.execute(parse("DELETE FROM users WHERE id = 'u1'").unwrap()).await;
        assert!(
            matches!(blocked_delete, Err(RymeError::Conflict(message)) if message.contains("foreign key"))
        );
        executor.execute(parse("DELETE FROM messages WHERE id = 'm1'").unwrap()).await.unwrap();
        executor.execute(parse("DELETE FROM users WHERE id = 'u1'").unwrap()).await.unwrap();

        let restored = Executor::with_manager(
            String::from("t"),
            String::from("d"),
            executor.manager().clone(),
        );
        restored.restore_schema_snapshot(executor.schema_snapshot()).unwrap();
        assert_eq!(restored.schema_snapshot().foreign_keys.len(), 1);
    }

    #[tokio::test]
    async fn cascading_foreign_keys_delete_dependent_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE rooms (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id) ON DELETE CASCADE)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE messages (id TEXT PRIMARY KEY, room_id TEXT REFERENCES rooms (id) ON DELETE CASCADE)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO users (id) VALUES ('u1')").unwrap()).await.unwrap();
        executor
            .execute(parse("INSERT INTO rooms (id, user_id) VALUES ('r1', 'u1')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, room_id) VALUES ('m1', 'r1')").unwrap())
            .await
            .unwrap();

        executor.execute(parse("DELETE FROM users WHERE id = 'u1'").unwrap()).await.unwrap();
        for table in ["users", "rooms", "messages"] {
            let result = executor.execute(parse(&format!("SELECT * FROM {table}")).unwrap()).await;
            assert!(matches!(result, Ok(QueryResult::Rows { rows }) if rows.is_empty()));
        }
    }

    #[tokio::test]
    async fn foreign_key_delete_actions_set_null_and_default() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE profiles (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id) ON DELETE SET NULL)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE sessions (id TEXT PRIMARY KEY, user_id TEXT DEFAULT 'u2' REFERENCES users (id) ON DELETE SET DEFAULT)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO users (id) VALUES ('u1'), ('u2')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO profiles (id, user_id) VALUES ('p1', 'u1')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO sessions (id, user_id) VALUES ('s1', 'u1')").unwrap())
            .await
            .unwrap();

        executor.execute(parse("DELETE FROM users WHERE id = 'u1'").unwrap()).await.unwrap();
        let profiles = executor.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        let sessions = executor.execute(parse("SELECT * FROM sessions").unwrap()).await.unwrap();
        let null_user = b"\"user_id\":null";
        let default_user = b"\"user_id\":\"u2\"";
        assert!(
            matches!(profiles, QueryResult::Rows { ref rows } if rows[0].1.windows(null_user.len()).any(|window| window == null_user))
        );
        assert!(
            matches!(sessions, QueryResult::Rows { ref rows } if rows[0].1.windows(default_user.len()).any(|window| window == default_user))
        );
    }

    #[tokio::test]
    async fn foreign_key_update_actions_protect_and_cascade_parent_keys() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY, code TEXT UNIQUE)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE cascades (id TEXT PRIMARY KEY, user_code TEXT REFERENCES users (code) ON UPDATE CASCADE)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE restricted (id TEXT PRIMARY KEY, user_code TEXT REFERENCES users (code))",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO users (id, code) VALUES ('u1', 'old')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO cascades (id, user_code) VALUES ('c1', 'old')").unwrap())
            .await
            .unwrap();
        let inserted = executor
            .execute(parse("INSERT INTO restricted (id, user_code) VALUES ('r1', 'old')").unwrap())
            .await
            .unwrap();
        assert!(matches!(inserted, QueryResult::Ok));

        let blocked = executor
            .execute(parse("UPDATE users SET code = 'blocked' WHERE id = 'u1'").unwrap())
            .await;
        assert!(
            matches!(blocked, Err(RymeError::Conflict(message)) if message.contains("foreign key"))
        );
        executor.execute(parse("DELETE FROM restricted WHERE id = 'r1'").unwrap()).await.unwrap();
        executor
            .execute(parse("UPDATE users SET code = 'new' WHERE id = 'u1'").unwrap())
            .await
            .unwrap();
        let cascaded = executor.execute(parse("SELECT * FROM cascades").unwrap()).await.unwrap();
        let new_user = b"\"user_code\":\"new\"";
        assert!(
            matches!(cascaded, QueryResult::Rows { ref rows } if rows[0].1.windows(new_user.len()).any(|window| window == new_user)),
            "{cascaded:?}"
        );
    }

    #[tokio::test]
    async fn updating_primary_keys_rekeys_rows_and_cascades_foreign_keys() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY, code TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id) ON UPDATE CASCADE)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO users (id, code) VALUES ('u1', 'room')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m1', 'u1')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX users_code_idx ON users (code)").unwrap())
            .await
            .unwrap();

        let returned = executor
            .execute(parse("UPDATE users SET id = 'u2' WHERE id = 'u1' RETURNING id").unwrap())
            .await
            .unwrap();
        assert!(matches!(returned, QueryResult::Returning { rows, .. }
            if rows == vec![vec![b"u2".to_vec()]]));
        let old =
            executor.execute(parse("SELECT * FROM users WHERE id = 'u1'").unwrap()).await.unwrap();
        assert!(matches!(old, QueryResult::Rows { rows } if rows.is_empty()));
        let indexed = executor
            .execute(parse("SELECT * FROM users WHERE code = 'room'").unwrap())
            .await
            .unwrap();
        assert!(matches!(indexed, QueryResult::Rows { rows }
            if rows.len() == 1 && rows[0].0 == b"u2"));
        let child = executor.execute(parse("SELECT user_id FROM messages").unwrap()).await.unwrap();
        assert!(matches!(child, QueryResult::Table { rows, .. }
            if rows == vec![vec![b"u2".to_vec()]]));
    }

    #[tokio::test]
    async fn composite_foreign_keys_match_composite_primary_keys() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE tenants (tenant_id TEXT, id TEXT, PRIMARY KEY (tenant_id, id))",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE memberships (id TEXT PRIMARY KEY, tenant_id TEXT, tenant_user TEXT, FOREIGN KEY (tenant_id, tenant_user) REFERENCES tenants (tenant_id, id))",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO tenants (tenant_id, id) VALUES ('t1', 'u1')").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO memberships (id, tenant_id, tenant_user) VALUES ('m1', 't1', 'u1')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let invalid = executor
            .execute(
                parse(
                    "INSERT INTO memberships (id, tenant_id, tenant_user) VALUES ('m2', 't1', 'missing')",
                )
                .unwrap(),
            )
            .await;
        assert!(invalid.is_err());
    }

    #[tokio::test]
    async fn alter_table_constraints_validate_existing_and_future_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();
        executor
            .execute(
                parse(
                    "CREATE TABLE messages (id TEXT PRIMARY KEY, user_id TEXT, room_id TEXT, length INTEGER)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO users (id) VALUES ('u1')").unwrap()).await.unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO messages (id, user_id, room_id, length) VALUES ('m1', 'u1', 'room-a', 5)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "ALTER TABLE messages ADD CONSTRAINT messages_user_fk FOREIGN KEY (user_id) REFERENCES users (id)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE messages ADD CONSTRAINT messages_room_unique UNIQUE (room_id)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE messages ADD CONSTRAINT messages_length CHECK (length > 0)")
                    .unwrap(),
            )
            .await
            .unwrap();
        let missing_parent = executor
            .execute(
                parse(
                    "INSERT INTO messages (id, user_id, room_id, length) VALUES ('m2', 'missing', 'room-b', 5)",
                )
                .unwrap(),
            )
            .await;
        assert!(missing_parent.is_err());
        let duplicate_room = executor
            .execute(
                parse(
                    "INSERT INTO messages (id, user_id, room_id, length) VALUES ('m2', 'u1', 'room-a', 5)",
                )
                .unwrap(),
            )
            .await;
        assert!(duplicate_room.is_err());
        let invalid_check = executor
            .execute(parse("UPDATE messages SET length = 0 WHERE id = 'm1'").unwrap())
            .await;
        assert!(invalid_check.is_err());
        assert_eq!(executor.schema_snapshot().foreign_keys.len(), 1);
        assert_eq!(executor.schema_snapshot().checks["messages"].len(), 1);
    }

    #[tokio::test]
    async fn indexed_reads_preserve_older_transaction_snapshots() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO docs (id, body) VALUES ('a', 'old')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX docs_body_idx ON docs (body)").unwrap())
            .await
            .unwrap();

        let mut snapshot = executor.begin_transaction(Isolation::Snapshot);
        executor
            .execute(parse("UPDATE docs SET body = 'new' WHERE id = 'a'").unwrap())
            .await
            .unwrap();
        let applied_ts = executor.indexes.read().unwrap()["docs"][0].applied_ts;
        assert!(snapshot.read_ts < applied_ts, "{} !< {applied_ts}", snapshot.read_ts);
        assert_eq!(
            executor.manager.get(&mut snapshot, &RecordKey::new("t", "d", "docs", b"a"),).unwrap(),
            Some(br#"{"body":"old","id":"a"}"#.to_vec())
        );
        let result = executor
            .execute_in_transaction(
                &mut snapshot,
                parse("SELECT * FROM docs WHERE body = 'old'").unwrap(),
            )
            .await
            .unwrap()
            .0;
        assert!(
            matches!(result, QueryResult::Rows { ref rows } if rows.len() == 1 && rows[0].0 == b"a"),
            "snapshot read_ts={}, result={result:?}",
            snapshot.read_ts
        );
    }

    #[tokio::test]
    async fn index_candidates_fall_back_during_and_after_out_of_order_commits() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO docs (id, body) VALUES ('a', 'body')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX docs_body_idx ON docs (body)").unwrap())
            .await
            .unwrap();

        let filter = vec![Predicate {
            field: Field::Value,
            column: Some(String::from("body")),
            op: Cmp::Eq,
            operand: b"body".to_vec(),
            operands: Vec::new(),
            alternatives: Vec::new(),
        }];
        let read_ts = executor.manager.latest_commit();
        let pending = executor.begin_index_commit(vec![String::from("docs")]);
        assert_eq!(executor.indexed_candidates("docs", &filter, read_ts).unwrap(), None);
        drop(pending);

        let applied_ts = executor.indexes.read().unwrap()["docs"][0].applied_ts;
        executor.apply_index_changes(
            &[TransactionChange {
                table: String::from("docs"),
                pk: b"a".to_vec(),
                previous_pk: None,
                op: Operation::Update,
                before: Some(br#"{"body":"body","id":"a"}"#.to_vec()),
                after: Some(br#"{"body":"new","id":"a"}"#.to_vec()),
            }],
            applied_ts.saturating_sub(1),
        );
        assert!(!executor.indexes.read().unwrap()["docs"][0].valid);
        assert_eq!(
            executor.indexed_candidates("docs", &filter, read_ts).unwrap(),
            Some(IndexedCandidates { pks: vec![b"a".to_vec()], covered_predicates: vec![0] })
        );
    }

    #[test]
    fn index_candidate_intersection_handles_dense_and_skewed_sets() {
        let dense_left: BTreeSet<Vec<u8>> =
            (0..80).map(|key| format!("key-{key:03}").into_bytes()).collect();
        let dense_right: BTreeSet<Vec<u8>> =
            (40..120).map(|key| format!("key-{key:03}").into_bytes()).collect();
        let dense = intersect_index_candidates(dense_left.iter().collect(), &dense_right);
        assert_eq!(
            dense.into_iter().cloned().collect::<Vec<_>>(),
            (40..80).map(|key| format!("key-{key:03}").into_bytes()).collect::<Vec<_>>()
        );

        let skewed_left = BTreeSet::from([b"key-050".to_vec()]);
        let skewed_right: BTreeSet<Vec<u8>> =
            (0..1000).map(|key| format!("key-{key:03}").into_bytes()).collect();
        let skewed = intersect_index_candidates(skewed_left.iter().collect(), &skewed_right);
        assert_eq!(skewed.into_iter().cloned().collect::<Vec<_>>(), vec![b"key-050".to_vec()]);
    }

    #[tokio::test]
    async fn indexed_candidates_intersect_matching_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE docs (id TEXT PRIMARY KEY, tenant TEXT, state TEXT)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO docs (id, tenant, state) VALUES \
                     ('a', 't1', 'active'), ('b', 't1', 'idle'), \
                     ('c', 't2', 'active'), ('d', 't3', 'active')",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX docs_tenant_idx ON docs (tenant)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX docs_state_idx ON docs (state)").unwrap())
            .await
            .unwrap();

        let filter = [
            Predicate {
                field: Field::Value,
                column: Some(String::from("tenant")),
                op: Cmp::Eq,
                operand: b"t1".to_vec(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            },
            Predicate {
                field: Field::Value,
                column: Some(String::from("state")),
                op: Cmp::Eq,
                operand: b"active".to_vec(),
                operands: Vec::new(),
                alternatives: Vec::new(),
            },
        ];
        assert_eq!(
            executor.indexed_candidates("docs", &filter, executor.manager.latest_commit()).unwrap(),
            Some(IndexedCandidates { pks: vec![b"a".to_vec()], covered_predicates: vec![0, 1] })
        );

        let result = executor
            .execute(parse("SELECT * FROM docs WHERE tenant = 't1' AND state = 'active'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.len() == 1 && rows[0].0 == b"a")
        );

        let contradictory = executor
            .execute(parse("SELECT * FROM docs WHERE tenant = 't1' AND tenant = 't2'").unwrap())
            .await
            .unwrap();
        assert!(matches!(contradictory, QueryResult::Rows { rows } if rows.is_empty()));
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
        executor
            .execute(parse("CREATE INDEX messages_key_idx ON messages (key)").unwrap())
            .await
            .unwrap();
        assert_eq!(
            executor
                .indexed_candidates(
                    "messages",
                    &[Predicate {
                        field: Field::Key,
                        column: None,
                        op: Cmp::Eq,
                        operand: b"absent".to_vec(),
                        operands: Vec::new(),
                        alternatives: Vec::new(),
                    }],
                    executor.manager.latest_commit(),
                )
                .unwrap(),
            Some(IndexedCandidates { pks: Vec::new(), covered_predicates: vec![0] })
        );

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
    async fn indexed_reads_fetch_large_candidate_sets_in_bounded_batches() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("CREATE TABLE messages").unwrap()).await.unwrap();
        let values =
            (0..600).map(|id| format!("('row-{id}', 'shared')")).collect::<Vec<_>>().join(", ");
        executor
            .execute(parse(&format!("INSERT INTO messages (id, value) VALUES {values}")).unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX messages_value_idx ON messages (value)").unwrap())
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE value = 'shared' LIMIT 1000").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows.len() == 600));
    }

    #[tokio::test]
    async fn secondary_indexes_are_scoped_per_tenant() {
        let manager = TxnManager::new();
        let tenant_a =
            Executor::with_manager(String::from("tenant-a"), String::from("d"), manager.clone());
        tenant_a.execute(parse("CREATE TABLE docs").unwrap()).await.unwrap();
        tenant_a.execute(parse("INSERT INTO docs KEY 'a' VALUE 'a'").unwrap()).await.unwrap();
        let tenant_b = tenant_a.clone().with_tenant(String::from("tenant-b"));
        tenant_b.execute(parse("INSERT INTO docs KEY 'b' VALUE 'b'").unwrap()).await.unwrap();

        tenant_a
            .execute(parse("CREATE INDEX docs_value_idx ON docs (value)").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            tenant_b.execute(parse("SELECT * FROM docs WHERE value = 'b'").unwrap()).await,
            Ok(QueryResult::Rows { rows }) if rows == vec![(b"b".to_vec(), b"b".to_vec())]
        ));

        tenant_a
            .execute(parse("CREATE UNIQUE INDEX docs_value_unique ON docs (value)").unwrap())
            .await
            .unwrap();
        tenant_b.execute(parse("INSERT INTO docs KEY 'same' VALUE 'a'").unwrap()).await.unwrap();
        assert!(tenant_a
            .execute(parse("INSERT INTO docs KEY 'duplicate' VALUE 'a'").unwrap())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn async_index_scan_applies_remaining_predicates_before_update() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE messages (id TEXT PRIMARY KEY, room TEXT, state TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse(
                "INSERT INTO messages (id, room, state) VALUES ('1', 'lobby', 'unread'), ('2', 'lobby', 'read'), ('3', 'game', 'unread')",
            ).unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX messages_room_idx ON messages (room)").unwrap())
            .await
            .unwrap();

        executor
            .execute(
                parse(
                    "UPDATE messages SET state = 'done' WHERE room = 'lobby' AND state = 'unread'",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT id, state FROM messages ORDER BY id").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Table { ref rows, .. }
                if rows == &vec![
                    vec![b"1".to_vec(), b"done".to_vec()],
                    vec![b"2".to_vec(), b"read".to_vec()],
                    vec![b"3".to_vec(), b"unread".to_vec()],
                ]
        ));
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
    async fn concurrent_unique_inserts_conflict_at_commit() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE profiles (id TEXT PRIMARY KEY, email TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE UNIQUE INDEX profiles_email_unique ON profiles (email)").unwrap(),
            )
            .await
            .unwrap();

        let mut first = executor.begin_transaction(Isolation::Snapshot);
        let mut second = executor.begin_transaction(Isolation::Snapshot);
        let (_, first_changes) = executor
            .execute_in_transaction(
                &mut first,
                parse("INSERT INTO profiles (id, email) VALUES ('p1', 'same@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let (_, second_changes) = executor
            .execute_in_transaction(
                &mut second,
                parse("INSERT INTO profiles (id, email) VALUES ('p2', 'same@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();

        let (first_result, second_result) = tokio::join!(
            executor.commit_transaction(first, first_changes),
            executor.commit_transaction(second, second_changes),
        );
        assert!(matches!(
            (first_result, second_result),
            (Ok(_), Err(RymeError::Conflict(_))) | (Err(RymeError::Conflict(_)), Ok(_))
        ));
    }

    #[tokio::test]
    async fn conflict_do_nothing_sees_unique_rows_staged_in_same_transaction() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE profiles (id TEXT PRIMARY KEY, email TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("CREATE UNIQUE INDEX profiles_email_unique ON profiles (email)").unwrap(),
            )
            .await
            .unwrap();

        let mut txn = executor.begin_transaction(Isolation::Serializable);
        let (_, mut changes) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO profiles (id, email) VALUES ('p1', 'same@example.com')")
                    .unwrap(),
            )
            .await
            .unwrap();
        let (_, mut ignored) = executor
            .execute_in_transaction(
                &mut txn,
                parse("INSERT INTO profiles (id, email) VALUES ('p2', 'same@example.com') ON CONFLICT DO NOTHING")
                    .unwrap(),
            )
            .await
            .unwrap();
        changes.append(&mut ignored);
        executor.commit_transaction(txn, changes).await.unwrap();

        let result = executor.execute(parse("SELECT * FROM profiles").unwrap()).await.unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.len() == 1 && rows[0].0 == b"p1")
        );
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
    async fn alter_table_add_column_backfills_defaults_and_persists_schema() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e1', 'hello')").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE events ADD COLUMN total INTEGER NOT NULL DEFAULT 7").unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT total FROM events WHERE id = 'e1'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"7".to_vec()]])
        );
        let snapshot = executor.schema_snapshot();
        assert!(snapshot.tables["events"].iter().any(|column| column.name == "total"));

        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e2', 'world')").unwrap())
            .await
            .unwrap();
        let result = executor
            .execute(parse("SELECT total FROM events WHERE id = 'e2'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"7".to_vec()]])
        );
    }

    #[tokio::test]
    async fn alter_table_rejects_not_null_without_default_on_existing_rows() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO events (id) VALUES ('e1')").unwrap()).await.unwrap();
        assert!(executor
            .execute(parse("ALTER TABLE events ADD COLUMN note TEXT NOT NULL").unwrap())
            .await
            .is_err());
        assert!(executor.catalog_columns("events").iter().all(|column| column.name != "note"));
    }

    #[tokio::test]
    async fn alter_table_column_type_converts_existing_values_and_rebuilds_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE events (id TEXT PRIMARY KEY, count TEXT UNIQUE, enabled TEXT, metadata TEXT)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO events (id, count, enabled, metadata) VALUES ('e1', '7', 'true', '{\"kind\":\"chat\"}')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE events ALTER COLUMN count TYPE INTEGER USING count::INTEGER")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE events ALTER COLUMN enabled SET DATA TYPE BOOLEAN").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE events ALTER COLUMN metadata TYPE JSONB").unwrap())
            .await
            .unwrap();

        let result = executor
            .execute(parse("SELECT count, enabled, metadata FROM events WHERE id = 'e1'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Table { rows, .. }
        if rows == vec![vec![
            b"7".to_vec(),
            b"true".to_vec(),
            br#"{"kind":"chat"}"#.to_vec()
        ]]));
        assert!(executor
            .catalog_columns("events")
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case("count"))
            .is_some_and(|column| column.data_type == "integer"));
        let indexed =
            executor.execute(parse("SELECT * FROM events WHERE count = 7").unwrap()).await.unwrap();
        assert!(matches!(indexed, QueryResult::Rows { rows } if rows.len() == 1));
    }

    #[tokio::test]
    async fn alter_table_drop_column_removes_data_and_dependent_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE events (id TEXT PRIMARY KEY, keep TEXT, obsolete TEXT UNIQUE)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO events (id, keep, obsolete) VALUES ('e1', 'hello', 'old')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX events_keep_idx ON events (keep)").unwrap())
            .await
            .unwrap();

        executor.execute(parse("ALTER TABLE events DROP COLUMN obsolete").unwrap()).await.unwrap();
        executor
            .execute(parse("ALTER TABLE events DROP COLUMN IF EXISTS missing").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE events ADD COLUMN IF NOT EXISTS keep TEXT").unwrap())
            .await
            .unwrap();

        assert!(executor
            .catalog_columns("events")
            .iter()
            .all(|column| !column.name.eq_ignore_ascii_case("obsolete")));
        let indexes = executor.catalog_indexes("events");
        assert!(indexes.iter().any(|index| index.name == "events_keep_idx"));
        assert!(indexes.iter().all(|index| {
            !index.column.as_deref().is_some_and(|column| column.eq_ignore_ascii_case("obsolete"))
        }));
        let result = executor
            .execute(parse("SELECT keep FROM events WHERE id = 'e1'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"hello".to_vec()]])
        );

        executor
            .execute(parse("INSERT INTO events (id, keep) VALUES ('e2', 'world')").unwrap())
            .await
            .unwrap();
        assert!(executor
            .execute(parse("ALTER TABLE events DROP COLUMN id").unwrap())
            .await
            .is_err());
        assert!(executor
            .catalog_columns("events")
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case("id")));
    }

    #[tokio::test]
    async fn alter_table_rename_column_migrates_rows_and_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT UNIQUE)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO users (id, email) VALUES ('u1', 'a@example.com')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX users_email_idx ON users (email)").unwrap())
            .await
            .unwrap();

        executor
            .execute(parse("ALTER TABLE users RENAME COLUMN email TO address").unwrap())
            .await
            .unwrap();

        let columns = executor.catalog_columns("users");
        assert!(columns.iter().any(|column| column.name == "address"));
        assert!(columns.iter().all(|column| column.name != "email"));
        let indexes = executor.catalog_indexes("users");
        assert!(indexes.iter().all(|index| {
            !index.column.as_deref().is_some_and(|column| column.eq_ignore_ascii_case("email"))
        }));
        assert!(indexes.iter().any(|index| {
            index.name == "users_email_idx"
                && index.column.as_deref().is_some_and(|column| column == "address")
        }));
        let result = executor
            .execute(parse("SELECT address FROM users WHERE id = 'u1'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"a@example.com".to_vec()]])
        );

        assert!(executor
            .execute(
                parse("INSERT INTO users (id, address) VALUES ('u2', 'a@example.com')").unwrap()
            )
            .await
            .is_err());
        executor
            .execute(
                parse("INSERT INTO users (id, address) VALUES ('u2', 'b@example.com')").unwrap(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn alter_table_column_updates_defaults_and_nullability() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE events (id TEXT PRIMARY KEY, state TEXT DEFAULT 'new')")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE events ALTER COLUMN state SET DEFAULT 'queued'").unwrap())
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO events (id) VALUES ('e1')").unwrap()).await.unwrap();
        let result = executor
            .execute(parse("SELECT state FROM events WHERE id = 'e1'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"queued".to_vec()]])
        );

        executor
            .execute(parse("ALTER TABLE events ALTER COLUMN state DROP DEFAULT").unwrap())
            .await
            .unwrap();
        assert!(executor
            .catalog_columns("events")
            .iter()
            .find(|column| column.name == "state")
            .is_some_and(|column| column.column_default.is_none()));

        executor
            .execute(parse("CREATE TABLE jobs (id TEXT PRIMARY KEY, status TEXT)").unwrap())
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO jobs (id) VALUES ('j1')").unwrap()).await.unwrap();
        assert!(executor
            .execute(parse("ALTER TABLE jobs ALTER COLUMN status SET NOT NULL").unwrap())
            .await
            .is_err());
        executor
            .execute(parse("UPDATE jobs SET status = 'ready' WHERE id = 'j1'").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE jobs ALTER COLUMN status SET NOT NULL").unwrap())
            .await
            .unwrap();
        assert!(executor
            .catalog_columns("jobs")
            .iter()
            .find(|column| column.name == "status")
            .is_some_and(|column| !column.nullable));
        executor
            .execute(parse("ALTER TABLE jobs ALTER COLUMN status DROP NOT NULL").unwrap())
            .await
            .unwrap();
        assert!(executor
            .catalog_columns("jobs")
            .iter()
            .find(|column| column.name == "status")
            .is_some_and(|column| column.nullable));
    }

    #[tokio::test]
    async fn drop_table_removes_rows_schema_and_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT UNIQUE)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e1', 'hello')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX events_name_idx ON events (name)").unwrap())
            .await
            .unwrap();

        executor.execute(parse("DROP TABLE events").unwrap()).await.unwrap();
        assert!(!executor.catalog_tables().iter().any(|table| table == "events"));
        assert!(executor.catalog_columns("events").is_empty());
        assert!(executor.catalog_indexes("events").is_empty());
        assert!(executor.execute(parse("DROP TABLE IF EXISTS events").unwrap()).await.is_ok());
        assert!(executor.execute(parse("DROP TABLE missing").unwrap()).await.is_err());

        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e2', 'world')").unwrap())
            .await
            .unwrap();
        let result = executor
            .execute(parse("SELECT name FROM events WHERE id = 'e2'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"world".to_vec()]])
        );
    }

    #[tokio::test]
    async fn truncate_table_clears_rows_but_preserves_schema_and_indexes() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE events (id TEXT PRIMARY KEY, name TEXT UNIQUE)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e1', 'hello')").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE INDEX events_name_idx ON events (name)").unwrap())
            .await
            .unwrap();

        let mut historical = executor.begin_transaction(Isolation::Snapshot);
        executor.execute(parse("TRUNCATE TABLE events").unwrap()).await.unwrap();
        assert_eq!(executor.catalog_tables(), vec![String::from("events")]);
        assert!(executor.catalog_columns("events").iter().any(|column| column.name == "name"));
        assert!(executor
            .catalog_indexes("events")
            .iter()
            .any(|index| index.name == "events_name_idx"));
        let result = executor.execute(parse("SELECT name FROM events").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Table { rows, .. } if rows.is_empty()));
        let historical_result = executor
            .execute_in_transaction(
                &mut historical,
                parse("SELECT * FROM events WHERE name = 'hello'").unwrap(),
            )
            .await
            .unwrap()
            .0;
        assert!(matches!(historical_result, QueryResult::Rows { rows } if rows.len() == 1));

        executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e2', 'world')").unwrap())
            .await
            .unwrap();
        assert!(executor
            .execute(parse("INSERT INTO events (id, name) VALUES ('e3', 'world')").unwrap())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn truncate_identity_options_and_cascade_match_postgres_semantics() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE users (id SERIAL PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        executor
            .execute(parse("CREATE TABLE messages (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users (id))").unwrap())
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO users DEFAULT VALUES").unwrap()).await.unwrap();
        executor
            .execute(parse("INSERT INTO messages (user_id) VALUES (1)").unwrap())
            .await
            .unwrap();

        assert!(executor.execute(parse("TRUNCATE TABLE users").unwrap()).await.is_err());
        executor
            .execute(parse("TRUNCATE TABLE users CASCADE CONTINUE IDENTITY").unwrap())
            .await
            .unwrap();
        executor.execute(parse("INSERT INTO users DEFAULT VALUES").unwrap()).await.unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT id FROM users").unwrap()).await.unwrap(),
            QueryResult::Table { rows, .. } if rows == vec![vec![b"2".to_vec()]]
        ));

        executor.execute(parse("TRUNCATE users CASCADE RESTART IDENTITY").unwrap()).await.unwrap();
        executor.execute(parse("INSERT INTO users DEFAULT VALUES").unwrap()).await.unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT id FROM users").unwrap()).await.unwrap(),
            QueryResult::Table { rows, .. } if rows == vec![vec![b"1".to_vec()]]
        ));
    }

    #[tokio::test]
    async fn drop_named_constraints_removes_enforcement_and_survives_schema_restore() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let accounts_statement = parse("CREATE TABLE accounts (id TEXT PRIMARY KEY, email TEXT, age INTEGER, CONSTRAINT accounts_email_key UNIQUE (email), CONSTRAINT accounts_age_check CHECK (age > 0))").unwrap();
        assert!(matches!(
            &accounts_statement,
            Statement::CreateTable { named_constraints, .. } if named_constraints.len() == 2
        ));
        executor.execute(accounts_statement).await.unwrap();
        executor.execute(parse("CREATE TABLE users (id TEXT PRIMARY KEY)").unwrap()).await.unwrap();
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, user_id TEXT, CONSTRAINT messages_user_fk FOREIGN KEY (user_id) REFERENCES users (id))").unwrap())
            .await
            .unwrap();
        let snapshot = executor.schema_snapshot();
        assert_eq!(snapshot.constraints["accounts"].len(), 2);
        assert_eq!(snapshot.constraints["messages"].len(), 1);

        executor
            .execute(
                parse("INSERT INTO accounts (id, email, age) VALUES ('1', 'a@example.com', 1)")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(executor
            .execute(
                parse("INSERT INTO accounts (id, email, age) VALUES ('2', 'a@example.com', 2)")
                    .unwrap()
            )
            .await
            .is_err());
        assert!(executor
            .execute(
                parse("INSERT INTO accounts (id, email, age) VALUES ('3', 'b@example.com', 0)")
                    .unwrap()
            )
            .await
            .is_err());
        assert!(executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m1', 'missing')").unwrap())
            .await
            .is_err());

        executor
            .execute(parse("ALTER TABLE accounts DROP CONSTRAINT accounts_email_key").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_age_check").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("ALTER TABLE messages DROP CONSTRAINT messages_user_fk").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO accounts (id, email, age) VALUES ('2', 'a@example.com', 0)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(parse("INSERT INTO messages (id, user_id) VALUES ('m1', 'missing')").unwrap())
            .await
            .unwrap();

        let restored = Executor::new(String::from("t"), String::from("d"));
        restored.restore_schema_snapshot(snapshot).unwrap();
        assert_eq!(restored.schema_snapshot().constraints["accounts"].len(), 2);
        restored
            .execute(parse("ALTER TABLE accounts DROP CONSTRAINT accounts_email_key").unwrap())
            .await
            .unwrap();
        assert!(restored
            .execute(
                parse("INSERT INTO accounts (id, email, age) VALUES ('4', 'a@example.com', 3)")
                    .unwrap()
            )
            .await
            .is_ok());
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

    #[test]
    fn bind_params_respects_sql_literals_comments_and_parameter_width() {
        let sql = bind(
            "SELECT '$1', \"$2\", $1, $10 -- $2\n/* $3 */",
            &(1..=10).map(|value| value.to_string()).collect::<Vec<_>>(),
        );
        assert_eq!(sql, "SELECT '$1', \"$2\", 1, 10 -- $2\n/* $3 */");
    }

    #[test]
    fn bind_params_respects_dollar_quotes_and_nested_block_comments() {
        let sql = bind(
            "SELECT field$tag$part, $body$ $1 /* not a comment */ $body$, $1, /* outer /* $1 */ still $1 */ $1",
            &[String::from("value")],
        );
        assert_eq!(
            sql,
            "SELECT field$tag$part, $body$ $1 /* not a comment */ $body$, 'value', /* outer /* $1 */ still $1 */ 'value'"
        );
    }

    #[test]
    fn bind_params_preserves_scalar_types_and_escapes_text() {
        assert_eq!(
            bind(
                "SELECT $1, $2, $3, $4, $5",
                &[
                    String::from("42"),
                    String::from("true"),
                    String::from("\0"),
                    String::from("O'Reilly"),
                    String::from("NULL"),
                ],
            ),
            "SELECT 42, true, NULL, 'O''Reilly', 'NULL'"
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
    async fn bulk_upsert_persists_implicit_table_schema() {
        let path = std::env::temp_dir().join(format!(
            "ryme-bulk-schema-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let mut executor = Executor::new(String::from("t"), String::from("d"));
        executor.set_schema_path(path.clone()).unwrap();

        executor
            .bulk_upsert(String::from("events"), vec![(b"e1".to_vec(), b"ok".to_vec())])
            .await
            .unwrap();

        let snapshot: SchemaSnapshot =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(snapshot.tables.contains_key("events"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn copy_to_projects_all_rows_and_preserves_nulls() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT, active BOOLEAN)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO docs (id, body, active) VALUES ('1', 'hello', true)").unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO docs (id, body, active) VALUES ('2', NULL, false)").unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(parse("COPY docs TO STDOUT").unwrap(), Statement::CopyTo { ref table, ref columns } if table == "docs" && columns.is_empty())
        );
        let result =
            executor.execute(parse("COPY docs (id, body) TO STDOUT").unwrap()).await.unwrap();
        assert!(matches!(result, QueryResult::Table { ref columns, ref rows }
        if columns == &[String::from("id"), String::from("body")]
            && rows == &vec![
                vec![b"1".to_vec(), b"hello".to_vec()],
                vec![b"2".to_vec(), SQL_NULL_SENTINEL.to_vec()]
            ]));
    }

    #[tokio::test]
    async fn scalar_select_values_use_the_core_executor() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        let statement =
            parse("SELECT 1, true, NULL, 'hello' AS greeting, version(), current_schema(), now()")
                .unwrap();
        assert!(matches!(statement, Statement::SelectValues { ref columns, .. } if columns == &[
            String::from("1"),
            String::from("true"),
            String::from("NULL"),
            String::from("greeting"),
            String::from("version()"),
            String::from("current_schema()"),
            String::from("now()"),
        ]));
        let result = executor.execute(statement).await.unwrap();
        match result {
            QueryResult::Table { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], b"1");
                assert_eq!(rows[0][1], b"true");
                assert_eq!(rows[0][2], SQL_NULL_SENTINEL);
                assert_eq!(rows[0][3], b"hello");
                assert!(String::from_utf8_lossy(&rows[0][4]).contains("rymeDB"));
                assert_eq!(rows[0][5], b"public");
                assert!(
                    String::from_utf8_lossy(&rows[0][6]).parse::<u64>().unwrap() > 1_700_000_000
                );
            }
            _ => panic!("expected scalar table"),
        }
        let bound = bind("SELECT $1::text AS echo", &[String::from("hi")]);
        let result = executor.execute(parse(&bound).unwrap()).await.unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"hi".to_vec()]])
        );
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
        let analyzed = parse("EXPLAIN ANALYZE SELECT * FROM docs LIMIT 10").unwrap();
        assert!(matches!(analyzed, Statement::Explain { analyze: true, .. }));
        assert!(!analyzed.is_write());
        assert!(parse("explain analyze insert into docs key 'k' value 'v'").unwrap().is_write());
    }

    #[tokio::test]
    async fn explain_requires_table_privileges() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE private_rows (id TEXT PRIMARY KEY)").unwrap())
            .await
            .unwrap();
        executor.execute(parse("CREATE ROLE app_reader LOGIN").unwrap()).await.unwrap();

        let reader = executor.clone().with_role(String::from("app_reader"));
        assert!(matches!(reader.explain("SELECT * FROM private_rows"), Err(RymeError::Forbidden)));

        executor
            .execute(parse("GRANT SELECT ON TABLE private_rows TO app_reader").unwrap())
            .await
            .unwrap();
        assert!(reader.explain("SELECT * FROM private_rows").is_ok());
    }

    #[tokio::test]
    async fn explain_analyze_executes_inner_statement() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor.execute(parse("INSERT INTO docs KEY 'k' VALUE 'v'").unwrap()).await.unwrap();

        let result = executor
            .execute(parse("EXPLAIN ANALYZE SELECT * FROM docs KEY 'k'").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::Row { value, .. }
                if String::from_utf8_lossy(&value).contains("actual rows=1")
                    && String::from_utf8_lossy(&value).contains("duration_us=")
        ));

        executor
            .execute(parse("EXPLAIN ANALYZE UPSERT INTO docs KEY 'k' VALUE 'updated'").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            executor.execute(parse("SELECT * FROM docs KEY 'k'").unwrap()).await.unwrap(),
            QueryResult::Row { value, .. } if value == b"updated"
        ));
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
            .execute(parse("SELECT * FROM docs WHERE value LIKE 'ap%' LIMIT 1 OFFSET 1").unwrap())
            .await
            .unwrap();
        assert!(matches!(rows, QueryResult::Rows { rows }
            if rows.len() == 1 && rows[0].0 == b"c"));
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
        let between = parse("SELECT * FROM docs WHERE key BETWEEN 'a' AND 'b'").unwrap();
        assert!(
            matches!(between, Statement::SelectScan { filter, .. } if filter[0].op == Cmp::Between)
        );
    }

    #[tokio::test]
    async fn select_distinct_deduplicates_before_limit_and_offset() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE messages (id TEXT PRIMARY KEY, room TEXT)").unwrap())
            .await
            .unwrap();
        executor
            .execute(
                parse(
                    "INSERT INTO messages (id, room) VALUES ('m1', 'a'), ('m2', 'a'), ('m3', 'b')",
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let result = executor
            .execute(
                parse("SELECT DISTINCT room FROM messages ORDER BY room ASC LIMIT 1 OFFSET 1")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![b"b".to_vec()]])
        );
        let plan =
            executor.explain("SELECT DISTINCT room FROM messages ORDER BY room ASC").unwrap();
        assert!(plan.contains("distinct"));
    }

    #[tokio::test]
    async fn where_in_and_not_in_match_multiple_operands() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (id, room) in [("m1", "lobby"), ("m2", "game"), ("m3", "support")] {
            executor
                .execute(
                    parse(&format!(
                        "INSERT INTO messages KEY '{id}' VALUE '{{\"room\":\"{room}\"}}'"
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let statement = parse("SELECT * FROM messages WHERE room IN ('lobby', 'game')").unwrap();
        let Statement::SelectScan { filter, .. } = &statement else {
            panic!("expected select scan")
        };
        assert_eq!(filter[0].op, Cmp::In);
        assert_eq!(filter[0].operands, vec![b"lobby".to_vec(), b"game".to_vec()]);
        let result = executor.execute(statement).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows.len() == 2));

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE room NOT IN ('lobby', 'game')").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows == vec![(b"m3".to_vec(), br#"{"room":"support"}"#.to_vec())])
        );
    }

    #[tokio::test]
    async fn where_between_and_not_between_match_inclusive_ranges() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (id, score) in [("a", 10), ("b", 20), ("c", 30)] {
            executor
                .execute(
                    parse(&format!("INSERT INTO scores KEY '{id}' VALUE '{{\"score\":{score}}}'"))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let statement = parse("SELECT * FROM scores WHERE score BETWEEN 10 AND 20").unwrap();
        let Statement::SelectScan { filter, .. } = &statement else {
            panic!("expected select scan")
        };
        assert_eq!(filter[0].op, Cmp::Between);
        assert_eq!(filter[0].operands, vec![b"10".to_vec(), b"20".to_vec()]);
        let result = executor.execute(statement).await.unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows.len() == 2));

        let result = executor
            .execute(parse("SELECT * FROM scores WHERE score NOT BETWEEN 10 AND 20").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows.len() == 1));
    }

    #[tokio::test]
    async fn where_or_preserves_and_precedence() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (id, room, state) in [
            ("m1", "lobby", "unread"),
            ("m2", "game", "unread"),
            ("m3", "game", "read"),
            ("m4", "support", "read"),
        ] {
            executor
                .execute(
                    parse(&format!(
                        "INSERT INTO messages KEY '{id}' VALUE '{{\"room\":\"{room}\",\"state\":\"{state}\"}}'"
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
        }

        let statement = parse(
            "SELECT * FROM messages WHERE room = 'lobby' OR room = 'game' AND state = 'unread'",
        )
        .unwrap();
        let Statement::SelectScan { filter, .. } = &statement else {
            panic!("expected select scan")
        };
        assert_eq!(filter.len(), 1);
        assert_eq!(filter[0].op, Cmp::AnyOf);
        assert_eq!(filter[0].alternatives.len(), 2);
        assert_eq!(filter[0].alternatives[1].len(), 2);
        let result = executor.execute(statement).await.unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"m1".as_slice(), b"m2".as_slice()])
        );
        assert!(parse("SELECT * FROM messages WHERE room = 'lobby' OR").is_err());
    }

    #[tokio::test]
    async fn where_is_distinct_from_is_null_safe() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (id, value) in
            [("m1", r#"{"state":null}"#), ("m2", r#"{"state":"ready"}"#), ("m3", r#"{}"#)]
        {
            executor
                .execute(
                    parse(&format!("INSERT INTO messages KEY '{id}' VALUE '{value}'")).unwrap(),
                )
                .await
                .unwrap();
        }

        let statement = parse("SELECT * FROM messages WHERE state IS DISTINCT FROM NULL").unwrap();
        let Statement::SelectScan { filter, .. } = &statement else {
            panic!("expected select scan")
        };
        assert_eq!(filter[0].op, Cmp::IsDistinct);
        let result = executor.execute(statement).await.unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"m2".as_slice()])
        );

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE state IS NOT DISTINCT FROM NULL").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"m1".as_slice(), b"m3".as_slice()])
        );

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE state IS DISTINCT FROM 'ready'").unwrap())
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::Rows { rows } if rows.len() == 2));
    }

    #[tokio::test]
    async fn where_not_like_and_not_ilike_negate_patterns() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        for (id, value) in [("m1", "Hello world"), ("m2", "HELLO agent"), ("m3", "bye")] {
            executor
                .execute(
                    parse(&format!("INSERT INTO messages KEY '{id}' VALUE '{value}'")).unwrap(),
                )
                .await
                .unwrap();
        }

        let statement = parse("SELECT * FROM messages WHERE value NOT LIKE 'Hello%'").unwrap();
        let Statement::SelectScan { filter, .. } = &statement else {
            panic!("expected select scan")
        };
        assert_eq!(filter[0].op, Cmp::NotLike);
        let result = executor.execute(statement).await.unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"m2".as_slice(), b"m3".as_slice()])
        );

        let result = executor
            .execute(parse("SELECT * FROM messages WHERE value NOT ILIKE 'hello%'").unwrap())
            .await
            .unwrap();
        assert!(
            matches!(result, QueryResult::Rows { rows } if rows.iter().map(|(pk, _)| pk.as_slice()).collect::<Vec<_>>() == vec![b"m3".as_slice()])
        );
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
            ("SELECT COUNT(*) FROM nums", "count", Some("4")),
            ("SELECT COUNT(*) FROM nums WHERE value != 'oops'", "count", Some("3")),
            ("SELECT SUM(value) FROM nums", "sum", Some("60")),
            ("SELECT AVG(value) FROM nums", "avg", Some("20")),
            ("SELECT MIN(value) FROM nums", "min", Some("10")),
            ("SELECT MAX(value) FROM nums", "max", Some("oops")),
            ("SELECT MIN(key) FROM nums", "min", Some("a")),
            ("SELECT AVG(value) FROM nums WHERE key = 'ghost'", "avg", None),
            ("SELECT SUM(value) FROM nums WHERE key = 'ghost'", "sum", None),
            ("SELECT MIN(value) FROM nums WHERE key = 'ghost'", "min", None),
            ("SELECT MAX(value) FROM nums WHERE key = 'ghost'", "max", None),
        ] {
            match executor.execute(parse(sql).unwrap()).await.unwrap() {
                QueryResult::Scalar { label: got_label, value: got_value } => {
                    assert_eq!(got_label, label, "{sql}");
                    match value {
                        Some(expected) => {
                            assert_eq!(String::from_utf8(got_value).unwrap(), expected, "{sql}");
                        }
                        None => assert_eq!(got_value, SQL_NULL_SENTINEL, "{sql}"),
                    }
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
    async fn aggregate_names_are_valid_projection_columns_without_parentheses() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(
                parse(
                    "CREATE TABLE metrics (id TEXT PRIMARY KEY, count INTEGER, sum INTEGER, avg INTEGER, min INTEGER, max INTEGER)",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO metrics (id, count, sum, avg, min, max) VALUES ('m1', 1, 2, 3, 4, 5)")
                    .unwrap(),
            )
            .await
            .unwrap();
        executor
            .execute(
                parse("INSERT INTO metrics (id, count, sum, avg, min, max) VALUES ('m2', NULL, NULL, NULL, NULL, NULL)")
                    .unwrap(),
            )
            .await
            .unwrap();

        for (column, expected) in
            [("count", b"1"), ("sum", b"2"), ("avg", b"3"), ("min", b"4"), ("max", b"5")]
        {
            let statement =
                parse(&format!("SELECT {column} FROM metrics WHERE id = 'm1'")).unwrap();
            assert!(matches!(statement, Statement::SelectColumns { .. }), "{column}");
            let result = executor.execute(statement).await.unwrap();
            assert!(
                matches!(result, QueryResult::Table { rows, .. } if rows == vec![vec![expected.to_vec()]]),
                "{column}"
            );
        }
        let sum = executor.execute(parse("SELECT SUM(sum) FROM metrics").unwrap()).await.unwrap();
        assert!(matches!(sum, QueryResult::Scalar { value, .. } if value == b"2"));
        let count =
            executor.execute(parse("SELECT COUNT(count) FROM metrics").unwrap()).await.unwrap();
        assert!(matches!(count, QueryResult::Scalar { value, .. } if value == b"1"));

        let groups = executor
            .execute(parse("SELECT sum, COUNT(*) FROM metrics GROUP BY sum").unwrap())
            .await
            .unwrap();
        match groups {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 2);
                let null_group: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert!(null_group["sum"].is_null());
                assert_eq!(null_group["count"], "1");
                let value_group: serde_json::Value = serde_json::from_slice(&rows[1].1).unwrap();
                assert_eq!(value_group["sum"], "2");
                assert_eq!(value_group["count"], "1");
            }
            _ => panic!("expected grouped rows"),
        }

        executor
            .execute(parse("CREATE TABLE teams (id TEXT PRIMARY KEY, payload JSONB)").unwrap())
            .await
            .unwrap();
        for (id, team) in [("t1", "red"), ("t2", "red"), ("t3", "blue")] {
            executor
                .execute(
                    parse(&format!(
                        "INSERT INTO teams (id, payload) VALUES ('{id}', '{{\"team\":\"{team}\"}}')"
                    ))
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let json_groups = executor
            .execute(
                parse(
                    "SELECT payload->>'team', COUNT(*) FROM teams GROUP BY payload->>'team' ORDER BY payload->>'team' DESC LIMIT 1",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        match json_groups {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 1);
                let row: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert_eq!(row["payload->>'team'"], "red");
                assert_eq!(row["count"], "2");
            }
            _ => panic!("expected ordered grouped rows"),
        }
    }

    #[tokio::test]
    async fn large_aggregates_groups_and_joins_read_past_first_page() {
        let executor = Executor::new(String::from("t"), String::from("d"));
        executor
            .execute(parse("CREATE TABLE metrics (id TEXT PRIMARY KEY, amount INTEGER)").unwrap())
            .await
            .unwrap();
        let values =
            (0..10_001).map(|index| format!("('m{index}', 1)")).collect::<Vec<_>>().join(", ");
        executor
            .execute(parse(&format!("INSERT INTO metrics (id, amount) VALUES {values}")).unwrap())
            .await
            .unwrap();

        let aggregate =
            executor.execute(parse("SELECT COUNT(amount) FROM metrics").unwrap()).await.unwrap();
        assert!(matches!(aggregate, QueryResult::Scalar { value, .. } if value == b"10001"));

        let groups = executor
            .execute(
                parse("SELECT key, SUM(amount) FROM metrics GROUP BY key OFFSET 10000 LIMIT 1")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(groups, QueryResult::Rows { rows } if rows.len() == 1));

        let joined = executor
            .execute(
                parse("SELECT * FROM metrics JOIN metrics ON KEY = KEY OFFSET 10000 LIMIT 1")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(joined, QueryResult::Rows { rows } if rows.len() == 1));
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
        let statement =
            parse("SELECT value, COUNT(*) FROM tags GROUP BY value HAVING COUNT(*) > 2").unwrap();
        let Statement::GroupBy { having, .. } = &statement else {
            panic!("expected grouped statement")
        };
        assert_eq!(having.len(), 1);
        let rows = executor.execute(statement).await.unwrap();
        match rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].0, b"red".to_vec());
                let only: serde_json::Value = serde_json::from_slice(&rows[0].1).unwrap();
                assert_eq!(only["count"], "3");
            }
            _ => panic!("expected having rows"),
        }
        let union = executor
            .execute(
                parse("SELECT value FROM tags WHERE value = 'red' UNION SELECT value FROM tags WHERE value = 'red'")
                    .unwrap(),
            )
            .await
            .unwrap();
        match union {
            QueryResult::Table { columns, rows } => {
                assert_eq!(columns, vec![String::from("value")]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], vec![b"red".to_vec()]);
            }
            _ => panic!("expected union table"),
        }
        let union_all = executor
            .execute(
                parse("SELECT value FROM tags WHERE value = 'red' UNION ALL SELECT value FROM tags WHERE value = 'red'")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(union_all, QueryResult::Table { rows, .. } if rows.len() == 6));
        let intersect = executor
            .execute(
                parse("SELECT value FROM tags WHERE value = 'red' INTERSECT SELECT value FROM tags WHERE value = 'red'")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(intersect, QueryResult::Table { rows, .. } if rows.len() == 1));
        let intersect_all = executor
            .execute(
                parse("SELECT value FROM tags WHERE value = 'red' INTERSECT ALL SELECT value FROM tags WHERE value = 'red'")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(intersect_all, QueryResult::Table { rows, .. } if rows.len() == 3));
        let except = executor
            .execute(
                parse("SELECT value FROM tags EXCEPT SELECT value FROM tags WHERE value = 'blue'")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(except, QueryResult::Table { rows, .. } if rows.len() == 1));
        let chained = executor
            .execute(
                parse("SELECT value FROM tags WHERE value = 'red' UNION SELECT value FROM tags WHERE value = 'blue' INTERSECT SELECT value FROM tags WHERE value = 'blue'")
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(chained, QueryResult::Table { rows, .. } if rows.len() == 2));
        let plan =
            executor.explain("SELECT value FROM tags UNION ALL SELECT value FROM tags").unwrap();
        assert!(plan.contains("union all"));
        let intersect_plan =
            executor.explain("SELECT value FROM tags INTERSECT SELECT value FROM tags").unwrap();
        assert!(intersect_plan.contains("intersect"));
        assert!(executor
            .execute(parse("SELECT value FROM tags UNION SELECT key, value FROM tags").unwrap())
            .await
            .is_err());
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
        let left_rows = executor
            .execute(
                parse("SELECT * FROM users LEFT JOIN orders ON KEY = KEY ORDER BY key ASC")
                    .unwrap(),
            )
            .await
            .unwrap();
        match left_rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 3);
                let orphan: serde_json::Value = serde_json::from_slice(&rows[2].1).unwrap();
                assert_eq!(orphan["left"], "orphan");
                assert!(orphan["right"].is_null());
            }
            _ => panic!("expected left join rows"),
        }
        let right_rows = executor
            .execute(
                parse("SELECT * FROM users RIGHT JOIN orders ON KEY = KEY ORDER BY key ASC")
                    .unwrap(),
            )
            .await
            .unwrap();
        match right_rows {
            QueryResult::Rows { rows } => {
                assert_eq!(rows.len(), 3);
                let stray: serde_json::Value = serde_json::from_slice(&rows[2].1).unwrap();
                assert!(stray["left"].is_null());
                assert_eq!(stray["right"], "stray");
            }
            _ => panic!("expected right join rows"),
        }
        let full_rows = executor
            .execute(parse("SELECT * FROM users FULL OUTER JOIN orders ON KEY = KEY").unwrap())
            .await
            .unwrap();
        assert!(matches!(full_rows, QueryResult::Rows { rows } if rows.len() == 4));
        let left_plan =
            executor.explain("SELECT * FROM users LEFT JOIN orders ON KEY = KEY").unwrap();
        assert!(left_plan.contains("hash_left_join(users,orders)"));
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
