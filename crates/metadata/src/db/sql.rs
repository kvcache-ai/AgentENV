//! Concrete PostgreSQL / SQLite SQL runner with native UUID binding and row-lock suffixes.

use std::fmt::Write;

use agentenv_domain::SnapshotId;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgQueryResult, PgRow};
use sqlx::sqlite::{SqliteArguments, SqliteQueryResult, SqliteRow};
use sqlx::{AssertSqlSafe, Connection, Decode, Postgres, Row, Sqlite, Transaction, Type};
use uuid::fmt::Hyphenated;
use uuid::Uuid;

use super::DatabasePool;
use crate::{MetaError, MetaResult};

const PG_BEGIN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";
const SQLITE_BEGIN: &str = "BEGIN IMMEDIATE";

pub(crate) enum DbVal {
    Text(String),
    OptText(Option<String>),
    Uuid(Uuid),
    OptUuid(Option<Uuid>),
    I64(i64),
}

macro_rules! impl_db_val {
    ($($ty:ty => |$v:ident| $body:expr),+ $(,)?) => {
        $(impl From<$ty> for DbVal {
            fn from($v: $ty) -> Self { $body }
        })+
    };
}

impl_db_val! {
    &str => |v| Self::Text(v.to_owned()),
    String => |v| Self::Text(v),
    &String => |v| Self::Text(v.clone()),
    Option<&str> => |v| Self::OptText(v.map(str::to_owned)),
    Option<String> => |v| Self::OptText(v),
    Uuid => |v| Self::Uuid(v),
    &Uuid => |v| Self::Uuid(*v),
    Option<Uuid> => |v| Self::OptUuid(v),
    &SnapshotId => |v| Self::Uuid(*v.as_uuid()),
    SnapshotId => |v| Self::Uuid(*v.as_uuid()),
    i64 => |v| Self::I64(v),
}

#[derive(Clone, Copy)]
enum RowLock {
    None,
    ForUpdate,
    ForShare,
}

/// Dialect-neutral SQL statement supporting static `$1..$N` placeholders and dynamic fragments.
pub(crate) struct Sql {
    text: String,
    params: Vec<DbVal>,
    lock: RowLock,
}

impl Sql {
    pub(crate) fn new(sql: impl Into<String>) -> Self {
        Self {
            text: sql.into(),
            params: Vec::new(),
            lock: RowLock::None,
        }
    }

    pub(crate) fn bind(mut self, value: impl Into<DbVal>) -> Self {
        self.params.push(value.into());
        self
    }

    pub(crate) fn for_update(mut self) -> Self {
        self.lock = RowLock::ForUpdate;
        self
    }

    pub(crate) fn for_share(mut self) -> Self {
        self.lock = RowLock::ForShare;
        self
    }

    pub(crate) fn push(&mut self, fragment: &str) -> &mut Self {
        self.text.push_str(fragment);
        self
    }

    pub(crate) fn push_bind(&mut self, value: impl Into<DbVal>) -> &mut Self {
        self.params.push(value.into());
        let _ = write!(self.text, "${}", self.params.len());
        self
    }

    pub(crate) fn push_sep<T>(
        &mut self,
        sep: &str,
        items: impl IntoIterator<Item = T>,
        mut f: impl FnMut(&mut Self, T),
    ) -> &mut Self {
        for (i, item) in items.into_iter().enumerate() {
            if i > 0 {
                self.text.push_str(sep);
            }
            f(self, item);
        }
        self
    }

    pub(crate) fn push_in<T: Into<DbVal>>(
        &mut self,
        values: impl IntoIterator<Item = T>,
    ) -> &mut Self {
        self.push_sep(", ", values, |q, v| {
            q.push_bind(v);
        })
    }

    fn render(&self, is_postgres: bool) -> String {
        match (is_postgres, self.lock) {
            (true, RowLock::ForUpdate) => format!("{} FOR UPDATE", self.text),
            (true, RowLock::ForShare) => format!("{} FOR SHARE", self.text),
            _ => self.text.clone(),
        }
    }
}

pub(crate) enum DbRow {
    Postgres(PgRow),
    Sqlite(SqliteRow),
}

impl DbRow {
    pub(crate) fn get<T>(&self, idx: usize) -> MetaResult<T>
    where
        T: for<'r> Decode<'r, Postgres>
            + Type<Postgres>
            + for<'r> Decode<'r, Sqlite>
            + Type<Sqlite>,
    {
        match self {
            Self::Postgres(row) => row.try_get(idx),
            Self::Sqlite(row) => row.try_get(idx),
        }
        .meta_err("decode column")
    }

    pub(crate) fn uuid(&self, idx: usize) -> MetaResult<Uuid> {
        match self {
            Self::Postgres(row) => row.try_get::<Uuid, _>(idx),
            Self::Sqlite(row) => row.try_get::<Hyphenated, _>(idx).map(Into::into),
        }
        .meta_err("decode uuid column")
    }

    pub(crate) fn opt_uuid(&self, idx: usize) -> MetaResult<Option<Uuid>> {
        match self {
            Self::Postgres(row) => row.try_get::<Option<Uuid>, _>(idx),
            Self::Sqlite(row) => row
                .try_get::<Option<Hyphenated>, _>(idx)
                .map(|v| v.map(Into::into)),
        }
        .meta_err("decode optional uuid column")
    }

    pub(crate) fn snapshot_id(&self, idx: usize) -> MetaResult<SnapshotId> {
        self.uuid(idx).map(SnapshotId::from_uuid)
    }
}

pub(crate) trait MetaSqlExt<T> {
    fn meta_err(self, context: &str) -> MetaResult<T>;
}

impl<T> MetaSqlExt<T> for Result<T, sqlx::Error> {
    fn meta_err(self, context: &str) -> MetaResult<T> {
        self.map_err(|error| MetaError::backend(context, error))
    }
}

pub(crate) fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

pub(crate) fn is_foreign_key_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
}

fn bind_pg<'q>(sql: &'q str, params: &'q [DbVal]) -> sqlx::query::Query<'q, Postgres, PgArguments> {
    params
        .iter()
        .fold(sqlx::query(AssertSqlSafe(sql)), |q, p| match p {
            DbVal::Text(v) => q.bind(v.as_str()),
            DbVal::OptText(v) => q.bind(v.as_deref()),
            DbVal::Uuid(v) => q.bind(*v),
            DbVal::OptUuid(v) => q.bind(*v),
            DbVal::I64(v) => q.bind(*v),
        })
}

fn bind_sqlite<'q>(
    sql: &'q str,
    params: &'q [DbVal],
) -> sqlx::query::Query<'q, Sqlite, SqliteArguments> {
    params
        .iter()
        .fold(sqlx::query(AssertSqlSafe(sql)), |q, p| match p {
            DbVal::Text(v) => q.bind(v.as_str()),
            DbVal::OptText(v) => q.bind(v.as_deref()),
            DbVal::Uuid(v) => q.bind(v.hyphenated()),
            DbVal::OptUuid(v) => q.bind(v.map(Uuid::hyphenated)),
            DbVal::I64(v) => q.bind(*v),
        })
}

pub(crate) enum DbConn {
    Postgres(PoolConnection<Postgres>),
    Sqlite(PoolConnection<Sqlite>),
}

pub(crate) enum DbTx<'a> {
    Postgres(Transaction<'a, Postgres>),
    Sqlite(Transaction<'a, Sqlite>),
}

macro_rules! dispatch_sql {
    ($self:expr, $sql:expr, |$e:ident| $exec:expr, $op:ident, $map_pg:expr, $map_sq:expr) => {
        match $self {
            Self::Postgres($e) => {
                let text = $sql.render(true);
                Ok(($map_pg)(bind_pg(&text, &$sql.params).$op($exec).await?))
            }
            Self::Sqlite($e) => {
                let text = $sql.render(false);
                Ok(($map_sq)(
                    bind_sqlite(&text, &$sql.params).$op($exec).await?,
                ))
            }
        }
    };
}

macro_rules! impl_exec {
    ($ty:ty, $self_ref:ty, |$e:ident| $exec:expr) => {
        impl $ty {
            pub(crate) async fn execute(self: $self_ref, sql: Sql) -> Result<u64, sqlx::Error> {
                dispatch_sql!(
                    self,
                    sql,
                    |$e| $exec,
                    execute,
                    |r: PgQueryResult| r.rows_affected(),
                    |r: SqliteQueryResult| r.rows_affected()
                )
            }
            pub(crate) async fn fetch_optional(
                self: $self_ref,
                sql: Sql,
            ) -> Result<Option<DbRow>, sqlx::Error> {
                dispatch_sql!(
                    self,
                    sql,
                    |$e| $exec,
                    fetch_optional,
                    |r: Option<_>| r.map(DbRow::Postgres),
                    |r: Option<_>| r.map(DbRow::Sqlite)
                )
            }
            pub(crate) async fn fetch_one(self: $self_ref, sql: Sql) -> Result<DbRow, sqlx::Error> {
                dispatch_sql!(
                    self,
                    sql,
                    |$e| $exec,
                    fetch_one,
                    DbRow::Postgres,
                    DbRow::Sqlite
                )
            }
            pub(crate) async fn fetch_all(
                self: $self_ref,
                sql: Sql,
            ) -> Result<Vec<DbRow>, sqlx::Error> {
                dispatch_sql!(
                    self,
                    sql,
                    |$e| $exec,
                    fetch_all,
                    |rs: Vec<_>| rs.into_iter().map(DbRow::Postgres).collect(),
                    |rs: Vec<_>| rs.into_iter().map(DbRow::Sqlite).collect()
                )
            }
        }
    };
}

impl_exec!(DatabasePool, &Self, |pool| pool);
impl_exec!(DbTx<'_>, &mut Self, |tx| &mut **tx);

impl DatabasePool {
    pub(crate) async fn begin(&self, context: &str) -> MetaResult<DbTx<'_>> {
        match self {
            Self::Postgres(p) => p.begin_with(PG_BEGIN).await.map(DbTx::Postgres),
            Self::Sqlite(p) => p.begin_with(SQLITE_BEGIN).await.map(DbTx::Sqlite),
        }
        .meta_err(context)
    }

    pub(crate) async fn acquire_for_gc(&self) -> MetaResult<DbConn> {
        match self {
            Self::Postgres(pool) => {
                let mut conn = pool
                    .acquire()
                    .await
                    .meta_err("acquire GC database connection")?;
                // Never return a connection that might still hold a session advisory lock.
                conn.close_on_drop();
                Ok(DbConn::Postgres(conn))
            }
            Self::Sqlite(pool) => pool
                .acquire()
                .await
                .map(DbConn::Sqlite)
                .meta_err("acquire GC database connection"),
        }
    }
}

impl DbConn {
    pub(crate) async fn fetch_all(&mut self, sql: Sql) -> Result<Vec<DbRow>, sqlx::Error> {
        dispatch_sql!(
            self,
            sql,
            |c| &mut **c,
            fetch_all,
            |rs: Vec<_>| rs.into_iter().map(DbRow::Postgres).collect(),
            |rs: Vec<_>| rs.into_iter().map(DbRow::Sqlite).collect()
        )
    }

    pub(crate) async fn begin(&mut self) -> Result<DbTx<'_>, sqlx::Error> {
        match self {
            Self::Postgres(c) => c.begin_with(PG_BEGIN).await.map(DbTx::Postgres),
            Self::Sqlite(c) => c.begin_with(SQLITE_BEGIN).await.map(DbTx::Sqlite),
        }
    }

    pub(crate) async fn try_advisory_lock(&mut self, key: i64) -> MetaResult<bool> {
        match self {
            Self::Postgres(conn) => sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(key)
                .fetch_one(&mut **conn)
                .await
                .meta_err("acquire GC advisory lock"),
            Self::Sqlite(_) => Ok(true),
        }
    }

    pub(crate) async fn unlock_and_close(self, key: i64) {
        if let Self::Postgres(mut conn) = self {
            let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(key)
                .execute(&mut *conn)
                .await;
            let _ = conn.close().await;
        }
    }
}

impl DbTx<'_> {
    pub(crate) fn is_postgres(&self) -> bool {
        matches!(self, Self::Postgres(_))
    }

    pub(crate) async fn commit(self, context: &str) -> MetaResult<()> {
        match self {
            Self::Postgres(tx) => tx.commit().await,
            Self::Sqlite(tx) => tx.commit().await,
        }
        .meta_err(context)
    }

    pub(crate) async fn rollback(self, context: &str) -> MetaResult<()> {
        match self {
            Self::Postgres(tx) => tx.rollback().await,
            Self::Sqlite(tx) => tx.rollback().await,
        }
        .meta_err(context)
    }
}
