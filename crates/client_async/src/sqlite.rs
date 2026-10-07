use deadpool_postgres::Object;
use deadpool_postgres::Transaction as PGTransaction;
use rusqlite::Params;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_postgres::error::SqlState;

const TRANSACTION_ABORTED: &str = "Transaction aborted by an earlier error";

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum DbError {
    DuplicateViolation,
    ForeignKeyViolation,
    Other(String),
}

impl From<rusqlite::Error> for DbError {
    fn from(err: rusqlite::Error) -> Self {
        if let Some(sqlite) = err.sqlite_error() {
            match (sqlite.code, sqlite.extended_code) {
                (rusqlite::ErrorCode::ConstraintViolation, 2067 /* UNIQUE */) => {
                    return DbError::DuplicateViolation;
                }
                (rusqlite::ErrorCode::ConstraintViolation, 787 /* FOREIGN KEY */) => {
                    return DbError::ForeignKeyViolation;
                }
                _ => {}
            }
        }

        DbError::Other(err.to_string())
    }
}

impl From<tokio_postgres::Error> for DbError {
    fn from(err: tokio_postgres::Error) -> Self {
        if let Some(db) = &err.as_db_error() {
            if *db.code() == SqlState::UNIQUE_VIOLATION {
                return DbError::DuplicateViolation;
            } else if *db.code() == SqlState::FOREIGN_KEY_VIOLATION {
                return DbError::ForeignKeyViolation;
            } else if *db.code() == SqlState::IN_FAILED_SQL_TRANSACTION {
                return DbError::Other(TRANSACTION_ABORTED.into());
            }
        }

        DbError::Other(err.to_string())
    }
}

impl Display for DbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for DbError {}

#[derive(Debug, Clone)]
pub enum DatabaseSource {
    Postgres(deadpool_postgres::Pool),
    Sqlite(Arc<SqliteSource>),
}

impl DatabaseSource {
    pub async fn client(&self) -> Result<Database<'_>, DbError> {
        Ok(match self {
            DatabaseSource::Postgres(p) => {
                Database::Postgres(p.get().await.map_err(|e| DbError::Other(e.to_string()))?)
            }
            DatabaseSource::Sqlite(p) => Database::Sqlite(SqliteWrapper::Connection(p.clone())),
        })
    }
}

#[derive(Debug)]
pub struct SqliteSource {
    connection: Mutex<Option<rusqlite::Connection>>,
    gate: Arc<AsyncMutex<()>>,
}

impl SqliteSource {
    pub fn new(connection: rusqlite::Connection) -> Self {
        Self {
            connection: Mutex::new(Some(connection)),
            gate: Arc::new(AsyncMutex::new(())),
        }
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Connection) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let connection = self
            .connection
            .lock()
            .map_err(|error| DbError::Other(error.to_string()))?;
        let connection = connection.as_ref().ok_or_else(|| {
            DbError::Other("SQLite connection discarded after failed rollback".into())
        })?;
        operation(connection)
    }
}

pub enum SqliteWrapper {
    Connection(Arc<SqliteSource>),
    Transaction(SqliteTransaction),
}

pub struct SqliteAccess<'connection> {
    source: &'connection SqliteSource,
    _permit: Option<OwnedMutexGuard<()>>,
    transaction_aborted: Option<&'connection AtomicBool>,
}

impl SqliteWrapper {
    pub async fn connection(&self) -> Result<SqliteAccess<'_>, DbError> {
        match self {
            Self::Connection(source) => Ok(SqliteAccess {
                _permit: Some(source.gate.clone().lock_owned().await),
                source,
                transaction_aborted: None,
            }),
            Self::Transaction(transaction) => Ok(SqliteAccess {
                source: &transaction.source,
                _permit: None,
                transaction_aborted: Some(&transaction.aborted),
            }),
        }
    }
}

impl SqliteAccess<'_> {
    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>,
    ) -> Result<T, DbError> {
        self.source.with_connection(|connection| {
            let Some(aborted) = self.transaction_aborted else {
                return Ok(operation(connection)?);
            };
            // Like PostgreSQL, a failed statement aborts the whole transaction, even though
            // SQLite itself would only undo that statement
            if aborted.load(Ordering::Relaxed) || connection.is_autocommit() {
                return Err(DbError::Other(TRANSACTION_ABORTED.into()));
            }
            let result = operation(connection);
            if result.is_err() {
                aborted.store(true, Ordering::Relaxed);
            }
            Ok(result?)
        })
    }

    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize, DbError> {
        self.with_connection(|connection| connection.execute(sql, params))
    }

    pub fn query_rows<P: Params, T, F: Fn(&rusqlite::Row) -> T>(
        &self,
        sql: &str,
        params: P,
        map: F,
    ) -> Result<Vec<T>, DbError> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(sql)?;
            let results = statement.query(params)?;
            results.mapped(|row| Ok(map(row))).collect()
        })
    }
}

pub struct SqliteTransaction {
    source: Arc<SqliteSource>,
    _permit: OwnedMutexGuard<()>,
    aborted: AtomicBool,
    finished: bool,
}

impl SqliteTransaction {
    fn commit(self) -> Result<(), DbError> {
        let aborted = self.aborted.load(Ordering::Relaxed);
        self.finish(|connection| {
            if aborted || connection.is_autocommit() {
                return Err(DbError::Other(TRANSACTION_ABORTED.into()));
            }
            Ok(connection.execute_batch("COMMIT")?)
        })
    }

    fn rollback(self) -> Result<(), DbError> {
        self.finish(|connection| {
            // SQLite may already have rolled back automatically after a failed statement
            if !connection.is_autocommit() {
                connection.execute_batch("ROLLBACK")?;
            }
            Ok(())
        })
    }

    fn finish(
        mut self,
        operation: impl FnOnce(&rusqlite::Connection) -> Result<(), DbError>,
    ) -> Result<(), DbError> {
        self.source.with_connection(operation)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for SqliteTransaction {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut connection = self
            .source
            .connection
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(database) = connection.as_ref() {
            if !database.is_autocommit() && database.execute_batch("ROLLBACK").is_err() {
                connection.take();
            }
        }
    }
}

pub enum Database<'a> {
    Postgres(Object),
    PostgresTx(PGTransaction<'a>),
    Sqlite(SqliteWrapper),
}

pub struct Transaction<'connection> {
    database: Database<'connection>,
}

impl<'connection> Deref for Transaction<'connection> {
    type Target = Database<'connection>;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

impl Database<'_> {
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, DbError> {
        let database = match self {
            Database::Postgres(client) => Database::PostgresTx(client.transaction().await?),
            Database::Sqlite(SqliteWrapper::Connection(source)) => {
                let permit = source.gate.clone().lock_owned().await;
                source.with_connection(|connection| {
                    Ok(connection.execute_batch("BEGIN IMMEDIATE")?)
                })?;
                Database::Sqlite(SqliteWrapper::Transaction(SqliteTransaction {
                    source: source.clone(),
                    _permit: permit,
                    aborted: AtomicBool::new(false),
                    finished: false,
                }))
            }
            _ => {
                return Err(DbError::Other(
                    "Nested transactions require savepoint support".into(),
                ))
            }
        };
        Ok(Transaction { database })
    }
}

impl Transaction<'_> {
    pub async fn commit(self) -> Result<(), DbError> {
        match self.database {
            Database::PostgresTx(transaction) => {
                // COMMIT in an aborted transaction silently rolls back and reports success,
                // so first check that the transaction can still run statements
                transaction.batch_execute("SELECT 1").await?;
                transaction.commit().await.map_err(DbError::from)
            }
            Database::Sqlite(SqliteWrapper::Transaction(transaction)) => transaction.commit(),
            _ => unreachable!(),
        }
    }

    pub async fn rollback(self) -> Result<(), DbError> {
        match self.database {
            Database::PostgresTx(transaction) => {
                transaction.rollback().await.map_err(DbError::from)
            }
            Database::Sqlite(SqliteWrapper::Transaction(transaction)) => transaction.rollback(),
            _ => unreachable!(),
        }
    }
}
