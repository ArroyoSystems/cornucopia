#[path = "fixtures/queries.rs"]
mod generated;

use cornucopia_async::{Database, DatabaseSource, DbError, SqliteSource, Transaction};
use generated::queries::items::{execute_insert_item, fetch_get_items};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

fn sqlite_source() -> DatabaseSource {
    sqlite_with_schema("CREATE TABLE items (value BIGINT NOT NULL UNIQUE, label TEXT NOT NULL)")
}

fn sqlite_with_schema(schema: &str) -> DatabaseSource {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch(schema).unwrap();
    DatabaseSource::Sqlite(Arc::new(SqliteSource::new(connection)))
}

async fn exercise_transactions(source: DatabaseSource) {
    let mut client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    {
        let label = String::from("committed");
        execute_insert_item(&transaction, &1, &label).await.unwrap();
    }
    tokio::task::yield_now().await;
    let items = fetch_get_items(&transaction).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "committed");
    transaction.commit().await.unwrap();
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);

    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &2, &"explicit rollback")
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);

    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &3, &"implicit rollback")
        .await
        .unwrap();
    drop(transaction);
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);

    async fn failing_operation(client: &mut Database<'_>) -> Result<(), DbError> {
        let transaction = client.transaction().await?;
        execute_insert_item(&transaction, &4, &"rolled back on error").await?;
        execute_insert_item(&transaction, &1, &"duplicate").await?;
        transaction.commit().await
    }
    assert_eq!(
        failing_operation(&mut client).await.unwrap_err(),
        DbError::DuplicateViolation
    );
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);

    let aborted = DbError::Other("Transaction aborted by an earlier error".into());
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &7, &"aborted")
        .await
        .unwrap();
    assert_eq!(
        execute_insert_item(&transaction, &1, &"ignored duplicate")
            .await
            .unwrap_err(),
        DbError::DuplicateViolation
    );
    assert_eq!(
        execute_insert_item(&transaction, &8, &"after error")
            .await
            .unwrap_err(),
        aborted
    );
    assert_eq!(fetch_get_items(&transaction).await.unwrap_err(), aborted);
    assert_eq!(transaction.commit().await.unwrap_err(), aborted);
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);

    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &7, &"aborted")
        .await
        .unwrap();
    assert!(execute_insert_item(&transaction, &1, &"ignored duplicate")
        .await
        .is_err());
    transaction.rollback().await.unwrap();
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);
    drop(client);

    let worker_source = source.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let worker = tokio::spawn(async move {
        let mut client = worker_source.client().await.unwrap();
        let transaction = client.transaction().await.unwrap();
        execute_insert_item(&transaction, &5, &"cancelled")
            .await
            .unwrap();
        started.send(()).unwrap();
        std::future::pending::<()>().await;
        transaction.commit().await.unwrap();
    });
    ready.await.unwrap();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    let client = source.client().await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), fetch_get_items(&client))
            .await
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    drop(client);

    let worker_source = source.clone();
    let worker = tokio::spawn(async move {
        let mut client = worker_source.client().await.unwrap();
        let transaction = client.transaction().await.unwrap();
        execute_insert_item(&transaction, &6, &"panic")
            .await
            .unwrap();
        panic!("abort transaction");
    });
    assert!(worker.await.unwrap_err().is_panic());
    let client = source.client().await.unwrap();
    assert_eq!(fetch_get_items(&client).await.unwrap().len(), 1);
}

#[test]
fn database_and_transaction_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DatabaseSource>();
    assert_send_sync::<Database<'static>>();
    assert_send_sync::<Transaction<'static>>();
}

#[tokio::test]
async fn sqlite_transactions_with_generated_queries() {
    tokio::spawn(exercise_transactions(sqlite_source()))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires DATABASE_URL pointing to a PostgreSQL test database"]
async fn postgres_transactions_with_generated_queries() {
    let config = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set")
        .parse::<tokio_postgres::Config>()
        .unwrap();
    let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    let pool = deadpool_postgres::Pool::builder(manager)
        .max_size(1)
        .build()
        .unwrap();
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE TEMP TABLE items (value BIGINT NOT NULL UNIQUE, label TEXT NOT NULL)",
        )
        .await
        .unwrap();
    tokio::spawn(exercise_transactions(DatabaseSource::Postgres(pool)))
        .await
        .unwrap();
}

#[tokio::test]
async fn competing_sqlite_writer_waits_without_joining_transaction() {
    let source = sqlite_source();
    let mut client = source.client().await.unwrap();
    let competing_client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"rolled back")
        .await
        .unwrap();
    let competing_write = execute_insert_item(&competing_client, &2, &"committed");
    tokio::pin!(competing_write);
    assert!(timeout(Duration::from_millis(25), &mut competing_write)
        .await
        .is_err());
    transaction.rollback().await.unwrap();
    timeout(Duration::from_secs(1), &mut competing_write)
        .await
        .unwrap()
        .unwrap();
    let items = fetch_get_items(&client).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value, 2);
}

#[tokio::test]
async fn sqlite_reader_waits_for_commit() {
    let source = sqlite_source();
    let mut client = source.client().await.unwrap();
    let reader = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"committed")
        .await
        .unwrap();
    let read = fetch_get_items(&reader);
    tokio::pin!(read);
    assert!(timeout(Duration::from_millis(25), &mut read).await.is_err());
    transaction.commit().await.unwrap();
    let items = timeout(Duration::from_secs(1), &mut read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value, 1);
}

#[tokio::test]
async fn cancelling_transaction_wait_does_not_affect_active_transaction() {
    let source = sqlite_source();
    let mut client = source.client().await.unwrap();
    let mut next_client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"committed")
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(25), next_client.transaction())
            .await
            .is_err()
    );
    transaction.commit().await.unwrap();
    let next_transaction = timeout(Duration::from_secs(1), next_client.transaction())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetch_get_items(&next_transaction).await.unwrap().len(), 1);
    next_transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn failed_sqlite_commit_rolls_back_before_reuse() {
    let source = sqlite_with_schema(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE parents (value INTEGER PRIMARY KEY);
         CREATE TABLE items (value INTEGER REFERENCES parents(value) DEFERRABLE INITIALLY DEFERRED, label TEXT NOT NULL);"
    );
    let mut client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"invalid foreign key")
        .await
        .unwrap();
    assert_eq!(
        transaction.commit().await.unwrap_err(),
        DbError::ForeignKeyViolation
    );
    assert!(fetch_get_items(&client).await.unwrap().is_empty());
    client
        .transaction()
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_automatic_rollback_prevents_autocommit_through_transaction() {
    let source = sqlite_with_schema(
        "CREATE TABLE items (value INTEGER UNIQUE ON CONFLICT ROLLBACK, label TEXT NOT NULL);",
    );
    let mut client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"rolled back")
        .await
        .unwrap();
    assert_eq!(
        execute_insert_item(&transaction, &1, &"duplicate")
            .await
            .unwrap_err(),
        DbError::DuplicateViolation
    );
    let aborted = DbError::Other("Transaction aborted by an earlier error".into());
    assert_eq!(
        execute_insert_item(&transaction, &2, &"must not commit")
            .await
            .unwrap_err(),
        aborted
    );
    assert_eq!(fetch_get_items(&transaction).await.unwrap_err(), aborted);
    assert_eq!(transaction.commit().await.unwrap_err(), aborted);
    assert!(fetch_get_items(&client).await.unwrap().is_empty());

    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"rolled back")
        .await
        .unwrap();
    assert!(execute_insert_item(&transaction, &1, &"duplicate")
        .await
        .is_err());
    transaction.rollback().await.unwrap();
    assert!(fetch_get_items(&client).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_sqlite_rollback_discards_connection() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

    let connection = rusqlite::Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE items (value BIGINT NOT NULL UNIQUE, label TEXT NOT NULL)")
        .unwrap();
    connection.authorizer(Some(|context: AuthContext<'_>| {
        if matches!(
            context.action,
            AuthAction::Transaction {
                operation: TransactionOperation::Rollback
            }
        ) {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }));
    let source = DatabaseSource::Sqlite(Arc::new(SqliteSource::new(connection)));
    let mut client = source.client().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    execute_insert_item(&transaction, &1, &"discarded")
        .await
        .unwrap();
    assert!(transaction.rollback().await.is_err());
    let error = timeout(Duration::from_secs(1), fetch_get_items(&client))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error,
        DbError::Other("SQLite connection discarded after failed rollback".into())
    );
}
