#[allow(clippy::all, clippy::pedantic)]
#[allow(unused_variables)]
#[allow(unused_imports)]
#[allow(dead_code)]
pub mod types {}
#[allow(clippy::all, clippy::pedantic)]
#[allow(unused_variables)]
#[allow(unused_imports)]
#[allow(dead_code)]
pub mod queries {
    pub mod items {
        #[derive(Debug)]
        pub struct InsertItemParams<T1: cornucopia_async::StringSql> {
            pub value: i64,
            pub label: T1,
        }
        #[derive(Debug, Clone, PartialEq)]
        pub struct Item {
            pub value: i64,
            pub label: String,
        }
        impl Item {
            pub fn from_sqlite<'a>(r: &cornucopia_async::rusqlite::Row<'a>) -> Self {
                Self {
                    value: r.get_unwrap(0),
                    label: r.get_unwrap(1),
                }
            }
        }
        pub struct ItemBorrowed<'a> {
            pub value: i64,
            pub label: &'a str,
        }
        impl<'a> From<ItemBorrowed<'a>> for Item {
            fn from(ItemBorrowed { value, label }: ItemBorrowed<'a>) -> Self {
                Self {
                    value,
                    label: label.into(),
                }
            }
        }
        use cornucopia_async::GenericClient;
        use futures;
        use futures::{StreamExt, TryStreamExt};
        pub struct ItemQuery<'a, C: GenericClient, T, const N: usize> {
            client: &'a C,
            params: [&'a (dyn postgres_types::ToSql + Sync); N],
            stmt: &'a mut cornucopia_async::private::Stmt,
            extractor: fn(&tokio_postgres::Row) -> ItemBorrowed,
            mapper: fn(ItemBorrowed) -> T,
        }
        impl<'a, C, T: 'a, const N: usize> ItemQuery<'a, C, T, N>
        where
            C: GenericClient,
        {
            pub fn map<R>(self, mapper: fn(ItemBorrowed) -> R) -> ItemQuery<'a, C, R, N> {
                ItemQuery {
                    client: self.client,
                    params: self.params,
                    stmt: self.stmt,
                    extractor: self.extractor,
                    mapper,
                }
            }
            pub async fn one(self) -> Result<T, tokio_postgres::Error> {
                let stmt = self.stmt.prepare(self.client).await?;
                let row = self.client.query_one(stmt, &self.params).await?;
                Ok((self.mapper)((self.extractor)(&row)))
            }
            pub async fn all(self) -> Result<Vec<T>, tokio_postgres::Error> {
                self.iter().await?.try_collect().await
            }
            pub async fn opt(self) -> Result<Option<T>, tokio_postgres::Error> {
                let stmt = self.stmt.prepare(self.client).await?;
                Ok(self
                    .client
                    .query_opt(stmt, &self.params)
                    .await?
                    .map(|row| (self.mapper)((self.extractor)(&row))))
            }
            pub async fn iter(
                self,
            ) -> Result<
                impl futures::Stream<Item = Result<T, tokio_postgres::Error>> + 'a,
                tokio_postgres::Error,
            > {
                let stmt = self.stmt.prepare(self.client).await?;
                let it = self
                    .client
                    .query_raw(stmt, cornucopia_async::private::slice_iter(&self.params))
                    .await?
                    .map(move |res| res.map(|row| (self.mapper)((self.extractor)(&row))))
                    .into_stream();
                Ok(it)
            }
        }
        pub fn insert_item() -> InsertItemStmt {
            InsertItemStmt(cornucopia_async::private::Stmt::new(
                "INSERT INTO items (value, label) VALUES ($1, $2)",
            ))
        }
        pub struct InsertItemStmt(cornucopia_async::private::Stmt);
        impl InsertItemStmt {
            pub async fn bind<'a, C: GenericClient, T1: cornucopia_async::StringSql>(
                &'a mut self,
                client: &'a C,
                value: &'a i64,
                label: &'a T1,
            ) -> Result<u64, tokio_postgres::Error> {
                let stmt = self.0.prepare(client).await?;
                client.execute(stmt, &[value, label]).await
            }
        }
        impl<'a, C: GenericClient + Send + Sync, T1: cornucopia_async::StringSql>
            cornucopia_async::Params<
                'a,
                InsertItemParams<T1>,
                std::pin::Pin<
                    Box<
                        dyn futures::Future<Output = Result<u64, tokio_postgres::Error>>
                            + Send
                            + 'a,
                    >,
                >,
                C,
            > for InsertItemStmt
        {
            fn params(
                &'a mut self,
                client: &'a C,
                params: &'a InsertItemParams<T1>,
            ) -> std::pin::Pin<
                Box<dyn futures::Future<Output = Result<u64, tokio_postgres::Error>> + Send + 'a>,
            > {
                Box::pin(self.bind(client, &params.value, &params.label))
            }
        }
        pub fn get_items() -> GetItemsStmt {
            GetItemsStmt(cornucopia_async::private::Stmt::new(
                "SELECT value, label FROM items ORDER BY value",
            ))
        }
        pub struct GetItemsStmt(cornucopia_async::private::Stmt);
        impl GetItemsStmt {
            pub fn bind<'a, C: GenericClient>(
                &'a mut self,
                client: &'a C,
            ) -> ItemQuery<'a, C, Item, 0> {
                ItemQuery {
                    client,
                    params: [],
                    stmt: &mut self.0,
                    extractor: |row| ItemBorrowed {
                        value: row.get(0),
                        label: row.get(1),
                    },
                    mapper: |it| <Item>::from(it),
                }
            }
        }
        pub async fn execute_insert_item<
            'a,
            T1: cornucopia_async::StringSql + cornucopia_async::rusqlite::types::ToSql,
        >(
            db: &cornucopia_async::Database<'a>,
            value: &'a i64,
            label: &'a T1,
        ) -> Result<u64, cornucopia_async::DbError> {
            Ok(match db {
                cornucopia_async::Database::Postgres(p) => {
                    insert_item().bind(p, value, label).await? as u64
                }
                cornucopia_async::Database::PostgresTx(p) => {
                    insert_item().bind(p, value, label).await? as u64
                }
                cornucopia_async::Database::Sqlite(c) => {
                    let c = c.connection().await?;
                    c.execute(
                        "INSERT INTO items (value, label) VALUES (?1, ?2)",
                        cornucopia_async::rusqlite::params![value, label,],
                    )? as u64
                }
            })
        }
        pub async fn fetch_get_items<'a>(
            db: &cornucopia_async::Database<'a>,
        ) -> Result<Vec<Item>, cornucopia_async::DbError> {
            Ok(match db {
                cornucopia_async::Database::Postgres(p) => {
                    get_items().bind(p).all().await?.into_iter().collect()
                }
                cornucopia_async::Database::PostgresTx(p) => {
                    get_items().bind(p).all().await?.into_iter().collect()
                }
                cornucopia_async::Database::Sqlite(c) => {
                    let c = c.connection().await?;
                    c.query_rows(
                        "SELECT value, label FROM items ORDER BY value",
                        cornucopia_async::rusqlite::params![],
                        |r| Item::from_sqlite(r),
                    )?
                }
            })
        }
    }
}
